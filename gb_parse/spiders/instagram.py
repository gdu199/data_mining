import datetime as dt
import json
import scrapy
from ..items import InstaUser, InstaFollow, InstaSubscription, InstaHandShake

from pymongo import MongoClient

#from selenium import webdriver # модуль для обращения к классу браузера
#from selenium.webdriver.common.keys import Keys # для имитации нажатия клавиатуры


class InstagramSpider(scrapy.Spider):
    name = 'instagram'
    db_type = 'MONGO'
    allowed_domains = ['www.instagram.com']
    start_urls = ['https://www.instagram.com/']
    login_url = 'https://www.instagram.com/accounts/login/ajax/'
    api_url = '/graphql/query/'
    # параметр query_hash определяет какой тип содержимого будет запрошен
    # ищем в заголовках запросов при переходе на страницу. Видимо содержится в скрипте при загрузке начальной страницы
    query_hash = {
        'tag_posts': "9b498c08113f1e09617a1703c22b2f32",  # получить посты по тегу
        'follow': "c76146de99bb02f6415203be841dd25a", # получить подписчиков пользователя
        'subscribe': "d04b0a864b4b54837c0d870b0e77e076" # получить подписки пользователя
    }
    
    def __init__(self, login, enc_password, user1, user2, *args, **kwargs):
        # self.browser = webdriver.Chrome()
        self.user1 = user1
        self.user2 = user2
        self.id_1 = 0
        self.id_2 = 0
        # self.max_chain_length = 2 # длина цепочки с каждой стороны, то есть *2
        self.current_chain_length = 2   # ограничение
        self.chain_founded = False

        self.login = login
        self.enc_passwd = enc_password  #пароль передается в зашифрованном виде. Его нужно взять из запроса при авторизации. Начинается с #pwd
        self.db = MongoClient('mongodb://localhost:27017')['Instagram']

        super().__init__(*args, **kwargs)
    
    def parse(self, response, **kwargs):
        try:
            # первый раз сайт выдаст форму аутентификации. На ней надо найти CSRFToken (для каждой страницы уникальный)
            # и использовать его в заголовке при отправке формы с логином и паролем
            js_data = self.js_data_extract(response)
            yield scrapy.FormRequest(
                self.login_url,
                method='POST',
                callback=self.parse,
                formdata={
                    'username': self.login,
                    'enc_password': self.enc_passwd,
                },
                headers={'X-CSRFToken': js_data['config']['csrf_token']}
            )
        except AttributeError as e:
            # Если нет такого скрипта xpath даст ошибку, то скорее всего мы уже авторизованы.
            # Проверим параметр authenticated в ответе. Если = True, то начинаем парсинг для каждого пользователя
            if response.json().get('authenticated'):
                # Запускаем парсинг каждого юзера отдельно
                yield response.follow(f'/{self.user1}/', callback=self.user_parse) #cb_kwargs=dict(chain=[])
                yield response.follow(f'/{self.user2}/', callback=self.user_parse) #cb_kwargs=dict(chain=[])

    def user_parse(self, response, start_user_id=None, chain=None):

        # Этот метод вызывается рекурсивно.

        # если цепочка найдена, прекращаем поиск
        if self.chain_founded:
            return None

        # получили страницу пользователя. Разбираем данные из скрипта
        user_data = self.js_data_extract(response)['entry_data']['ProfilePage'][0]['graphql']['user']
        # yield InstaUser(
        #     date_parse=dt.datetime.utcnow(),
        #     data=user_data
        # )

        # При первых вызовах запоминаем id стартовых пользователей
        if self.user1 == user_data['username'] and self.id_1 == 0:
            self.id_1 = user_data['id']
        if self.user2 == user_data['username'] and self.id_2 == 0:
            self.id_2 = user_data['id']

        # проверяем есть ли цепочка, если есть, то отправляем ее в базу
        yield from self.is_chain_founded()

        # еще раз проверим признак
        if self.chain_founded:
            return None

        # ограничимся только пользователями для которых есть relation (двухсторонняя связь)
        if self.id_1 != user_data['id'] and self.id_2 != user_data['id']:
            cursor_user = self.db['relations'].find({'user_id': user_data['id']})
            if not cursor_user.count():
                return None

        # идем с двух сторон. Запоминаем корневой узел, чтобы понимать откуда появилась связь
        if not start_user_id:
            start_user_id = user_data['id']

        # формируем цепочку связей. Начинаем от корня
        if not chain:
            chain = [user_data['username']]
        else:
            if len(chain) >= self.current_chain_length:  #
                return None
            else:
                chain.append(user_data['username'])


        # декомпозиция для подписчиков, чтобы не загромождать код в одной процедуре
        yield from self.get_api_follow_request(response, user_data, start_user_id, chain)
        # декомпозиция для подписок, чтобы не загромождать код в одной процедуре
        yield from self.get_api_subscribe_request(response, user_data, start_user_id, chain)

    def get_api_follow_request(self, response, user_data, start_user_id, chain, variables=None):

        if self.chain_founded:
            return None

        # создаем задачу зайти на страницу подписчиков
        if not variables:
            variables = {
                'id': user_data['id'],
                'first': 100,
            }
        url = f'{self.api_url}?query_hash={self.query_hash["follow"]}&variables={json.dumps(variables)}'
        yield response.follow(url, callback=self.get_api_follow, cb_kwargs={'user_data': user_data, 'start_user_id': start_user_id, 'chain': chain})

    def get_api_subscribe_request(self, response, user_data, start_user_id, chain, variables=None):

        if self.chain_founded:
            return None

        # создаем задачу зайти на страницу c подписками
        if not variables:
            variables = {
                'id': user_data['id'],
                'first': 100,
            }
        url = f'{self.api_url}?query_hash={self.query_hash["subscribe"]}&variables={json.dumps(variables)}'
        yield response.follow(url, callback=self.get_api_subscribe, cb_kwargs={'user_data': user_data, 'start_user_id': start_user_id, 'chain': chain})

    def get_api_follow(self, response, user_data, start_user_id, chain):
        # получаем подписчиков
        if self.chain_founded:
            return None

        if b'application/json' in response.headers['Content-Type']:
            data = response.json()
            # получаем карточки подписчиков
            yield from self.get_follow_item(response, user_data, data['data']['user']['edge_followed_by']['edges'], start_user_id, chain)
            #Если на странице есть ссылка на следующую страницу пагинации, то просим сделать запрос на эту страницу
            if data ['data']['user']['edge_follow']['page_info']['has_next_page']:
                variables = {
                    'id': user_data['id'],
                    'first': 100,
                    'after': data['data']['user']['edge_follow']['page_info']['end_cursor'],
                }
                yield from self.get_api_follow_request(response, user_data, start_user_id, chain, variables)

    def get_api_subscribe(self, response, user_data, start_user_id, chain):
        # получаем подписки
        if self.chain_founded:
            return None

        if b'application/json' in response.headers['Content-Type']:
            data = response.json()
            # получаем подписки
            yield from self.get_subscribe_item(response, user_data, data['data']['user']['edge_follow']['edges'], start_user_id, chain)
            #Если на странице есть ссылка на следующую страницу пагинации, то просим сделать запрос на эту страницу
            if data ['data']['user']['edge_follow']['page_info']['has_next_page']:
                variables = {
                    'id': user_data['id'],
                    'first': 100,
                    'after': data['data']['user']['edge_follow']['page_info']['end_cursor'],
                }
                yield from self.get_api_subscribe_request(response, user_data, start_user_id, chain, variables)


    def get_follow_item(self, response, user_data, follow_users_data, start_user_id, chain):
        for user in follow_users_data:

            if self.chain_founded:
                return None

            follow_name = user['node']['username']

            yield InstaFollow(
                user_id=user_data['id'],
                user_name = user_data['username'],
                follow_id=user['node']['id'],
                follow_name=follow_name,
                start_user_id=start_user_id,
                chain=chain
            )

            # теперь InstaUser создается только в user_parse
            # yield InstaUser(
            #     date_parse=dt.datetime.utcnow(),
            #     data=user['node']
            # )
            # рекурсивно обрабатываем каждого пользователя

            yield response.follow(f'/{follow_name}/', callback=self.user_parse, cb_kwargs={'start_user_id': start_user_id, 'chain': chain})


    def get_subscribe_item(self, response, user_data, subscribe_users_data, start_user_id, chain):
        for user in subscribe_users_data:

            if self.chain_founded:
                return None

            subscribe_name = user['node']['username']

            yield InstaSubscription(
                user_id=user_data['id'],
                user_name = user_data['username'],
                subscribe_id=user['node']['id'],
                subscribe_name=user['node']['username'],
                start_user_id=start_user_id,
                chain=chain
            )

            # теперь InstaUser создается только в user_parse
            # yield InstaUser(
            #     date_parse=dt.datetime.utcnow(),
            #     data=user['node']
            # )
            # рекурсивно обрабатываем каждого пользователя

            yield response.follow(f'/{subscribe_name}/', callback=self.user_parse, cb_kwargs={'start_user_id': start_user_id, 'chain': chain})


    @staticmethod
    def js_data_extract(response):
        #загрузка json из скрипта
        script = response.xpath('//script[contains(text(), "window._sharedData =")]/text()').get()
        return json.loads(script.replace("window._sharedData =", '')[:-1])


    def is_chain_founded(self):

        cur_user1 = self.db['relations'].find({'start_user_id': self.id_1})
        cur_user2 = self.db['relations'].find({'start_user_id': self.id_2})

        # Сначала проверим может быть среди связей уже есть второй пользователь
        for doc in cur_user1:
            if doc['user_id'] == self.id_2:
                self.chain_founded = True
                doc['chain'].append(self.user2)
                yield InstaHandShake(chain = doc['chain'])
                return

        for doc in cur_user2:
            if doc['user_id'] == self.id_1:
                self.chain_founded = True
                doc['chain'].append(self.user1)
                yield InstaHandShake(chain = doc['chain'])
                return

        # Далее сравниваем списки найденных связей для обоих пользователей
        for doc1 in cur_user1:
            for doc2 in cur_user2:
                if doc1['user_id'] == doc2['user_id']:
                    self.chain_founded = True
                    for el in doc2['chain'][:-1].reverse():
                        doc1['chain'].append(el)
                    yield InstaHandShake(chain = doc1['chain'])


