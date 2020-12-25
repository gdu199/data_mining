import datetime as dt
import json
import scrapy
from ..items import InstaPost, InstaUser, InstaFollow, InstaSubscription
#from selenium import webdriver # модуль для обращения к классу браузера
#from selenium.webdriver.common.keys import Keys # для имитации нажатия клавиатуры


class InstagramSpider(scrapy.Spider):
    name = 'instagram'
    db_type = 'MONGO'
    allowed_domains = ['www.instagram.com']
    start_urls = ['https://www.instagram.com/']
    login_url = 'https://www.instagram.com/accounts/login/ajax/'
    api_url = '/graphql/query/'
    query_hash = {
        'tag_posts': "9b498c08113f1e09617a1703c22b2f32", # параметр query_hash определяет какой тип содержимого будет запрошен
        'follow': "c76146de99bb02f6415203be841dd25a", # ищем в заголовках запросов при переходе на страницу. Видимо содержится в скрипте при загрузке начальной страницы
        'subscribe': "d04b0a864b4b54837c0d870b0e77e076"
    }
    
    def __init__(self, login, enc_password, *args, **kwargs):
        # self.browser = webdriver.Chrome()
        self.tags = ['python']#, 'программирование', 'developers'
        self.users = ['geekbrains', ] #teslamotors'
        self.login = login
        self.enc_passwd = enc_password  #пароль передается в зашифрованном виде. Его нужно взять из запроса при авторизации. Начинается с #pwd
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
                for user in self.users:
                    yield response.follow(f'/{user}/', callback=self.user_parse)
    
    def user_parse(self, response):
        # получили страницу пользователя. Разбираем данные из скрипта
        user_data = self.js_data_extract(response)['entry_data']['ProfilePage'][0]['graphql']['user']
        yield InstaUser(
            date_parse=dt.datetime.utcnow(),
            data=user_data
        )
        # декомпозиция для подписчиков, чтобы не загромождать код в одной процедуре
        yield from self.get_api_follow_request(response, user_data)
        # декомпозиция для подписок, чтобы не загромождать код в одной процедуре
        yield from self.get_api_subscribe_request(response, user_data)

    def get_api_follow_request(self, response, user_data, variables=None):
        # создаем задачу зайти на страницу подписчиков
        if not variables:
            variables = {
                'id': user_data['id'],
                'first': 100,
            }
        url = f'{self.api_url}?query_hash={self.query_hash["follow"]}&variables={json.dumps(variables)}'
        yield response.follow(url, callback=self.get_api_follow, cb_kwargs={'user_data': user_data})

    def get_api_subscribe_request(self, response, user_data, variables=None):
        # создаем задачу зайти на страницу c подписками
        if not variables:
            variables = {
                'id': user_data['id'],
                'first': 100,
            }
        url = f'{self.api_url}?query_hash={self.query_hash["subscribe"]}&variables={json.dumps(variables)}'
        yield response.follow(url, callback=self.get_api_subscribe, cb_kwargs={'user_data': user_data})

    def get_api_follow(self, response, user_data):
        # получаем подписчиков
        if b'application/json' in response.headers['Content-Type']:
            data = response.json()
            # получаем карточки подписчиков
            yield from self.get_follow_item(user_data, data['data']['user']['edge_followed_by']['edges'])
            #Если на странице есть ссылка на следующую страницу пагинации, то просим сделать запрос на эту страницу
            if data ['data']['user']['edge_follow']['page_info']['has_next_page']:
                variables = {
                    'id': user_data['id'],
                    'first': 100,
                    'after': data['data']['user']['edge_follow']['page_info']['end_cursor'],
                }
            yield from self.get_api_follow_request(response, user_data, variables)

    def get_api_subscribe(self, response, user_data):
        # получаем подписки
        if b'application/json' in response.headers['Content-Type']:
            data = response.json()
            # получаем подписки
            yield from self.get_subscribe_item(user_data, data['data']['user']['edge_follow']['edges'])
            #Если на странице есть ссылка на следующую страницу пагинации, то просим сделать запрос на эту страницу
            if data ['data']['user']['edge_follow']['page_info']['has_next_page']:
                variables = {
                    'id': user_data['id'],
                    'first': 100,
                    'after': data['data']['user']['edge_follow']['page_info']['end_cursor'],
                }
            yield from self.get_api_subscribe_request(response, user_data, variables)


    def get_follow_item(self, user_data, follow_users_data):
        for user in follow_users_data:
            yield InstaFollow(
                user_id=user_data['id'],
                user_name = user_data['username'],
                follow_id=user['node']['id'],
                follow_name=user['node']['username'],
            )
            yield InstaUser(
                date_parse=dt.datetime.utcnow(),
                data=user['node']
            )

    def get_subscribe_item(self, user_data, subscribe_users_data):
        for user in subscribe_users_data:
            yield InstaSubscription(
                user_id=user_data['id'],
                user_name = user_data['username'],
                subscribe_id=user['node']['id'],
                subscribe_name=user['node']['username'],
            )
            yield InstaUser(
                date_parse=dt.datetime.utcnow(),
                data=user['node']
            )




    @staticmethod
    def js_data_extract(response):
        #загрузка json из скрипта
        script = response.xpath('//script[contains(text(), "window._sharedData =")]/text()').get()
        return json.loads(script.replace("window._sharedData =", '')[:-1])
