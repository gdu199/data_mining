import datetime as dt
import json
import scrapy
from ..items import InstaTag, InstaPost

class InstagramSpider(scrapy.Spider):
    name = 'instagram'
    db_type = 'MONGO'
    allowed_domains = ['www.instagram.com']
    start_urls = ['https://www.instagram.com/']
    login_url = 'https://www.instagram.com/accounts/login/ajax/'
    api_url = '/graphql/query/'
    query_hash = {
        'tag_posts': "9b498c08113f1e09617a1703c22b2f32", # параметр query_hash определяет какой тип содержимого будет запрошен
    }
    
    def __init__(self, login, enc_password, *args, **kwargs):
        self.tags = ['python']#, 'программирование', 'developers'
        self.users = ['teslamotors', ]
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
            #Если нет такого скрипта xpath даст ошибку, то скорее всего мы уже авторизованы.
            # Проверим параметр authenticated в ответе. Если = True, то начинаем парсинг для каждого тега, который нас интересует
            if response.json().get('authenticated'):
                for tag in self.tags:
                    yield response.follow(f'/explore/tags/{tag}/', callback=self.tag_parse)
    
    def tag_parse(self, response):
        # результат по тегу
        # instagramm не хранит данные в html. Все данные хранятся в структуре скрипта
        tag = self.js_data_extract(response)['entry_data']['TagPage'][0]['graphql']['hashtag']
        # создаем item тега и отправляем его в pipeline
        yield InstaTag(
            date_parse=dt.datetime.utcnow(),
            data={
                'id': tag['id'],
                'name': tag['name'],
                'profile_pic_url': tag['profile_pic_url'],
            }
        )
        # Запускаем обход постов и пагинации
        yield from self.get_tag_posts(tag, response)
    
    def tag_api_parse(self, response):
        # получаем json из скрипта и перезапускаем анализ страницы
        yield from self.get_tag_posts(response.json()['data']['hashtag'], response)
    
    def get_tag_posts(self, tag, response):
        # здесь находим ссылку на следующую страницу пагинации, если есть, то даем задание открыть следующую порцию данных
        if tag['edge_hashtag_to_media']['page_info']['has_next_page']:
            variables = {
                'tag_name': tag['name'],
                'first': 100, # минимальное количество контента, которое вы ожидаете получить (предположительно)
                'after': tag['edge_hashtag_to_media']['page_info']['end_cursor'],  # указатель на каких данных закончилась текущая страница, с чего начать следующую
            } #нам нужен json, то есть сериализованный словарь. Ниже преобразуем в json при построении url
            # отправляем задачу перейти на следующую страницу пагинации
            url = f'{self.api_url}?query_hash={self.query_hash["tag_posts"]}&variables={json.dumps(variables)}'
            yield response.follow(
                url,
                callback=self.tag_api_parse,
            )
        # получаем посты со страницы и отправляем в pipeline
        yield from self.get_post_item(tag['edge_hashtag_to_media']['edges'])
    
    @staticmethod
    def get_post_item(edges):
        for node in edges:
            yield InstaPost(
                date_parse=dt.datetime.utcnow(),
                data=node['node']
            )
    
    @staticmethod
    def js_data_extract(response):
        #загрузка json из скрипта
        script = response.xpath('//script[contains(text(), "window._sharedData =")]/text()').get()
        return json.loads(script.replace("window._sharedData =", '')[:-1])
