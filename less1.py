import os
from pathlib import Path
import json
import time

import requests

class ServerError(Exception):
    def __init__ (self, txt):
        self.txt = txt

class Parse5ka:
    _headers = {
        'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:82.0) Gecko/20100101 Firefox/82.0",
    }
    _params = {
        'records_per_page': 50,
    }

    def __init__(self, start_url):
        self.start_url = start_url

    @staticmethod
    def _get(*args, **kwargs) -> requests.Response:
        while True:
            try:
                response = requests.get(*args, **kwargs)
                if response.status_code != 200:
                    # todo Создать класс исключение
                    # raise Exception
                    raise ServerError("Ошибка обработки запроса!")
                return response
            except ServerError as err:
                time.sleep(0.25)


    def parse(self, url):
        params = self._params
        while url:
            response: requests.Response = self._get(url, params=params, headers=self._headers)
            if params:
                params = {}
            data: dict = response.json()
            url = data.get('next')
            yield data.get('results')

    def run(self):
        for products in self.parse(self.start_url):
            for product in products:
                self._save_to_file(product, product['id'])
            time.sleep(0.1)

    @staticmethod
    def _save_to_file(product, file_name):
        dir_path = Path(os.path.dirname(__file__)).joinpath('products')
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
        path = Path(os.path.dirname(__file__)).joinpath('products').joinpath(f'{file_name}.json')
        with open(path, 'w', encoding='UTF-8') as file:
            json.dump(product, file, ensure_ascii=False)


class ParserCatalog(Parse5ka):

    def __init__(self, start_url, category_url):
        self.category_url = category_url
        super().__init__(start_url)

    def get_categories(self, url):
        response = self._get(url, headers=self._headers)
        return response.json()

    def run(self):
        for category in self.get_categories(self.category_url):
            data = {
                "name": category['parent_group_name'],
                'code': category['parent_group_code'],
                "products": [],
            }

            # print(f'Имя категории: {data["name"]}')

            self._params['categories'] = category['parent_group_code']

            for products in self.parse(self.start_url):
                data["products"].extend(products)

            #Сохраним только категории в которых есть спец предложения
            if len(data["products"]) == 0:
                continue

            self._save_to_file(
                data,
                category['parent_group_code']
            )


if __name__ == '__main__':
    parser = ParserCatalog('https://5ka.ru/api/v2/special_offers/', 'https://5ka.ru/api/v2/categories/')
    parser.run()