import os
import datetime as dt
import dotenv
import requests
from urllib.parse import urljoin
import bs4
import pymongo as pm

dotenv.load_dotenv('.env')
class ServerError(Exception):
    def __init__ (self, txt):
        self.txt = txt

MONTHS = {
    "янв": 1,
    "фев": 2,
    "мар": 3,
    "апр": 4,
    "май": 5,
    "мая": 5,
    "июн": 6,
    "июл": 7,
    "авг": 8,
    "сен": 9,
    "окт": 10,
    "ноя": 11,
    "дек": 12,
}

class MagnitParser:

    def __init__(self, start_url):
        self.start_url = start_url
        mongo_client = pm.MongoClient(os.getenv('DATA_BASE'))
        self.db = mongo_client['Magnit']

    def _get(self, *args, **kwargs) -> bs4.BeautifulSoup:
        while True:
            try:
                response = requests.get(*args, **kwargs)#response = requests.get(url)
                if response.status_code != 200:
                    raise ServerError("Ошибка обработки запроса!")
                return bs4.BeautifulSoup(response.text, 'lxml')
            except ServerError as err:
                time.sleep(0.5)

    def run(self):
        soup = self._get(self.start_url)
        for product in self.parse(soup):
            self.save(product)

    def parse(self, soup: bs4.BeautifulSoup) -> dict:
        catalog = soup.find('div', attrs= {'class': "сatalogue__main js-promo-container"})

        for product in catalog.findChildren('a', attrs={'class':'card-sale card-sale_catalogue'}):
            try:
                pr_data = self.get_product(product)
            except AttributeError:
                continue
            yield pr_data

    def get_product(self, prod_soup):

        product_template = {
            'url': lambda soups: urljoin(self.start_url, soups.attrs.get('href')),
            'promo_name': lambda soups: soups.find('div', attrs={'class': 'card-sale__header'}).text,
            'product_name': lambda soups: str(soups.find('div', attrs={'class': 'card-sale__title'}).text),
            'old_price': lambda soups: float(
                '.'.join(itm for itm in soups.find('div', attrs={'class': 'label__price_old'}).text.split())),
            'new_price': lambda soups: float(
                '.'.join(itm for itm in soups.find('div', attrs={'class': 'label__price_new'}).text.split())),
            'image_url': lambda soups: urljoin(self.start_url, soups.find('img').attrs.get('data-src')),
            # 'date_str' : lambda soups: soups.find('div', attrs={'class': 'card-sale__date'}).text,
            'date_from': lambda _: next(dt_parser),
            'date_to': lambda _: next(dt_parser)
        }

        dt_parser = MagnitParser.date_parse(prod_soup.find('div', attrs={'class': 'card-sale__date'}).text)
        # print(next(dt_parser))
        # print(next(dt_parser))

        product_result = {}
        for key, value in product_template.items():
            try:
                product_result[key] = value(prod_soup)
            except (AttributeError, ValueError, StopIteration):
                continue
        return product_result

    @staticmethod
    def date_parse(date_string: str):
        date_list = date_string.replace('с ', '', 1).replace('\n', '').split('до')
        for date in date_list:
            temp_date = date.split()
            yield dt.datetime(year=dt.datetime.now().year, day=int(temp_date[0]), month=MONTHS[temp_date[1][:3]])

    def save(self, prod_data):
        print(prod_data.get('product_name'))
        collection = self.db['SpecialOffers']
        collection.insert_one(prod_data)

if __name__ == '__main__':
    parser = MagnitParser('https://magnit.ru/promo/?geo=moskva')
    parser.run()