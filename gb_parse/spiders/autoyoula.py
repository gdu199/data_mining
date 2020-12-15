import os
import re
import scrapy
import pymongo
import base64

class AutoyoulaSpider(scrapy.Spider):
    name = 'autoyoula'
    allowed_domains = ['auto.youla.ru']
    start_urls = ['https://auto.youla.ru/']
    
    ccs_query = {
        'brands': 'div.ColumnItemList_container__5gTrc div.ColumnItemList_column__5gjdt a.blackLink',
        'pagination': '.Paginator_block__2XAPy a.Paginator_button__u1e7D',
        'ads': 'article.SerpSnippet_snippet__3O1t2 a.SerpSnippet_name__3F7Yu'
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = pymongo.MongoClient('mongodb://localhost:27017')[self.name]['ads']
        print(1)


    # Принимает response от главного процесса с начальной страницей
    def parse(self, response):
        for brand in response.css(self.ccs_query['brands']):
            if brand.attrib.get('title') != 'Toyota':
                # print(brand.attrib.get('title'))
                continue # для теста хватит
            #отправляем запрос по каждой ссылке на марку авто. Результат вернется в brand_page_parse(
            yield response.follow(brand.attrib.get('href'), callback=self.brand_page_parse)

    def brand_page_parse(self, response):
        for pag_page in response.css(self.ccs_query['pagination']):
            # Проходим по всем страницам пагинации. Выполняем запрос к следующей странице пагинации
            # Следующая страница пагинации приходит в копию этой же процедуры в параллельном процессе
            # то есть процедура сама себя тиражирует для каждой страницы пагинации
            yield response.follow(pag_page.attrib.get('href'), callback=self.brand_page_parse)

        for ads_page in response.css(self.ccs_query['ads']):
            # Заходим на страничку каждого объявления. Результат прилетает в ads_parse
            yield response.follow(ads_page.attrib.get('href'), callback=self.ads_parse)
    
    def ads_parse(self, response):
        data = {
            'title': response.css('.AdvertCard_advertTitle__1S1Ak::text').get(),
            'images': [img.attrib.get('src') for img in response.css('figure.PhotoGallery_photo__36e_r img')],
            'description': response.css('div.AdvertCard_descriptionInner__KnuRi::text').get(),
            'url': response.url,
            'autor': self.js_decoder_autor(response),
            'specification': self.get_specifications(response),
            'phone': self.js_decoder_phone(response),
        }

        self.db.insert_one(data)
    
    def get_specifications(self, response):
        return {itm.css('.AdvertSpecs_label__2JHnS::text').get(): itm.css(
            '.AdvertSpecs_data__xK2Qx::text').get() or itm.css('a::text').get() for itm in
                response.css('.AdvertSpecs_row__ljPcX')}
    
    def js_decoder_autor(self, response):
        # script = response.xpath('//script[contains(text(), "window.transitState =")]/text()').get()
        script = response.css('script:contains("window.transitState = decodeURIComponent")::text').get()
        re_str = re.compile(r"youlaId%22%2C%22([0-9|a-zA-Z]+)%22%2C%22avatar")
        result = re.findall(re_str, script)
        return f'https://youla.ru/user/{result[0]}' if result else None

    def js_decoder_phone(self, response):
        script = response.css('script:contains("window.transitState = decodeURIComponent")::text').get()
        re_str = re.compile(r"phone%22%2C%22([0-9|a-zA-Z]+)Xw%3D%3D%22%2C%22time")
        result = re.findall(re_str, script)
        return str(base64.b64decode(base64.b64decode(result[0]))).split("'")[1] if result else None
