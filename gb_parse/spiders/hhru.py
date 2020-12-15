import scrapy
from ..loaders import HHVacancyLoader, HHCompanyLoader
from ..items import HHCompanyItem


class HhruSpider(scrapy.Spider):
    name = 'hhru'
    db_type = 'MONGO'
    allowed_domains = ['hh.ru']
    start_urls = ['https://hh.ru/search/vacancy?schedule=remote&L_profession_id=0&area=113']
    _xpath = {
        'pagination': '//div[@data-qa="pager-block"]//a[@data-qa="pager-page"]/@href',
        'vacancy_urls': '//a[@data-qa="vacancy-serp__vacancy-title"]/@href',
    }
    vacancy_xpath = {
        "title": '//h1[@data-qa="vacancy-title"]/text()',
        "salary": '//p[@class="vacancy-salary"]//text()',
        "description": '//div[@data-qa="vacancy-description"]//text()',
        "skills": '//div[@class="bloko-tag-list"]//span[@data-qa="bloko-tag__text"]/text()',
        "company_url": '//a[@data-qa="vacancy-company-name"]/@href',
    }
    
    company_xpath = {
        'name': '//h1/span[contains(@class, "company-header-title-name")]/text()',
        'url': '//a[contains(@data-qa, "company-site")]/@href',
        'description': '//div[contains(@data-qa, "company-description")]//text()',
        'activity_field': '//div[contains(@class, "employer-sidebar-block")]//p/text()',
    }


    def parse(self, response, **kwargs):
        for pag_page in response.xpath(self._xpath['pagination']):
            yield response.follow(pag_page, callback=self.parse)
        
        for vacancy_page in response.xpath(self._xpath['vacancy_urls']):
            yield response.follow(vacancy_page, callback=self.vacancy_parse)
    
    def vacancy_parse(self, response, **kwargs):
        loader = HHVacancyLoader(response=response)
        loader.add_value('url', response.url)
        for key, value in self.vacancy_xpath.items():
            loader.add_xpath(key, value)
        
        yield loader.load_item()
        yield response.follow(response.xpath(self.vacancy_xpath['company_url']).get(), callback=self.company_parse)


    def company_parse(self, response, **kwargs):
        loader = HHCompanyLoader(response=response)
        loader.add_value('url', response.url)

        for key, value in self.company_xpath.items():
            loader.add_xpath(key, value)

        yield loader.load_item()

        # vacancyes_url = response.xpath('//a[contains(@data-qa, "employer-page__employer-vacancies-link")]/@href').get()
        # yield response.follow(vacancyes_url, callback=self.company_parse_B, cb_kwargs = {'resp' : response, 'item' : co_item})


    # def company_parse_B(self, response, **kwargs):
    #
    #     loader = HHCompanyLoader(response=kwargs.get('resp'))
    #     loader.add_value('url', kwargs.get('resp').url)
    #
    #     co_item = kwargs.get('item')
    #     co_item['vacancy'] = response.xpath('//a[contains(@data-qa, "vacancy-serp__vacancy-title")]/@href').extract()
    #
    #     for key, value in co_item.items():
    #         loader.item[key] = value
    #
    #     yield loader.load_item()

    # for key, value in self.company_xpath.items():
    #     loader.add_xpath(key, value)
    # loader.add_xpath('vacancy', '//a[contains(@data-qa, "vacancy-serp__vacancy-title")]/@href')
    #     for ids in range(10):
    #         yield {'itm': ids}
    
