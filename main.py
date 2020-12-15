from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from gb_parse.spiders.hhru import HhruSpider

# from gb_parse import settings

if __name__ == '__main__':
    crawl_settings = Settings()
    crawl_settings.setmodule('gb_parse.settings')
    # crawl_settings.setmodule(settings)
    crawl_proc = CrawlerProcess(settings=crawl_settings)
    crawl_proc.crawl(HhruSpider)
    crawl_proc.start()
