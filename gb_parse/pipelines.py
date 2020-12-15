# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pymongo import MongoClient
from .items import AutoYoulaItem, HHVacancyItem, HHCompanyItem

class GbParsePipeline:
    def __init__(self):
        self.db = MongoClient('mongodb://localhost:27017')['HH']
    
    def process_item(self, item, spider):
        if spider.db_type == 'MONGO':
            if isinstance(item, HHCompanyItem):
                collection = self.db['company']
            elif isinstance(item, HHVacancyItem):
                collection = self.db['vacancy']
            collection.insert_one(item)
        return item
