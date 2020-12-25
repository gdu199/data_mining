# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy import Request
from scrapy.pipelines.images import ImagesPipeline
from pymongo import MongoClient
from .items import InstaTag, InstaPost, InstaUser, InstaFollow, InstaSubscription


class GbParsePipeline:
    def __init__(self):
        self.db = MongoClient('mongodb://localhost:27017')['Instagram']
    
    def process_item(self, item, spider):
        if spider.db_type == 'MONGO':
            if isinstance(item, InstaTag):
                collection = self.db['tags']
            elif isinstance(item, InstaPost):
                collection = self.db['posts']
            elif isinstance(item, InstaUser):
                collection = self.db['users']
            elif isinstance(item, InstaFollow):
                collection = self.db['followers']
            elif isinstance(item, InstaSubscription):
                collection = self.db['subscriptions']

            collection.insert_one(item)
        return item


class GbImagePipeline(ImagesPipeline):
    #обходим item берем адреса изображений и делаем задачи на запрос на скачивание
    #скачиваем в pipline, потому что медиа файлы могут находиться на сторонних сервисах (CDN), а в пауке есть ограничение по домену
    def get_media_requests(self, item, info):
        if isinstance(item, InstaTag):
            img_url = item.get('data').get('profile_pic_url')
            yield Request(img_url)
        elif isinstance(item, InstaPost):
            # for img_url in item.get('images', []): #если нет изображений, то возвращаем пустой список
            img_url = item['data']['thumbnail_src']
            yield Request(img_url)

    #когда файлы будут скачаны, результат скачивания попадет в results
    #result содержит спсиок кортежей по каждому файлу,
    # с индексом 0 идет булево: возможность скачивания,
    # с индексом 1 информация о файле и его статусе. ее сохраним в item
    # в info очередь задач
    def item_completed(self, results, item, info):
        if isinstance(item, InstaTag) and len(results) > 0:
            item['img'] = results[0][1].get('path')
        elif isinstance(item, InstaPost):
            item['img'] = results[0][1].get('path')
            # [itm[1] for itm in results]
        return item
