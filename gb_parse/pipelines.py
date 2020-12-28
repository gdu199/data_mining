# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy import Request
from scrapy.pipelines.images import ImagesPipeline
from pymongo import MongoClient
from .items import InstaTag, InstaPost, InstaUser, InstaFollow, InstaSubscription, InstaRelation, InstaHandShake


class GbParsePipeline:
    def __init__(self):
        self.db = MongoClient('mongodb://localhost:27017')['Instagram']
    
    def process_item(self, item, spider):

        if spider.db_type == 'MONGO':

            if isinstance(item, InstaUser):
                collection = self.db['users']
                collection.insert_one(item)

            elif isinstance(item, InstaHandShake):
                collection = self.db['handshake']
                collection.insert_one(item)

            elif isinstance(item, InstaFollow):
                collection = self.db['followers']
                collection.insert_one(item)
                # проверяем есть ли встречная связь. Если есть, то создаем связь двух пользователей
                cur = self.db['subscriptions'].find({'user_id': item['user_id'], 'subscribe_id': item['follow_id']})
                if cur.count():
                    ir = InstaRelation(user_name = item['follow_name'],
                                  user_id = item['follow_id'],
                                  parent_name = item['user_name'],
                                  parent_id = item['user_id'],
                                  start_user_id = item['start_user_id'],
                                  chain = item['chain']
                                  )
                    collection = self.db['relations']
                    collection.insert_one(ir)


            elif isinstance(item, InstaSubscription):
                collection = self.db['subscriptions']
                collection.insert_one(item)
                # проверяем есть ли встречная связь. Если есть, то создаем связь двух пользователей
                cur = self.db['followers'].find({'user_id': item['user_id'], 'follow_id': item['subscribe_id']})
                if cur.count():
                    ir = InstaRelation(user_name = item['subscribe_name'],
                                  user_id = item['subscribe_id'],
                                  parent_name = item['user_name'],
                                  parent_id = item['user_id'],
                                  start_user_id = item['start_user_id'],
                                  chain = item['chain']
                                  )
                    collection = self.db['relations']
                    collection.insert_one(ir)

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
