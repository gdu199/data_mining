from pymongo import MongoClient

class Quaryes():

    def __init__(self):
        self.db = MongoClient('mongodb://localhost:27017')['Instagram']
        self.user_id = '3630859142'
        self.follow_id = '42529182314'

    def quary_relation(self):
        # cur = self.db['subscriptions'].find({'user_id': self.user_id, 'subscribe_id': self.follow_id})
        cur = self.db['relations'].find({'user_id': self.follow_id, 'parent_id': self.user_id})
        # cur = self.db.relations.find({}, {'user_name': 1, 'user_id': 1})
        # for doc in cur:
        #     print(doc)  # or do something with the document
        if cur.count():
            print('да')
        else:
            print('нет')

if __name__ == '__main__':
    qq = Quaryes()
    qq.quary_relation()