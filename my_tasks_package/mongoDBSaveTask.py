from pymongo import MongoClient

from scheduler.task import Task

class MongoDBSaveTask(Task):
    DATABASE = "dbname"
    COLLECTION = "collection"

    def __init__(self, json_definition):
        super(MongoDBSaveTask, self).__init__(json_definition)
        self.client = MongoClient()

        db_name = json_definition[MongoDBSaveTask.DATABASE]
        collection_name = json_definition[MongoDBSaveTask.COLLECTION]
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def save(self, data):
        self.collection.insert_one(data)

    def close(self):
        self.client.close()