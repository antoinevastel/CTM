import requests
from scheduler.task import Task


class SaveAPIRaspberryTask(Task):
    DATABASE = "dbname"
    COLLECTION = "collection"

    def __init__(self, json_definition):
        super(SaveAPIRaspberryTask, self).__init__(json_definition)


    def save(self, data):
        url = 'http://crowdspeed.lille.inria.fr//add_data_raspberry'
        headers = {'Content-Type': 'application/json'}
        requests.post(url, data=data, headers=headers)

    def close(self):
        pass
