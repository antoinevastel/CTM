from ctm.task import Task


class TestSaveTask(Task):
    def __init__(self, json_definition):
        super(TestSaveTask, self).__init__(json_definition)


    def save(self, data):
        return ""

    def close(self):
        pass
