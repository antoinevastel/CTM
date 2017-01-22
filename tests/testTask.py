from ctm.task import Task


class TestTask(Task):

    def __init__(self, json_definition):
        super(TestTask, self).__init__(json_definition)

    def run(self):
        return ""