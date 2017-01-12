from scheduler.task import Task


class TestContextTask(Task):

    def __init__(self, json_definition):
        super(TestContextTask, self).__init__(json_definition)

    def run(self):
        return ""