import time


class TaskSequence:
    TASK_CLASS = "class"

    MIN_TO_S = 60

    def __init__(self, tasks_ids, frequency, context, id_to_task_definition, task_class_directory):
        self.frequency = frequency
        self.tasks_ids = tasks_ids
        self.context = context
        self.id_to_task_definition = id_to_task_definition
        self.task_class_directory = task_class_directory

        self.timestamp_last_run = None

    def add_task(self, id_task):
        self.tasks_ids.append(id_task)

    @staticmethod
    def import_task_class(task_class_name, module):
        task_class_name_package = task_class_name[0].lower() + task_class_name[1:]
        return __import__(module + "." + task_class_name_package, fromlist=[task_class_name])

    def must_trigger(self):
        if self.timestamp_last_run is None:
            return True

        if time.time() - self.timestamp_last_run > self.frequency*TaskSequence.MIN_TO_S:
            return True

        return False

    def execute(self):
        # We get the context first
        context_class_name = self.id_to_task_definition[self.context][TaskSequence.TASK_CLASS]

        module = self.import_task_class(context_class_name, self.task_class_directory)
        context_class = getattr(module, context_class_name)
        context_instance = context_class(self.id_to_task_definition[self.context])
        context_res = context_instance.run()

        tasks_res = list()
        tasks_res.append(context_res)
        for task_id in self.tasks_ids:
            task_class_name = self.id_to_task_definition[task_id][TaskSequence.TASK_CLASS]
            module = self.import_task_class(task_class_name, self.task_class_directory)
            task_class = getattr(module, task_class_name)
            task_instance = task_class(self.id_to_task_definition[task_id])
            if task_instance.must_be_saved():
                tasks_res.append(task_instance.run())

        self.timestamp_last_run = time.time()
        return '{"sequence_output":['+(",".join(tasks_res))+']}'

    def __str__(self):
        return ", ".join(self.tasks_ids)
