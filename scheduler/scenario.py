import json
import time
import logging

from scheduler.taskSequence import TaskSequence
from scheduler.task import Task


class ScenarioManager:
    TASK_ID = "id"
    TASKS_TO_RUN = "tasks_to_run"
    CONTEXT = "context"
    RUN_WITH = "run_with"
    FREQUENCY = "frequency"
    TASKS_MODULE = "my_tasks_package"
    SAVE_ALL_OUTPUTS = "save_all_outputs"
    SAVE_TASK = "save_task"
    TASKS_CLASSES_DIR = "tasks_classes_dir"

    SCHEDULER_DEFAULT_FREQUENCY = 5

    def __init__(self, tasks_classes_module, scenario_file_path):
        self.tasks_classes_module = tasks_classes_module
        self.scenario_file_path = scenario_file_path

        self.scheduler_frequency = ScenarioManager.SCHEDULER_DEFAULT_FREQUENCY

        self.tasks = []
        # Contains json task definition associated to a given task id
        self.id_to_task_definition = dict()

        # For a given id associate it with its sequence
        # Contains also sub sequences, not only sequences that must be run
        self.id_to_task_sequence = dict()

        # Contains only the task sequences that must be run
        self.task_sequences_to_run = list()

        # Task class used to save data
        self.save_output_task_id = None

    def read_scenario(self):
        id_tasks = set()
        with open(self.scenario_file_path, "r") as scenario_definition_file:
            json_scenario_definition = json.loads(scenario_definition_file.read())
            tasks_to_run = json_scenario_definition[ScenarioManager.TASKS_TO_RUN]

            try:
                # If the user defines this constant it is applied to all the scheduler of the scenario
                # However if the attribute save_output is defined in a task it overrides save_all_outputs for this task
                save_all_outputs = json_scenario_definition[ScenarioManager.SAVE_ALL_OUTPUTS]
            except KeyError:
                save_all_outputs = False

            try:
                context_id = json_scenario_definition[ScenarioManager.CONTEXT]["id"]
                self.id_to_task_definition[context_id] = json_scenario_definition[ScenarioManager.CONTEXT]
            except KeyError:
                context_id = None

            try:
                self.save_output_task_id = json_scenario_definition[ScenarioManager.SAVE_TASK][ScenarioManager.TASK_ID]
                self.id_to_task_definition[self.save_output_task_id] = json_scenario_definition[ScenarioManager.SAVE_TASK]
            except:
                pass

            for task in tasks_to_run:
                id_tasks.add(task[ScenarioManager.TASK_ID])

                # We declare the task sequence which will contains the task and the scheduler that must be run before
                # TODO: if scheduler of the first level have the same frequency, group them together
                # For the moment we create 1 task sequence by task declared at the first level
                tasks_ids = self.task_to_sequence(task, id_tasks, save_all_outputs)
                if not self.is_task_definition(task):
                    # We create the task sequence associated, the frequency is the one of the higher level task
                    id_to_task_definition_seq = dict()
                    for task_id in tasks_ids:
                        id_to_task_definition_seq[task_id] = self.id_to_task_definition[task_id]

                    if context_id is not None:
                        id_to_task_definition_seq[context_id] = self.id_to_task_definition[context_id]

                    self.task_sequences_to_run.append(
                        TaskSequence(tasks_ids, task[ScenarioManager.FREQUENCY], context_id, id_to_task_definition_seq,
                                     self.tasks_classes_module))

    def task_to_sequence(self, json_def, id_tasks, save_all_outputs):
        try:
            # if the task sequence has already been generated previously we return it
            # return self.id_to_task_sequence[json_def[ScenarioManager.TASK_ID]]
            return self.id_to_task_sequence[json_def[ScenarioManager.TASK_ID]]
        except KeyError:
            pass

        # we add the current task to id_to_task dict
        self.add_task(json_def, save_all_outputs)

        # we create the task sequence that will contain the set of scheduler (ie the current task
        # + all the scheduler to run before
        # the frequency associated with the task sequence if the one of the higher level task

        task_sequence = {json_def[ScenarioManager.TASK_ID]}
        try:
            # scheduler to run before may be an id or a json object
            tasks_to_run_with = json_def[ScenarioManager.RUN_WITH]
            for task_with in tasks_to_run_with:
                # scheduler may be json task objects or id that references a task
                if type(task_with) is dict:
                    self.id_to_task_sequence[task_with[ScenarioManager.TASK_ID]] = self.task_to_sequence(task_with,
                                                                                                           id_tasks, save_all_outputs)
                    task_sequence = task_sequence.union(self.id_to_task_sequence[task_with[ScenarioManager.TASK_ID]])
                else:
                    # since it is an id, the task having this id must have been declared before
                    if not id_tasks.__contains__(task_with):
                        raise ValueError("Id of the task must be declared before it is referenced")

                    # we load the json associated
                    task_def_json = self.id_to_task_definition[task_with]
                    self.id_to_task_sequence[task_with] = self.task_to_sequence(task_def_json, id_tasks, save_all_outputs)
                    task_sequence = task_sequence.union(self.id_to_task_sequence[task_with])
        except KeyError:
            pass

        self.id_to_task_sequence[json_def[ScenarioManager.TASK_ID]] = task_sequence
        return task_sequence

    def add_task(self, json_def, save_all_outputs):
        try:
            json_def[Task.SAVE_OUTPUT]
        except KeyError:
            json_def[Task.SAVE_OUTPUT] = save_all_outputs

        self.id_to_task_definition[json_def[ScenarioManager.TASK_ID]] = json_def

    """
        If a task has no frequency we consider it is only a definition of the task
    """

    @staticmethod
    def is_task_definition(json_def):
        try:
            json_def[ScenarioManager.FREQUENCY]
            return False
        except KeyError:
            return True

    def get_tasks_to_trigger(self):
        return [sequence for sequence in self.task_sequences_to_run if sequence.must_trigger()]

    def instanciate_save_task(self):
        save_output_class_name = self.id_to_task_definition[self.save_output_task_id][TaskSequence.TASK_CLASS]

        module = TaskSequence.import_task_class(save_output_class_name, self.tasks_classes_module)
        save_output_class = getattr(module, save_output_class_name)
        return save_output_class(self.id_to_task_definition[self.save_output_task_id])

    def run_scenario(self):
        save_output_instance = self.instanciate_save_task()
        try:
            while True:
                sequences_to_trigger = self.get_tasks_to_trigger()
                for sequence in sequences_to_trigger:
                    # TODO: maybe cases where there would be nothing to save in the sequence
                    data = sequence.execute()
                    save_output_instance.save(data)
                    logging.info("Ran sequence: {sequence}".format(sequence=str(sequence)))
                time.sleep(self.scheduler_frequency)
        finally:
            save_output_instance.close()
