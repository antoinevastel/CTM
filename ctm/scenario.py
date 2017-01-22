import json
import time
import logging

from ctm.taskSequence import TaskSequence
from ctm.task import Task


"""
    Given a start_time timestamp and duration parameter in second
    Returns if the scenario must stop
    If duration is None then it runs indefinitely
"""


def stop_scenario(start_time, duration=None):
    if duration is None:
        return False

    if time.time() - start_time > duration:
        return True

    return False


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

    SCHEDULER_DEFAULT_FREQUENCY = 1
    SCHEDULER_INF_DURATION = 9999

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

        # General save output policy
        self.save_all_outputs = None

    def read_scenario(self):
        id_tasks = set()
        with open(self.scenario_file_path, "r") as scenario_definition_file:
            json_scenario_definition = json.loads(scenario_definition_file.read())
            tasks_to_run = json_scenario_definition[ScenarioManager.TASKS_TO_RUN]

            try:
                # If the user defines this constant it is applied to all the ctm of the scenario
                # However if the attribute save_output is defined in a task it overrides save_all_outputs for this task
                self.save_all_outputs = json_scenario_definition[ScenarioManager.SAVE_ALL_OUTPUTS]
            except KeyError:
                self.save_all_outputs = False

            try:
                context_id = json_scenario_definition[ScenarioManager.CONTEXT]["id"]
                self.id_to_task_definition[context_id] = json_scenario_definition[ScenarioManager.CONTEXT]
            except KeyError:
                context_id = None

            try:
                self.save_output_task_id = json_scenario_definition[ScenarioManager.SAVE_TASK][ScenarioManager.TASK_ID]
                self.id_to_task_definition[self.save_output_task_id] = json_scenario_definition[
                    ScenarioManager.SAVE_TASK]
            except:
                pass

            for task in tasks_to_run:
                id_tasks.add(task[ScenarioManager.TASK_ID])

                # We declare the task sequence which will contains the task and the ctm that must be run before
                # TODO: if tasks of the first level have the same frequency, group them together
                # For the moment we create 1 task sequence by task declared at the first level
                tasks_ids = self.task_to_sequence(task, id_tasks)
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

    def task_to_sequence(self, json_def, id_tasks):
        try:
            # if the task sequence has already been generated previously we return it
            # return self.id_to_task_sequence[json_def[ScenarioManager.TASK_ID]]
            return self.id_to_task_sequence[json_def[ScenarioManager.TASK_ID]]
        except KeyError:
            pass

        # we add the current task to id_to_task dict
        self.add_task(json_def)

        # We create the task sequence that will contain the set of ctm (ie the current task
        # + all the tasks to run with
        # The frequency associated with the task sequence if the one of the higher level task

        task_sequence = {json_def[ScenarioManager.TASK_ID]}
        try:
            # ctm to run before may be an id or a json object
            tasks_to_run_with = json_def[ScenarioManager.RUN_WITH]
            for task_with in tasks_to_run_with:
                # ctm may be json task objects or id that references a task
                if type(task_with) is dict:
                    self.id_to_task_sequence[task_with[ScenarioManager.TASK_ID]] = self.task_to_sequence(task_with,
                                                                                                         id_tasks)
                    task_sequence = task_sequence.union(self.id_to_task_sequence[task_with[ScenarioManager.TASK_ID]])
                else:
                    # since it is an id, the task having this id must have been declared before
                    if not id_tasks.__contains__(task_with):
                        raise ValueError("Id of the task must be declared before it is referenced")

                    # we load the json associated
                    task_def_json = self.id_to_task_definition[task_with]
                    self.id_to_task_sequence[task_with] = self.task_to_sequence(task_def_json, id_tasks)
                    task_sequence = task_sequence.union(self.id_to_task_sequence[task_with])
        except KeyError:
            pass

        self.id_to_task_sequence[json_def[ScenarioManager.TASK_ID]] = task_sequence
        return task_sequence

    def add_task(self, json_def):
        try:
            json_def[Task.SAVE_OUTPUT]
        except KeyError:
            json_def[Task.SAVE_OUTPUT] = self.save_all_outputs

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

    def instantiate_save_task(self):
        if self.save_output_task_id is None and self.task_must_be_saved():
            raise ValueError("A save task is required to save outputs")

        if self.save_output_task_id is not None:
            save_output_class_name = self.id_to_task_definition[self.save_output_task_id][TaskSequence.TASK_CLASS]

            module = TaskSequence.import_task_class(save_output_class_name, self.tasks_classes_module)
            save_output_class = getattr(module, save_output_class_name)
            return save_output_class(self.id_to_task_definition[self.save_output_task_id])
        else:
            return None

    """
        Returns true if at least one task must be saved, else false
    """

    def task_must_be_saved(self):
        if self.save_all_outputs:
            return True

        for task_def in self.id_to_task_definition:
            try:
                if task_def[Task.SAVE_OUTPUT]:
                    return True
            except KeyError:
                pass

        return False

    """
        Runs a given scenario for duration seconds
        If duration parameter is None, then it runs indefinitely
    """
    def run_scenario(self, duration=None):
        save_output_instance = self.instantiate_save_task()
        start_time = time.time()
        try:
            while not stop_scenario(start_time, duration):
                sequences_to_trigger = self.get_tasks_to_trigger()
                for sequence in sequences_to_trigger:
                    print(sequence)
                    # Before launching a sequence we check if the duration has exceeded the limit
                    if stop_scenario(start_time, duration):
                        break

                    data = sequence.execute()
                    if save_output_instance is not None:
                        save_output_instance.save(data)
                    logging.info("Ran sequence: {sequence}".format(sequence=str(sequence)))

                time.sleep(self.scheduler_frequency)
        finally:
            if save_output_instance is not None:
                save_output_instance.close()

