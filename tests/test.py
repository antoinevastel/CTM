import unittest
import os

from scheduler import ScenarioManager
from scheduler import TaskSequence

path = os.path.dirname(__file__)


class MyTestCase(unittest.TestCase):
    """
        Test if the task sequences generated are correct
    """
    def test_task_sequences(self):
        scenario_manager = ScenarioManager("tests", path+"/scenario_test1.json")
        scenario_manager.read_scenario()

        expected_sequences = [{"testTask1"}, {"testTask2"}, {"testTask3", "testTask1", "testTask2", "testTask4"},
                              {"testTask6", "testTask5"}]

        for i in range(0, len(scenario_manager.task_sequences_to_run)):
            self.assertSetEqual(scenario_manager.task_sequences_to_run[i].tasks_ids, expected_sequences[i])

    def test_undeclared_id(self):
        scenario_manager = ScenarioManager("tests", path+"/scenario_test2.json")
        with self.assertRaises(ValueError):
            scenario_manager.read_scenario()

    def test_is_task_definition(self):
        scenario_manager = ScenarioManager("tests", path + "/scenario_test1.json")
        scenario_manager.read_scenario()

        task1_json_definition = scenario_manager.id_to_task_definition["testTask1"]
        # testTask1 is not a pure task definition since it has a frequency associated with
        self.assertEqual(scenario_manager.is_task_definition(task1_json_definition), False)

        task4_json_definition = scenario_manager.id_to_task_definition["testTask4"]
        # testTask4 is a pure task definition since it has no frequency associated with
        self.assertEqual(scenario_manager.is_task_definition(task4_json_definition), True)

    """
        Assert that a context has been correctly assigned to all the tasks sequences
    """
    def test_context(self):
        scenario_manager = ScenarioManager("tests", path + "/scenario_test1.json")
        scenario_manager.read_scenario()

        for tasks_sequence in scenario_manager.task_sequences_to_run:
            self.assertEqual(tasks_sequence.context, "testContext")

    """
        Assert that save_all attributes is correctly propagated
    """
    def test_save_all(self):
        scenario_manager = ScenarioManager("tests", path + "/scenario_test1.json")
        scenario_manager.read_scenario()
        tasks_not_to_save = {"testTask2", "testTask4"}

        for tasks_sequence in scenario_manager.task_sequences_to_run:
            for task_id in tasks_sequence.tasks_ids:
                task_class_name = tasks_sequence.id_to_task_definition[task_id][TaskSequence.TASK_CLASS]

                module = tasks_sequence.import_task_class(task_class_name, tasks_sequence.task_class_directory)
                task_class = getattr(module, task_class_name)
                task_instance = task_class(tasks_sequence.id_to_task_definition[task_id])
                must_instance_be_save = task_instance.must_be_saved()

                if must_instance_be_save and tasks_not_to_save.__contains__(task_id):
                    assert(False)
                elif not must_instance_be_save and not tasks_not_to_save.__contains__(task_id):
                    assert(False)
                assert(True)

    """
        Assert that the frequency of a tasks sequence corresponds to the frequency of
        the higher level task as defined in the scenario file
    """
    def test_frequencies(self):
        scenario_manager = ScenarioManager("tests", path + "/scenario_test1.json")
        scenario_manager.read_scenario()

        sequence_to_frequency = dict()
        sequence_to_frequency[("testTask1",)] = 1
        sequence_to_frequency[("testTask2",)] = 3
        sequence_to_frequency[("testTask1", "testTask2", "testTask3", "testTask4")] = 2
        sequence_to_frequency[("testTask5", "testTask6")] = 12

        for tasks_sequence in scenario_manager.task_sequences_to_run:
            list_tasks_id = list(tasks_sequence.tasks_ids)
            list_tasks_id.sort()
            self.assertEqual(tasks_sequence.frequency, sequence_to_frequency[tuple(list_tasks_id)])

    """
        Test if save task is correctly assigned
    """
    def test_save_task(self):
        scenario_manager = ScenarioManager("tests", path + "/scenario_test1.json")
        scenario_manager.read_scenario()
        self.assertEqual(scenario_manager.save_output_task_id, "testSaveTask")

    # todo test when save output = true but no save task is assigned
    """
        Test that program doesn't crash when
    """
    def test_save_task_missing(self):
        scenario_manager = ScenarioManager("tests", path + "/scenario_test3.json")
        scenario_manager.read_scenario()
        with self.assertRaises(ValueError):
            scenario_manager.run_scenario()

if __name__ == '__main__':
    unittest.main()
