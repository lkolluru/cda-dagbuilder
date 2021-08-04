import unittest
import os
from cdadagbuilder.genflow.steps.tasksdata import TasksData
from cdadagbuilder.genflow.steps.taskgroup import TaskGroup
from cdadagbuilder.genflow.steps.taskgroupsubset import TaskGroupSubset


class TestTaskGroupSubset(unittest.TestCase):
    def setUp(self):
        tests_root_path = os.path.dirname(os.path.realpath(__file__))
        self.taskdataconfigroot = os.path.join(tests_root_path, "tasksdata")
        self.expectedtasksdatacolumnlist = ["variabletablegroupname"]

    def test_taskgroupsubset(self):
        tasksdata = TasksData(
            functionalgroup_name="success", taskdataconfigroot=self.taskdataconfigroot
        )
        data = tasksdata.tasksdata
        #print(data.info)
        taskgroup = TaskGroup(tasksdata=data).taskgroup
        taskgroupsubset = TaskGroupSubset(
            taskgroup=taskgroup, group_execution_order=1
        ).taskgroupsubset
        print(taskgroupsubset)
       # for _task_group_index, _task_group in taskgroupsubset.iterrows():
       #     print(_task_group.to_dict())

    def test_get_taskgroup_column_names(self):
        tasksdata = TasksData(
            functionalgroup_name="success", taskdataconfigroot=self.taskdataconfigroot
        )
        data = tasksdata.tasksdata
        taskgroup = TaskGroup(tasksdata=data).taskgroup
        taskgroupsubset = TaskGroupSubset(
            taskgroup=taskgroup, group_execution_order=1
        ).taskgroupsubset
        result_column_list = list(taskgroupsubset)
        self.assertListEqual(result_column_list, self.expectedtasksdatacolumnlist)

    @unittest.expectedFailure
    def test_invalid_taskgroupsubset(self):
        tasksdata = TasksData(
            functionalgroup_name="success", taskdataconfigroot=self.taskdataconfigroot
        )
        data = tasksdata.tasksdata
        taskgroup = TaskGroup(tasksdata=data).taskgroup
        TaskGroupSubset(taskgroup=taskgroup, group_execution_order=-1)
