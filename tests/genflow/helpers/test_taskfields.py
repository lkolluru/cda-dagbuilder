import unittest
import os
from cdadagbuilder.genflow.helpers.taskfields import TaskFields, get_task_fields
from cdadagbuilder.genflow.steps.tasksdata import TasksData
from cdadagbuilder.genflow.steps.taskflowdata import TaskFlowData
from cdadagbuilder.genflow.steps.taskflowdatasubset import TaskFlowDataSubset


class TestTaskFields(unittest.TestCase):
    def setUp(self):
        tests_root_path = os.path.dirname(os.path.realpath(__file__))
        self.taskdataconfigroot = os.path.join(tests_root_path, "tasksdata")

    def test_get_tasksfields(self):
        tasksdata = TasksData(
            functionalgroup_name="test_get_tasksfields",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="test1"
        ).taskflowdatasubset
        tasksdata_row = taskflowdatasubset.iloc[0]
        print(tasksdata_row)
        task_fields = TaskFields(tasksdata_row=tasksdata_row)
        print(task_fields)
        task_test = get_task_fields(tasksdata_row)
        print(task_test)

    @unittest.expectedFailure
    def test_null_get_task_name(self):
        tasksdata = TasksData(
            functionalgroup_name="test_get_tasksfields",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="test5"
        ).taskflowdatasubset
        tasksdata_row = taskflowdatasubset.iloc[1]
        print(tasksdata_row)

        task_fields = TaskFields(tasksdata_row=tasksdata_row)

    @unittest.expectedFailure
    def test_null_get_task_template_code(self):
        tasksdata = TasksData(
            functionalgroup_name="test_get_tasksfields",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="test5"
        ).taskflowdatasubset
        tasksdata_row = taskflowdatasubset.iloc[2]
        print(tasksdata_row)

        task_fields = TaskFields(tasksdata_row=tasksdata_row)

    @unittest.expectedFailure
    def test_null_get_task_command(self):
        tasksdata = TasksData(
            functionalgroup_name="test_get_tasksfields",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="test5"
        ).taskflowdatasubset
        tasksdata_row = taskflowdatasubset.iloc[3]
        print(tasksdata_row)

        task_fields = TaskFields(tasksdata_row=tasksdata_row)

    @unittest.expectedFailure
    def test_null_get_task_parameters(self):
        tasksdata = TasksData(
            functionalgroup_name="test_get_tasksfields",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="test5"
        ).taskflowdatasubset
        tasksdata_row = taskflowdatasubset.iloc[4]
        print(tasksdata_row)

        task_fields = TaskFields(tasksdata_row=tasksdata_row)
