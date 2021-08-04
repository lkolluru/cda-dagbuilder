import unittest
import os
from cdadagbuilder.genflow.helpers.taskfields import TaskFields
from cdadagbuilder.genflow.steps.tasksdata_2 import TasksData
from cdadagbuilder.genflow.steps.taskflowdata import TaskFlowData
from cdadagbuilder.genflow.steps.taskflowdatasubset import TaskFlowDataSubset
from cdadagbuilder.genflow.helpers.taskconfig import TaskConfig


class TestTaskConfig(unittest.TestCase):
    def setUp(self):
        tests_root_path = os.path.dirname(os.path.realpath(__file__))
        self.taskdataconfigroot = os.path.join(tests_root_path, "tasksdata")

    def test_get_taskconfig(self):
        tasksdata = TasksData(
            functionalgroup_name="test_get_taskconfig",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="test5"
        ).taskflowdatasubset
        tasksdata_row = taskflowdatasubset.iloc[0]
        print(tasksdata_row)
        task_fields = TaskFields(tasksdata_row=tasksdata_row)
        print(task_fields.get_task_name)
        print(task_fields.get_task_command)
        print(task_fields.get_task_template_code)
        print(task_fields.get_task_parameters)
        task_config = TaskConfig(taskfields=task_fields)
        print(task_config.get_task_command)
        print(task_config.get_task_name)
        print(task_config.get_task_process_params)
        print(task_config.get_task_operator_params)
