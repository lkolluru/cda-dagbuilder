import unittest
import os
from cdadagbuilder.genflow.helpers.taskfields import TaskFields
from cdadagbuilder.genflow.steps.tasksdata import TasksData
from cdadagbuilder.genflow.steps.taskflowdata import TaskFlowData
from cdadagbuilder.genflow.steps.taskflowdatasubset import TaskFlowDataSubset
from cdadagbuilder.genflow.helpers.taskoptions import TaskOptions


class TestTaskOptions(unittest.TestCase):
    def setUp(self):
        tests_root_path = os.path.dirname(os.path.realpath(__file__))
        self.taskdataconfigroot = os.path.join(tests_root_path, "tasksdata")

    def test_get_taskoptions(self):
        tasksdata = TasksData(
            functionalgroup_name="test_get_taskoptions",
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
        task_options = TaskOptions(task_parameters=task_fields.get_task_parameters)
        print(task_options.operator_params)
        print(task_options.process_config)

    @unittest.expectedFailure
    def test_param_invalid_operator_param_keys(self):
        tasksdata = TasksData(
            functionalgroup_name="test_get_taskoptions",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="test5"
        ).taskflowdatasubset
        tasksdata_row = taskflowdatasubset.iloc[1]
        task_fields = TaskFields(tasksdata_row=tasksdata_row)
        task_options = TaskOptions(task_parameters=task_fields.get_task_parameters)

    @unittest.expectedFailure
    def test_param_invalid_process_config_keys(self):
        tasksdata = TasksData(
            functionalgroup_name="test_get_taskoptions",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="test5"
        ).taskflowdatasubset
        tasksdata_row = taskflowdatasubset.iloc[2]
        task_fields = TaskFields(tasksdata_row=tasksdata_row)
        task_options = TaskOptions(task_parameters=task_fields.get_task_parameters)

    @unittest.expectedFailure
    def test_param_null_operator_param_values(self):
        tasksdata = TasksData(
            functionalgroup_name="test_get_taskoptions",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="test5"
        ).taskflowdatasubset
        tasksdata_row = taskflowdatasubset.iloc[3]
        task_fields = TaskFields(tasksdata_row=tasksdata_row)
        task_options = TaskOptions(task_parameters=task_fields.get_task_parameters)

    @unittest.expectedFailure
    def test_param_null_process_config_values(self):
        tasksdata = TasksData(
            functionalgroup_name="test_get_taskoptions",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="test5"
        ).taskflowdatasubset
        tasksdata_row = taskflowdatasubset.iloc[4]
        task_fields = TaskFields(tasksdata_row=tasksdata_row)
        task_options = TaskOptions(task_parameters=task_fields.get_task_parameters)

    @unittest.expectedFailure
    def test_param_invalid_json_values(self):
        tasksdata = TasksData(
            functionalgroup_name="test_get_taskoptions",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="test5"
        ).taskflowdatasubset
        tasksdata_row = taskflowdatasubset.iloc[5]
        task_fields = TaskFields(tasksdata_row=tasksdata_row)
        task_options = TaskOptions(task_parameters=task_fields.get_task_parameters)
