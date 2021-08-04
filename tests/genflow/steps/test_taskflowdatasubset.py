import unittest
from datetime import datetime
from typing import List
import pytest
import os
import pandas as pd
from cdadagbuilder.genflow.steps.tasksdata import TasksData
from cdadagbuilder.genflow.steps.taskflowdata import TaskFlowData
from cdadagbuilder.genflow.steps.taskflowdatasubset import TaskFlowDataSubset


class TestTaskFlowDataSubset(unittest.TestCase):
    def setUp(self):
        tests_root_path = os.path.dirname(os.path.realpath(__file__))
        self.taskdataconfigroot = os.path.join(tests_root_path, "tasksdata")
        self.expectedtasksdatacolumnlist = [
            "variableworkflowstepname",
            "variableworkflowstepexecutionorder",
            "variableworkflowstepquery",
            "workflowstepqueryparameters",
            "variableworkflowstepquerytype",
            "variableworkflowstepschema",
        ]

    def test_taskflowdatasubset(self):
        tasksdata = TasksData(
            functionalgroup_name="success", taskdataconfigroot=self.taskdataconfigroot
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata,
            taskgroupname="cleanup_cloud_storage_nested_tdmcuod",
        ).taskflowdatasubset
        print(taskflowdatasubset)

    def test_taskflowdatasubset_column_names(self):
        tasksdata = TasksData(
            functionalgroup_name="success", taskdataconfigroot=self.taskdataconfigroot
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata,
            taskgroupname="cleanup_cloud_storage_nested_tdmcuod",
        ).taskflowdatasubset
        result_column_list = list(taskflowdatasubset)
        self.assertListEqual(result_column_list, self.expectedtasksdatacolumnlist)

    @unittest.expectedFailure
    def test_taskflowdatasubset_invalid_group_name(self):
        tasksdata = TasksData(
            functionalgroup_name="success", taskdataconfigroot=self.taskdataconfigroot
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="bad_group_name"
        ).taskflowdatasubset

    @unittest.expectedFailure
    def test_taskflowdatasubset_null_task_column(self):
        tasksdata = TasksData(
            functionalgroup_name="test_taskflowdatasubset_null_task_column",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="test1"
        ).taskflowdatasubset

    @unittest.expectedFailure
    def test_taskflowdatasubset_duplicate_tasks(self):
        tasksdata = TasksData(
            functionalgroup_name="test_taskflowdatasubset_duplicate_tasks",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="test5"
        ).taskflowdatasubset

    @unittest.expectedFailure
    def test_taskflowdatasubset_invalid_tasksbuildorder(self):
        tasksdata = TasksData(
            functionalgroup_name="test_taskflowdatasubset_invalid_tasksbuildorder",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="test5"
        ).taskflowdatasubset

    @unittest.expectedFailure
    def test_taskflowdatasubset_missing_tasksbuildorder(self):
        tasksdata = TasksData(
            functionalgroup_name="test_taskflowdatasubset_missing_tasksbuildorder",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        taskflowdata = TaskFlowData(tasksdata=data).taskflowdata
        taskflowdatasubset = TaskFlowDataSubset(
            taskflowdata=taskflowdata, taskgroupname="test5"
        ).taskflowdatasubset
