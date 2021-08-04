import unittest
from datetime import datetime
from typing import List
import pytest
import os
import pandas as pd
from cdadagbuilder.genflow.steps.tasksdata import TasksData


class TestTasksData(unittest.TestCase):
    def setUp(self):
        tests_root_path = os.path.dirname(os.path.realpath(__file__))
        self.taskdataconfigroot = os.path.join(tests_root_path, "tasksdata")
        self.expectedtasksdatacolumnlist = [
            "variablefunctionalgroupcode",
            "variabletablegroupname",
            "variableworkflowstepname",
            "variableworkflowstepquerytype",
            "variabletablegroupbuildorder",
            "variableworkflowstepexecutionorder",
            "variableworkflowstepschema",
            "variableworkflowstepquery",
            "workflowstepqueryparameters",
            "schemarebuildflag",
        ]

    def test_get_tasksdata(self):
        tasksdata = TasksData(
            functionalgroup_name="success", taskdataconfigroot=self.taskdataconfigroot
        )
        data = tasksdata.tasksdata
        print(data.info())

    def test_get_tasksdata_column_names(self):
        td = TasksData(functionalgroup_name="success", taskdataconfigroot=self.taskdataconfigroot)
        tasksdata = td.tasksdata
        result_column_list = list(tasksdata)
        self.assertListEqual(result_column_list, self.expectedtasksdatacolumnlist)

    @unittest.expectedFailure
    def test_invalid_configfile_dir(self):
        TasksData(functionalgroup_name="fail", taskdataconfigroot=self.taskdataconfigroot)

    @unittest.expectedFailure
    def test_invalid_configfile_data(self):
        TasksData(
            functionalgroup_name="test_invalid_configfile_data",
            taskdataconfigroot=self.taskdataconfigroot,
        )

    @unittest.expectedFailure
    def test_empty_configfile_data(self):
        data = TasksData(
            functionalgroup_name="test_empty_configfile_data",
            taskdataconfigroot=self.taskdataconfigroot,
        ).tasksdata
        print(data)
        if data.empty:
            print("data is empty")

    @unittest.expectedFailure
    def test_invalid_functionalgroup(self):
        TasksData(
            functionalgroup_name="test_invalid_functionalgroup",
            taskdataconfigroot=self.taskdataconfigroot,
        )
