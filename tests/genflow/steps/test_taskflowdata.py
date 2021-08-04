import unittest
from datetime import datetime
from typing import List
import pytest
import os
import pandas as pd
from cdadagbuilder.genflow.steps.tasksdata import TasksData
from cdadagbuilder.genflow.steps.taskflowdata import TaskFlowData


class TestTaskFlowData(unittest.TestCase):
    def setUp(self):
        tests_root_path = os.path.dirname(os.path.realpath(__file__))
        self.taskdataconfigroot = os.path.join(tests_root_path, "tasksdata")
        self.expectedtasksdatacolumnlist = [
            "variabletablegroupname",
            "variableworkflowstepname",
            "variableworkflowstepquerytype",
            "variableworkflowstepexecutionorder",
            "variableworkflowstepschema",
            "variableworkflowstepquery",
            "workflowstepqueryparameters",
        ]

    def test_taskflowdata(self):
        tasksdata = TasksData(
            functionalgroup_name="success", taskdataconfigroot=self.taskdataconfigroot
        )
        data = tasksdata.tasksdata
        print(data.info())
        taskgroup = TaskFlowData(tasksdata=data)
        #print(taskgroup.taskflowdata)

    def test_get_taskgroup_column_names(self):
        td = TasksData(functionalgroup_name="success", taskdataconfigroot=self.taskdataconfigroot)
        tasksdata = td.tasksdata
        taskflowdata = TaskFlowData(tasksdata=tasksdata).taskflowdata
        result_column_list = list(taskflowdata)
        self.assertListEqual(result_column_list, self.expectedtasksdatacolumnlist)

    @unittest.expectedFailure
    def test_missingtaskflowsteps(self):
        tasksdata = TasksData(
            functionalgroup_name="test_missingtaskflowsteps",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        data1 = TaskFlowData(tasksdata=data)
        print(data1.taskflowdata)
