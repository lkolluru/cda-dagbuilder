import unittest
from datetime import datetime
from typing import List
import pytest
import os
import pandas as pd
from cdadagbuilder.genflow.steps.tasksdata import TasksData
from cdadagbuilder.genflow.steps.taskgroup import TaskGroup


class TestTaskGroup(unittest.TestCase):
    def setUp(self):
        tests_root_path = os.path.dirname(os.path.realpath(__file__))
        self.taskdataconfigroot = os.path.join(tests_root_path, "tasksdata")
        self.expectedtasksdatacolumnlist = [
            "variabletablegroupname",
            "variabletablegroupbuildorder",
        ]

    def test_taskgroup(self):
        tasksdata = TasksData(
            functionalgroup_name="success", taskdataconfigroot=self.taskdataconfigroot
        )
        data = tasksdata.tasksdata
        taskgroup = TaskGroup(tasksdata=data)
        
        print(taskgroup.taskgroup.info())
        #print(taskgroup.taskgroupbuildorder)

    def test_taskgroup_sets(self):
        tasksdata = TasksData(
            functionalgroup_name="success", taskdataconfigroot=self.taskdataconfigroot
        )
        data = tasksdata.tasksdata
        taskgroup = TaskGroup(tasksdata=data)
        print(taskgroup.taskgroup)
        print(taskgroup.taskgroupbuildorder)
        _taskgroupbuildorder = taskgroup.taskgroupbuildorder
        for _build_order_index, _build_execution_order in sorted(enumerate(_taskgroupbuildorder)):
            print(_build_execution_order)
            param = (_build_execution_order * 2) - 1
            print("param is {0} ".format(param))
            new_buildorder = _build_execution_order + param
            print(new_buildorder)
            start = new_buildorder - 1
            print(start)
            end = new_buildorder + 1
            print(end)
            print(_build_execution_order, param, start, new_buildorder, end)

    def test_get_taskgroup_column_names(self):
        td = TasksData(functionalgroup_name="success", taskdataconfigroot=self.taskdataconfigroot)
        tasksdata = td.tasksdata
        taskgroup = TaskGroup(tasksdata=tasksdata).taskgroup
        result_column_list = list(taskgroup)
        self.assertListEqual(result_column_list, self.expectedtasksdatacolumnlist)

    @unittest.expectedFailure
    def test_missing_taskgroup_sequence(self):
        tasksdata = TasksData(
            functionalgroup_name="test_missing_taskgroup_sequence",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        TaskGroup(tasksdata=data)

    @unittest.expectedFailure
    def test_duplicate_taskgroup(self):
        tasksdata = TasksData(
            functionalgroup_name="test_duplicate_taskgroup",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        test = TaskGroup(tasksdata=data)
        print(test.taskgroup)
        print(test.taskgroupbuildorder)

    @unittest.expectedFailure
    def test_invalid_taskgroupbuildorder(self):
        tasksdata = TasksData(
            functionalgroup_name="test_invalid_taskgroupbuildorder",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        TaskGroup(tasksdata=data)

    @unittest.expectedFailure
    def test_null_taskgroup_name_buildorder(self):
        tasksdata = TasksData(
            functionalgroup_name="test_null_taskgroup_name_buildorder",
            taskdataconfigroot=self.taskdataconfigroot,
        )
        data = tasksdata.tasksdata
        TaskGroup(tasksdata=data)
