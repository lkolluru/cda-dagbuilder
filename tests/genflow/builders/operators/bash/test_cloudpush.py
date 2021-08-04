import unittest
from datetime import datetime
import os
from typing import List
import pytest
import pandas as pd
from cdadagbuilder.genflow.steps.tasksdata_2 import TasksData
from cdadagbuilder.genflow.steps.taskflowdata import TaskFlowData
from cdadagbuilder.genflow.steps.taskflowdatasubset import TaskFlowDataSubset

from cdadagbuilder.genflow.builders.flows.taskbuilder import TaskBuilder
from airflow.models.dag import DAG


class TestCloudPush(unittest.TestCase):
    def setUp(self):
        tests_root_path = os.path.dirname(os.path.realpath(__file__))
        self.taskdataconfigroot = os.path.join(tests_root_path, "tasksdata")
        self.dag: DAG = DAG(
            dag_id="test_taskgroup_builder",
            schedule_interval=None,
            description="description",
            start_date=datetime.now(),
        )

    def test_task_builder(self):
        tasks_data = TasksData(
            functionalgroup_name="success", taskdataconfigroot=self.taskdataconfigroot
        ).tasksdata
        taskflow_data = TaskFlowData(tasksdata=tasks_data).taskflowdata
        tasks_data_subset = TaskFlowDataSubset(
            taskflowdata=taskflow_data, taskgroupname="test_group_success"
        ).taskflowdatasubset
        tgb = TaskBuilder(taskflowdatasubset=tasks_data_subset, dag=self.dag)
        print(tgb.taskflow)

    @unittest.expectedFailure
    def test_invalid_task_template_config(self):
        tasks_data = TasksData(
            functionalgroup_name="test_invalid_task_template_config",
            taskdataconfigroot=self.taskdataconfigroot,
        ).tasksdata
        taskflow_data = TaskFlowData(tasksdata=tasks_data).taskflowdata
        tasks_data_subset = TaskFlowDataSubset(
            taskflowdata=taskflow_data, taskgroupname="test1"
        ).taskflowdatasubset
        tgb = TaskBuilder(taskflowdatasubset=tasks_data_subset, dag=self.dag)
        print(tgb.taskflow)
