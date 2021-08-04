import unittest
from datetime import datetime
from typing import List
import pytest
import os
import pandas as pd
from cdadagbuilder.genflow.builders.flows.taskgroupbuilder import TaskGroupBuilder
from airflow.models.dag import DAG


class TestTaskGroupBuilder(unittest.TestCase):
    def setUp(self):
        tests_root_path = os.path.dirname(os.path.realpath(__file__))
        self.taskdataconfigroot = os.path.join(tests_root_path, "tasksdata")
        self.dag: DAG = DAG(
            dag_id="test_taskgroup_builder",
            schedule_interval=None,
            description="description",
            start_date=datetime.now(),
        )

    def test_taskgroupbuilder(self):
        tgb = TaskGroupBuilder(
            functionalgroup_name="success",
            taskdataconfigroot=self.taskdataconfigroot,
            dag=self.dag,
        )
        print(tgb.taskgroupbuilder)
        tg = [x[1] for x in tgb.taskgroupbuilder]
