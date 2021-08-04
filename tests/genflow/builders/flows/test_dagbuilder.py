import unittest
from datetime import datetime
from typing import List
import pytest
import os
import pandas as pd

# from cdadagbuilder.genflow.builders.flows.dagbuilder import DagBuilder

from cdadagbuilder import DagBuilder

from airflow.models.dag import DAG


class TestDagBuilder(unittest.TestCase):
    def setUp(self):
        tests_root_path = os.path.dirname(os.path.realpath(__file__))
        self.taskdataconfigroot = os.path.join(tests_root_path, "tasksdata")
        self.dag_docs = """
                        # Test
                        Dag documentation
                        """

    def test_dag_builder(self):
        dag: DAG = DagBuilder(
            functionalgroup_name="success",
            configuration_root=self.taskdataconfigroot,
            dag_docs=self.dag_docs,
        ).get_airflow_dag

        print(dag)
