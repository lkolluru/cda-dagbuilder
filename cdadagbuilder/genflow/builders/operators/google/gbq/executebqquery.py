"""
    Prepare tasks to execute the query/store procedures in google cloud environment.
"""
from datetime import timedelta
from typing import Any
from dataclasses import dataclass
from dataclasses import field

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.models import DAG
from cdadagbuilder.genflow.helpers.taskconfig import TaskConfig


def _validate_bq_command(parameter_value):
    pass


def _validate_bq_configuration(parameter_value):
    pass


@dataclass
class ExecuteBqQuery:
    """
    Prepare tasks to execute the query/store procedures in
    google cloud environment.

    1. only non destination table/non output queries are to
       be executed using this process.
    2. queries/procedure codes must be stored in the gen_sql
       folder with appropriate folder structure.

    :param dag: dynamic dag being programmed for ETL
    :type dag: Any
    :param taskconfig: prepare the subset of the task data
                       particular to a task group
    :type taskconfig: Any
    """

    dag: DAG
    taskconfig: TaskConfig
    flowoperator: Any = field(repr=False, init=False, default=None)

    def __post_init__(self):
        self.flowoperator = self.get_operator()

    def get_operator(self):
        """
        function leverages airflow deprecated bigquery operator to
        execute queries stored in gen_sql folder
        """

        # Generate the parameters
        _task_name = self.taskconfig.get_task_name
        _task_command = self.taskconfig.get_task_command
        _process_params = self.taskconfig.get_task_process_params
        _operator_params = self.taskconfig.get_task_operator_params

        # Validate the parameter keys
        _validate_bq_command(_task_command)

        _validate_bq_configuration(_operator_params)

        # Prepare and return the airflow task object
        workflow_operator = BigQueryExecuteQueryOperator(
            task_id=_task_name,
            use_legacy_sql=False,
            sql=_task_command,
            params=_operator_params,
            retries=2,
            retry_delay=timedelta(seconds=40),
            dag=self.dag,
        )
        return workflow_operator
