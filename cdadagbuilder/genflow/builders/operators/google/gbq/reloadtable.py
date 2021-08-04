"""
Prepare tasks to execute native bq queries to reload a non partitioned
table in bigquery.
"""
from datetime import timedelta
from typing import Any
from dataclasses import dataclass
from dataclasses import field

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.models import DAG
from airflow.exceptions import AirflowException

from cdadagbuilder.genflow.helpers.taskconfig import TaskConfig


def _validate_bq_projectname(parameter_value):
    pass


def _validate_bq_tablename(parameter_value):
    pass


def _validate_task_parameters(task_parameters, parameter_key):
    if parameter_key not in task_parameters.keys():
        raise AirflowException(
            "task_parameters obtained from the configuration needs to have the following keys "
            "cloud_location_key and bucketname, but "
            "obtained the value of {0} failing the process".format(
                task_parameters.keys()
            )
        )


@dataclass
class ReloadTable:
    """
     Prepare tasks to execute native bq queries to reload a non partitioned
     table in bigquery.

    :param dag: dynamic dag being programmed for ETL
    :type dag: Any
    :param taskconfig: prepare the subset of the task data particular to a task group
    :type taskconfig: Any
    """

    dag: DAG
    taskconfig: TaskConfig
    flowoperator: Any = field(repr=False, init=False, default=None)

    def __post_init__(self):
        self.flowoperator = self.get_operator()

    def get_operator(self):
        """
        Use the bigquery operator to execute the script template
        """

        # Generate the parameters
        _task_name = self.taskconfig.get_task_name
        _task_command = self.taskconfig.get_task_command
        _process_params = self.taskconfig.get_task_process_params
        _operator_params = self.taskconfig.get_task_operator_params

        # Validate the parameter keys
        _validate_task_parameters(_operator_params, "projectname")
        _validate_task_parameters(_operator_params, "destinationtablename")

        # Parse and assign the required parameter values to variables
        _projectname = _operator_params["projectname"]
        _table_name = _operator_params["destinationtablename"]

        # Validate the parameters values assigned to variables

        _validate_bq_projectname(_projectname)
        _validate_bq_tablename(_table_name)

        # Prepare and return the airflow task object
        workflow_operator = BigQueryExecuteQueryOperator(
            task_id=_task_name,
            destination_dataset_table=_projectname + ":" + _table_name,
            use_legacy_sql=False,
            sql=_task_command,
            write_disposition="WRITE_TRUNCATE",
            params=_operator_params,
            retries=2,
            retry_delay=timedelta(seconds=40),
            dag=self.dag,
        )
        return workflow_operator
