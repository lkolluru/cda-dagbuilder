"""
Prepare tasks to execute the hive query using the hive cli in the edge box.
"""

from dataclasses import dataclass
from dataclasses import field
from typing import Any

from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.models import DAG
from cdadagbuilder.genflow.helpers.taskconfig import TaskConfig


def _validate_hql_command(parameter_value):
    pass


def _validate_hql_configuration(parameter_value):
    pass


def _validate_hql_environment(parameter_value):
    pass


@dataclass
class ExecuteHiveQuery:
    """
    Prepare tasks to execute the hive query using the hive cli in the edge box.

    1. customizable hive configurations are prepared and stored in the
       process configs, eg. num of mappers etc.
    2. environment parameters are programmed in the operator configs
       eg. env_name etc.
    3. hive queries must be stored in the gen_sql folder with appropriate
       folder structure.

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
        Functions leverages airflow.providers.apache.hive.operators.hive operator to prepare a hive cli query to
        be
        executed in the default queue associated with the service account.
        """

        # Generate the parameters
        _task_name = self.taskconfig.get_task_name
        _task_command = self.taskconfig.get_task_command
        _process_params = self.taskconfig.get_task_process_params
        _operator_params = self.taskconfig.get_task_operator_params

        # Validate the parameters values assigned to variables
        _validate_hql_command(_task_command)
        _validate_hql_configuration(_process_params)
        _validate_hql_environment(_operator_params)

        # Prepare and return the airflow task object
        workflow_operator = HiveOperator(
            task_id=_task_name,
            hql=_task_command,
            hiveconfs=_process_params,
            params=_operator_params,
            mapred_job_name="{{task_instance_key_str}}",
            dag=self.dag,
        )
        return workflow_operator
