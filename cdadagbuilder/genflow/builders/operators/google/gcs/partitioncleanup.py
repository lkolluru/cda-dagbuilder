"""
Prepare tasks to cleanup the folders in a google cloud storage bucket based
on the airflow back-fill date being passed to the dag.
"""

from dataclasses import dataclass
from dataclasses import field
from typing import Any

from airflow.providers.google.cloud.operators.gcs import (
    GCSDeleteObjectsOperator,
)
from airflow.models import DAG
from airflow.exceptions import AirflowException

from cdadagbuilder.genflow.helpers.taskconfig import TaskConfig


def _validate_task_parameters(task_parameters, parameter_key):
    if parameter_key not in task_parameters.keys():
        raise AirflowException(
            "task_parameters obtained from the configuration needs to have the following keys "
            "cloud_location_key and bucketname, but "
            "obtained the value of {0} failing the process".format(
                task_parameters.keys()
            )
        )


def _validate_bucket_name(parameter_value):
    pass


def _validate_cloud_location_key(parameter_value):
    pass


@dataclass
class PartitionCleanup:
    """
    Prepare tasks to cleanup the folders in a google cloud storage
    bucket based on the airflow back-fill date being passed to the dag.

    The cloud location key needs to have a place holder to replace
    its value to a airflow date.

    example: cloud location key->/folder1/folder2/partition_folder/{0}
                     bucket name-> bucketname

    :param dag: dynamic dag being programmed for ETL
    :type dag: Any
    :param taskconfig: prepare the subset of the task data particular
                       to a task group
    :type taskconfig: Any
    """

    dag: DAG  # airflow dag for which task is getting appended to
    taskconfig: TaskConfig  # task configuration being parsed out of the configuration file
    flowoperator: Any = field(
        repr=False, init=False, default=None
    )  # task object returned for dag integration

    def __post_init__(self):
        self.flowoperator = self.get_operator()

    def get_operator(self):
        """
        function leverages airflow google cloud storage operator to remove partition data in google cloud
        storage bucket,partition values are mapped to the airflow date macro.
        e.g. {{ds}} is the value of the date for which the workflow is being back filled.
        """

        # Generate the parameters
        _task_name = self.taskconfig.get_task_name
        _task_command = self.taskconfig.get_task_command
        _process_params = self.taskconfig.get_task_process_params
        _operator_params = self.taskconfig.get_task_operator_params

        # Validate the parameter keys
        _validate_task_parameters(_operator_params, "cloud_location_key")
        _validate_task_parameters(_operator_params, "bucketname")

        # Parse and assign the required parameter values to variables
        _snapshotdate = "{{ ds }}"
        _gcs_prefix = str(_operator_params["cloud_location_key"]).format(
            _snapshotdate
        )
        _gcs_bucketname = str(_operator_params["bucketname"])

        # Validate the parameters values assigned to variables
        _validate_bucket_name(_gcs_bucketname)
        _validate_cloud_location_key(_gcs_prefix)

        # Prepare and return the airflow task object
        workflow_operator = GCSDeleteObjectsOperator(
            task_id=_task_name,
            bucket_name=_gcs_bucketname,
            prefix=_gcs_prefix,
            dag=self.dag,
        )
        return workflow_operator
