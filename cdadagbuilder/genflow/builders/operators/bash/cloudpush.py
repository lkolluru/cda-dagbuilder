"""
Prepare tasks to execute the distcp bash command to push the data
from on-prem hadoop cluster to gcp buckets.
"""
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.exceptions import AirflowException

from cdadagbuilder.genflow.helpers.taskconfig import TaskConfig


def _validate_task_parameters(task_parameters, parameter_key):
    if parameter_key not in task_parameters.keys():
        raise AirflowException(
            "task_parameters obtained from the configuration needs to have the following keys "
            "cloud_location_key and hdfs_location_key, but "
            "obtained the value of {0} failing the process".format(
                task_parameters.keys()
            )
        )


def _validate_hdfs_location(parameter_value):
    pass


def _validate_gcs_location(parameter_value):
    pass


@dataclass
class CloudPush:
    """
    Prepare tasks to execute the distcp bash command to push the data from
    on-prem hadoop cluster to gcp buckets.

    1. cloud location key -> full path of the gcs
                            gs://bucket/folder/partition_name
    2. hdfs location key -> full path of the hdfs folder
                            maprfs://volumename/foldername/partition_name

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
        Use the airflow bash operator airflow.operators.bash to execute the scripts in template location,
        most of the bash operations are templatize and
        """

        # Generate the parameters
        _taskid = self.taskconfig.get_task_name
        _script_location = self.taskconfig.get_task_command
        _process_params = self.taskconfig.get_task_process_params
        _operator_params = self.taskconfig.get_task_operator_params

        # Validate the parameter keys
        _validate_task_parameters(_operator_params, "cloud_location_key")

        _validate_task_parameters(_operator_params, "hdfs_location_key")

        # Assign the parameter values
        _snapshotdate = "{{ ds }}"
        _hdfs_location = str(_operator_params["hdfs_location_key"]).format(
            _snapshotdate
        )
        _gcs_location = str(_operator_params["cloud_location_key"]).format(
            _snapshotdate
        )
        _snap_operator_params = {
            "snapshotdate": _snapshotdate,
            "hdfs_location": _hdfs_location,
            "cloud_location": _gcs_location,
        }
        _bash_script_repo = ""

        # Validate the parameters values assigned to variables
        _validate_hdfs_location(_hdfs_location)

        _validate_gcs_location(_gcs_location)

        # Prepare and return the airflow task object
        workflow_operator = BashOperator(
            task_id=_taskid,
            bash_command=_bash_script_repo.format("copy_items_cloud.sh"),
            env=_snap_operator_params,
            dag=self.dag,
        )
        return workflow_operator
