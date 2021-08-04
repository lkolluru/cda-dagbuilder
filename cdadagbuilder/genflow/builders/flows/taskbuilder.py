"""
    Module Prepare the tasks or task sub groups based on the template
    configured in the .csv file every task needs to have the template
    prefix programmed before preparing the dynamic dag.
"""

import re
from dataclasses import dataclass, field

from pandas import DataFrame
from airflow.models.dag import DAG
from airflow.exceptions import AirflowException
from cdadagbuilder.genflow.helpers.taskconfig import TaskConfig
from cdadagbuilder.genflow.helpers.taskfields import TaskFields
from cdadagbuilder.genflow.builders.operators.hadoop.executehivequery import (
    ExecuteHiveQuery,
)
from cdadagbuilder.genflow.builders.operators.bash.cloudpush import CloudPush
from cdadagbuilder.genflow.builders.operators.google.gbq.reloadpartitiontable import (
    ReloadPartitionTable,
)
from cdadagbuilder.genflow.builders.operators.google.gbq.executebqquery import (
    ExecuteBqQuery,
)
from cdadagbuilder.genflow.builders.operators.google.gcs.partitioncleanup import (
    PartitionCleanup,
)
from cdadagbuilder.genflow.builders.operators.google.gbq.reloadtable import (
    ReloadTable,
)


# FIXME prepare extensive regex repository
# Regex prefix repository
#    1. All the configured task templates are programmed as prefixes to be compiled into regex.
#    2. If configuration template is not present fail the process.


HIVE_PREFIX = "execute_hive_query"
CLOUD_PUSH_PREFIX = "cloud_push"
CLOUD_PARTITION_CLEANUP_PREFIX = "cloud_partition_cleanup"
BIGQUERY_PREFIX = "execute_bq_query"
BIGQUERY_RELOAD_PARTITION_PREFIX = "reload_bq_partitioned_table"
BIGQUERY_RELOAD_PREFIX = "reload_bq_table"
SPARK_PREFIX = "spark"
BEAM_PREFIX = "beam"

"""
#TODO prepare extensive regex repository
Regex repository
    1. All the configured task template prefixes are complied into regex.
"""

HIVE_REGEX = re.compile("^%s" % re.escape(HIVE_PREFIX))
CLOUD_PUSH_REGEX = re.compile("^%s" % re.escape(CLOUD_PUSH_PREFIX))
CLOUD_PARTITION_CLEANUP_REGEX = re.compile(
    "^%s" % re.escape(CLOUD_PARTITION_CLEANUP_PREFIX)
)
BIGQUERY_REGEX = re.compile("^%s" % re.escape(BIGQUERY_PREFIX))
BIGQUERY_RELOAD_PARTITION_REGEX = re.compile(
    "^%s" % re.escape(BIGQUERY_RELOAD_PARTITION_PREFIX)
)
BIGQUERY_RELOAD_REGEX = re.compile("^%s" % re.escape(BIGQUERY_RELOAD_PREFIX))
SPARK_REGEX = re.compile("^%s" % re.escape(SPARK_PREFIX))
BEAM_REGEX = re.compile("^%s" % re.escape(BEAM_PREFIX))


@dataclass
class TaskBuilder:
    """
    Prepare the tasks or task sub groups based on the template configured in the .csv file,
    every task needs to have the template prefix programmed before preparing the dynamic dag.

    :param dag: dynamic dag being programmed for ETL
    :type dag: Any
    :param taskflowdatasubset: prepare the subset of the task data particular to a task group
    :type taskflowdatasubset: Any
    """

    dag: DAG
    taskflowdatasubset: DataFrame = field(repr=False, default=None)
    taskflow: list = field(repr=False, default=None, init=False)

    def __post_init__(self):
        self.taskflow = self._get_taskflow()

    def _get_taskflow(self):
        """
         Prepares a list of tasks and task build order
        :return: _taskflow
        :rtype: list
        """
        _taskflow = []

        for _task_data_index, _task_data in self.taskflowdatasubset.iterrows():
            _task_name = _task_data["variableworkflowstepname"]
            _task_build_order = int(
                _task_data["variableworkflowstepexecutionorder"]
            )
            _task_fields = TaskFields(tasksdata_row=_task_data)
            _task_config = TaskConfig(taskfields=_task_fields)
            _task = self.fetch_flow_operator(_task_fields, _task_config)
            _taskflow.append((_task_build_order, _task))
        return _taskflow

    def fetch_flow_operator(self, _task_fields, _task_config):
        """
        Function responsible for preparing the tasks for a task builder
        from the configuration file template

        :param _task_config: Configurations including the operator parameters
        :type: TaskConfig
        :param _task_fields: All the configurations excluding the operator and task parameters
        :type: TaskFields
        :returns flowoperator: conditional based airflow task operator
        """

        if HIVE_REGEX.match(_task_fields.get_task_template_code):
            # Hive CLI query to be executed from the dag
            return ExecuteHiveQuery(
                taskconfig=_task_config, dag=self.dag
            ).flowoperator
        if CLOUD_PUSH_REGEX.match(_task_fields.get_task_template_code):
            # Distcp operator to be executed from the dag
            return CloudPush(taskconfig=_task_config, dag=self.dag).flowoperator
        if BIGQUERY_RELOAD_PARTITION_REGEX.match(
            _task_fields.get_task_template_code
        ):
            # Bigquery Reload partition table task to be executed from the dag
            return ReloadPartitionTable(
                taskconfig=_task_config, dag=self.dag
            ).flowoperator
        if BIGQUERY_RELOAD_REGEX.match(_task_fields.get_task_template_code):
            # Bigquery Reload table task to be executed from the dag
            return ReloadTable(
                taskconfig=_task_config, dag=self.dag
            ).flowoperator
        if BIGQUERY_REGEX.match(_task_fields.get_task_template_code):
            # Execute a query or a procedure task from the dag
            return ExecuteBqQuery(
                taskconfig=_task_config, dag=self.dag
            ).flowoperator
        if CLOUD_PARTITION_CLEANUP_REGEX.match(
            _task_fields.get_task_template_code
        ):
            # Execute a gcs cloud cleanup process task from the dag
            return PartitionCleanup(
                taskconfig=_task_config, dag=self.dag
            ).flowoperator
        if BEAM_REGEX.match(_task_fields.get_task_template_code):
            raise NotImplementedError
        if SPARK_REGEX.match(_task_fields.get_task_template_code):
            raise NotImplementedError

        if not (
            HIVE_REGEX.match(_task_fields.get_task_template_code)
            and CLOUD_PUSH_REGEX.match(_task_fields.get_task_template_code)
            and BIGQUERY_RELOAD_PARTITION_REGEX.match(
                _task_fields.get_task_template_code
            )
            and BIGQUERY_RELOAD_REGEX.match(_task_fields.get_task_template_code)
            and BIGQUERY_REGEX.match(_task_fields.get_task_template_code)
            and CLOUD_PARTITION_CLEANUP_REGEX.match(
                _task_fields.get_task_template_code
            )
            and BEAM_REGEX.match(_task_fields.get_task_template_code)
            and SPARK_REGEX.match(_task_fields.get_task_template_code)
        ):
            raise AirflowException(
                "step query type must be a "
                "HIVE_PREFIX (%s), "
                "CLOUD_PUSH_PREFIX (%s), "
                "BIGQUERY_PREFIX (%s),"
                "BIGQUERY_RELOAD_PARTITION_PREFIX (%s),"
                "SPARK_PREFIX (%s),"
                "BEAM_PREFIX (%s), "
                "BIGQUERY_RELOAD_PREFIX "
                "(%s), got %s"
                % (
                    HIVE_PREFIX,
                    CLOUD_PUSH_PREFIX,
                    BIGQUERY_PREFIX,
                    BIGQUERY_RELOAD_PARTITION_PREFIX,
                    SPARK_PREFIX,
                    BEAM_PREFIX,
                    BIGQUERY_RELOAD_PREFIX,
                    _task_fields.get_task_template_code,
                )
            )
