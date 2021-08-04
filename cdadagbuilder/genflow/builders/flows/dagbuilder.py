"""
Dag Builder module required to prepare the dynamic dags
for ETL processing.
"""


import os
from typing import Dict, Any
from datetime import datetime
from dataclasses import dataclass, field
import yaml
from airflow import DAG, configuration
from airflow.exceptions import AirflowException
from airflow.operators.dummy import DummyOperator
from cdadagbuilder.genflow.utils.file import load_yaml, is_directory, is_file
from cdadagbuilder.genflow.builders.flows.taskgroupbuilder import (
    TaskGroupBuilder,
)


@dataclass
class DagBuilder:
    """
    Dag Builder module required to prepare the dynamic dags for ETL processing.

    :param functionalgroup_name: business unit short code for etl being prepared
    :type functionalgroup_name: str
    :param configuration_root: root location for etl dag configuration
    :type configuration_root: str
    :param dag_docs: dag documentation which is required for preparation of dags
    :type dag_docs: str
    """

    functionalgroup_name: str  # business unit short code for etl being prepared
    configuration_root: str  # location for etl dag configuration
    dag_docs: str  # dag documentation which is required for preparation of dags
    get_airflow_dag: DAG = field(repr=False, init=False, default=None)

    def __post_init__(self):
        self.get_airflow_dag = self._get_airflow_dag()

    def _get_dag_configuration_functionalgroup_root(self):
        """
         Prepares the folder location for loading dag configuration
        :return: _dag_configuration_functionalgroup_root
        :rtype: string
        """
        _dag_configuration_root = self.configuration_root
        _dag_configuration_functionalgroup_root = os.path.join(
            _dag_configuration_root, "config", self.functionalgroup_name
        )
        if not is_directory(_dag_configuration_functionalgroup_root):
            raise AirflowException(
                "Error in Dag Builder module for functional group {0}.{1} dag configuration folder"
                "is not a valid directory".format(
                    self.functionalgroup_name,
                    _dag_configuration_functionalgroup_root,
                )
            )
        return _dag_configuration_functionalgroup_root

    def _get_dag_configuration_functionalgroup_info(self):
        """
         Identifies and prepares the .yml configuration file storing all the dag
         configuration information per functional group

        :return: _dag_configuration_dir
        :rtype: string
        :return: _dag_configuration_file
        :rtype: string
        """
        _dag_configuration_functionalgroup_root = (
            self._get_dag_configuration_functionalgroup_root()
        )
        _dag_configuration_dir = os.path.join(
            _dag_configuration_functionalgroup_root, "dags"
        )
        _dag_configuration_file_name = "{0}.{1}".format(
            self.functionalgroup_name, "yml"
        )
        _dag_configuration_file = os.path.join(
            _dag_configuration_dir, _dag_configuration_file_name
        )
        if not is_file(_dag_configuration_file):
            raise FileNotFoundError(
                "Error in Dag Builder module for functional group {0}.{1} "
                "is not a valid config file".format(
                    self.functionalgroup_name, _dag_configuration_file
                )
            )
        return _dag_configuration_dir, _dag_configuration_file

    def _load_flow_dag_config(self):
        """
        Read dag configurations from YAML file and returns a dictionary for
        parsing dag parameters downstream

        :return: _dag_config
        :rtype: dict
        """
        (
            _dag_configuration_dir,
            _dag_configuration_file,
        ) = self._get_dag_configuration_functionalgroup_info()
        try:
            _dag_config = load_yaml(
                _dag_configuration_dir, _dag_configuration_file
            )
        except yaml.YAMLError as failure_message:

            raise AirflowException from failure_message

        return _dag_config

    def _create_dag(self):
        """
        prepare the base dag object with out tasks and only with the parameters being applied

        returns:
            DAG object
        """
        dag_params: Dict[str, Any] = self._load_flow_dag_config()

        dag: DAG = DAG(
            dag_id=self.functionalgroup_name,
            start_date=datetime.now(),
            schedule_interval=dag_params.get("schedule_interval", None),
            # airflow scheduler is turned off by default to support external
            # schedulers(control M, autosys, etc.)
            description=(dag_params.get("description", None)),
            concurrency=dag_params.get(
                "concurrency",
                configuration.conf.getint("core", "dag_concurrency"),
            ),
            catchup=dag_params.get(
                "catchup",
                configuration.conf.getboolean(
                    "scheduler", "catchup_by_default"
                ),
            ),
            max_active_runs=dag_params.get(
                "max_active_runs",
                configuration.conf.getint("core", "max_active_runs_per_dag"),
            ),
            dagrun_timeout=dag_params.get("dagrun_timeout", None),
            default_view=dag_params.get(
                "default_view",
                configuration.conf.get("webserver", "dag_default_view"),
            ),
            orientation=dag_params.get(
                "orientation",
                configuration.conf.get("webserver", "dag_orientation"),
            ),
            sla_miss_callback=dag_params.get("sla_miss_callback", None),
            on_success_callback=dag_params.get("on_success_callback", None),
            on_failure_callback=dag_params.get("on_failure_callback", None),
            default_args=dag_params.get("default_args", None),
            doc_md=self.dag_docs,
        )

        return dag

    def _add_tasks_to_dag(self):
        """
        prepare the tasks and tasks groups and dependencies and apply them to the dags

        returns:
                dag(DAG) with tasks
        """
        _dag: DAG = self._create_dag()

        with _dag as dag:

            _groups = TaskGroupBuilder(
                _dag, self.functionalgroup_name, self.configuration_root
            ).taskgroupbuilder

            for _group in [_group for _group in _groups if _group[0] != 1]:
                _upstream_groups = list(
                    filter(lambda x: _group[0] - 1 == x[0], _groups)
                )
                for _upstream_group in _upstream_groups:
                    _group[1].set_upstream(_upstream_group[1])

        return dag

    def _add_start_boundary_tasks(self):
        _task_name = "start" + self.functionalgroup_name
        return DummyOperator(task_id=_task_name)

    def _add_end_boundary_tasks(self):
        _task_name = "end" + self.functionalgroup_name
        return DummyOperator(task_id=_task_name)

    def _get_airflow_dag(self):
        """
        # TODO add validations and failure mechanism to determine if this is viable.
        prepares dags with tasks and publish to airflow repository

        returns:
            DAG object
        """
        dag = self._add_tasks_to_dag()

        return dag
