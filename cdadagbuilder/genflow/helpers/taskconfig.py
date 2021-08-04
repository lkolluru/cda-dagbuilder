"""
Module to pull all the information together with respect to
task configurations and prepare the fields required to prepare task groups
"""

from dataclasses import dataclass, field
from typing import Dict
from cdadagbuilder.genflow.helpers.taskoptions import TaskOptions
from cdadagbuilder.genflow.helpers.taskfields import TaskFields


@dataclass
class TaskConfig:
    """
    Class to pull all the information together with respect to
    task configurations and prepare the fields required to prepare task groups

    :param taskfields:
    :type taskfields: Any
    """

    template_fields = "stepinfo"
    taskfields: TaskFields
    get_task_name: str = field(
        repr=False, init=False, default=None
    )  # Task name obtained from the configuration file
    get_task_command: str = field(
        repr=False, init=False, default=None
    )  # Task command to be executed
    get_task_process_params: Dict = field(
        repr=False, init=False, default=None
    )  # Parameters for operator envs
    get_task_operator_params: Dict = field(
        repr=False, init=False, default=None
    )  # Parameters for airflow operator

    def __post_init__(self):
        self.get_task_name = self.taskfields.get_task_name
        self.get_task_command = self.taskfields.get_task_command
        taskoptions = TaskOptions(
            task_parameters=self.taskfields.get_task_parameters
        )
        self.get_task_process_params = taskoptions.process_config
        self.get_task_operator_params = taskoptions.operator_params
