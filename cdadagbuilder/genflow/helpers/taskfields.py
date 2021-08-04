"""
Each task has predefined parameters programmed in the configuration file,
each row is broken into fields for cleaner code maintenance and flexibility for
refactoring in the future.
"""

from dataclasses import dataclass, field
import pandas as pd
from pandas import Series
from airflow.exceptions import AirflowException


def _validate_taskfields(task_info):
    """
    Validate if the task fields are having issues throw error in the preparation
    :param task_info: Prepared data row from the task group data frame
    :type task_info: Series
    """
    if pd.isnull(task_info):
        raise AirflowException(
            "task_info obtained from the dataframe for is nan failing the process"
        )

    if task_info.isspace():
        raise AirflowException(
            "task_info obtained from the dataframe for is null failing the process"
        )


@dataclass
class TaskFields:
    """
    Each task has predefined parameters programmed in the configuration file,
    each row is broken into fields for cleaner code maintenance and flexibility for
    refactoring in the future.

    :param tasksdata_row: Prepared data row from the task group data frame
    :type tasksdata_row: Series
    """

    template_fields = "tasksdata_row"
    tasksdata_row: Series
    get_task_name: str = field(repr=False, init=False, default=None)
    get_task_command: str = field(repr=False, init=False, default=None)
    get_task_template_code: str = field(repr=False, init=False, default=None)
    get_task_parameters: str = field(repr=False, init=False, default=None)

    def __post_init__(self):
        self._validate_fields()
        self.get_task_name = self.tasksdata_row["variableworkflowstepname"]
        self.get_task_command = self.tasksdata_row["variableworkflowstepquery"]
        self.get_task_template_code = self.tasksdata_row[
            "variableworkflowstepquerytype"
        ]
        self.get_task_parameters = self.tasksdata_row[
            "workflowstepqueryparameters"
        ]

    def _validate_fields(self):
        _validate_taskfields(self.tasksdata_row["variableworkflowstepname"])
        _validate_taskfields(self.tasksdata_row["variableworkflowstepquery"])
        _validate_taskfields(
            self.tasksdata_row["variableworkflowstepquerytype"]
        )
        _validate_taskfields(self.tasksdata_row["workflowstepqueryparameters"])
