"""
To be implemented to handle push to and from google cloud and amazon s3 bucket

"""

from dataclasses import dataclass
from dataclasses import field
from typing import Any

from airflow.models import DAG
from cdadagbuilder.genflow.helpers.taskconfig import TaskConfig


@dataclass
class CloudPush:
    """
    To be implemented to handle push to and from google cloud and amazon s3 bucket

    """

    dag: DAG
    taskconfig: TaskConfig
    flowoperator: Any = field(repr=False, init=False, default=None)

    def __post_init__(self):
        self.flowoperator = self.get_operator()

    def get_operator(self):
        raise NotImplementedError
