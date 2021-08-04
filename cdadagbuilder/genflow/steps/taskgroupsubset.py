"""
    Module to prepare task groups subset which contains all the
    groups to be executed in parallel filtered output based on
    the build execution order
"""

from dataclasses import dataclass, field
from pandas import DataFrame
from pandera import check_output
from cdadagbuilder.genflow.schema.taskgroupschema import TaskGroupSubsetSchema


@dataclass
class TaskGroupSubset:
    """
    Preparation of dynamic dags require the isolation of task groups to create task
    dependencies for the group. Class prepares a subset of the task data which has
    all the groups required for the workflow orchestration.

    :param taskgroup: Task Group Dataframe being evaluated for dynamic dags
    :type taskgroup: DataFrame
    :param group_execution_order: Filter required for preparing the subset of the groups
    :type group_execution_order: int
    """

    taskgroup: DataFrame
    group_execution_order: int
    taskgroupsubset: DataFrame = field(repr=False, init=False, default=None)

    def __post_init__(self):
        self.taskgroupsubset = self._get_taskgroupsubset()

    @check_output(TaskGroupSubsetSchema.to_schema())
    def _get_taskgroupsubset(self):
        """
        Prepares the names of the task group required for generating
        the tasks in the execution order

        :return: taskgroupsubset
        :rtype: DataFrame
        """
        _taskgroup = self.taskgroup
        taskgroupsubset = _taskgroup[
            _taskgroup["variabletablegroupbuildorder"]
            == self.group_execution_order
        ].drop_duplicates(["variabletablegroupname"])[
            ["variabletablegroupname"]
        ]

        return taskgroupsubset
