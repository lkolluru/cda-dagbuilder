"""
    Module to prepare task groups configuration
    Groups can contain tasks or templates which are sub groups
"""

from dataclasses import dataclass, field
from typing import List
from pandera import check_output
from pandas import DataFrame
from cdadagbuilder.genflow.schema.taskgroupschema import TaskGroupSchema


@dataclass
class TaskGroup:
    """
    All workflows are further broken down into task groups to facilitate
    pipeline flexibility and provide a way program dependencies between
    task groups directly from the configuration file.
    This class is primarily responsible for isolating all the
    task groups provided in the configuration file,and order of e
    execution of the task groups to prepare the dag.

    :param tasksdata: Prepared data frame from the configuration file
    :type tasksdata: DataFrame
    """

    tasksdata: DataFrame
    taskgroup: DataFrame = field(repr=False, init=False, default=None)
    taskgroupbuildorder: List = field(repr=False, default=None)

    def __post_init__(self):
        self.taskgroup = self._get_taskgroup()
        self.taskgroupbuildorder = self._get_taskgroupbuildorder()

    @check_output(TaskGroupSchema.to_schema())
    def _get_taskgroup(self):
        """
        Returns the unique list of task groups and execution orders from
        the tasks data frame
        :return: taskgroup
        :rtype: DataFrame
        """

        _tasksdata = self.tasksdata

        taskgroup = _tasksdata.drop_duplicates(
            subset=["variabletablegroupname", "variabletablegroupbuildorder"]
        ).sort_values(by=["variabletablegroupbuildorder"])[
            ["variabletablegroupname", "variabletablegroupbuildorder"]
        ]

        return taskgroup

    def _get_taskgroupbuildorder(self):
        """
        Returns the task group build order to determine the
        execution and dependencies
        :return: taskgroupbuildorder
        :rtype: List
        """
        taskgroup = self.taskgroup

        buildorder = taskgroup.variabletablegroupbuildorder.unique()

        taskgroupbuildorder = buildorder.tolist()

        return taskgroupbuildorder
