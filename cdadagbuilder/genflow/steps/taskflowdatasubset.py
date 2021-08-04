"""
Modules provides task flow data frame for a given table group
"""
from dataclasses import dataclass, field
from pandas import DataFrame
from pandera import check_output
from cdadagbuilder.genflow.schema.taskflowdataschema import (
    TaskFlowDataSubsetSchema,
)


@dataclass
class TaskFlowDataSubset:
    """
    Preparation of dynamic dags require the isolation of task groups to create task
    dependencies for the group. Class prepares a subset of the task data which has
    all the groups required for the workflow orchestration.

    :param taskgroupname: Group name to filter the data required for tasks preparation
    :type taskgroupname: String
    :param taskflowdata: Prepared data frame from the configuration file
                        for task flow data
    :type taskflowdata: DataFrame
    """

    taskgroupname: str = field(repr=False, default=None)
    taskflowdata: DataFrame = field(repr=False, default=None)
    taskflowdatasubset: DataFrame = field(repr=False, init=False, default=None)

    def __post_init__(self):
        self.taskflowdatasubset = self._get_taskflowdatasubset()

    @check_output(TaskFlowDataSubsetSchema.to_schema())
    def _get_taskflowdatasubset(self):
        """
        Return the list of tasks and parameters for a given task group
        :return: taskflowdatasubset
        :rtype: DataFrame
        """
        _taskflowdata = self.taskflowdata
        _taskgroupname = self.taskgroupname
        taskflowdatasubset = _taskflowdata[
            _taskflowdata["variabletablegroupname"] == _taskgroupname
        ][
            [
                "variableworkflowstepname",
                "variableworkflowstepexecutionorder",
                "variableworkflowstepquery",
                "workflowstepqueryparameters",
                "variableworkflowstepquerytype",
                "variableworkflowstepschema",
            ]
        ]
        taskflowdatasubset["variableworkflowstepname"] = taskflowdatasubset[
            "variableworkflowstepname"
        ].replace([":"], "_", regex=True)

        return taskflowdatasubset
