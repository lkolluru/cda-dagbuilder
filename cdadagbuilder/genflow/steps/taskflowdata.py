"""
Task Flow Module required to prepare the dynamic dags for ETL processing.
"""

from dataclasses import dataclass, field
from pandas import DataFrame
from pandera import check_output
from cdadagbuilder.genflow.schema.taskflowdataschema import TaskFlowDataSchema


@dataclass
class TaskFlowData:
    """
    Task Flow Data class required to prepare the dynamic dags for ETL processing.

    :param tasksdata: dataframe of all the tasks configured in the .csv file
    :type tasksdata: DataFrame
    """

    template_fields = "tasksdata"
    tasksdata: DataFrame
    taskflowdata: DataFrame = field(repr=False, init=False, default=None)

    def __post_init__(self):
        self.taskflowdata = self._get_taskflowdata()

    @check_output(TaskFlowDataSchema.to_schema())
    def _get_taskflowdata(self):
        """
        Prepare the workflow step data frame to be leveraged for template tasks
        creation
        :return:taskflowdata
        :rtype: DataFrame
        """
        _tasksdata = self.tasksdata

        taskflowdata = _tasksdata.drop_duplicates(
            subset=[
                "variabletablegroupname",
                "variableworkflowstepname",
                "variableworkflowstepquerytype",
                "variableworkflowstepexecutionorder",
                "variableworkflowstepschema",
                "variableworkflowstepquery",
                "workflowstepqueryparameters",
            ]
        )[
            [
                "variabletablegroupname",
                "variableworkflowstepname",
                "variableworkflowstepquerytype",
                "variableworkflowstepexecutionorder",
                "variableworkflowstepschema",
                "variableworkflowstepquery",
                "workflowstepqueryparameters",
            ]
        ]

        return taskflowdata
