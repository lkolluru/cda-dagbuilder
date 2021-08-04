"""
Schema repository for task flow data and task flow data subsets
"""
from pandera.typing import Series, String
import pandas as pd
import pandera as pa

from cdadagbuilder.genflow.utils.check import (
    check_empty_df,
    invalid_seq_number,
)


class TaskFlowDataSchema(pa.SchemaModel):
    """
    Pandera schema model including the validations for fields
    in the configuration files for task flow data dataframe
    """

    variabletablegroupname: Series[String] = pa.Field(
        nullable=False, coerce=True
    )
    variableworkflowstepname: Series[String] = pa.Field(
        nullable=False, coerce=True, allow_duplicates=False
    )
    variableworkflowstepquerytype: Series[String] = pa.Field(
        nullable=False, coerce=True
    )
    variableworkflowstepexecutionorder: Series[int] = pa.Field(
        nullable=False, coerce=True
    )
    variableworkflowstepschema: Series[String] = pa.Field(
        nullable=False, coerce=True
    )
    variableworkflowstepquery: Series[String] = pa.Field(
        nullable=False, coerce=True
    )
    workflowstepqueryparameters: Series[String] = pa.Field(
        nullable=False, coerce=True
    )

    # pylint: disable=R0201
    @pa.dataframe_check
    def validate_taskflow_data_dataframe(
        self, task_flow_data: pd.DataFrame
    ) -> bool:
        """
        Raise a error if TaskDataSchema dataframe is empty

        :param task_flow_data:
        :type task_flow_data: DataFrame
        """
        _indicator = check_empty_df(task_flow_data)

        return _indicator == 0


class TaskFlowDataSubsetSchema(pa.SchemaModel):
    """
    Pandera schema model including the validations for fields
    in the configuration files for task flow data subset dataframe
    """

    variableworkflowstepname: Series[String] = pa.Field(
        nullable=False, coerce=True, allow_duplicates=False
    )
    variableworkflowstepquerytype: Series[String] = pa.Field(
        nullable=False, coerce=True
    )
    variableworkflowstepexecutionorder: Series[int] = pa.Field(
        nullable=False, coerce=True, ge=1
    )
    variableworkflowstepschema: Series[String] = pa.Field(
        nullable=False, coerce=True
    )
    variableworkflowstepquery: Series[String] = pa.Field(
        nullable=False, coerce=True
    )
    workflowstepqueryparameters: Series[String] = pa.Field(
        nullable=False, coerce=True
    )

    # pylint: disable=R0201
    @pa.dataframe_check
    def validate_taskflow_data_subset_dataframe(
        self, task_flow_data_subset: pd.DataFrame
    ) -> bool:
        """
        Raise a error if TaskDataFlowSubsetSchema dataframe is empty

        :param task_flow_data_subset:
        :type task_flow_data_subset: DataFrame
        """
        _indicator = check_empty_df(task_flow_data_subset)

        return _indicator == 0

    # pylint: disable=R0201
    @pa.dataframe_check
    def validate_task_step_buildorder(
        self, task_flow_data_subset: pd.DataFrame
    ) -> bool:
        """
        Identify the missing task step build sequence numbers
        configured in the tasks data config file

        :param task_flow_data_subset:
        :type task_flow_data_subset: DataFrame
        """
        _taskflow_step_buildorder = task_flow_data_subset[
            "variableworkflowstepexecutionorder"
        ].tolist()
        _invalid_num = invalid_seq_number(_taskflow_step_buildorder)
        return _invalid_num == 0

    # pylint: disable=R0201
    @pa.dataframe_check
    def validate_min_task_step_buildorder(
        self, task_flow_data_subset: pd.DataFrame
    ) -> bool:
        """
        Minimum build order has to be 1

        :param task_flow_data_subset:
        :type task_flow_data_subset: DataFrame
        """
        _taskflow_step_buildorder = task_flow_data_subset[
            "variableworkflowstepexecutionorder"
        ].tolist()
        _min_build_order = min(_taskflow_step_buildorder)
        return _min_build_order == 1
