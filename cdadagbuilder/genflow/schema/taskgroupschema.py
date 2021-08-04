"""
Schema repository for task groups and task group subsets
"""

from pandera.typing import Series, String
import pandas as pd
import pandera as pa

from cdadagbuilder.genflow.utils.check import (
    invalid_seq_number,
    check_empty_df,
)


class TaskGroupSchema(pa.SchemaModel):
    """
    Pandera schema model including the validations for fields
    in the configuration files for task group dataframe
    """

    variabletablegroupname: Series[String] = pa.Field(
        nullable=False, coerce=True, allow_duplicates=False
    )

    variabletablegroupbuildorder: Series[int] = pa.Field(
        nullable=False, coerce=True, allow_duplicates=True, ge=1
    )

    # pylint: disable=R0201
    @pa.dataframe_check
    def validate_taskgroup_buildorder(self, task_group: pd.DataFrame) -> bool:
        """
        Identify the missing task group build sequence numbers
        configured in the tasks data config file

        :param task_group:
        :type task_group: DataFrame
        """
        _taskgroup_buildorder = task_group[
            "variabletablegroupbuildorder"
        ].tolist()
        _invalid_num = invalid_seq_number(_taskgroup_buildorder)
        return _invalid_num == 0

    # pylint: disable=R0201
    @pa.dataframe_check
    def validate_min_taskgroup_buildorder(
        self, task_group: pd.DataFrame
    ) -> bool:
        """
        Minimum build order has to be 1

        :param task_group:
        :type task_group: DataFrame
        """
        _taskgroup_buildorder = task_group[
            "variabletablegroupbuildorder"
        ].tolist()
        _min_build_order = min(_taskgroup_buildorder)
        return _min_build_order == 1


class TaskGroupSubsetSchema(pa.SchemaModel):
    """
    Pandera schema model including the validations for fields
    in the configuration files for task group subset dataframe
    """

    variabletablegroupname: Series[String] = pa.Field(
        nullable=False, coerce=True, allow_duplicates=False
    )

    # pylint: disable=R0201
    @pa.dataframe_check
    def validate_empty_taskgroup_subset_dataframe(
        self, task_group_subset: pd.DataFrame
    ) -> bool:
        """
        Raise a error if TaskGroupSubsetSchema dataframe is empty

        :param task_group_subset:
        :type task_group_subset: DataFrame
        """
        _indicator = check_empty_df(task_group_subset)

        return _indicator == 0
