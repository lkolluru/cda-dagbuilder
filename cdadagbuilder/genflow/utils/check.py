"""
Main module responsible for preparing utilities for
performing checks and various validations
"""
from typing import Dict, List
from airflow.exceptions import AirflowException
import pandera as pa
import pandera.extensions as extensions
import pandas as pd
from pandas import DataFrame


def invalid_seq_number(build_order: List):
    """
    Identify missing sequence numbers in the sequence list

    :param build_order:
    :type build_order: List
    """
    _buildorder = build_order
    invalid_order = [
        buildorder
        for buildorder in range(_buildorder[0], _buildorder[-1] + 1)
        if buildorder not in _buildorder
    ]
    return len(invalid_order)


def check_empty_df(df: DataFrame):
    """
    Validate if the data frame is empty, if so the process would throw the error

    :param: df
    :type: DataFrame
    """
    if df.empty:
        return 1
    else:
        return 0
