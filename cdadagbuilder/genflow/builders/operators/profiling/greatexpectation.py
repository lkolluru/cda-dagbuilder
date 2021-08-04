"""
Prepare tasks to execute the data profiling using great expectations module in airflow
"""

from dataclasses import dataclass
from dataclasses import field
from typing import Any

from airflow.models import DAG
from airflow import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator

import great_expectations as ge
from great_expectations.checkpoint import LegacyCheckpoint
from great_expectations.data_context import DataContext
from cdadagbuilder.genflow.helpers.taskconfig import TaskConfig


_GE_CONTEXT_VAR = Variable.get("s_cda_genvar_great_expectations_repo")
_GE_QUERY_TEMPLATE = (
    """SELECT * FROM `{0}.{1}.{2}` where snapshotdate = '{3}'"""
)


def run_data_profile(d_row: dict, **kwargs):
    """
    Run the great expectations profiling process
    """

    # Prepare the variables necessary for the profiling task
    _ge_context_var = _GE_CONTEXT_VAR
    _date = kwargs["ds"]
    _date_id = kwargs["ds_nodash"]
    _ge_assetname = d_row["ge_assetname"]
    _ge_suitename = d_row["ge_suitename"]
    _ge_datasource = d_row["ge_datasource"]
    _bqprojectname = d_row["bqprojectname"]
    _bqdatasetname = d_row["bqdatasetname"]
    _bqtablename = d_row["bqtablename"]
    _bq_temp_table_name = "{0}.{1}.ge_tmp_{2}_{3}".format(
        _bqprojectname, "it_etl_profiling", _ge_assetname, _date_id
    )
    _bq_profile_query = _GE_QUERY_TEMPLATE.format(
        _bqprojectname, _bqdatasetname, _bqtablename, _date
    )

    # Retrieve your data context
    context = ge.data_context.DataContext(_ge_context_var)
    expectation_suite_name = _ge_suitename
    suite = context.get_expectation_suite(expectation_suite_name)
    suite.expectations = []

    # Create your batch_kwargs
    batch_kwargs = {
        "query": _bq_profile_query,
        "datasource": _ge_datasource,
        "bigquery_temp_table": _bq_temp_table_name,
        "data_asset_name": _ge_assetname,
    }

    # Run the validation using legacy checkpoint process as the actions are limited to pass or fail
    results = LegacyCheckpoint(
        name="_temp_checkpoint",
        data_context=context,
        batches=[
            {
                "batch_kwargs": batch_kwargs,
                "expectation_suite_names": [expectation_suite_name],
            }
        ],
    ).run(
        run_id=f"airflow: {kwargs['dag_run'].run_id}:{kwargs['dag_run'].start_date}"
    )

    # Build the data docs for the expectations which are executed
    context.build_data_docs()

    # Handle result of validation
    if not results["success"]:
        raise AirflowException(
            "{0} Validation of the data is not successful".format(
                _ge_suitename
            )
        )
    if results["success"]:
        print("validation_successful")


def _validate_task_parameters(task_parameters, parameter_key):
    if parameter_key not in task_parameters.keys():
        raise AirflowException(
            "task_parameters obtained from the configuration needs to have the following keys "
            "ge_assetname,ge_datasource, ge_suitename,bqprojectname,bqdatasetname,bqtablename but "
            "obtained the value of {0} failing the process".format(
                task_parameters.keys()
            )
        )


@dataclass
class GreatExpectation:
    """
    Prepare tasks to execute the data profiling using great expectations module in airflow

    :param dag: dynamic dag being programmed for ETL
    :type dag: Any
    :param taskconfig: prepare the subset of the task data particular to a task group
    :type taskconfig: Any
    """

    dag: DAG
    taskconfig: TaskConfig
    flowoperator: Any = field(repr=False, init=False, default=None)

    def __post_init__(self):
        self.flowoperator = self.get_operator()

    def get_operator(self):
        """
        Return a python operator which fails the process once the validation fails.
        # TODO convert the process to a checkpoint based approach and leverage ge3.0
        # TODO prepare a task group with a branch operator + labels for branch operators
        # TODO leverage airflow 2.0 process to leverage task decorators
        # TODO add validations for parameter values
        """
        # Generate the parameters
        _task_name = self.taskconfig.get_task_name
        _task_command = self.taskconfig.get_task_command
        _process_params = self.taskconfig.get_task_process_params
        _operator_params = self.taskconfig.get_task_operator_params

        # Validate the parameter keys
        _validate_task_parameters(_operator_params, "ge_assetname")
        _validate_task_parameters(_operator_params, "ge_suitename")
        _validate_task_parameters(_operator_params, "ge_datasource")
        _validate_task_parameters(_operator_params, "bqprojectname")
        _validate_task_parameters(_operator_params, "bqdatasetname")
        _validate_task_parameters(_operator_params, "bqtablename")

        # Prepare and return the airflow task object
        workflow_operator = PythonOperator(
            task_id=_task_name,
            python_callable=run_data_profile,
            provide_context=True,
            op_kwargs={"d_row": _operator_params},
            dag=self.dag,
        )
        return workflow_operator
