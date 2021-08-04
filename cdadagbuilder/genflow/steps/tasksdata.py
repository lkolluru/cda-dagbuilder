"""
    Primary objective of the Module is to parse out the configurations based on the
    functional group name and prepare a data frame for downstream breakdowns.
    All of CDA workflows are bundled into functional groups(eg.omnicore,ocm)
    which represent a unique business use case. Configurations created are placed into
    predefined locations preserved in the airflow variables to support CI-CD

"""

import os
from dataclasses import dataclass, field
import pandas as pd
from pandas import DataFrame
import pandera as pa
from pandera import (
    Column,
    Check,
    PandasDtype,
)
from airflow.exceptions import AirflowException
from cdadagbuilder.genflow.utils.file import is_directory, is_file


@dataclass
class TasksData:
    """
    Primary objective of the class is to parse out the configurations based on the
    functional group name and prepare a data frame for downstream breakdowns,
    all of CDA workflows are bundled into functional groups(eg.omnicore,ocm)
    which represent a unique business use case. Configurations created are placed into
    predefined locations preserved in the airflow variables to support CI-CD

    :param functionalgroup_name : business unit short code for etl being prepared
    :type functionalgroup_name: str
    :param taskdataconfigroot : root location for etl dag configuration
    :type taskdataconfigroot: str
    """

    functionalgroup_name: str
    taskdataconfigroot: str
    tasksdata: DataFrame = field(repr=False, init=False, default=None)

    def __post_init__(self):
        self.tasksdata = self._get_tasksdata()

    def _get_config_root(self):
        """
         Return the root configuration folder location parameter expected
         to be programmed in airflow variables to support CI-CD

        :return: _config_root
        :rtype: string
        """
        _airflow_var = self.taskdataconfigroot
        _config_root = os.path.join(_airflow_var, "config")
        if not is_directory(_config_root):
            raise NotADirectoryError(
                "{0} is not a valid directory".format(_config_root)
            )
        return _config_root

    def _get_tasksdata_root(self):
        """
         Return the functional group steps file root location it is mandatory to define the
         configuration folder prior to creation of any cda dags. Folder name should match functional
         group name.

        :return: _flow_step_root
        :rtype: string
        """
        _config_root = self._get_config_root()
        _flow_step_root = os.path.join(_config_root, self.functionalgroup_name)
        if not is_directory(_flow_step_root):
            raise NotADirectoryError(
                "{0} is not a valid directory".format(_flow_step_root)
            )
        return _flow_step_root

    def _get_tasksdata_configfile(self):
        """
         Return the steps file full csv path location for data frame conversion.
         Standard folder structure formatting needs to include the following,
         {root}/{functionalgroupname}/steps/{functionalgroupname}.csv

        :return: _flow_step_script
        :rtype: string
        """
        _flow_step_root = self._get_tasksdata_root()
        _flow_step_script = os.path.join(
            _flow_step_root,
            "steps",
            "{0}.{1}".format(self.functionalgroup_name, "csv"),
        )
        if not is_file(_flow_step_script):
            raise FileNotFoundError(
                "{0} is not a valid config file".format(_flow_step_script)
            )
        return _flow_step_script

    def _get_tasksdata(self):
        """
         Load the configuration csv file into a dataframe for dynamic dag
         processing down stream critical piece of the workflow only empty
         exception is being evaluated

        :return: tasksdata
        :rtype: DataFrame
        """
        try:
            db_workflow_step_script = self._get_tasksdata_configfile()
            tasksdata = pd.read_csv(
                db_workflow_step_script,
                header=0,
                dtype={
                    "variablefunctionalgroupcode": "string",
                    "variabletablegroupname": "string",
                    "variableworkflowstepname": "string",
                    "variableworkflowstepquerytype": "string",
                    "variabletablegroupbuildorder": int,
                    "variableworkflowstepexecutionorder": int,
                    "variableworkflowstepschema": "string",
                    "variableworkflowstepquery": "string",
                    "workflowstepqueryparameters": "string",
                    "schemarebuildflag": "bool",
                },
            )

        except pd.errors.EmptyDataError as empty_data_error:
            raise AirflowException(
                "Configuration file is empty for functional group {0} "
                "failed with error.. {1}".format(
                    self.functionalgroup_name, empty_data_error
                )
            ) from empty_data_error
        except pd.errors.ParserError as parse_data_error:
            raise AirflowException(
                "Parsing of the configuration file for functional group {0} failed "
                "with error {1}".format(
                    self.functionalgroup_name, parse_data_error
                )
            ) from parse_data_error

        except BaseException as gen_exception:
            raise AirflowException(
                "Invalid Values are in configuration file for functional group {0} failed "
                "with error {1}".format(
                    self.functionalgroup_name, gen_exception
                )
            ) from gen_exception

        self._validate_tasksdata_schema(tasksdata)

        return tasksdata

    def _validate_tasksdata_schema(self, tasksdata):
        """
        Prepare pandera schema repository for tasksdata data dataframe and validate the results

        :param tasksdata:
        :type tasksdata: DataFrame
        """

        _l_check_functionalgroup_name = [self.functionalgroup_name]

        tasksdata_schema = pa.DataFrameSchema(
            {
                "variablefunctionalgroupcode": Column(
                    pandas_dtype=PandasDtype.String,
                    checks=[Check.isin(_l_check_functionalgroup_name)],
                    nullable=False,
                    allow_duplicates=True,
                    coerce=True,
                    required=True,
                ),
                "variabletablegroupname": Column(
                    pandas_dtype=PandasDtype.String,
                    checks=None,
                    nullable=False,
                    allow_duplicates=True,
                    coerce=True,
                    required=True,
                ),
                "variableworkflowstepname": Column(
                    pandas_dtype=PandasDtype.String,
                    checks=None,
                    nullable=False,
                    allow_duplicates=False,
                    coerce=True,
                    required=True,
                ),
                "variableworkflowstepquerytype": Column(
                    pandas_dtype=PandasDtype.String,
                    checks=None,
                    nullable=False,
                    allow_duplicates=True,
                    coerce=True,
                    required=True,
                ),
                "variabletablegroupbuildorder": Column(
                    pandas_dtype=PandasDtype.Int64,
                    checks=[
                        Check.greater_than(0),
                        Check.greater_than_or_equal_to(min_value=1),
                        Check(lambda s: s.min() == 1, element_wise=False),
                    ],
                    nullable=False,
                    allow_duplicates=True,
                    coerce=True,
                    required=True,
                ),
                "variableworkflowstepexecutionorder": Column(
                    pandas_dtype=PandasDtype.Int64,
                    checks=[Check.greater_than(0)],
                    nullable=False,
                    allow_duplicates=True,
                    coerce=True,
                    required=True,
                ),
                "variableworkflowstepschema": Column(
                    pandas_dtype=PandasDtype.String,
                    checks=None,
                    nullable=False,
                    allow_duplicates=True,
                    coerce=True,
                    required=True,
                ),
                "variableworkflowstepquery": Column(
                    pandas_dtype=PandasDtype.String,
                    checks=None,
                    nullable=False,
                    allow_duplicates=True,
                    coerce=True,
                    required=True,
                ),
                "workflowstepqueryparameters": Column(
                    pandas_dtype=PandasDtype.String,
                    checks=None,
                    nullable=False,
                    allow_duplicates=True,
                    coerce=True,
                    required=True,
                ),
                "schemarebuildflag": Column(
                    pandas_dtype=PandasDtype.Bool,
                    checks=None,
                    nullable=False,
                    allow_duplicates=True,
                    coerce=True,
                    required=True,
                ),
            }
        )
        try:
            tasksdata_schema.validate(tasksdata)

        except pa.errors.SchemaError as schema_errors:
            raise AirflowException(
                "Schema Errors failed with error {0}".format(schema_errors)
            ) from schema_errors
