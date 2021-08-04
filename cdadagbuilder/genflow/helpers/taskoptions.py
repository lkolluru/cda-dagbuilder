"""
All the configuration and values to be passed for the operator are classified into
operator_parameters -> responsible for populating airflow base template fields
process_config -> operators also accept a dict of additional values mainly leveraged in
                      bash and python, hive operators, docker operators etc.
they are bundled into a same column in the dataframe for ease of expansion e.g. for
docker operators a new value for the configuration can be added parsing of those
fields into a dict are handled in this Moudle.
"""

import json
from dataclasses import dataclass, field
from airflow.exceptions import AirflowException


def _validate_task_parameters(task_parameters, parameter_key):
    """
    validate dict keys

    :param task_parameters:
    :type task_parameters: Dict
    :param parameter_key:
    :type parameter_key: Str
    """
    if parameter_key not in task_parameters.keys():
        raise AirflowException(
            "task_parameters obtained from the needs to have the following keys "
            "operator_parameters and process_config, but "
            "obtained the value of {0} failing the process".format(
                task_parameters.keys()
            )
        )


def _validate_task_parameter_values(parameter_value):
    """
    validate parameter values existence

    :param parameter_value:
    :type parameter_value: Str
    """
    if len(parameter_value) == 0:
        raise AirflowException(
            "task_parameters obtained from the config needs to have values at-least defaults "
            "populated but obtained blank values of {0} failing the process".format(
                parameter_value
            )
        )


@dataclass
class TaskOptions:
    """
    All the configuration and values to be passed for the operator are classified into
    operator_parameters -> responsible for populating airflow base template fields
    process_config -> operators also accept a dict of additional values mainly leveraged in
                      bash and python, hive operators, docker operators etc.
    they are bundled into a same column in the dataframe for ease of expansion e.g. for docker
    operators a new value for the configuration can be added parsing of those fields into a dict
    are handled in this class.

    :param task_parameters : core and additional parameters are
    :type task_parameters: str
    """

    template_fields = "task_parameters"
    task_parameters: str
    operator_params: dict = field(repr=False, init=False, default=None)
    process_config: dict = field(repr=False, init=False, default=None)

    def __post_init__(self):
        _task_config = self._loadtaskconfig
        self.operator_params = _task_config["operator_params"]
        self.process_config = _task_config["process_config"]

    @property
    def _loadtaskconfig(self):
        """
         Configuration files the holds the information in following format,
         eg. {"operator_params":{"bucketname":"bkt_offline",},
              "process_config":{"default":"default"},... }
         once parsed can be accessed as fields in the helpers data class.
        :return: _task_parameters
        :rtype: Dict
        """
        try:
            _task_parameters = json.loads(
                self.task_parameters.replace(": ", ":")
            )
        except Exception as base_exception:
            raise AirflowException from base_exception

        _validate_task_parameters(_task_parameters, "operator_params")

        _validate_task_parameters(_task_parameters, "process_config")

        _validate_task_parameter_values(_task_parameters["operator_params"])

        _validate_task_parameter_values(_task_parameters["process_config"])

        return _task_parameters
