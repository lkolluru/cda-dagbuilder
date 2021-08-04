import codecs
import os
import yaml
from airflow.exceptions import AirflowException

try:
    from yaml import (
        CSafeLoader as YamlSafeLoader,
        CSafeDumper as YamlSafeDumper,
    )
except ImportError:
    from yaml import SafeLoader as YamlSafeLoader, SafeDumper as YamlSafeDumper


ENCODING = "utf-8"


def is_directory(name):
    """
    Checks if the directory is valid
    :param name: Directory name
    :return: Bool True if its valid
    """
    return os.path.isdir(name)


def is_file(name):
    """
    Checks if the file is valid

    :param name: file name
    :return: Bool True if its valid
    """
    return os.path.isfile(name)


def exists(name):
    """
    Checks if the file is valid

    :param name: file name
    :return: Bool True if its valid
    """
    return os.path.exists(name)


def load_yaml(dir_name, file_name):
    """
    Read data from yaml file and return a python dictionary

    :param dir_name: Directory name
    :param file_name: File name. Expects to have '.yaml' extension
    :return: Data in yaml file as dictionary
    """
    if not exists(dir_name):
        raise AirflowException(
            "Cannot read '%s'. Parent dir '%s' does not exist."
            % (file_name, dir_name)
        )

    file_path = os.path.join(dir_name, file_name)
    if not exists(file_path):
        raise AirflowException("Yaml file '%s' does not exist." % file_path)
    try:
        with codecs.open(file_path, mode="r", encoding=ENCODING) as yaml_file:
            return yaml.load(yaml_file, Loader=YamlSafeLoader)
    except Exception as e:
        raise e
