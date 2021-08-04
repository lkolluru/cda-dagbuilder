import os
import unittest
import pendulum
from airflow.exceptions import AirflowException
from cdadagbuilder.genflow.utils.file import is_file, is_directory, load_yaml, exists

UTC = pendulum.timezone("UTC")


class TestFile(unittest.TestCase):
    def setUp(self):
        tests_root_path = os.path.dirname(os.path.realpath(__file__))
        self.test_dir = os.path.join(tests_root_path, "test_folder")
        self.fail_dir = os.path.join(tests_root_path, "fail_folder")
        self.filename = "test_file.yaml"
        self.fail_filename = "fail_file.yaml"
        self.test_file_path = os.path.join(self.test_dir, self.filename)
        self.fail_file_path = os.path.join(self.test_dir, self.fail_filename)

    def test_is_directory(self):
        if not is_directory(self.test_dir):
            raise AirflowException("invalid directory {0}".format(self.test_dir))

    @unittest.expectedFailure
    def test_fail_is_directory(self):
        if not is_directory(self.fail_dir):
            raise AirflowException("invalid folder specified {0}".format(self.fail_dir))

    def test_is_file(self):
        if not is_file(self.test_file_path):
            raise AirflowException("invalid file {0}".format(self.test_dir))

    @unittest.expectedFailure
    def test_fail_is_file(self):
        if not is_file(self.fail_file_path):
            raise AirflowException("invalid file {0}".format(self.fail_file_path))

    def test_exists(self):
        if not exists(self.test_file_path):
            raise AirflowException("invalid file {0}".format(self.test_file_path))

    def test_load_yaml(self):
        var = load_yaml(self.test_dir, self.filename)
        print(var)
