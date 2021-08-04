import os
import sys
import pytest


@pytest.fixture
def taskdataconfigroot():
    tests_root_path = os.path.dirname(os.path.realpath(__file__))
    taskdataconfigroot = os.path.join(tests_root_path, "genflow/steps/tasksdata")
    return taskdataconfigroot
