"""
Main module responsible for preparing the DAGS with tasks + taskgroups and
their dependencies
"""

from dataclasses import dataclass, field
from typing import List

import pandas
from pandas import DataFrame

from airflow.utils.task_group import TaskGroup as _TaskGroup
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator

from cdadagbuilder.genflow.steps.tasksdata import TasksData
from cdadagbuilder.genflow.steps.taskgroup import TaskGroup
from cdadagbuilder.genflow.steps.taskflowdata import TaskFlowData
from cdadagbuilder.genflow.steps.taskgroupsubset import TaskGroupSubset
from cdadagbuilder.genflow.steps.taskflowdatasubset import TaskFlowDataSubset
from cdadagbuilder.genflow.builders.flows.taskbuilder import TaskBuilder


@dataclass
class TaskGroupBuilder:
    """
     Main process responsible for preparing the DAGS with tasks + taskgroups and their dependencies

    :param dag: dynamic dag being programmed for ETL
    :type dag: Any
    :param functionalgroup_name: business unit short code for etl being prepared
    :type functionalgroup_name: str
    :param taskdataconfigroot: root location for etl dag configuration
    :type taskdataconfigroot: str
    """

    dag: DAG
    functionalgroup_name: str
    taskdataconfigroot: str
    taskgroupbuilder: List = field(repr=False, init=False, default=None)

    def __post_init__(self):

        self.taskgroupbuilder = self._get_taskgroup_builder()

    def _get_taskgroup_builder(self):
        """
        Function responsible for preparing the task groups and prepare the
        dependencies list

        :returns: build_taskgroups
        """
        # get the tasks data
        _tasksdata = TasksData(
            functionalgroup_name=self.functionalgroup_name,
            taskdataconfigroot=self.taskdataconfigroot,
        ).tasksdata

        # get the tasks groups
        _taskgroup = TaskGroup(tasksdata=_tasksdata).taskgroup

        # prepare df to avoid object is not subscriptable error
        _df_taskgroup = pandas.DataFrame(data=_taskgroup)

        # get the group build order
        _taskgroupbuildorder = TaskGroup(
            tasksdata=_tasksdata
        ).taskgroupbuildorder

        # get the full task flow data looping
        _taskflowdata = TaskFlowData(tasksdata=_tasksdata).taskflowdata

        # prepare empty set
        build_taskgroups = []

        # Loop through all the execution orders to get the parallel and
        # sequential groups handled directly from the configuration file
        for _build_order_index, _build_execution_order in sorted(
            enumerate(_taskgroupbuildorder)
        ):

            # each parallel group needs to have a start and end dummy operator
            # for simpler dag visualization
            _build_boundary_param = (_build_execution_order * 2) - 1

            # apply the boundary param to update the value of the parallel groups
            _new_build_execution_order = (
                _build_execution_order + _build_boundary_param
            )

            # start execution order for parallel groups
            _start_execution_order = _new_build_execution_order - 1
            _start_group_task_id = "start_{0}".format(_build_execution_order)
            _start_group_label = DummyOperator(
                task_id=_start_group_task_id, dag=self.dag
            )
            build_taskgroups.append(
                (_start_execution_order, _start_group_label)
            )

            # end execution order for parallel groups
            _end_execution_order = _new_build_execution_order + 1
            _end_group_task_id = "end_{0}".format(_build_execution_order)
            _end_group_label = DummyOperator(
                task_id=_end_group_task_id, dag=self.dag
            )
            build_taskgroups.append((_end_execution_order, _end_group_label))

            # prepare the data frame for all groups in the group execution order
            _task_groups = TaskGroupSubset(
                taskgroup=_df_taskgroup,
                group_execution_order=_build_execution_order,
            ).taskgroupsubset

            # Loop through all the configured tasks in the loop and prepare a task group
            for _task_group_index, _task_group in _task_groups.iterrows():
                _task_group_name = _task_group["variabletablegroupname"].lower()

                # get task flow data only for the selected table group
                _task_flow_data = TaskFlowDataSubset(
                    taskgroupname=_task_group_name, taskflowdata=_taskflowdata
                ).taskflowdatasubset

                # prepare the task groups
                with _TaskGroup(
                    group_id=_task_group_name, tooltip="Testing", dag=self.dag
                ) as _taskgroup:

                    # get all tasks and dependencies for the group
                    _tasks = TaskBuilder(
                        taskflowdatasubset=_task_flow_data, dag=self.dag
                    ).taskflow

                    # prepare the dependencies for the tasks
                    for _task in [_task for _task in _tasks if _task[0] != 1]:
                        _upstream_tasks = list(
                            filter(lambda x: _task[0] - 1 == x[0], _tasks)
                        )
                        for _upstream_task in _upstream_tasks:
                            _task[1].set_upstream(_upstream_task[1])

                    # associate with the new build order and append to task group builder list
                    build_taskgroups.append(
                        (_new_build_execution_order, _taskgroup)
                    )

        return build_taskgroups
