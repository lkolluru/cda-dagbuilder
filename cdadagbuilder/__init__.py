r"""
# What is cdadagbuilder?
cdadagbuilder auto-generates the task templates and dags required
for execution of complex ETL pipelines with multiple dependencies
and provides a boiler plate coding format for dag creations and maintenance
"""


from .genflow.builders.flows.dagbuilder import DagBuilder
