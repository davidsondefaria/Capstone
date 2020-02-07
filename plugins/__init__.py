from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin
import operators
import queries

class CapstonePlugin(AirflwPlugin):
    name = "capstone_plugin"
    operators = [
        operators.S3ToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    queries = [
        queries.SqlQueries
    ]