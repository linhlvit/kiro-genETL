"""DAG module for dbt Job Generator."""

from dbt_job_generator.dag.dag_builder import DependencyDAGBuilder

__all__ = ["DependencyDAGBuilder"]
