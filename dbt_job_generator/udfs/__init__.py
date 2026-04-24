"""Custom Spark UDFs for dbt Job Generator."""

from dbt_job_generator.udfs.hash_id import register_hash_id

__all__ = ["register_hash_id"]
