"""Helper utilities for running generated dbt SQL on Spark local."""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def strip_dbt_directives(sql: str) -> str:
    """Remove dbt-specific directives from SQL so it can run on plain Spark.

    - Removes {{ config(...) }} block
    - Replaces {{ var("etl_date") }} with a fixed test date
    """
    # Remove config block line
    sql = re.sub(r"\{\{.*?config\(.*?\).*?\}\}\s*\n?", "", sql)
    # Replace var("etl_date") with test date
    sql = sql.replace('{{ var("etl_date") }}', "2024-01-01")
    # Remove trailing semicolon (Spark SQL doesn't need it)
    sql = sql.rstrip().rstrip(";")
    return sql


def replace_schema_refs(sql: str, schema: str = "bronze") -> str:
    """Replace schema.TABLE references with just TABLE (temp view names).

    E.g., bronze.FUNDCOMPANY → FUNDCOMPANY
    """
    return re.sub(rf"\b{schema}\.", "", sql)


def convert_double_colon_cast(sql: str) -> str:
    """Convert PostgreSQL-style :: casting to Spark SQL CAST().

    E.g., ``fu_co.id :: string`` → ``CAST(fu_co.id AS string)``
          ``NULL :: date``       → ``CAST(NULL AS date)``
          ``'VALUE' :: string``  → ``CAST('VALUE' AS string)``
    """
    # Pattern: <expression> :: <type>
    # expression can be: alias.col, NULL, 'string', number, or complex expr
    # type can be: string, bigint, date, timestamp, decimal(23,2), etc.
    pattern = re.compile(
        r"(\S+(?:\([^)]*\))?)\s*::\s*(\w+(?:\([^)]*\))?)",
    )
    return pattern.sub(r"CAST(\1 AS \2)", sql)


def replace_hash_id(sql: str) -> str:
    """Replace hash_id(a, b) calls with sha2(concat_ws('|', a, CAST(b AS STRING)), 256).

    This makes the SQL runnable on Spark without needing a custom UDF.
    """
    import re
    pattern = re.compile(r"hash_id\(([^,]+),\s*([^)]+)\)")
    return pattern.sub(r"sha2(concat_ws('|', \1, CAST(\2 AS STRING)), 256)", sql)


def replace_array_agg(sql: str) -> str:
    """Replace array_agg(col) with concat_ws(',', collect_list(CAST(col AS string))).

    This avoids Python worker crashes on Windows when Spark tries to
    serialize array types through the Python UDF pathway.
    """
    pattern = re.compile(r'\barray_agg\((\w+)\)')
    return pattern.sub(r"concat_ws(',', collect_list(CAST(\1 AS string)))", sql)


def prepare_sql(sql: str) -> str:
    """Full preparation: strip dbt directives + replace schema refs + convert casts + replace hash_id + fix array_agg."""
    sql = strip_dbt_directives(sql)
    sql = replace_schema_refs(sql, "bronze")
    sql = replace_schema_refs(sql, "silver")
    sql = convert_double_colon_cast(sql)
    sql = replace_hash_id(sql)
    sql = replace_array_agg(sql)
    return sql
