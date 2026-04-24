"""Fake data factory — reads Bronze schema files and generates test data.

Schema files are the single source of truth for table structure.
Test data is generated from schema, not hardcoded in test files.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "schemas")

# Map schema file data_type strings to PySpark types
_TYPE_MAP = {
    "string": StringType(),
    "bigint": LongType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}


def _parse_spark_type(data_type: str):
    """Convert a schema file data_type string to a PySpark type."""
    dt = data_type.lower().strip()
    if dt in _TYPE_MAP:
        return _TYPE_MAP[dt]
    if dt.startswith("decimal"):
        inner = dt.replace("decimal", "").strip("()")
        parts = inner.split(",")
        p = int(parts[0].strip())
        s = int(parts[1].strip()) if len(parts) > 1 else 0
        return DecimalType(p, s)
    return StringType()


def load_bronze_schema(table_name: str) -> dict:
    """Load a Bronze schema file and return the parsed JSON."""
    path = os.path.join(SCHEMA_DIR, "bronze", f"{table_name}.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_spark_schema(schema_json: dict) -> StructType:
    """Build a PySpark StructType from a schema JSON dict."""
    fields = []
    for col in schema_json["columns"]:
        spark_type = _parse_spark_type(col["data_type"])
        nullable = col.get("nullable", True)
        fields.append(StructField(col["name"], spark_type, nullable))
    return StructType(fields)


def _to_sql_literal(value: Any) -> str:
    """Convert a Python value to a SQL literal string."""
    if value is None:
        return "NULL"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    return f"'{value}'"


def register_fake_table(
    spark: SparkSession,
    table_name: str,
    rows: list[dict[str, Any]],
) -> None:
    """Register fake data as a temp view using pure SQL (no Python worker).

    Instead of createDataFrame (which needs Python worker serialization),
    we build a UNION ALL of SELECT literals — runs entirely in JVM.
    """
    if not rows:
        schema_json = load_bronze_schema(table_name)
        spark_schema = build_spark_schema(schema_json)
        df = spark.createDataFrame([], schema=spark_schema)
        df.createOrReplaceTempView(table_name)
        return

    columns = list(rows[0].keys())
    selects = []
    for row in rows:
        literals = []
        for col in columns:
            literals.append(f"{_to_sql_literal(row[col])} AS {col}")
        selects.append("SELECT " + ", ".join(literals))

    union_sql = " UNION ALL ".join(selects)
    df = spark.sql(union_sql)
    df.createOrReplaceTempView(table_name)
