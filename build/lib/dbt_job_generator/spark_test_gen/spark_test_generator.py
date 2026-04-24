"""Spark Test Generator — auto-generates pytest files for dbt models.

Reads MappingSpec + generated SQL + Bronze schema files to produce
a complete pytest file that validates the SQL on PySpark local.
"""

from __future__ import annotations

import json
import os
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

from dbt_job_generator.models.enums import SourceType
from dbt_job_generator.models.mapping import MappingSpec, SourceEntry


class SparkTestGenerator:
    """Generates pytest files for Spark integration testing of dbt models."""

    def generate_test(
        self, mapping: MappingSpec, sql_content: str, schema_dir: str
    ) -> str:
        """Generate a pytest file content for a single dbt model."""
        model_name = mapping.name
        class_name = _to_camel_case(model_name)
        prepared_sql = _prepare_sql(sql_content)
        expected_columns = [e.target_column for e in mapping.mappings]

        # Collect physical tables
        physical_tables = [
            inp for inp in mapping.inputs
            if inp.source_type == SourceType.PHYSICAL_TABLE
        ]

        # Build fake data + fixture lines + TODO comments
        fake_data_blocks: list[str] = []
        fixture_lines: list[str] = []
        todo_comments: list[str] = []

        for inp in physical_tables:
            table_name = inp.table_name
            schema = _load_bronze_schema(schema_dir, table_name)
            if schema is None:
                todo_comments.append(
                    f"# TODO: Missing Bronze schema file for '{table_name}'. "
                    f"Create schemas/bronze/{table_name}.json to enable fake data."
                )
                fake_data_blocks.append(
                    f"FAKE_{table_name} = []  "
                    f"# TODO: Add fake data after creating schema file"
                )
            else:
                rows = _generate_fake_rows(schema)
                fake_data_blocks.append(
                    f"FAKE_{table_name} = {_format_rows(rows)}"
                )
            fixture_lines.append(
                f'    register_fake_table(spark, "{table_name}", FAKE_{table_name})'
            )

        # Assemble the file
        parts: list[str] = []
        parts.append(_build_header(model_name))
        parts.append("")
        if todo_comments:
            for tc in todo_comments:
                parts.append(tc)
            parts.append("")
        parts.append("TEST_DATE = date(2024, 1, 1)")
        parts.append("")
        for block in fake_data_blocks:
            parts.append(block)
            parts.append("")
        parts.append(_build_sql_constant(prepared_sql))
        parts.append("")
        parts.append(_build_expected_columns(expected_columns))
        parts.append("")
        parts.append(_build_fixture(fixture_lines))
        parts.append("")
        parts.append(_build_test_class(class_name))
        parts.append("")

        return "\n".join(parts)

    def generate_test_batch(
        self,
        models: list[tuple[MappingSpec, str]],
        schema_dir: str,
    ) -> dict[str, str]:
        """Generate test files for multiple models.

        Returns {model_name: test_content}.
        """
        result: dict[str, str] = {}
        for mapping, sql_content in models:
            result[mapping.name] = self.generate_test(
                mapping, sql_content, schema_dir
            )
        return result


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _to_camel_case(name: str) -> str:
    """Convert snake_case to CamelCase: fund_management_company → FundManagementCompany."""
    return "".join(word.capitalize() for word in name.split("_"))


def _prepare_sql(sql: str) -> str:
    """Prepare SQL for Spark execution — same logic as sql_test_helper.prepare_sql."""
    # Strip config block
    sql = re.sub(r"\{\{.*?config\(.*?\).*?\}\}\s*\n?", "", sql)
    # Replace var("etl_date")
    sql = sql.replace('{{ var("etl_date") }}', "2024-01-01")
    # Remove trailing semicolon
    sql = sql.rstrip().rstrip(";")
    # Replace schema refs
    sql = re.sub(r"\bbronze\.", "", sql)
    sql = re.sub(r"\bsilver\.", "", sql)
    # Convert :: to CAST()
    pattern = re.compile(r"(\S+(?:\([^)]*\))?)\s*::\s*(\w+(?:\([^)]*\))?)")
    sql = pattern.sub(r"CAST(\1 AS \2)", sql)
    # Replace hash_id
    hash_pattern = re.compile(r"hash_id\(([^,]+),\s*([^)]+)\)")
    sql = hash_pattern.sub(
        r"sha2(concat_ws('|', \1, CAST(\2 AS STRING)), 256)", sql
    )
    return sql


def _load_bronze_schema(schema_dir: str, table_name: str) -> Optional[dict]:
    """Load a Bronze schema JSON file. Returns None if not found."""
    path = os.path.join(schema_dir, "bronze", f"{table_name}.json")
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _generate_fake_rows(schema: dict) -> list[dict[str, Any]]:
    """Generate 2 fake rows based on column types from schema."""
    columns = schema.get("columns", [])
    row1: dict[str, Any] = {}
    row2: dict[str, Any] = {}

    for col in columns:
        name = col["name"]
        dtype = col["data_type"].lower().strip()

        if name == "data_date":
            row1[name] = date(2024, 1, 1)
            row2[name] = date(2023, 12, 31)
        elif dtype == "bigint":
            row1[name] = 1
            row2[name] = 2
        elif dtype == "string":
            row1[name] = "test_value_1"
            row2[name] = "test_value_2"
        elif dtype.startswith("decimal"):
            row1[name] = Decimal("100.00")
            row2[name] = Decimal("200.00")
        elif dtype == "timestamp":
            row1[name] = datetime(2024, 1, 1)
            row2[name] = datetime(2024, 1, 2)
        elif dtype == "date":
            row1[name] = date(2024, 1, 1)
            row2[name] = date(2024, 1, 2)
        else:
            # Fallback to string
            row1[name] = "test_value_1"
            row2[name] = "test_value_2"

    return [row1, row2]


def _format_rows(rows: list[dict[str, Any]]) -> str:
    """Format a list of row dicts as a Python literal string."""
    lines = ["["]
    for i, row in enumerate(rows):
        trailing = "," if i < len(rows) - 1 else ","
        lines.append(f"    {_format_dict(row)}{trailing}")
    lines.append("]")
    return "\n".join(lines)


def _format_dict(d: dict[str, Any]) -> str:
    """Format a single dict as a Python literal."""
    parts: list[str] = []
    for k, v in d.items():
        parts.append(f'"{k}": {_format_value(v)}')
    return "{" + ", ".join(parts) + "}"


def _format_value(v: Any) -> str:
    """Format a Python value as its repr for code generation."""
    if isinstance(v, date) and not isinstance(v, datetime):
        return f"date({v.year}, {v.month}, {v.day})"
    if isinstance(v, datetime):
        return f"datetime({v.year}, {v.month}, {v.day})"
    if isinstance(v, Decimal):
        return f'Decimal("{v}")'
    if isinstance(v, str):
        return f'"{v}"'
    if v is None:
        return "None"
    return repr(v)


def _build_header(model_name: str) -> str:
    """Build the file header with imports."""
    return f'''"""Spark integration test for {model_name} dbt model.
Auto-generated by dbt Job Generator.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from tests.spark.fake_data_factory import register_fake_table
from tests.spark.sql_test_helper import prepare_sql'''


def _build_sql_constant(prepared_sql: str) -> str:
    """Build the GENERATED_SQL constant."""
    return f'GENERATED_SQL = """\\\n{prepared_sql}\n"""'


def _build_expected_columns(columns: list[str]) -> str:
    """Build the EXPECTED_COLUMNS list."""
    lines = ["EXPECTED_COLUMNS = ["]
    for i, col in enumerate(columns):
        trailing = "," if i < len(columns) - 1 else ","
        lines.append(f'    "{col}"{trailing}')
    lines.append("]")
    return "\n".join(lines)


def _build_fixture(fixture_lines: list[str]) -> str:
    """Build the pytest fixture."""
    lines = [
        "@pytest.fixture(autouse=True)",
        "def setup_source_tables(spark: SparkSession):",
    ]
    if fixture_lines:
        for fl in fixture_lines:
            lines.append(fl)
    else:
        lines.append("    pass")
    return "\n".join(lines)


def _build_test_class(class_name: str) -> str:
    """Build the test class with 3 standard test methods."""
    return f'''class Test{class_name}Spark:
    def test_sql_executes_without_error(self, spark: SparkSession):
        result = spark.sql(GENERATED_SQL)
        assert result is not None

    def test_output_has_correct_columns(self, spark: SparkSession):
        result = spark.sql(GENERATED_SQL)
        assert result.columns == EXPECTED_COLUMNS

    def test_has_rows(self, spark: SparkSession):
        result = spark.sql(GENERATED_SQL)
        assert result.count() > 0'''
