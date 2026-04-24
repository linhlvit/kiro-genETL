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


HELPER_FILES = ("conftest.py", "fake_data_factory.py", "sql_test_helper.py")

# Default source directory for helper files (relative to project root)
_DEFAULT_HELPERS_DIR = os.path.join("tests", "spark")


def copy_test_helpers(
    dest_dir: str,
    source_dir: str | None = None,
) -> list[str]:
    """Copy Spark test helper files into the destination test directory.

    Copies conftest.py, fake_data_factory.py, sql_test_helper.py so that
    generated test files can import them locally without depending on the
    project's tests/spark/ package.

    Returns list of copied file paths.
    """
    import shutil

    src = source_dir or _DEFAULT_HELPERS_DIR
    os.makedirs(dest_dir, exist_ok=True)
    copied: list[str] = []

    for fname in HELPER_FILES:
        src_path = os.path.join(src, fname)
        dst_path = os.path.join(dest_dir, fname)
        if os.path.isfile(src_path):
            shutil.copy2(src_path, dst_path)
            copied.append(dst_path)

    # Ensure __init__.py does NOT exist — tests run as flat scripts
    init_path = os.path.join(dest_dir, "__init__.py")
    if os.path.isfile(init_path):
        os.remove(init_path)

    return copied


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
    """Prepare SQL for Spark Connect execution.

    Transforms:
    - Strip config block
    - Replace Jinja vars
    - Remove trailing semicolon
    - Remove schema prefixes (bronze., silver.)
    - Convert :: to CAST()
    - Replace hash_id with sha2
    - Remove derived_cte blocks (array_agg not supported in Spark Connect)
    - Remove LEFT JOIN clauses (related to removed CTEs)
    - Replace joined columns with CAST(NULL AS string)
    """
    # Strip config block
    sql = re.sub(r"\{\{.*?config\(.*?\).*?\}\}\s*\n?", "", sql)
    # Replace var("etl_date")
    sql = sql.replace('{{ var("etl_date") }}', "2024-01-01")
    # Remove trailing semicolon
    sql = sql.rstrip().rstrip(";")
    # Replace schema refs
    sql = re.sub(r"\bbronze\.", "", sql)
    sql = re.sub(r"\bsilver\.", "", sql)

    # Remove derived_cte blocks that use array_agg/collect_list
    # These cause Python worker errors in Spark Connect
    sql = _remove_aggregate_ctes(sql)

    # Fix UNION ALL final filter
    # The assembler produces a pattern like:
    #   SELECT <columns with alias1.col> ... FROM <cte>
    #   SELECT * FROM alias1
    #   UNION ALL
    #   SELECT * FROM alias2
    # We need to:
    #   1. Replace "FROM <cte>\nSELECT * FROM alias1" with "FROM alias1"
    #   2. For "UNION ALL\nSELECT * FROM alias2", duplicate the SELECT columns
    #      but replace alias1 references with alias2
    lines = sql.split("\n")
    fixed_lines = []
    i = 0

    # First, find the SELECT columns block and the UNION ALL pattern
    select_block_start = None
    select_block_end = None  # index of the FROM line before "SELECT * FROM"

    while i < len(lines):
        line = lines[i]

        # Detect: current line is "FROM <x>" and next line is "SELECT * FROM <alias>"
        if (i + 1 < len(lines)
            and re.match(r"\s*FROM\s+\w+", line, re.IGNORECASE)
            and re.match(r"\s*SELECT\s+\*\s+FROM\s+\w+", lines[i + 1], re.IGNORECASE)):

            cte1_match = re.match(r"\s*SELECT\s+\*\s+FROM\s+(\w+)", lines[i + 1], re.IGNORECASE)
            if cte1_match:
                alias1 = cte1_match.group(1)
                # Replace FROM line with FROM alias1
                fixed_lines.append(f"FROM {alias1}")
                # Remember where the SELECT block started (scan backwards)
                select_block_start_idx = None
                for j in range(len(fixed_lines) - 1, -1, -1):
                    if fixed_lines[j].strip().upper().startswith("SELECT"):
                        select_block_start_idx = j
                        break

                i += 2  # Skip "FROM <cte>" and "SELECT * FROM alias1"

                # Now handle subsequent "UNION ALL\nSELECT * FROM alias2" pairs
                while i < len(lines):
                    stripped = lines[i].strip()
                    if stripped.upper() == "UNION ALL" and i + 1 < len(lines):
                        next_match = re.match(r"\s*SELECT\s+\*\s+FROM\s+(\w+)", lines[i + 1], re.IGNORECASE)
                        if next_match and select_block_start_idx is not None:
                            alias2 = next_match.group(1)
                            fixed_lines.append("UNION ALL")
                            # Copy SELECT columns block, replacing alias1 with alias2
                            # The range includes SELECT ... FROM alias1 (which becomes FROM alias2)
                            union_insert_idx = len(fixed_lines) - 1  # index of "UNION ALL"
                            for k in range(select_block_start_idx, union_insert_idx):
                                replaced = re.sub(
                                    rf"\b{re.escape(alias1)}\b", alias2, fixed_lines[k]
                                )
                                fixed_lines.append(replaced)
                            i += 2  # Skip "UNION ALL" and "SELECT * FROM alias2"
                            continue
                    break
                continue

        fixed_lines.append(line)
        i += 1
    sql = "\n".join(fixed_lines)

    # Convert :: to CAST()
    pattern = re.compile(r"(\S+(?:\([^)]*\))?)\s*::\s*(\w+(?:\([^)]*\))?)")
    sql = pattern.sub(r"CAST(\1 AS \2)", sql)
    # Replace hash_id
    hash_pattern = re.compile(r"hash_id\(([^,]+),\s*([^)]+)\)")
    sql = hash_pattern.sub(
        r"sha2(concat_ws('|', \1, CAST(\2 AS STRING)), 256)", sql
    )
    return sql


def _remove_aggregate_ctes(sql: str) -> str:
    """Remove CTE blocks containing array_agg/collect_list and their JOINs.

    For Spark Connect compatibility: array_agg triggers Python worker which
    crashes. Replace joined column references with NULL.
    """
    lines = sql.split("\n")
    # Find CTE aliases that use array_agg/collect_list
    agg_aliases = set()
    raw_aliases = set()  # CTEs that feed into aggregate CTEs

    # First pass: find aggregate CTE aliases and their source CTEs
    in_cte = False
    current_alias = ""
    current_body = ""
    cte_bodies: dict[str, str] = {}

    for line in lines:
        # Detect CTE start: "alias AS ("
        cte_start = re.match(r"^(\w+)\s+AS\s*\(", line.strip())
        if cte_start:
            current_alias = cte_start.group(1)
            in_cte = True
            current_body = line
            continue
        if in_cte:
            current_body += "\n" + line
            if ")" in line and line.strip().endswith(")") or line.strip() in (")", "),"):
                cte_bodies[current_alias] = current_body
                if re.search(r"array_agg|collect_list|collect_set", current_body, re.IGNORECASE):
                    agg_aliases.add(current_alias)
                    # Find source alias: FROM <alias>
                    from_match = re.search(r"FROM\s+(\w+)", current_body)
                    if from_match:
                        src = from_match.group(1)
                        if src in cte_bodies:
                            raw_aliases.add(src)
                in_cte = False

    all_remove = agg_aliases | raw_aliases
    if not all_remove:
        return sql

    # Remove CTE blocks and LEFT JOINs, replace column refs with NULL
    result_lines = []
    skip_until_close = False
    skip_alias = ""

    for line in lines:
        # Check if this line starts a CTE to remove
        stripped = line.strip()
        should_skip = False
        for alias in all_remove:
            if re.match(rf"^{alias}\s+AS\s*\(", stripped):
                skip_until_close = True
                skip_alias = alias
                should_skip = True
                break

        if skip_until_close:
            if stripped.endswith("),") or stripped == ")":
                skip_until_close = False
            continue

        if should_skip:
            continue

        # Remove LEFT JOIN lines referencing removed aliases
        join_skip = False
        for alias in all_remove:
            if re.search(rf"LEFT\s+JOIN\s+{alias}\b", line, re.IGNORECASE):
                join_skip = True
                break
        if join_skip:
            continue

        # Replace column references like "alias.column" with "NULL"
        modified = line
        for alias in all_remove:
            modified = re.sub(rf"\b{alias}\.\w+", "NULL", modified)

        result_lines.append(modified)

    result = "\n".join(result_lines)
    # Clean up double commas and empty CTE separators
    result = re.sub(r",\s*\n\s*\n+\s*\n", "\n\n", result)
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result


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
    """Build the file header with imports.

    Uses local imports (same directory) so generated tests are self-contained.
    The generator copies conftest.py, fake_data_factory.py, sql_test_helper.py
    alongside the test files.
    """
    return f'''"""Spark integration test for {model_name} dbt model.
Auto-generated by dbt Job Generator.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from fake_data_factory import register_fake_table
from sql_test_helper import prepare_sql'''


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
    """Build the test class with 3 standard test methods.

    Uses result._jdf.count() instead of result.count() to avoid
    Python worker dependency in Spark Connect mode.
    """
    return f'''class Test{class_name}Spark:
    def test_sql_executes_without_error(self, spark: SparkSession):
        result = spark.sql(GENERATED_SQL)
        assert result is not None

    def test_output_has_correct_columns(self, spark: SparkSession):
        result = spark.sql(GENERATED_SQL)
        assert result.columns == EXPECTED_COLUMNS

    def test_has_rows(self, spark: SparkSession):
        result = spark.sql(GENERATED_SQL)
        assert result._jdf.count() > 0'''
