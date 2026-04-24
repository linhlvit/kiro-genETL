"""Tests for SparkTestGenerator — auto-generates pytest files for dbt models."""

from __future__ import annotations

import ast
import json
import os
import re

import pytest

from dbt_job_generator.models.enums import Layer, RulePattern, SourceType
from dbt_job_generator.models.mapping import (
    MappingEntry,
    MappingRule,
    MappingSpec,
    SourceEntry,
    TargetSpec,
)
from dbt_job_generator.spark_test_gen.spark_test_generator import (
    SparkTestGenerator,
    _prepare_sql,
    _to_camel_case,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_SQL = """\
{{ config(materialized='table', schema='silver', tags=['SCD4A']) }}

WITH src AS (
    SELECT id, name
    FROM bronze.MY_TABLE
    WHERE data_date = to_date('{{ var("etl_date") }}', 'yyyy-MM-dd')
)

SELECT
    hash_id('SRC', src.id) AS surrogate_key,
    src.id :: string AS code,
    src.name :: string AS name
FROM src
;
"""

SAMPLE_SCHEMA = {
    "table_name": "MY_TABLE",
    "columns": [
        {"name": "id", "data_type": "bigint", "nullable": False},
        {"name": "name", "data_type": "string", "nullable": True},
        {"name": "data_date", "data_type": "date", "nullable": False},
    ],
}


def _make_mapping(
    name: str = "test_model",
    tables: list[tuple[str, str]] | None = None,
    columns: list[tuple[str, str]] | None = None,
) -> MappingSpec:
    """Build a minimal MappingSpec for testing."""
    if tables is None:
        tables = [("MY_TABLE", "src")]
    if columns is None:
        columns = [
            ("surrogate_key", "string"),
            ("code", "string"),
            ("name", "string"),
        ]

    inputs = [
        SourceEntry(
            index=i + 1,
            source_type=SourceType.PHYSICAL_TABLE,
            schema="bronze",
            table_name=tbl,
            alias=alias,
            select_fields="id, name",
        )
        for i, (tbl, alias) in enumerate(tables)
    ]
    mappings = [
        MappingEntry(
            index=i + 1,
            target_column=col,
            transformation=f"src.{col}",
            data_type=dtype,
            description=None,
            mapping_rule=MappingRule(pattern=RulePattern.DIRECT_MAP),
        )
        for i, (col, dtype) in enumerate(columns)
    ]
    return MappingSpec(
        name=name,
        layer=Layer.BRONZE_TO_SILVER,
        target=TargetSpec(
            database="db", schema="silver",
            table_name=name, etl_handle="SCD1",
        ),
        inputs=inputs,
        mappings=mappings,
    )


def _write_schema(schema_dir: str, table_name: str, schema: dict) -> None:
    bronze_dir = os.path.join(schema_dir, "bronze")
    os.makedirs(bronze_dir, exist_ok=True)
    path = os.path.join(bronze_dir, f"{table_name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(schema, f)


# ---------------------------------------------------------------------------
# _to_camel_case
# ---------------------------------------------------------------------------

class TestToCamelCase:
    def test_simple(self):
        assert _to_camel_case("fund_management_company") == "FundManagementCompany"

    def test_single_word(self):
        assert _to_camel_case("model") == "Model"

    def test_already_single(self):
        assert _to_camel_case("test") == "Test"


# ---------------------------------------------------------------------------
# _prepare_sql
# ---------------------------------------------------------------------------

class TestPrepareSql:
    def test_strips_config_block(self):
        result = _prepare_sql(SAMPLE_SQL)
        assert "config(" not in result

    def test_replaces_etl_date_var(self):
        result = _prepare_sql(SAMPLE_SQL)
        assert '{{ var("etl_date") }}' not in result
        assert "2024-01-01" in result

    def test_removes_trailing_semicolon(self):
        result = _prepare_sql(SAMPLE_SQL)
        assert not result.rstrip().endswith(";")

    def test_removes_bronze_schema_prefix(self):
        result = _prepare_sql(SAMPLE_SQL)
        assert "bronze.MY_TABLE" not in result
        assert "MY_TABLE" in result

    def test_converts_double_colon_to_cast(self):
        result = _prepare_sql(SAMPLE_SQL)
        assert "::" not in result
        assert "CAST(" in result

    def test_replaces_hash_id(self):
        result = _prepare_sql(SAMPLE_SQL)
        assert "hash_id(" not in result
        assert "sha2(concat_ws(" in result


# ---------------------------------------------------------------------------
# SparkTestGenerator.generate_test
# ---------------------------------------------------------------------------

class TestGenerateTest:
    def test_output_is_valid_python(self, tmp_path):
        _write_schema(str(tmp_path), "MY_TABLE", SAMPLE_SCHEMA)
        gen = SparkTestGenerator()
        mapping = _make_mapping()

        result = gen.generate_test(mapping, SAMPLE_SQL, str(tmp_path))

        # Should parse as valid Python
        ast.parse(result)

    def test_contains_three_test_methods(self, tmp_path):
        _write_schema(str(tmp_path), "MY_TABLE", SAMPLE_SCHEMA)
        gen = SparkTestGenerator()
        mapping = _make_mapping()

        result = gen.generate_test(mapping, SAMPLE_SQL, str(tmp_path))

        assert "def test_sql_executes_without_error" in result
        assert "def test_output_has_correct_columns" in result
        assert "def test_has_rows" in result

    def test_contains_expected_columns(self, tmp_path):
        _write_schema(str(tmp_path), "MY_TABLE", SAMPLE_SCHEMA)
        gen = SparkTestGenerator()
        mapping = _make_mapping(
            columns=[("col_a", "string"), ("col_b", "bigint")]
        )

        result = gen.generate_test(mapping, SAMPLE_SQL, str(tmp_path))

        assert '"col_a"' in result
        assert '"col_b"' in result

    def test_contains_test_class(self, tmp_path):
        _write_schema(str(tmp_path), "MY_TABLE", SAMPLE_SCHEMA)
        gen = SparkTestGenerator()
        mapping = _make_mapping(name="fund_management_company")

        result = gen.generate_test(mapping, SAMPLE_SQL, str(tmp_path))

        assert "class TestFundManagementCompanySpark:" in result

    def test_contains_fake_data(self, tmp_path):
        _write_schema(str(tmp_path), "MY_TABLE", SAMPLE_SCHEMA)
        gen = SparkTestGenerator()
        mapping = _make_mapping()

        result = gen.generate_test(mapping, SAMPLE_SQL, str(tmp_path))

        assert "FAKE_MY_TABLE" in result
        assert "register_fake_table" in result

    def test_sql_is_prepared(self, tmp_path):
        _write_schema(str(tmp_path), "MY_TABLE", SAMPLE_SCHEMA)
        gen = SparkTestGenerator()
        mapping = _make_mapping()

        result = gen.generate_test(mapping, SAMPLE_SQL, str(tmp_path))

        assert "config(" not in result
        assert '{{ var("etl_date") }}' not in result
        assert "bronze.MY_TABLE" not in result

    def test_missing_schema_adds_todo(self, tmp_path):
        # Don't write any schema file
        gen = SparkTestGenerator()
        mapping = _make_mapping()

        result = gen.generate_test(mapping, SAMPLE_SQL, str(tmp_path))

        assert "TODO" in result
        assert "MY_TABLE" in result
        # Should still be valid Python
        ast.parse(result)

    def test_header_contains_model_name(self, tmp_path):
        _write_schema(str(tmp_path), "MY_TABLE", SAMPLE_SCHEMA)
        gen = SparkTestGenerator()
        mapping = _make_mapping(name="my_model")

        result = gen.generate_test(mapping, SAMPLE_SQL, str(tmp_path))

        assert "my_model" in result

    def test_imports_present(self, tmp_path):
        _write_schema(str(tmp_path), "MY_TABLE", SAMPLE_SCHEMA)
        gen = SparkTestGenerator()
        mapping = _make_mapping()

        result = gen.generate_test(mapping, SAMPLE_SQL, str(tmp_path))

        assert "from fake_data_factory import register_fake_table" in result
        assert "from sql_test_helper import prepare_sql" in result
        assert "import pytest" in result


# ---------------------------------------------------------------------------
# SparkTestGenerator.generate_test_batch
# ---------------------------------------------------------------------------

class TestGenerateTestBatch:
    def test_batch_returns_dict(self, tmp_path):
        _write_schema(str(tmp_path), "MY_TABLE", SAMPLE_SCHEMA)
        gen = SparkTestGenerator()
        mapping1 = _make_mapping(name="model_a")
        mapping2 = _make_mapping(name="model_b")

        result = gen.generate_test_batch(
            [(mapping1, SAMPLE_SQL), (mapping2, SAMPLE_SQL)],
            str(tmp_path),
        )

        assert "model_a" in result
        assert "model_b" in result
        assert len(result) == 2

    def test_batch_all_valid_python(self, tmp_path):
        _write_schema(str(tmp_path), "MY_TABLE", SAMPLE_SCHEMA)
        gen = SparkTestGenerator()
        mapping = _make_mapping(name="model_x")

        result = gen.generate_test_batch(
            [(mapping, SAMPLE_SQL)], str(tmp_path)
        )

        for content in result.values():
            ast.parse(content)

    def test_batch_empty_input(self, tmp_path):
        gen = SparkTestGenerator()

        result = gen.generate_test_batch([], str(tmp_path))

        assert result == {}


# ---------------------------------------------------------------------------
# copy_test_helpers
# ---------------------------------------------------------------------------

class TestCopyTestHelpers:
    def test_copies_helper_files(self, tmp_path):
        from dbt_job_generator.spark_test_gen.spark_test_generator import copy_test_helpers

        # Create fake source helpers
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        for fname in ("conftest.py", "fake_data_factory.py", "sql_test_helper.py"):
            (src_dir / fname).write_text(f"# {fname}")

        dest_dir = tmp_path / "dest"
        copied = copy_test_helpers(str(dest_dir), source_dir=str(src_dir))

        assert len(copied) == 3
        for fname in ("conftest.py", "fake_data_factory.py", "sql_test_helper.py"):
            assert (dest_dir / fname).exists()

    def test_removes_init_py(self, tmp_path):
        from dbt_job_generator.spark_test_gen.spark_test_generator import copy_test_helpers

        src_dir = tmp_path / "src"
        src_dir.mkdir()
        for fname in ("conftest.py", "fake_data_factory.py", "sql_test_helper.py"):
            (src_dir / fname).write_text(f"# {fname}")

        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()
        (dest_dir / "__init__.py").write_text("")

        copy_test_helpers(str(dest_dir), source_dir=str(src_dir))

        assert not (dest_dir / "__init__.py").exists()
