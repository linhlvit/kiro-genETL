"""Tests for Model Assembler, ConfigBlockGenerator, and FromClauseGenerator."""

import pytest

from dbt_job_generator.assembler.config_block import ConfigBlockGenerator
from dbt_job_generator.assembler.from_clause import FromClauseGenerator
from dbt_job_generator.assembler.model_assembler import ModelAssembler
from dbt_job_generator.models.enums import (
    FinalFilterType,
    JoinType,
    Layer,
    RulePattern,
    SourceType,
)
from dbt_job_generator.models.generation import CTEDefinition, SelectColumn
from dbt_job_generator.models.mapping import (
    FinalFilter,
    JoinRelationship,
    MappingEntry,
    MappingMetadata,
    MappingRule,
    MappingSpec,
    SourceEntry,
    TargetSpec,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mapping(
    layer: Layer = Layer.BRONZE_TO_SILVER,
    etl_handle: str = "SCD4A",
    inputs: list[SourceEntry] | None = None,
    relationships: list[JoinRelationship] | None = None,
    final_filter: FinalFilter | None = None,
) -> MappingSpec:
    return MappingSpec(
        name="test_model",
        layer=layer,
        target=TargetSpec(
            database="analytics",
            schema="silver",
            table_name="test_table",
            etl_handle=etl_handle,
        ),
        inputs=inputs or [],
        relationships=relationships or [],
        mappings=[],
        final_filter=final_filter,
        metadata=MappingMetadata(),
    )


# ===========================================================================
# ConfigBlockGenerator
# ===========================================================================

class TestConfigBlockGenerator:
    def test_bronze_to_silver_schema(self):
        gen = ConfigBlockGenerator()
        mapping = _make_mapping(layer=Layer.BRONZE_TO_SILVER, etl_handle="SCD4A")
        result = gen.generate(mapping)
        assert "schema='silver'" in result
        assert "tags=['SCD4A']" in result
        assert "materialized='table'" in result

    def test_silver_to_gold_schema(self):
        gen = ConfigBlockGenerator()
        mapping = _make_mapping(layer=Layer.SILVER_TO_GOLD, etl_handle="GOLD_AGG")
        result = gen.generate(mapping)
        assert "schema='gold'" in result
        assert "tags=['GOLD_AGG']" in result

    def test_template_overrides_materialization(self):
        gen = ConfigBlockGenerator()
        mapping = _make_mapping()
        result = gen.generate(mapping, template={"materialization": "incremental"})
        assert "materialized='incremental'" in result

    def test_default_materialization_is_table(self):
        gen = ConfigBlockGenerator()
        mapping = _make_mapping()
        result = gen.generate(mapping)
        assert "materialized='table'" in result

    def test_output_format(self):
        gen = ConfigBlockGenerator()
        mapping = _make_mapping(etl_handle="ETL1")
        result = gen.generate(mapping)
        assert result.startswith("{{ config(")
        assert result.endswith(") }}")


# ===========================================================================
# FromClauseGenerator
# ===========================================================================

class TestFromClauseGenerator:
    def test_no_relationships_uses_first_input(self):
        gen = FromClauseGenerator()
        mapping = _make_mapping(
            inputs=[
                SourceEntry(
                    index=1,
                    source_type=SourceType.PHYSICAL_TABLE,
                    schema="bronze",
                    table_name="my_table",
                    alias="mt",
                    select_fields="*",
                ),
            ],
        )
        result = gen.generate(mapping)
        assert result == "FROM mt"

    def test_no_relationships_no_inputs(self):
        gen = FromClauseGenerator()
        mapping = _make_mapping()
        result = gen.generate(mapping)
        assert result == "FROM unknown"

    def test_single_join(self):
        gen = FromClauseGenerator()
        mapping = _make_mapping(
            relationships=[
                JoinRelationship(
                    main_alias="fu_co",
                    join_type=JoinType.LEFT_JOIN,
                    join_alias="fu_bu",
                    join_condition="fu_co.id = fu_bu.fundid",
                ),
            ],
        )
        result = gen.generate(mapping)
        assert "FROM fu_co" in result
        assert "LEFT JOIN fu_bu ON fu_co.id = fu_bu.fundid" in result

    def test_multiple_joins(self):
        gen = FromClauseGenerator()
        mapping = _make_mapping(
            relationships=[
                JoinRelationship(
                    main_alias="main",
                    join_type=JoinType.LEFT_JOIN,
                    join_alias="t1",
                    join_condition="main.id = t1.id",
                ),
                JoinRelationship(
                    main_alias="main",
                    join_type=JoinType.INNER_JOIN,
                    join_alias="t2",
                    join_condition="main.id = t2.id",
                ),
            ],
        )
        result = gen.generate(mapping)
        lines = result.split("\n")
        assert lines[0] == "FROM main"
        assert "LEFT JOIN t1 ON main.id = t1.id" in lines[1]
        assert "INNER JOIN t2 ON main.id = t2.id" in lines[2]


# ===========================================================================
# ModelAssembler
# ===========================================================================

class TestModelAssembler:
    def test_basic_assembly_no_ctes_no_filter(self):
        assembler = ModelAssembler()
        result = assembler.assemble(
            config_block="{{ config(materialized='table', schema='silver', tags=['X']) }}",
            cte_definitions=[],
            select_columns=[
                SelectColumn(expression="src.col1 :: string", target_alias="col1"),
                SelectColumn(expression="src.col2 :: bigint", target_alias="col2"),
            ],
            from_clause="FROM src",
        )
        assert "{{ config(" in result
        assert "SELECT" in result
        assert "src.col1 :: string" in result
        assert "AS col1" in result
        assert "FROM src" in result
        assert result.strip().endswith(";")

    def test_assembly_with_ctes(self):
        assembler = ModelAssembler()
        ctes = [
            CTEDefinition(
                alias="raw_data",
                sql_body="SELECT id, name\nFROM bronze.my_table\nWHERE data_date = '2024-01-01'",
                source_type=SourceType.PHYSICAL_TABLE,
            ),
        ]
        result = assembler.assemble(
            config_block="{{ config(materialized='table', schema='silver', tags=['T']) }}",
            cte_definitions=ctes,
            select_columns=[
                SelectColumn(expression="raw_data.id :: string", target_alias="id"),
            ],
            from_clause="FROM raw_data",
        )
        assert "WITH raw_data AS (" in result
        assert "SELECT id, name" in result
        assert "FROM bronze.my_table" in result
        assert result.strip().endswith(";")

    def test_assembly_with_multiple_ctes(self):
        assembler = ModelAssembler()
        ctes = [
            CTEDefinition(
                alias="cte1",
                sql_body="SELECT a FROM t1",
                source_type=SourceType.PHYSICAL_TABLE,
            ),
            CTEDefinition(
                alias="cte2",
                sql_body="SELECT b FROM cte1",
                source_type=SourceType.DERIVED_CTE,
                depends_on=["cte1"],
            ),
        ]
        result = assembler.assemble(
            config_block="{{ config(materialized='table', schema='silver', tags=['T']) }}",
            cte_definitions=ctes,
            select_columns=[
                SelectColumn(expression="cte2.b :: string", target_alias="col_b"),
            ],
            from_clause="FROM cte2",
        )
        # Both CTEs present, separated by comma
        assert "WITH cte1 AS (" in result
        assert "cte2 AS (" in result
        # Verify ordering: cte1 before cte2
        assert result.index("cte1 AS (") < result.index("cte2 AS (")

    def test_assembly_with_where_filter(self):
        assembler = ModelAssembler()
        result = assembler.assemble(
            config_block="{{ config(materialized='table', schema='silver', tags=['T']) }}",
            cte_definitions=[],
            select_columns=[
                SelectColumn(expression="src.id :: string", target_alias="id"),
            ],
            from_clause="FROM src",
            final_filter=FinalFilter(
                filter_type=FinalFilterType.WHERE_CLAUSE,
                expression="src.active = 1",
            ),
        )
        assert "WHERE src.active = 1" in result
        assert result.strip().endswith(";")

    def test_assembly_with_union_all_filter(self):
        assembler = ModelAssembler()
        union_expr = (
            "UNION ALL\n"
            "SELECT\n"
            "    t2.id :: string AS id\n"
            "FROM t2"
        )
        result = assembler.assemble(
            config_block="{{ config(materialized='table', schema='silver', tags=['T']) }}",
            cte_definitions=[],
            select_columns=[
                SelectColumn(expression="t1.id :: string", target_alias="id"),
            ],
            from_clause="FROM t1",
            final_filter=FinalFilter(
                filter_type=FinalFilterType.UNION_ALL,
                expression=union_expr,
            ),
        )
        assert "UNION ALL" in result
        assert "FROM t1" in result
        assert "FROM t2" in result
        assert result.strip().endswith(";")

    def test_trailing_semicolon(self):
        assembler = ModelAssembler()
        result = assembler.assemble(
            config_block="{{ config(materialized='table', schema='silver', tags=['T']) }}",
            cte_definitions=[],
            select_columns=[
                SelectColumn(expression="1", target_alias="one"),
            ],
            from_clause="FROM dual",
        )
        assert result.strip().endswith(";")

    def test_select_column_alignment(self):
        assembler = ModelAssembler()
        result = assembler.assemble(
            config_block="{{ config(materialized='table', schema='silver', tags=['T']) }}",
            cte_definitions=[],
            select_columns=[
                SelectColumn(expression="short", target_alias="a"),
                SelectColumn(expression="much_longer_expression", target_alias="b"),
            ],
            from_clause="FROM src",
        )
        # Both columns should have AS
        lines = result.split("\n")
        select_lines = [l for l in lines if "AS a" in l or "AS b" in l]
        assert len(select_lines) == 2

    def test_empty_select_columns_fallback(self):
        assembler = ModelAssembler()
        result = assembler.assemble(
            config_block="{{ config(materialized='table', schema='silver', tags=['T']) }}",
            cte_definitions=[],
            select_columns=[],
            from_clause="FROM src",
        )
        assert "SELECT *" in result

    def test_end_to_end_realistic(self):
        """Realistic end-to-end test mimicking involved-party-electronic-address pattern."""
        assembler = ModelAssembler()
        config = "{{ config(materialized='table', schema='silver', tags=['SCD4A']) }}"
        ctes = [
            CTEDefinition(
                alias="th_ti_dk_th",
                sql_body=(
                    "SELECT id, email_kd, fax_kd\n"
                    "FROM bronze.THONG_TIN_DK_THUE\n"
                    "WHERE data_date = to_date('{{ var(\"etl_date\") }}', 'yyyy-MM-dd')"
                ),
                source_type=SourceType.PHYSICAL_TABLE,
            ),
            CTEDefinition(
                alias="leg_th_ti_dk_th",
                sql_body=(
                    "SELECT\n"
                    "    id AS ip_code,\n"
                    "    'EMAIL' AS type_code,\n"
                    "    email_kd AS address_value,\n"
                    "    source_system_code\n"
                    "FROM th_ti_dk_th\n"
                    "WHERE email_kd IS NOT NULL\n"
                    "UNION ALL\n"
                    "SELECT\n"
                    "    id AS ip_code,\n"
                    "    'FAX' AS type_code,\n"
                    "    fax_kd AS address_value,\n"
                    "    source_system_code\n"
                    "FROM th_ti_dk_th\n"
                    "WHERE fax_kd IS NOT NULL"
                ),
                source_type=SourceType.UNPIVOT_CTE,
                depends_on=["th_ti_dk_th"],
            ),
        ]
        columns = [
            SelectColumn(
                expression="hash_id(leg_th_ti_dk_th.source_system_code, leg_th_ti_dk_th.ip_code)",
                target_alias="involved_party_id",
            ),
            SelectColumn(
                expression="leg_th_ti_dk_th.ip_code :: string",
                target_alias="involved_party_code",
            ),
        ]
        result = assembler.assemble(
            config_block=config,
            cte_definitions=ctes,
            select_columns=columns,
            from_clause="FROM leg_th_ti_dk_th",
        )
        # Verify structure
        assert "{{ config(" in result
        assert "WITH th_ti_dk_th AS (" in result
        assert "leg_th_ti_dk_th AS (" in result
        assert "SELECT" in result
        assert "AS involved_party_id" in result
        assert "AS involved_party_code" in result
        assert "FROM leg_th_ti_dk_th" in result
        assert result.strip().endswith(";")
