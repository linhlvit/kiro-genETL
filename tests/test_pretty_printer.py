"""Tests for PrettyPrinter — round-trip property with CSVParser."""

import os
import tempfile

import pytest

from dbt_job_generator.models.enums import (
    FinalFilterType,
    JoinType,
    Layer,
    RulePattern,
    SourceType,
)
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
from dbt_job_generator.parser.csv_parser import CSVParser
from dbt_job_generator.parser.pretty_printer import PrettyPrinter


@pytest.fixture
def printer():
    return PrettyPrinter()


@pytest.fixture
def parser():
    return CSVParser()


def _round_trip(spec: MappingSpec, printer: PrettyPrinter, parser: CSVParser) -> MappingSpec:
    """Pretty-print a MappingSpec to CSV, write to a temp file, parse it back."""
    csv_text = printer.print(spec)
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8",
    )
    f.write(csv_text)
    f.close()
    try:
        return parser.parse(f.name)
    finally:
        os.unlink(f.name)


def _specs_equal(a: MappingSpec, b: MappingSpec) -> bool:
    """Compare two MappingSpecs ignoring metadata (source_file differs)."""
    return (
        a.name == b.name
        and a.layer == b.layer
        and a.target == b.target
        and a.inputs == b.inputs
        and a.relationships == b.relationships
        and a.mappings == b.mappings
        and a.final_filter == b.final_filter
    )


class TestPrettyPrinterRoundTrip:
    """Verify parse(pretty_print(spec)) == spec for various MappingSpecs."""

    def test_full_spec_round_trip(self, printer, parser):
        """Round-trip a MappingSpec with all sections populated."""
        spec = MappingSpec(
            name="involved_party_electronic_address",
            layer=Layer.BRONZE_TO_SILVER,
            target=TargetSpec(
                database="",
                schema="silver",
                table_name="Involved Party Electronic Address",
                etl_handle="SCD4A",
                description="Description text",
            ),
            inputs=[
                SourceEntry(
                    index=1,
                    source_type=SourceType.PHYSICAL_TABLE,
                    schema="bronze",
                    table_name="TABLE_NAME",
                    alias="alias",
                    select_fields="field1, field2",
                    filter="filter_expr",
                ),
                SourceEntry(
                    index=2,
                    source_type=SourceType.UNPIVOT_CTE,
                    schema=None,
                    table_name="alias",
                    alias="new_alias",
                    select_fields="ip_code=id | EMAIL:email_kd | FAX:fax",
                    filter="address_value IS NOT NULL",
                ),
                SourceEntry(
                    index=3,
                    source_type=SourceType.DERIVED_CTE,
                    schema=None,
                    table_name="alias",
                    alias="agg_alias",
                    select_fields="fundid, array_agg(buid) AS codes",
                    filter="GROUP BY fundid",
                ),
            ],
            relationships=[
                JoinRelationship(
                    main_alias="alias",
                    join_type=JoinType.LEFT_JOIN,
                    join_alias="agg_alias",
                    join_condition="alias.id = agg_alias.fundid",
                ),
            ],
            mappings=[
                MappingEntry(
                    index=1,
                    target_column="target_col",
                    transformation="alias.source_col",
                    data_type="string",
                    description=None,
                    mapping_rule=MappingRule(pattern=RulePattern.DIRECT_MAP),
                ),
                MappingEntry(
                    index=2,
                    target_column="hash_col",
                    transformation="hash_id('SRC', alias.id)",
                    data_type="string",
                    description=None,
                    mapping_rule=MappingRule(
                        pattern=RulePattern.HASH,
                        params={"args": "'SRC', alias.id"},
                    ),
                ),
                MappingEntry(
                    index=3,
                    target_column="hardcode_col",
                    transformation="'HARDCODED_VALUE'",
                    data_type="string",
                    description=None,
                    mapping_rule=MappingRule(
                        pattern=RulePattern.HARDCODE_STRING,
                        params={"value": "HARDCODED_VALUE"},
                    ),
                ),
                MappingEntry(
                    index=4,
                    target_column="null_col",
                    transformation=None,
                    data_type="date",
                    description=None,
                    mapping_rule=MappingRule(pattern=RulePattern.NULL_MAP),
                ),
                MappingEntry(
                    index=5,
                    target_column="num_col",
                    transformation="42",
                    data_type="bigint",
                    description=None,
                    mapping_rule=MappingRule(
                        pattern=RulePattern.HARDCODE_NUMERIC,
                        params={"value": "42"},
                    ),
                ),
                MappingEntry(
                    index=6,
                    target_column="logic_col",
                    transformation="CASE WHEN x THEN y END",
                    data_type="string",
                    description=None,
                    mapping_rule=MappingRule(pattern=RulePattern.BUSINESS_LOGIC),
                ),
            ],
            final_filter=FinalFilter(
                filter_type=FinalFilterType.UNION_ALL,
                expression="SELECT * FROM cte1 UNION ALL SELECT * FROM cte2",
            ),
            metadata=MappingMetadata(),
        )

        result = _round_trip(spec, printer, parser)
        assert _specs_equal(spec, result)

    def test_minimal_spec_round_trip(self, printer, parser):
        """Round-trip a minimal MappingSpec (no relationships, no final filter)."""
        spec = MappingSpec(
            name="simple_table",
            layer=Layer.BRONZE_TO_SILVER,
            target=TargetSpec(
                database="",
                schema="silver",
                table_name="Simple Table",
                etl_handle="ETL1",
                description=None,
            ),
            inputs=[
                SourceEntry(
                    index=1,
                    source_type=SourceType.PHYSICAL_TABLE,
                    schema="bronze",
                    table_name="SRC",
                    alias="src",
                    select_fields="col1",
                    filter=None,
                ),
            ],
            relationships=[],
            mappings=[
                MappingEntry(
                    index=1,
                    target_column="col1",
                    transformation="src.col1",
                    data_type="string",
                    description=None,
                    mapping_rule=MappingRule(pattern=RulePattern.DIRECT_MAP),
                ),
            ],
            final_filter=None,
            metadata=MappingMetadata(),
        )

        result = _round_trip(spec, printer, parser)
        assert _specs_equal(spec, result)

    def test_gold_layer_round_trip(self, printer, parser):
        """Round-trip a Gold layer MappingSpec."""
        spec = MappingSpec(
            name="my_gold_table",
            layer=Layer.SILVER_TO_GOLD,
            target=TargetSpec(
                database="",
                schema="gold",
                table_name="My Gold Table",
                etl_handle="HANDLE",
                description=None,
            ),
            inputs=[
                SourceEntry(
                    index=1,
                    source_type=SourceType.PHYSICAL_TABLE,
                    schema="silver",
                    table_name="SRC_TABLE",
                    alias="src",
                    select_fields="col1, col2",
                    filter=None,
                ),
            ],
            relationships=[],
            mappings=[
                MappingEntry(
                    index=1,
                    target_column="col1",
                    transformation="src.col1",
                    data_type="string",
                    description=None,
                    mapping_rule=MappingRule(pattern=RulePattern.DIRECT_MAP),
                ),
            ],
            final_filter=None,
            metadata=MappingMetadata(),
        )

        result = _round_trip(spec, printer, parser)
        assert _specs_equal(spec, result)

    def test_where_final_filter_round_trip(self, printer, parser):
        """Round-trip with a WHERE final filter."""
        spec = MappingSpec(
            name="filtered_table",
            layer=Layer.BRONZE_TO_SILVER,
            target=TargetSpec(
                database="",
                schema="silver",
                table_name="Filtered Table",
                etl_handle="ETL2",
                description=None,
            ),
            inputs=[
                SourceEntry(
                    index=1,
                    source_type=SourceType.PHYSICAL_TABLE,
                    schema="bronze",
                    table_name="SRC",
                    alias="src",
                    select_fields="col1",
                    filter=None,
                ),
            ],
            relationships=[],
            mappings=[
                MappingEntry(
                    index=1,
                    target_column="col1",
                    transformation="src.col1",
                    data_type="string",
                    description=None,
                    mapping_rule=MappingRule(pattern=RulePattern.DIRECT_MAP),
                ),
            ],
            final_filter=FinalFilter(
                filter_type=FinalFilterType.WHERE_CLAUSE,
                expression="status = 'ACTIVE'",
            ),
            metadata=MappingMetadata(),
        )

        result = _round_trip(spec, printer, parser)
        assert _specs_equal(spec, result)

    def test_commas_in_transformation_round_trip(self, printer, parser):
        """Round-trip with commas in transformation and select_fields (CSV quoting)."""
        spec = MappingSpec(
            name="comma_test",
            layer=Layer.BRONZE_TO_SILVER,
            target=TargetSpec(
                database="",
                schema="silver",
                table_name="Comma Test",
                etl_handle="ETL3",
                description=None,
            ),
            inputs=[
                SourceEntry(
                    index=1,
                    source_type=SourceType.PHYSICAL_TABLE,
                    schema="bronze",
                    table_name="SRC",
                    alias="src",
                    select_fields="a, b, c",
                    filter=None,
                ),
            ],
            relationships=[],
            mappings=[
                MappingEntry(
                    index=1,
                    target_column="computed",
                    transformation="CASE WHEN a > 0 THEN b ELSE c END",
                    data_type="string",
                    description="Coalesce multiple cols",
                    mapping_rule=MappingRule(pattern=RulePattern.BUSINESS_LOGIC),
                ),
            ],
            final_filter=None,
            metadata=MappingMetadata(),
        )

        result = _round_trip(spec, printer, parser)
        assert _specs_equal(spec, result)

    def test_multiple_relationships_round_trip(self, printer, parser):
        """Round-trip with multiple JOIN relationships."""
        spec = MappingSpec(
            name="multi_join",
            layer=Layer.BRONZE_TO_SILVER,
            target=TargetSpec(
                database="",
                schema="silver",
                table_name="Multi Join",
                etl_handle="ETL4",
                description=None,
            ),
            inputs=[
                SourceEntry(
                    index=1,
                    source_type=SourceType.PHYSICAL_TABLE,
                    schema="bronze",
                    table_name="MAIN",
                    alias="main",
                    select_fields="id, name",
                    filter=None,
                ),
                SourceEntry(
                    index=2,
                    source_type=SourceType.PHYSICAL_TABLE,
                    schema="bronze",
                    table_name="LOOKUP1",
                    alias="lk1",
                    select_fields="id, val1",
                    filter=None,
                ),
                SourceEntry(
                    index=3,
                    source_type=SourceType.PHYSICAL_TABLE,
                    schema="bronze",
                    table_name="LOOKUP2",
                    alias="lk2",
                    select_fields="id, val2",
                    filter=None,
                ),
            ],
            relationships=[
                JoinRelationship(
                    main_alias="main",
                    join_type=JoinType.LEFT_JOIN,
                    join_alias="lk1",
                    join_condition="main.id = lk1.id",
                ),
                JoinRelationship(
                    main_alias="main",
                    join_type=JoinType.INNER_JOIN,
                    join_alias="lk2",
                    join_condition="main.id = lk2.id",
                ),
            ],
            mappings=[
                MappingEntry(
                    index=1,
                    target_column="name",
                    transformation="main.name",
                    data_type="string",
                    description=None,
                    mapping_rule=MappingRule(pattern=RulePattern.DIRECT_MAP),
                ),
            ],
            final_filter=None,
            metadata=MappingMetadata(),
        )

        result = _round_trip(spec, printer, parser)
        assert _specs_equal(spec, result)
