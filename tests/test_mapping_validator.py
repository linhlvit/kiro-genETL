"""Unit tests for MappingValidator."""

from __future__ import annotations

import pytest

from dbt_job_generator.models.dag import DAGNode, DependencyDAG
from dbt_job_generator.models.enums import Layer, SourceType
from dbt_job_generator.models.mapping import (
    MappingMetadata,
    MappingSpec,
    SourceEntry,
    TargetSpec,
)
from dbt_job_generator.models.validation import (
    ModelInfo,
    ProjectContext,
    SourceInfo,
)
from dbt_job_generator.validator import MappingValidator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _target() -> TargetSpec:
    return TargetSpec(
        database="db", schema="silver", table_name="tbl",
        etl_handle="etl", description=None,
    )


def _mapping(
    name: str = "test_model",
    layer: Layer = Layer.BRONZE_TO_SILVER,
    inputs: list[SourceEntry] | None = None,
) -> MappingSpec:
    from dbt_job_generator.models.enums import RulePattern
    from dbt_job_generator.models.mapping import MappingEntry, MappingRule
    return MappingSpec(
        name=name,
        layer=layer,
        target=_target(),
        inputs=inputs or [],
        mappings=[
            MappingEntry(
                index=1, target_column="col1", transformation="src.col1",
                data_type="string", description=None,
                mapping_rule=MappingRule(pattern=RulePattern.DIRECT_MAP),
            ),
        ],
    )


def _physical(
    index: int, table_name: str, alias: str, schema: str = "bronze",
) -> SourceEntry:
    return SourceEntry(
        index=index, source_type=SourceType.PHYSICAL_TABLE,
        schema=schema, table_name=table_name, alias=alias,
        select_fields="col1, col2",
    )


def _unpivot(index: int, table_name: str, alias: str) -> SourceEntry:
    return SourceEntry(
        index=index, source_type=SourceType.UNPIVOT_CTE,
        schema=None, table_name=table_name, alias=alias,
        select_fields="key=id | EMAIL:email",
    )


def _derived(index: int, table_name: str, alias: str) -> SourceEntry:
    return SourceEntry(
        index=index, source_type=SourceType.DERIVED_CTE,
        schema=None, table_name=table_name, alias=alias,
        select_fields="id, count(*) as cnt",
        filter="GROUP BY id",
    )


# ---------------------------------------------------------------------------
# Tests — source table existence
# ---------------------------------------------------------------------------

class TestSourceTableValidation:
    """Tests for source table existence checks."""

    def test_bronze_to_silver_all_sources_present(self):
        validator = MappingValidator()
        mapping = _mapping(
            layer=Layer.BRONZE_TO_SILVER,
            inputs=[_physical(1, "TABLE_A", "a"), _physical(2, "TABLE_B", "b")],
        )
        ctx = ProjectContext(
            source_declarations={
                "TABLE_A": SourceInfo(name="TABLE_A", schema="bronze"),
                "TABLE_B": SourceInfo(name="TABLE_B", schema="bronze"),
            },
        )
        result = validator.validate(mapping, ctx)
        assert result.is_valid is True
        assert result.missing_sources == []

    def test_bronze_to_silver_missing_source(self):
        validator = MappingValidator()
        mapping = _mapping(
            layer=Layer.BRONZE_TO_SILVER,
            inputs=[_physical(1, "TABLE_A", "a"), _physical(2, "TABLE_B", "b")],
        )
        ctx = ProjectContext(
            source_declarations={
                "TABLE_A": SourceInfo(name="TABLE_A", schema="bronze"),
            },
        )
        result = validator.validate(mapping, ctx)
        # Missing sources are now WARNINGs — is_valid stays True
        assert result.is_valid is True
        assert result.has_warnings is True
        assert len(result.missing_sources) == 1
        assert result.missing_sources[0].table_name == "TABLE_B"
        assert result.missing_sources[0].required_layer == Layer.BRONZE_TO_SILVER
        assert any(w.warning_type == "MISSING_SOURCE" for w in result.warnings)

    def test_silver_to_gold_source_in_existing_models(self):
        validator = MappingValidator()
        mapping = _mapping(
            layer=Layer.SILVER_TO_GOLD,
            inputs=[_physical(1, "silver_model", "sm", schema="silver")],
        )
        dag = DependencyDAG()
        dag.add_node(DAGNode(model_name="silver_model", layer=Layer.BRONZE_TO_SILVER))
        ctx = ProjectContext(
            existing_models={
                "silver_model": ModelInfo(
                    name="silver_model", layer=Layer.BRONZE_TO_SILVER,
                ),
            },
            dependency_dag=dag,
        )
        result = validator.validate(mapping, ctx)
        assert result.is_valid is True
        assert result.missing_sources == []

    def test_silver_to_gold_source_in_source_declarations(self):
        validator = MappingValidator()
        mapping = _mapping(
            layer=Layer.SILVER_TO_GOLD,
            inputs=[_physical(1, "ext_source", "es", schema="silver")],
        )
        ctx = ProjectContext(
            source_declarations={
                "ext_source": SourceInfo(name="ext_source", schema="silver"),
            },
        )
        result = validator.validate(mapping, ctx)
        assert result.is_valid is True

    def test_silver_to_gold_missing_source(self):
        validator = MappingValidator()
        mapping = _mapping(
            layer=Layer.SILVER_TO_GOLD,
            inputs=[_physical(1, "missing_model", "mm", schema="silver")],
        )
        ctx = ProjectContext()
        result = validator.validate(mapping, ctx)
        # Missing sources are now WARNINGs — is_valid stays True
        assert result.is_valid is True
        assert result.has_warnings is True
        assert len(result.missing_sources) == 1
        assert result.missing_sources[0].table_name == "missing_model"
        assert result.missing_sources[0].required_layer == Layer.SILVER_TO_GOLD

    def test_multiple_missing_sources_aggregated(self):
        validator = MappingValidator()
        mapping = _mapping(
            layer=Layer.BRONZE_TO_SILVER,
            inputs=[
                _physical(1, "A", "a"),
                _physical(2, "B", "b"),
                _physical(3, "C", "c"),
            ],
        )
        ctx = ProjectContext()  # no sources declared
        result = validator.validate(mapping, ctx)
        # Missing sources are WARNINGs — is_valid stays True
        assert result.is_valid is True
        assert result.has_warnings is True
        assert len(result.missing_sources) == 3


# ---------------------------------------------------------------------------
# Tests — prerequisite jobs
# ---------------------------------------------------------------------------

class TestPrerequisiteJobValidation:
    """Tests for prerequisite job checks (SILVER_TO_GOLD only)."""

    def test_prerequisite_present_in_dag(self):
        validator = MappingValidator()
        dag = DependencyDAG()
        dag.add_node(DAGNode(model_name="silver_model", layer=Layer.BRONZE_TO_SILVER))
        mapping = _mapping(
            name="gold_model",
            layer=Layer.SILVER_TO_GOLD,
            inputs=[_physical(1, "silver_model", "sm", schema="silver")],
        )
        ctx = ProjectContext(
            existing_models={
                "silver_model": ModelInfo(
                    name="silver_model", layer=Layer.BRONZE_TO_SILVER,
                ),
            },
            dependency_dag=dag,
        )
        result = validator.validate(mapping, ctx)
        assert result.is_valid is True
        assert result.missing_prerequisites == []

    def test_prerequisite_missing_from_dag(self):
        validator = MappingValidator()
        mapping = _mapping(
            name="gold_model",
            layer=Layer.SILVER_TO_GOLD,
            inputs=[_physical(1, "silver_model", "sm", schema="silver")],
        )
        ctx = ProjectContext(
            existing_models={
                "silver_model": ModelInfo(
                    name="silver_model", layer=Layer.BRONZE_TO_SILVER,
                ),
            },
            dependency_dag=DependencyDAG(),  # empty DAG
        )
        result = validator.validate(mapping, ctx)
        # Missing prerequisites are now WARNINGs — is_valid stays True
        assert result.is_valid is True
        assert result.has_warnings is True
        assert len(result.missing_prerequisites) == 1
        assert result.missing_prerequisites[0].job_name == "silver_model"
        assert result.missing_prerequisites[0].required_by == "gold_model"

    def test_no_prerequisite_check_for_bronze_to_silver(self):
        """Bronze→Silver mappings should not check prerequisites."""
        validator = MappingValidator()
        mapping = _mapping(
            layer=Layer.BRONZE_TO_SILVER,
            inputs=[_physical(1, "TABLE_A", "a")],
        )
        ctx = ProjectContext(
            source_declarations={
                "TABLE_A": SourceInfo(name="TABLE_A", schema="bronze"),
            },
        )
        result = validator.validate(mapping, ctx)
        assert result.is_valid is True
        assert result.missing_prerequisites == []


# ---------------------------------------------------------------------------
# Tests — CTE pipeline validation
# ---------------------------------------------------------------------------

class TestCTEPipelineValidation:
    """Tests for CTE pipeline dependency validation."""

    def test_valid_cte_pipeline(self):
        validator = MappingValidator()
        inputs = [
            _physical(1, "TABLE_A", "a"),
            _unpivot(2, "a", "unpivot_a"),
            _derived(3, "a", "derived_a"),
        ]
        errors = validator.validate_cte_pipeline(inputs)
        assert errors == []

    def test_unpivot_references_unknown_alias(self):
        validator = MappingValidator()
        inputs = [
            _physical(1, "TABLE_A", "a"),
            _unpivot(2, "nonexistent", "unpivot_a"),
        ]
        errors = validator.validate_cte_pipeline(inputs)
        assert len(errors) == 1
        assert errors[0].alias == "unpivot_a"
        assert errors[0].referenced_alias == "nonexistent"

    def test_derived_references_unknown_alias(self):
        validator = MappingValidator()
        inputs = [
            _physical(1, "TABLE_A", "a"),
            _derived(2, "unknown", "derived_a"),
        ]
        errors = validator.validate_cte_pipeline(inputs)
        assert len(errors) == 1
        assert errors[0].alias == "derived_a"
        assert errors[0].referenced_alias == "unknown"

    def test_cte_references_later_alias_is_invalid(self):
        """A CTE cannot reference an alias declared after it."""
        validator = MappingValidator()
        inputs = [
            _unpivot(1, "b", "unpivot_b"),  # references "b" which comes later
            _physical(2, "TABLE_B", "b"),
        ]
        errors = validator.validate_cte_pipeline(inputs)
        assert len(errors) == 1
        assert errors[0].referenced_alias == "b"

    def test_chained_ctes_valid(self):
        """physical → unpivot → derived chain should be valid."""
        validator = MappingValidator()
        inputs = [
            _physical(1, "TABLE_A", "a"),
            _unpivot(2, "a", "unpivot_a"),
            _derived(3, "unpivot_a", "derived_a"),
        ]
        errors = validator.validate_cte_pipeline(inputs)
        assert errors == []

    def test_multiple_cte_errors_aggregated(self):
        validator = MappingValidator()
        inputs = [
            _unpivot(1, "missing1", "u1"),
            _derived(2, "missing2", "d1"),
        ]
        errors = validator.validate_cte_pipeline(inputs)
        assert len(errors) == 2

    def test_physical_table_not_checked_for_alias_reference(self):
        """Physical tables don't reference other aliases — no CTE error."""
        validator = MappingValidator()
        inputs = [
            _physical(1, "TABLE_A", "a"),
            _physical(2, "TABLE_B", "b"),
        ]
        errors = validator.validate_cte_pipeline(inputs)
        assert errors == []


# ---------------------------------------------------------------------------
# Tests — error aggregation
# ---------------------------------------------------------------------------

class TestErrorAggregation:
    """Tests that all errors are collected, not just the first."""

    def test_mixed_errors_all_collected(self):
        validator = MappingValidator()
        mapping = _mapping(
            name="gold_model",
            layer=Layer.SILVER_TO_GOLD,
            inputs=[
                _physical(1, "missing_src", "ms", schema="silver"),
                _physical(2, "silver_model", "sm", schema="silver"),
                _unpivot(3, "nonexistent_alias", "u1"),
            ],
        )
        ctx = ProjectContext(
            existing_models={
                "silver_model": ModelInfo(
                    name="silver_model", layer=Layer.BRONZE_TO_SILVER,
                ),
            },
            dependency_dag=DependencyDAG(),  # silver_model not in DAG
        )
        result = validator.validate(mapping, ctx)
        # CTE error is BLOCK → is_valid = False
        assert result.is_valid is False
        assert result.has_warnings is True
        # missing source: missing_src (WARNING)
        assert len(result.missing_sources) == 1
        # missing prerequisite: silver_model not in DAG (WARNING)
        assert len(result.missing_prerequisites) == 1
        # CTE error: nonexistent_alias (BLOCK)
        assert len(result.cte_errors) == 1
        assert len(result.block_errors) == 1
