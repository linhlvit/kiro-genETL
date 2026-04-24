"""Tests for TestGenerator — schema test generation and compliance checking."""

from __future__ import annotations

import pytest

from dbt_job_generator.models.enums import Layer, RulePattern
from dbt_job_generator.models.mapping import (
    MappingEntry,
    MappingMetadata,
    MappingRule,
    MappingSpec,
    TargetSpec,
)
from dbt_job_generator.models.testing import (
    RelationshipTest,
    SchemaTestConfig,
    TestComplianceReport,
    TestRequirement,
    UnitTestConfig,
)
from dbt_job_generator.test_gen.test_generator import FieldConstraints, TestGenerator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_entry(
    index: int,
    target_column: str,
    description: str | None = None,
    data_type: str = "string",
) -> MappingEntry:
    return MappingEntry(
        index=index,
        target_column=target_column,
        transformation=f"src.{target_column}",
        data_type=data_type,
        description=description,
        mapping_rule=MappingRule(pattern=RulePattern.DIRECT_MAP),
    )


def _make_mapping(entries: list[MappingEntry]) -> MappingSpec:
    return MappingSpec(
        name="test_model",
        layer=Layer.BRONZE_TO_SILVER,
        target=TargetSpec(
            database="db",
            schema="silver",
            table_name="test_table",
            etl_handle="etl_test",
        ),
        mappings=entries,
    )


# ---------------------------------------------------------------------------
# generate_schema_tests — convention-based
# ---------------------------------------------------------------------------

class TestGenerateSchemaTestsConvention:
    """Convention-based test generation from description text."""

    def test_not_null_from_description(self):
        gen = TestGenerator()
        mapping = _make_mapping([
            _make_entry(1, "col_a", description="NOT NULL field"),
            _make_entry(2, "col_b", description="optional field"),
        ])
        result = gen.generate_schema_tests(mapping)
        assert result.not_null_tests == ["col_a"]
        assert result.unique_tests == []
        assert result.relationship_tests == []

    def test_unique_from_description_pk(self):
        gen = TestGenerator()
        mapping = _make_mapping([
            _make_entry(1, "id_col", description="PK surrogate key"),
        ])
        result = gen.generate_schema_tests(mapping)
        assert result.unique_tests == ["id_col"]

    def test_unique_from_description_unique(self):
        gen = TestGenerator()
        mapping = _make_mapping([
            _make_entry(1, "code", description="unique business code"),
        ])
        result = gen.generate_schema_tests(mapping)
        assert result.unique_tests == ["code"]

    def test_relationship_from_description_fk(self):
        gen = TestGenerator()
        mapping = _make_mapping([
            _make_entry(1, "party_id", description="FK to dim_party.party_key"),
        ])
        result = gen.generate_schema_tests(mapping)
        assert len(result.relationship_tests) == 1
        rel = result.relationship_tests[0]
        assert rel.field_name == "party_id"
        assert rel.related_model == "dim_party"
        assert rel.related_field == "party_key"

    def test_relationship_from_description_relationship_keyword(self):
        gen = TestGenerator()
        mapping = _make_mapping([
            _make_entry(1, "account_id", description="relationship to dim_account.id"),
        ])
        result = gen.generate_schema_tests(mapping)
        assert len(result.relationship_tests) == 1
        rel = result.relationship_tests[0]
        assert rel.related_model == "dim_account"
        assert rel.related_field == "id"

    def test_relationship_fallback_placeholder(self):
        gen = TestGenerator()
        mapping = _make_mapping([
            _make_entry(1, "ref_id", description="FK reference"),
        ])
        result = gen.generate_schema_tests(mapping)
        assert len(result.relationship_tests) == 1
        rel = result.relationship_tests[0]
        assert rel.field_name == "ref_id"
        assert rel.related_model == "unknown"

    def test_combined_constraints_from_description(self):
        gen = TestGenerator()
        mapping = _make_mapping([
            _make_entry(1, "pk_col", description="PK NOT NULL"),
            _make_entry(2, "fk_col", description="FK to other_model.other_id"),
            _make_entry(3, "plain_col", description="just a column"),
        ])
        result = gen.generate_schema_tests(mapping)
        assert "pk_col" in result.not_null_tests
        assert "pk_col" in result.unique_tests
        assert len(result.relationship_tests) == 1
        assert result.relationship_tests[0].field_name == "fk_col"

    def test_no_description_yields_no_tests(self):
        gen = TestGenerator()
        mapping = _make_mapping([
            _make_entry(1, "col_x", description=None),
            _make_entry(2, "col_y", description=""),
        ])
        result = gen.generate_schema_tests(mapping)
        assert result.not_null_tests == []
        assert result.unique_tests == []
        assert result.relationship_tests == []

    def test_empty_mapping_yields_empty_config(self):
        gen = TestGenerator()
        mapping = _make_mapping([])
        result = gen.generate_schema_tests(mapping)
        assert result == SchemaTestConfig()


# ---------------------------------------------------------------------------
# generate_schema_tests — explicit field_constraints
# ---------------------------------------------------------------------------

class TestGenerateSchemaTestsExplicit:
    """Explicit field_constraints dict takes priority over conventions."""

    def test_explicit_not_null(self):
        gen = TestGenerator()
        mapping = _make_mapping([_make_entry(1, "col_a")])
        constraints = {"col_a": FieldConstraints(not_null=True)}
        result = gen.generate_schema_tests(mapping, field_constraints=constraints)
        assert result.not_null_tests == ["col_a"]

    def test_explicit_unique(self):
        gen = TestGenerator()
        mapping = _make_mapping([_make_entry(1, "col_a")])
        constraints = {"col_a": FieldConstraints(unique=True)}
        result = gen.generate_schema_tests(mapping, field_constraints=constraints)
        assert result.unique_tests == ["col_a"]

    def test_explicit_relationship(self):
        gen = TestGenerator()
        rel = RelationshipTest(field_name="col_a", related_model="ref_model", related_field="ref_id")
        mapping = _make_mapping([_make_entry(1, "col_a")])
        constraints = {"col_a": FieldConstraints(relationship=rel)}
        result = gen.generate_schema_tests(mapping, field_constraints=constraints)
        assert result.relationship_tests == [rel]

    def test_explicit_overrides_convention(self):
        """When field_constraints is provided for a field, description is ignored."""
        gen = TestGenerator()
        entry = _make_entry(1, "col_a", description="NOT NULL PK FK to x.y")
        mapping = _make_mapping([entry])
        # Explicit says only unique — description hints should be ignored.
        constraints = {"col_a": FieldConstraints(unique=True)}
        result = gen.generate_schema_tests(mapping, field_constraints=constraints)
        assert result.not_null_tests == []
        assert result.unique_tests == ["col_a"]
        assert result.relationship_tests == []

    def test_mixed_explicit_and_convention(self):
        """Fields not in constraints dict fall back to convention."""
        gen = TestGenerator()
        mapping = _make_mapping([
            _make_entry(1, "col_a", description="NOT NULL"),
            _make_entry(2, "col_b"),
        ])
        constraints = {"col_b": FieldConstraints(unique=True)}
        result = gen.generate_schema_tests(mapping, field_constraints=constraints)
        assert result.not_null_tests == ["col_a"]
        assert result.unique_tests == ["col_b"]


# ---------------------------------------------------------------------------
# check_required_tests
# ---------------------------------------------------------------------------

class TestCheckRequiredTests:
    """Compliance checking against UnitTestConfig."""

    def test_compliant_when_all_tests_present(self):
        gen = TestGenerator()
        existing = SchemaTestConfig(
            not_null_tests=["col_a"],
            unique_tests=["col_b"],
            relationship_tests=[
                RelationshipTest("col_c", "ref", "id"),
            ],
        )
        config = UnitTestConfig(
            required_tests_by_layer={
                Layer.BRONZE_TO_SILVER: [
                    TestRequirement(test_type="not_null"),
                    TestRequirement(test_type="unique"),
                    TestRequirement(test_type="relationship"),
                ],
            },
        )
        report = gen.check_required_tests("my_model", existing, config)
        assert report.is_compliant is True
        assert report.missing_tests == []
        assert report.model_name == "my_model"

    def test_missing_unique_test(self):
        gen = TestGenerator()
        existing = SchemaTestConfig(not_null_tests=["col_a"])
        config = UnitTestConfig(
            required_tests_by_layer={
                Layer.BRONZE_TO_SILVER: [
                    TestRequirement(test_type="not_null"),
                    TestRequirement(test_type="unique", description="need unique"),
                ],
            },
        )
        report = gen.check_required_tests("model_x", existing, config)
        assert report.is_compliant is False
        assert len(report.missing_tests) == 1
        assert report.missing_tests[0].test_type == "unique"

    def test_missing_all_tests(self):
        gen = TestGenerator()
        existing = SchemaTestConfig()
        config = UnitTestConfig(
            required_tests_by_layer={
                Layer.SILVER_TO_GOLD: [
                    TestRequirement(test_type="not_null"),
                    TestRequirement(test_type="unique"),
                ],
            },
        )
        report = gen.check_required_tests("model_y", existing, config)
        assert report.is_compliant is False
        assert len(report.missing_tests) == 2

    def test_empty_config_always_compliant(self):
        gen = TestGenerator()
        existing = SchemaTestConfig()
        config = UnitTestConfig()
        report = gen.check_required_tests("model_z", existing, config)
        assert report.is_compliant is True
        assert report.required_tests == []

    def test_multiple_layers_aggregated(self):
        gen = TestGenerator()
        existing = SchemaTestConfig(not_null_tests=["a"])
        config = UnitTestConfig(
            required_tests_by_layer={
                Layer.BRONZE_TO_SILVER: [
                    TestRequirement(test_type="not_null"),
                ],
                Layer.SILVER_TO_GOLD: [
                    TestRequirement(test_type="unique"),
                ],
            },
        )
        report = gen.check_required_tests("model_multi", existing, config)
        assert report.is_compliant is False
        assert len(report.missing_tests) == 1
        assert report.missing_tests[0].test_type == "unique"

    def test_report_contains_all_required(self):
        gen = TestGenerator()
        existing = SchemaTestConfig(not_null_tests=["a"])
        reqs = [
            TestRequirement(test_type="not_null"),
            TestRequirement(test_type="relationship"),
        ]
        config = UnitTestConfig(
            required_tests_by_layer={Layer.BRONZE_TO_SILVER: reqs},
        )
        report = gen.check_required_tests("m", existing, config)
        assert len(report.required_tests) == 2
