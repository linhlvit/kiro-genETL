"""Testing data models for dbt Job Generator."""

from __future__ import annotations

from dataclasses import dataclass, field

from dbt_job_generator.models.enums import Layer


@dataclass(frozen=True)
class RelationshipTest:
    """A relationship test configuration."""
    field_name: str
    related_model: str
    related_field: str


@dataclass
class SchemaTestConfig:
    """Schema test configuration generated from a mapping."""
    not_null_tests: list[str] = field(default_factory=list)
    unique_tests: list[str] = field(default_factory=list)
    relationship_tests: list[RelationshipTest] = field(default_factory=list)


@dataclass(frozen=True)
class TestRequirement:
    """A required test specification."""
    test_type: str
    description: str = ""


@dataclass
class UnitTestConfig:
    """Configuration for required unit tests by layer."""
    required_tests_by_layer: dict[Layer, list[TestRequirement]] = field(default_factory=dict)


@dataclass
class TestComplianceReport:
    """Report on test compliance for a model."""
    model_name: str = ""
    required_tests: list[TestRequirement] = field(default_factory=list)
    missing_tests: list[TestRequirement] = field(default_factory=list)
    is_compliant: bool = True
