"""Test Generator — generates schema tests and checks unit test compliance."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from dbt_job_generator.models.mapping import MappingSpec, MappingEntry
from dbt_job_generator.models.testing import (
    RelationshipTest,
    SchemaTestConfig,
    TestComplianceReport,
    TestRequirement,
    UnitTestConfig,
)


@dataclass
class FieldConstraints:
    """Constraints for a single field, used to drive test generation."""
    not_null: bool = False
    unique: bool = False
    relationship: Optional[RelationshipTest] = None


class TestGenerator:
    """Generates schema tests and checks unit test compliance for dbt models."""

    def generate_schema_tests(
        self,
        mapping: MappingSpec,
        field_constraints: Optional[dict[str, FieldConstraints]] = None,
    ) -> SchemaTestConfig:
        """Generate schema tests from a mapping specification.

        Tests are derived from two sources (in priority order):
        1. Explicit ``field_constraints`` dict when provided.
        2. Convention-based detection from ``MappingEntry.description``:
           - "NOT NULL" → not_null test
           - "unique" or "PK" → unique test
           - "FK" or "relationship" → relationship test (placeholder)
        """
        config = SchemaTestConfig()

        for entry in mapping.mappings:
            target = entry.target_column

            if field_constraints and target in field_constraints:
                fc = field_constraints[target]
                if fc.not_null:
                    config.not_null_tests.append(target)
                if fc.unique:
                    config.unique_tests.append(target)
                if fc.relationship is not None:
                    config.relationship_tests.append(fc.relationship)
            else:
                self._apply_convention_tests(entry, config)

        return config

    def check_required_tests(
        self,
        model_name: str,
        existing_tests: SchemaTestConfig,
        config: UnitTestConfig,
    ) -> TestComplianceReport:
        """Check whether a model has all required tests per UnitTestConfig.

        Compares existing schema tests against the required tests defined in
        ``config.required_tests_by_layer`` for *every* layer present in the
        config.  Returns a compliance report listing any missing tests.
        """
        report = TestComplianceReport(model_name=model_name)

        all_required: list[TestRequirement] = []
        for reqs in config.required_tests_by_layer.values():
            all_required.extend(reqs)

        report.required_tests = list(all_required)

        existing_test_types = self._collect_existing_test_types(existing_tests)

        for req in all_required:
            if req.test_type not in existing_test_types:
                report.missing_tests.append(req)

        report.is_compliant = len(report.missing_tests) == 0
        return report

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _apply_convention_tests(entry: MappingEntry, config: SchemaTestConfig) -> None:
        """Derive tests from description text using naming conventions."""
        desc = (entry.description or "").upper()
        target = entry.target_column

        if "NOT NULL" in desc:
            config.not_null_tests.append(target)

        if "UNIQUE" in desc or "PK" in desc:
            config.unique_tests.append(target)

        if "FK" in desc or "RELATIONSHIP" in desc:
            # Build a placeholder relationship test from description hints.
            rel = _parse_relationship_hint(entry.description or "", target)
            if rel is not None:
                config.relationship_tests.append(rel)

    @staticmethod
    def _collect_existing_test_types(tests: SchemaTestConfig) -> set[str]:
        """Return a set of test type strings present in the config."""
        types: set[str] = set()
        if tests.not_null_tests:
            types.add("not_null")
        if tests.unique_tests:
            types.add("unique")
        if tests.relationship_tests:
            types.add("relationship")
        return types


def _parse_relationship_hint(description: str, field_name: str) -> Optional[RelationshipTest]:
    """Try to extract a RelationshipTest from a description string.

    Supported patterns (case-insensitive):
      - "FK to <model>.<field>"
      - "relationship to <model>.<field>"

    Falls back to a generic placeholder when the pattern doesn't match but
    the description still contains "FK" or "relationship".
    """
    pattern = re.compile(
        r"(?:FK|relationship)\s+to\s+(\w+)\.(\w+)",
        re.IGNORECASE,
    )
    match = pattern.search(description)
    if match:
        return RelationshipTest(
            field_name=field_name,
            related_model=match.group(1),
            related_field=match.group(2),
        )
    # Fallback: we know the description mentions FK/relationship but we
    # can't parse the target — return a placeholder.
    return RelationshipTest(
        field_name=field_name,
        related_model="unknown",
        related_field="unknown",
    )
