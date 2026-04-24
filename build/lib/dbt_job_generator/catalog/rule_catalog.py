"""Transformation Rule Catalog for dbt Job Generator."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dbt_job_generator.models.enums import RuleType
from dbt_job_generator.models.errors import CatalogError


@dataclass(frozen=True)
class TransformationRule:
    """A transformation rule in the catalog."""

    name: str
    type: RuleType  # HASH | BUSINESS_LOGIC | DERIVED
    sql_template: str
    is_exception: bool = False
    description: str = ""


class TransformationRuleCatalog:
    """Manages transformation rules used across the dbt project.

    Rules are stored in a dict keyed by name for O(1) lookup.
    """

    def __init__(
        self,
        hash_function: str = "hash_id",
        rules: list[TransformationRule] | None = None,
    ) -> None:
        self._hash_function = hash_function
        self._rules: dict[str, TransformationRule] = {}
        if rules:
            for rule in rules:
                if rule.name in self._rules:
                    raise CatalogError(
                        f"Duplicate rule name: '{rule.name}'",
                        rule_name=rule.name,
                    )
                self._rules[rule.name] = rule

    def get_hash_function(self) -> str:
        """Return the standard hash function name (e.g., 'hash_id')."""
        return self._hash_function

    def get_rule(self, rule_name: str) -> Optional[TransformationRule]:
        """Get a rule by name. Returns None if not found."""
        return self._rules.get(rule_name)

    def validate_reference(self, rule_name: str) -> bool:
        """Check if a rule name exists in the catalog."""
        return rule_name in self._rules

    def list_rules(self) -> list[TransformationRule]:
        """List all rules in the catalog."""
        return list(self._rules.values())

    def add_rule(self, rule: TransformationRule) -> None:
        """Add a rule to the catalog. Raises CatalogError if duplicate name."""
        if rule.name in self._rules:
            raise CatalogError(
                f"Duplicate rule name: '{rule.name}'",
                rule_name=rule.name,
            )
        self._rules[rule.name] = rule

    @classmethod
    def load_from_csv(cls, file_path: str) -> "TransformationRuleCatalog":
        """Load a TransformationRuleCatalog from a CSV file.

        CSV columns: Rule Name, Rule Type, SQL Template, Is Exception, Description
        """
        import csv

        _TYPE_MAP = {
            "HASH": RuleType.HASH,
            "BUSINESS_LOGIC": RuleType.BUSINESS_LOGIC,
            "DERIVED": RuleType.DERIVED,
        }

        rules: list[TransformationRule] = []
        try:
            with open(file_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name = row.get("Rule Name", "").strip()
                    if not name:
                        continue
                    rule_type_str = row.get("Rule Type", "").strip().upper()
                    rule_type = _TYPE_MAP.get(rule_type_str, RuleType.BUSINESS_LOGIC)
                    sql_template = row.get("SQL Template", "").strip()
                    is_exception = row.get("Is Exception", "false").strip().lower() == "true"
                    description = row.get("Description", "").strip()
                    rules.append(TransformationRule(
                        name=name,
                        type=rule_type,
                        sql_template=sql_template,
                        is_exception=is_exception,
                        description=description,
                    ))
        except OSError as e:
            raise CatalogError(f"Cannot read rules CSV: {e}")

        return cls(rules=rules)
