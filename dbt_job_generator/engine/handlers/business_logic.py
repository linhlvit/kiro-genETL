"""BusinessLogicHandler — generates SQL from business logic rules."""

from __future__ import annotations

from typing import Optional

from dbt_job_generator.catalog.rule_catalog import TransformationRuleCatalog
from dbt_job_generator.models.generation import SelectColumn
from dbt_job_generator.models.mapping import MappingEntry


class BusinessLogicHandler:
    """Handle BUSINESS_LOGIC pattern: use transformation directly, with exception handling."""

    def __init__(self, catalog: Optional[TransformationRuleCatalog] = None) -> None:
        self._catalog = catalog

    def generate(self, entry: MappingEntry) -> SelectColumn:
        """Generate a SelectColumn for a BUSINESS_LOGIC entry.

        If the rule is an exception (from catalog lookup), generates a NULL placeholder
        with a TODO comment. Otherwise uses the transformation with :: type casting.
        """
        is_exception = self._check_is_exception(entry)

        if is_exception:
            expression = f"NULL :: {entry.data_type} /* TODO: implement manually */"
        else:
            expression = f"{entry.transformation} :: {entry.data_type}"

        return SelectColumn(expression=expression, target_alias=entry.target_column)

    def _check_is_exception(self, entry: MappingEntry) -> bool:
        """Check if the rule is marked as exception in the catalog."""
        if self._catalog is None:
            return False
        rule_name = entry.mapping_rule.params.get("rule_name")
        if rule_name:
            rule = self._catalog.get_rule(rule_name)
            if rule is not None:
                return rule.is_exception
        return False
