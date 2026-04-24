"""HardcodeHandler — generates hardcoded value expressions."""

from __future__ import annotations

from dbt_job_generator.models.enums import RulePattern
from dbt_job_generator.models.generation import SelectColumn
from dbt_job_generator.models.mapping import MappingEntry


class HardcodeHandler:
    """Handle HARDCODE_STRING and HARDCODE_NUMERIC patterns."""

    def generate(self, entry: MappingEntry) -> SelectColumn:
        """Generate a SelectColumn for a HARDCODE entry.

        HARDCODE_STRING: 'value' :: string
        HARDCODE_NUMERIC: 42 :: data_type
        """
        if entry.mapping_rule.pattern == RulePattern.HARDCODE_STRING:
            # String values are wrapped in single quotes with :: string
            value = entry.transformation or ""
            # Strip existing quotes if present, then re-wrap
            stripped = value.strip("'")
            expression = f"'{stripped}' :: string"
        else:
            # Numeric values are used as-is with :: data_type
            expression = f"{entry.transformation} :: {entry.data_type}"

        return SelectColumn(expression=expression, target_alias=entry.target_column)
