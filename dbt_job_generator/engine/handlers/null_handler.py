"""NullHandler — generates NULL :: data_type expressions."""

from __future__ import annotations

from dbt_job_generator.models.generation import SelectColumn
from dbt_job_generator.models.mapping import MappingEntry


class NullHandler:
    """Handle NULL_MAP pattern: unmapped fields get NULL with type casting."""

    def generate(self, entry: MappingEntry) -> SelectColumn:
        """Generate a SelectColumn for a NULL_MAP entry.

        Output: NULL :: data_type
        """
        expression = f"NULL :: {entry.data_type}"
        return SelectColumn(expression=expression, target_alias=entry.target_column)
