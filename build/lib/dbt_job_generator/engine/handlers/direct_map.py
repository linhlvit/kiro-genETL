"""DirectMapHandler — generates alias.col :: data_type expressions."""

from __future__ import annotations

from dbt_job_generator.models.generation import SelectColumn
from dbt_job_generator.models.mapping import MappingEntry


class DirectMapHandler:
    """Handle DIRECT_MAP pattern: pass through source column with type casting."""

    def generate(self, entry: MappingEntry) -> SelectColumn:
        """Generate a SelectColumn for a DIRECT_MAP entry.

        Input transformation is like "alias.col", output is "alias.col :: data_type".
        """
        expression = f"{entry.transformation} :: {entry.data_type}"
        return SelectColumn(expression=expression, target_alias=entry.target_column)
