"""HashHandler — generates hash_id() expressions."""

from __future__ import annotations

from dbt_job_generator.models.generation import SelectColumn
from dbt_job_generator.models.mapping import MappingEntry


class HashHandler:
    """Handle HASH pattern: pass through the hash_id() call from transformation."""

    def generate(self, entry: MappingEntry) -> SelectColumn:
        """Generate a SelectColumn for a HASH entry.

        The transformation already contains the full hash_id() call,
        so we just pass it through as the expression.
        """
        return SelectColumn(
            expression=entry.transformation,
            target_alias=entry.target_column,
        )
