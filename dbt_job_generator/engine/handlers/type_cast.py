"""TypeCastHandler — generates CAST expressions for Currency Amount and Date."""

from __future__ import annotations

from dbt_job_generator.models.errors import GenerationError
from dbt_job_generator.models.generation import SelectColumn
from dbt_job_generator.models.mapping import MappingEntry


class TypeCastHandler:
    """Handle CAST pattern: explicit type conversions for Currency Amount and Date."""

    def generate(self, entry: MappingEntry) -> SelectColumn:
        """Generate a SelectColumn for a CAST entry.

        Currency Amount: CAST(source_col AS DECIMAL(p,s))
        Date: TO_DATE(source_col, 'format')
        Raises GenerationError if required params are missing.
        """
        params = entry.mapping_rule.params
        cast_type = params.get("cast_type", "")

        if cast_type == "currency_amount":
            return self._generate_currency(entry, params)
        elif cast_type == "date":
            return self._generate_date(entry, params)
        else:
            # Fallback: generic cast using :: syntax
            expression = f"{entry.transformation} :: {entry.data_type}"
            return SelectColumn(expression=expression, target_alias=entry.target_column)

    def _generate_currency(self, entry: MappingEntry, params: dict) -> SelectColumn:
        precision = params.get("precision")
        scale = params.get("scale")
        if precision is None or scale is None:
            raise GenerationError(
                f"CAST Currency Amount for '{entry.target_column}' missing "
                f"precision/scale (precision={precision}, scale={scale})"
            )
        expression = f"CAST({entry.transformation} AS DECIMAL({precision},{scale}))"
        return SelectColumn(expression=expression, target_alias=entry.target_column)

    def _generate_date(self, entry: MappingEntry, params: dict) -> SelectColumn:
        fmt = params.get("format")
        if not fmt:
            raise GenerationError(
                f"CAST Date for '{entry.target_column}' missing format"
            )
        expression = f"TO_DATE({entry.transformation}, '{fmt}')"
        return SelectColumn(expression=expression, target_alias=entry.target_column)
