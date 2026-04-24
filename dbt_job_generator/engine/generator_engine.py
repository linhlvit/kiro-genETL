"""Generator Engine — dispatches MappingEntries to appropriate handlers."""

from __future__ import annotations

from typing import Optional

from dbt_job_generator.catalog.rule_catalog import TransformationRuleCatalog
from dbt_job_generator.engine.handlers.business_logic import BusinessLogicHandler
from dbt_job_generator.engine.handlers.direct_map import DirectMapHandler
from dbt_job_generator.engine.handlers.hardcode import HardcodeHandler
from dbt_job_generator.engine.handlers.hash_handler import HashHandler
from dbt_job_generator.engine.handlers.null_handler import NullHandler
from dbt_job_generator.engine.handlers.type_cast import TypeCastHandler
from dbt_job_generator.models.enums import RulePattern
from dbt_job_generator.models.errors import GenerationError
from dbt_job_generator.models.generation import SelectColumn
from dbt_job_generator.models.mapping import MappingEntry


class GeneratorEngine:
    """Central engine that dispatches each MappingEntry to the right handler.

    Dispatches based on ``MappingEntry.mapping_rule.pattern``:
    - DIRECT_MAP → DirectMapHandler
    - CAST → TypeCastHandler
    - HASH → HashHandler
    - BUSINESS_LOGIC → BusinessLogicHandler
    - HARDCODE_STRING, HARDCODE_NUMERIC → HardcodeHandler
    - NULL_MAP → NullHandler
    """

    def __init__(self, catalog: Optional[TransformationRuleCatalog] = None) -> None:
        self._direct_map = DirectMapHandler()
        self._type_cast = TypeCastHandler()
        self._hash = HashHandler()
        self._business_logic = BusinessLogicHandler(catalog=catalog)
        self._hardcode = HardcodeHandler()
        self._null = NullHandler()

    def generate_select_columns(
        self, mappings: list[MappingEntry]
    ) -> tuple[list[SelectColumn], list[GenerationError]]:
        """Generate SELECT columns from mapping entries.

        Dispatches each MappingEntry to the appropriate handler based on
        mapping_rule.pattern. Returns (columns, errors).
        """
        columns: list[SelectColumn] = []
        errors: list[GenerationError] = []

        for entry in mappings:
            try:
                col = self._dispatch(entry)
                columns.append(col)
            except GenerationError as exc:
                errors.append(exc)

        return columns, errors

    def _dispatch(self, entry: MappingEntry) -> SelectColumn:
        """Route a single MappingEntry to its handler."""
        pattern = entry.mapping_rule.pattern

        if pattern == RulePattern.DIRECT_MAP:
            return self._direct_map.generate(entry)
        elif pattern == RulePattern.CAST:
            return self._type_cast.generate(entry)
        elif pattern == RulePattern.HASH:
            return self._hash.generate(entry)
        elif pattern == RulePattern.BUSINESS_LOGIC:
            return self._business_logic.generate(entry)
        elif pattern in (RulePattern.HARDCODE_STRING, RulePattern.HARDCODE_NUMERIC):
            return self._hardcode.generate(entry)
        elif pattern == RulePattern.NULL_MAP:
            return self._null.generate(entry)
        else:
            raise GenerationError(
                f"Unsupported mapping rule pattern: {pattern.value} "
                f"for column '{entry.target_column}'"
            )
