"""Generation result data models for dbt Job Generator."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from dbt_job_generator.models.enums import SourceType


@dataclass(frozen=True)
class CTEDefinition:
    """A CTE definition in the WITH clause."""
    alias: str
    sql_body: str
    source_type: SourceType
    depends_on: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ConfigBlock:
    """dbt model config block."""
    materialization: str
    schema: str
    tags: list[str] = field(default_factory=list)
    custom_config: dict = field(default_factory=dict)


@dataclass(frozen=True)
class SelectColumn:
    """A column expression in the Main SELECT."""
    expression: str       # e.g., "alias.col :: string"
    target_alias: str     # AS target_col


@dataclass
class GenerationResult:
    """Result of the Generator Engine."""
    cte_definitions: list[CTEDefinition] = field(default_factory=list)
    select_columns: list[SelectColumn] = field(default_factory=list)
    from_clause: str = ""
    final_filter: Optional[str] = None
    errors: list = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
