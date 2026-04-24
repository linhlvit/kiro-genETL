"""Schema data models for target schema validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class SchemaColumn:
    """A column definition in a target schema file."""
    name: str
    data_type: str
    nullable: bool = True
    description: Optional[str] = None


@dataclass(frozen=True)
class TargetSchemaFile:
    """Target schema file defining expected columns for a table."""
    table_name: str
    columns: list[SchemaColumn] = field(default_factory=list)


@dataclass(frozen=True)
class SchemaValidationError:
    """Error from schema validation."""
    error_type: str  # COLUMN_NOT_FOUND, DATA_TYPE_MISMATCH
    column_name: str
    expected: str
    actual: str
    message: str
