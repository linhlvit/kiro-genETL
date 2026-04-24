"""Error data models and exception classes for dbt Job Generator."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from dbt_job_generator.models.enums import Severity


@dataclass(frozen=True)
class ErrorLocation:
    """Location of an error in a file."""
    file: str
    line: Optional[int] = None
    column: Optional[int] = None
    section: Optional[str] = None
    field_name: Optional[str] = None


@dataclass
class ErrorResponse:
    """Structured error response."""
    error_type: str
    message: str
    location: Optional[ErrorLocation] = None
    context: dict = field(default_factory=dict)
    severity: Severity = Severity.ERROR


class ParseError(Exception):
    """Error raised when parsing a CSV mapping file fails."""

    def __init__(self, message: str, location: Optional[ErrorLocation] = None) -> None:
        super().__init__(message)
        self.location = location


class GenerationError(Exception):
    """Error raised during dbt model generation."""

    def __init__(self, message: str, location: Optional[ErrorLocation] = None) -> None:
        super().__init__(message)
        self.location = location


class CatalogError(Exception):
    """Error raised when a rule reference is not found in the catalog."""

    def __init__(self, message: str, rule_name: Optional[str] = None) -> None:
        super().__init__(message)
        self.rule_name = rule_name


class DAGError(Exception):
    """Error raised during DAG construction or validation."""

    def __init__(self, message: str, cycle: Optional[list[str]] = None) -> None:
        super().__init__(message)
        self.cycle = cycle


class ChangeError(Exception):
    """Error raised during change management processing."""

    def __init__(self, message: str, conflict: Optional[str] = None) -> None:
        super().__init__(message)
        self.conflict = conflict


class UnpivotParseError(Exception):
    """Error raised when parsing unpivot select_fields format fails."""

    def __init__(self, message: str, position: Optional[int] = None) -> None:
        super().__init__(message)
        self.position = position
