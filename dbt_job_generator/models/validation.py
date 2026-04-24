"""Validation data models for dbt Job Generator."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from dbt_job_generator.models.dag import DependencyDAG
from dbt_job_generator.models.enums import Layer
from dbt_job_generator.models.errors import ErrorLocation


@dataclass(frozen=True)
class ModelInfo:
    """Information about an existing dbt model."""
    name: str
    layer: Layer
    path: Optional[str] = None


@dataclass(frozen=True)
class SourceInfo:
    """Information about a declared source."""
    name: str
    schema: str
    database: Optional[str] = None


@dataclass
class ProjectContext:
    """Context of the dbt project for validation."""
    existing_models: dict[str, ModelInfo] = field(default_factory=dict)
    source_declarations: dict[str, SourceInfo] = field(default_factory=dict)
    dependency_dag: DependencyDAG = field(default_factory=DependencyDAG)


@dataclass(frozen=True)
class MissingSource:
    """A source table that is missing from the project."""
    table_name: str
    schema: str
    required_layer: Layer


@dataclass(frozen=True)
class MissingPrerequisite:
    """A prerequisite job that is missing from the DAG."""
    job_name: str
    required_by: str


@dataclass(frozen=True)
class CTEValidationError:
    """Validation error for CTE pipeline dependencies."""
    alias: str
    referenced_alias: str
    message: str


@dataclass(frozen=True)
class BlockError:
    """A blocking validation error that prevents generation."""
    error_type: str  # PARSE_ERROR, CTE_DEPENDENCY, SCHEMA_COLUMN_NOT_FOUND, SCHEMA_DATA_TYPE_MISMATCH
    message: str
    location: Optional[ErrorLocation] = None
    context: dict = field(default_factory=dict)


@dataclass(frozen=True)
class ValidationError:
    """A validation error."""
    message: str
    field_name: Optional[str] = None
    section: Optional[str] = None


@dataclass(frozen=True)
class ValidationWarning:
    """A validation warning — does not prevent generation."""
    message: str
    field_name: Optional[str] = None
    section: Optional[str] = None
    warning_type: Optional[str] = None  # MISSING_SOURCE, MISSING_PREREQUISITE, MISSING_SCHEMA_FILE, DOWNSTREAM_IMPACT
    context: dict = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Result of mapping validation."""
    is_valid: bool = True
    # Legacy fields — kept for backward compatibility
    missing_sources: list[MissingSource] = field(default_factory=list)
    missing_prerequisites: list[MissingPrerequisite] = field(default_factory=list)
    cte_errors: list[CTEValidationError] = field(default_factory=list)
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationWarning] = field(default_factory=list)
    # New fields for WARNING/BLOCK classification
    block_errors: list[BlockError] = field(default_factory=list)
    has_warnings: bool = False


@dataclass
class BatchValidationResult:
    """Result of batch validation across multiple mapping files."""
    total: int = 0
    valid_count: int = 0      # No BLOCK errors, no warnings
    warning_count: int = 0    # Has warnings but no BLOCK errors
    blocked_count: int = 0    # Has BLOCK errors
    results: dict[str, ValidationResult] = field(default_factory=dict)
