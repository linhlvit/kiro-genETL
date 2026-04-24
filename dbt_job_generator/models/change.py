"""Change management data models for dbt Job Generator."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from dbt_job_generator.models.enums import ChangeType
from dbt_job_generator.models.mapping import MappingEntry, MappingSpec, SourceEntry


@dataclass(frozen=True)
class InputModification:
    """A modification to a source entry."""
    index: int
    old_entry: SourceEntry
    new_entry: SourceEntry


@dataclass(frozen=True)
class MappingModification:
    """A modification to a mapping entry."""
    target_column: str
    old_entry: MappingEntry
    new_entry: MappingEntry


@dataclass
class MappingDiff:
    """Diff between two versions of a mapping."""
    mapping_name: str
    added_inputs: list[SourceEntry] = field(default_factory=list)
    removed_inputs: list[SourceEntry] = field(default_factory=list)
    modified_inputs: list[InputModification] = field(default_factory=list)
    added_mappings: list[MappingEntry] = field(default_factory=list)
    removed_mappings: list[MappingEntry] = field(default_factory=list)
    modified_mappings: list[MappingModification] = field(default_factory=list)
    relationships_changed: bool = False
    final_filter_changed: bool = False


@dataclass
class MappingChangeRequest:
    """A request to change a mapping."""
    type: ChangeType
    mapping_name: str
    changes: dict = field(default_factory=dict)


@dataclass
class ChangeResult:
    """Result of processing a change request."""
    affected_models: list[str] = field(default_factory=list)
    diff_report: str = ""
    downstream_warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class VersionInfo:
    """Summary info for a mapping version."""
    version_id: str
    mapping_name: str
    timestamp: datetime
    change_description: str = ""


@dataclass(frozen=True)
class MappingVersion:
    """A stored version of a mapping."""
    version_id: str
    mapping_name: str
    timestamp: datetime
    mapping_spec: MappingSpec
    change_description: str = ""
