"""Mapping data models for dbt Job Generator."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from dbt_job_generator.models.enums import (
    FinalFilterType,
    JoinType,
    Layer,
    RulePattern,
    SourceType,
)


@dataclass(frozen=True)
class TargetSpec:
    """Target table specification from the Target section."""
    database: str
    schema: str
    table_name: str
    etl_handle: str
    description: Optional[str] = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TargetSpec):
            return NotImplemented
        return (
            self.database == other.database
            and self.schema == other.schema
            and self.table_name == other.table_name
            and self.etl_handle == other.etl_handle
            and self.description == other.description
        )

    def __hash__(self) -> int:
        return hash((self.database, self.schema, self.table_name, self.etl_handle, self.description))


@dataclass(frozen=True)
class KeyField:
    """Key field mapping in unpivot: target_name=source_name."""
    target_name: str
    source_name: str


@dataclass(frozen=True)
class TypeValuePair:
    """Type-value pair in unpivot: TYPE_CODE:source_column."""
    type_code: str
    source_column: str


@dataclass(frozen=True)
class UnpivotFieldSpec:
    """Parsed unpivot select_fields specification."""
    key_mapping: KeyField
    type_value_pairs: list[TypeValuePair] = field(default_factory=list)
    passthrough_fields: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SourceEntry:
    """A source entry in the Input section — represents one CTE."""
    index: int
    source_type: SourceType
    schema: Optional[str]
    table_name: str
    alias: str
    select_fields: str  # Raw string from CSV
    filter: Optional[str] = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SourceEntry):
            return NotImplemented
        return (
            self.index == other.index
            and self.source_type == other.source_type
            and self.schema == other.schema
            and self.table_name == other.table_name
            and self.alias == other.alias
            and self.select_fields == other.select_fields
            and self.filter == other.filter
        )

    def __hash__(self) -> int:
        return hash((
            self.index, self.source_type, self.schema,
            self.table_name, self.alias, self.select_fields, self.filter,
        ))


@dataclass(frozen=True)
class JoinRelationship:
    """A JOIN relationship from the Relationship section."""
    main_alias: str
    join_type: JoinType
    join_alias: str
    join_condition: str

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JoinRelationship):
            return NotImplemented
        return (
            self.main_alias == other.main_alias
            and self.join_type == other.join_type
            and self.join_alias == other.join_alias
            and self.join_condition == other.join_condition
        )

    def __hash__(self) -> int:
        return hash((self.main_alias, self.join_type, self.join_alias, self.join_condition))


@dataclass(frozen=True)
class MappingRule:
    """Parsed mapping rule from the transformation column."""
    pattern: RulePattern
    params: dict = field(default_factory=dict)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MappingRule):
            return NotImplemented
        return self.pattern == other.pattern and self.params == other.params

    def __hash__(self) -> int:
        return hash((self.pattern, tuple(sorted(self.params.items()))))


@dataclass(frozen=True)
class MappingEntry:
    """A column mapping entry from the Mapping section."""
    index: int
    target_column: str
    transformation: Optional[str]  # Can be empty for NULL_MAP
    data_type: str
    description: Optional[str]
    mapping_rule: MappingRule

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MappingEntry):
            return NotImplemented
        return (
            self.index == other.index
            and self.target_column == other.target_column
            and self.transformation == other.transformation
            and self.data_type == other.data_type
            and self.description == other.description
            and self.mapping_rule == other.mapping_rule
        )

    def __hash__(self) -> int:
        return hash((
            self.index, self.target_column, self.transformation,
            self.data_type, self.description, self.mapping_rule,
        ))


@dataclass(frozen=True)
class FinalFilter:
    """Final filter specification from the Final Filter section."""
    filter_type: FinalFilterType
    expression: str  # Raw expression string

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FinalFilter):
            return NotImplemented
        return self.filter_type == other.filter_type and self.expression == other.expression

    def __hash__(self) -> int:
        return hash((self.filter_type, self.expression))


@dataclass(frozen=True)
class MappingMetadata:
    """Additional metadata for a mapping."""
    source_file: Optional[str] = None
    version: Optional[str] = None
    author: Optional[str] = None


@dataclass(frozen=True)
class MappingSpec:
    """Complete mapping specification parsed from a CSV file."""
    name: str
    layer: Layer
    target: TargetSpec
    inputs: list[SourceEntry] = field(default_factory=list)
    relationships: list[JoinRelationship] = field(default_factory=list)
    mappings: list[MappingEntry] = field(default_factory=list)
    final_filter: Optional[FinalFilter] = None
    metadata: MappingMetadata = field(default_factory=MappingMetadata)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MappingSpec):
            return NotImplemented
        return (
            self.name == other.name
            and self.layer == other.layer
            and self.target == other.target
            and self.inputs == other.inputs
            and self.relationships == other.relationships
            and self.mappings == other.mappings
            and self.final_filter == other.final_filter
            and self.metadata == other.metadata
        )

    def __hash__(self) -> int:
        return hash((
            self.name, self.layer, self.target,
            tuple(self.inputs), tuple(self.relationships),
            tuple(self.mappings), self.final_filter, self.metadata,
        ))
