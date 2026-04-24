"""Data models for dbt Job Generator."""

from dbt_job_generator.models.enums import (
    ChangeType,
    FinalFilterType,
    JoinType,
    Layer,
    RulePattern,
    RuleType,
    Severity,
    SourceType,
    ValidationSeverity,
)
from dbt_job_generator.models.mapping import (
    FinalFilter,
    JoinRelationship,
    KeyField,
    MappingEntry,
    MappingMetadata,
    MappingRule,
    MappingSpec,
    SourceEntry,
    TargetSpec,
    TypeValuePair,
    UnpivotFieldSpec,
)
from dbt_job_generator.models.generation import (
    ConfigBlock,
    CTEDefinition,
    GenerationResult,
    SelectColumn,
)
from dbt_job_generator.models.dag import (
    DAGEdge,
    DAGNode,
    DependencyDAG,
)
from dbt_job_generator.models.validation import (
    BatchValidationResult,
    BlockError,
    CTEValidationError,
    MissingPrerequisite,
    MissingSource,
    ModelInfo,
    ProjectContext,
    SourceInfo,
    ValidationError,
    ValidationResult,
    ValidationWarning,
)
from dbt_job_generator.models.schema import (
    SchemaColumn,
    SchemaValidationError,
    TargetSchemaFile,
)
from dbt_job_generator.models.change import (
    ChangeResult,
    InputModification,
    MappingChangeRequest,
    MappingDiff,
    MappingModification,
    MappingVersion,
    VersionInfo,
)
from dbt_job_generator.models.testing import (
    RelationshipTest,
    SchemaTestConfig,
    TestComplianceReport,
    TestRequirement,
    UnitTestConfig,
)
from dbt_job_generator.models.report import (
    AttentionItem,
    BatchSummary,
    ModelReviewDetail,
    ReviewReport,
)
from dbt_job_generator.models.errors import (
    CatalogError,
    ChangeError,
    DAGError,
    ErrorLocation,
    ErrorResponse,
    GenerationError,
    ParseError,
    UnpivotParseError,
)

__all__ = [
    # Enums
    "ChangeType",
    "FinalFilterType",
    "JoinType",
    "Layer",
    "RulePattern",
    "RuleType",
    "Severity",
    "SourceType",
    "ValidationSeverity",
    # Mapping
    "FinalFilter",
    "JoinRelationship",
    "KeyField",
    "MappingEntry",
    "MappingMetadata",
    "MappingRule",
    "MappingSpec",
    "SourceEntry",
    "TargetSpec",
    "TypeValuePair",
    "UnpivotFieldSpec",
    # Generation
    "ConfigBlock",
    "CTEDefinition",
    "GenerationResult",
    "SelectColumn",
    # DAG
    "DAGEdge",
    "DAGNode",
    "DependencyDAG",
    # Validation
    "BatchValidationResult",
    "BlockError",
    "CTEValidationError",
    "MissingPrerequisite",
    "MissingSource",
    "ModelInfo",
    "ProjectContext",
    "SourceInfo",
    "ValidationError",
    "ValidationResult",
    "ValidationWarning",
    # Schema
    "SchemaColumn",
    "SchemaValidationError",
    "TargetSchemaFile",
    # Change
    "ChangeResult",
    "InputModification",
    "MappingChangeRequest",
    "MappingDiff",
    "MappingModification",
    "MappingVersion",
    "VersionInfo",
    # Testing
    "RelationshipTest",
    "SchemaTestConfig",
    "TestComplianceReport",
    "TestRequirement",
    "UnitTestConfig",
    # Report
    "AttentionItem",
    "BatchSummary",
    "ModelReviewDetail",
    "ReviewReport",
    # Errors
    "CatalogError",
    "ChangeError",
    "DAGError",
    "ErrorLocation",
    "ErrorResponse",
    "GenerationError",
    "ParseError",
    "UnpivotParseError",
]
