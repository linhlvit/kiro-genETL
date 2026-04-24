"""Enum definitions for dbt Job Generator."""

from __future__ import annotations

from enum import Enum


class Layer(Enum):
    """Medallion architecture layer transition."""
    BRONZE_TO_SILVER = "bronze_to_silver"
    SILVER_TO_GOLD = "silver_to_gold"


class SourceType(Enum):
    """Type of source entry in the Input section."""
    PHYSICAL_TABLE = "physical_table"
    UNPIVOT_CTE = "unpivot_cte"
    DERIVED_CTE = "derived_cte"


class JoinType(Enum):
    """SQL JOIN types."""
    LEFT_JOIN = "LEFT JOIN"
    INNER_JOIN = "INNER JOIN"
    RIGHT_JOIN = "RIGHT JOIN"
    FULL_OUTER_JOIN = "FULL OUTER JOIN"
    CROSS_JOIN = "CROSS JOIN"


class RulePattern(Enum):
    """Mapping rule patterns."""
    DIRECT_MAP = "DIRECT_MAP"
    HASH = "HASH"
    HARDCODE_STRING = "HARDCODE_STRING"
    HARDCODE_NUMERIC = "HARDCODE_NUMERIC"
    NULL_MAP = "NULL_MAP"
    BUSINESS_LOGIC = "BUSINESS_LOGIC"
    CAST = "CAST"


class FinalFilterType(Enum):
    """Type of final filter."""
    UNION_ALL = "UNION_ALL"
    WHERE_CLAUSE = "WHERE_CLAUSE"
    NONE = "NONE"


class Severity(Enum):
    """Error severity level."""
    ERROR = "ERROR"
    WARNING = "WARNING"


class ChangeType(Enum):
    """Type of mapping change request."""
    UPDATE_RULE = "UPDATE_RULE"
    ADD_FIELD = "ADD_FIELD"
    NEW_MAPPING = "NEW_MAPPING"


class RuleType(Enum):
    """Transformation rule type in the catalog."""
    HASH = "HASH"
    BUSINESS_LOGIC = "BUSINESS_LOGIC"
    DERIVED = "DERIVED"


class ValidationSeverity(Enum):
    """Validation severity level — BLOCK prevents generation, WARNING does not."""
    BLOCK = "BLOCK"
    WARNING = "WARNING"
