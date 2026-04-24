"""Report data models for dbt Job Generator."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class AttentionItem:
    """An item that needs human attention in the review."""
    model_name: str
    reason: str
    details: str = ""


@dataclass
class ModelReviewDetail:
    """Review detail for a single model."""
    model_name: str
    patterns_used: list[str] = field(default_factory=list)
    has_complex_logic: bool = False
    has_exceptions: bool = False
    cte_count: int = 0
    test_compliance_status: str = ""


@dataclass
class BatchSummary:
    """Summary of a batch processing run."""
    total: int = 0
    success_count: int = 0
    error_count: int = 0
    warning_count: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class ReviewReport:
    """Complete review report for a batch."""
    summary: BatchSummary = field(default_factory=BatchSummary)
    model_details: list[ModelReviewDetail] = field(default_factory=list)
    execution_order: list[str] = field(default_factory=list)
    attention_items: list[AttentionItem] = field(default_factory=list)
