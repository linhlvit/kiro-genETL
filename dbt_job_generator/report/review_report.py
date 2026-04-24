"""Review Report Generator — generates review reports for human review.

Produces a ReviewReport with model details, execution order,
and attention items that need manual review.
"""

from __future__ import annotations

from typing import Optional

from dbt_job_generator.batch.batch_processor import BatchResult
from dbt_job_generator.models.dag import DependencyDAG
from dbt_job_generator.models.enums import RulePattern, SourceType
from dbt_job_generator.models.report import (
    AttentionItem,
    BatchSummary,
    ModelReviewDetail,
    ReviewReport,
)
from dbt_job_generator.models.testing import TestComplianceReport


class ReviewReportGenerator:
    """Generates review reports from batch processing results."""

    def generate(
        self,
        batch_result: BatchResult,
        dag: Optional[DependencyDAG] = None,
        test_compliance: Optional[list[TestComplianceReport]] = None,
        execution_order: Optional[list[str]] = None,
    ) -> ReviewReport:
        """Generate a review report.

        Args:
            batch_result: Result from batch processing.
            dag: Optional dependency DAG for execution order.
            test_compliance: Optional test compliance reports per model.
            execution_order: Optional pre-computed execution order.

        Returns:
            A ReviewReport with summary, model details, execution order,
            and attention items.
        """
        report = ReviewReport()

        # Summary from batch result
        report.summary = batch_result.summary

        # Build compliance lookup
        compliance_map: dict[str, TestComplianceReport] = {}
        if test_compliance:
            for tc in test_compliance:
                compliance_map[tc.model_name] = tc

        # Model details
        for model in batch_result.successful:
            spec = model.mapping_spec
            detail = ModelReviewDetail(model_name=model.model_name)

            # Patterns used
            patterns = self._detect_patterns(spec)
            detail.patterns_used = patterns

            # Complex logic detection
            detail.has_complex_logic = self._has_complex_logic(spec)
            detail.has_exceptions = self._has_exceptions(spec)

            # CTE count
            detail.cte_count = len(spec.inputs)

            # Test compliance
            if model.model_name in compliance_map:
                tc = compliance_map[model.model_name]
                detail.test_compliance_status = (
                    "compliant" if tc.is_compliant else "non-compliant"
                )
            else:
                detail.test_compliance_status = "not-checked"

            report.model_details.append(detail)

            # Attention items
            if detail.has_exceptions:
                report.attention_items.append(
                    AttentionItem(
                        model_name=model.model_name,
                        reason="Contains business logic exceptions",
                        details="Manual review required for exception rules.",
                    )
                )
            if detail.has_complex_logic:
                report.attention_items.append(
                    AttentionItem(
                        model_name=model.model_name,
                        reason="Complex transformation logic",
                        details="Model uses unpivot, derived CTEs, or multiple JOINs.",
                    )
                )
            if detail.test_compliance_status == "non-compliant":
                report.attention_items.append(
                    AttentionItem(
                        model_name=model.model_name,
                        reason="Missing required tests",
                        details="Model does not meet test compliance requirements.",
                    )
                )

        # Failed mappings as attention items
        for failed in batch_result.failed:
            report.attention_items.append(
                AttentionItem(
                    model_name=failed.model_name or failed.file_path,
                    reason="Processing failed",
                    details=failed.error,
                )
            )

        # Execution order
        if execution_order:
            report.execution_order = execution_order
        elif dag:
            from dbt_job_generator.dag.dag_builder import DependencyDAGBuilder
            builder = DependencyDAGBuilder()
            try:
                report.execution_order = builder.topological_sort(dag)
            except Exception:
                report.execution_order = sorted(dag.nodes.keys())
        else:
            report.execution_order = [
                m.model_name for m in batch_result.successful
            ]

        return report

    @staticmethod
    def _detect_patterns(spec) -> list[str]:
        """Detect patterns used in a mapping spec."""
        patterns: list[str] = []
        source_types = {e.source_type for e in spec.inputs}

        if SourceType.PHYSICAL_TABLE in source_types:
            patterns.append("physical_table")
        if SourceType.UNPIVOT_CTE in source_types:
            patterns.append("unpivot_cte")
        if SourceType.DERIVED_CTE in source_types:
            patterns.append("derived_cte")
        if spec.relationships:
            patterns.append("JOIN")
        if spec.final_filter:
            from dbt_job_generator.models.enums import FinalFilterType
            if spec.final_filter.filter_type == FinalFilterType.UNION_ALL:
                patterns.append("UNION_ALL")
            elif spec.final_filter.filter_type == FinalFilterType.WHERE_CLAUSE:
                patterns.append("WHERE")

        return patterns

    @staticmethod
    def _has_complex_logic(spec) -> bool:
        """Check if a mapping has complex transformation logic."""
        has_unpivot = any(
            e.source_type == SourceType.UNPIVOT_CTE for e in spec.inputs
        )
        has_derived = any(
            e.source_type == SourceType.DERIVED_CTE for e in spec.inputs
        )
        has_multiple_joins = len(spec.relationships) > 1
        return has_unpivot or has_derived or has_multiple_joins

    @staticmethod
    def _has_exceptions(spec) -> bool:
        """Check if a mapping has business logic exception rules."""
        return any(
            entry.mapping_rule.pattern == RulePattern.BUSINESS_LOGIC
            for entry in spec.mappings
        )
