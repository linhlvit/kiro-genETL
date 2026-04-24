"""Tests for Pipeline and ReviewReportGenerator."""

from __future__ import annotations

import os

import pytest

from dbt_job_generator.batch.batch_processor import BatchResult, GeneratedModel
from dbt_job_generator.models.enums import (
    FinalFilterType,
    Layer,
    RulePattern,
    SourceType,
)
from dbt_job_generator.models.mapping import (
    FinalFilter,
    MappingEntry,
    MappingMetadata,
    MappingRule,
    MappingSpec,
    SourceEntry,
    TargetSpec,
)
from dbt_job_generator.models.report import BatchSummary
from dbt_job_generator.models.testing import TestComplianceReport
from dbt_job_generator.models.validation import ProjectContext, SourceInfo
from dbt_job_generator.pipeline import Pipeline
from dbt_job_generator.report.review_report import ReviewReportGenerator


VALID_CSV = """\
Target
Database,Schema,Table Name,ETL Handle,,,,Description
TEST_DB,silver,test_table,SCD1,,,,Test table

Input
#,Source Type,Schema,Table Name,Alias,Select Fields,Filter
1,physical_table,bronze,src_table,src,col1,

Relationship
#,Main Alias,Join Type,Join Alias,Join Condition

Mapping
#,Target Column,Transformation,Data Type,,,,Description
1,target_col1,src.col1,string,,,,Test col
"""


def _write_csv(directory: str, filename: str, content: str) -> str:
    path = os.path.join(directory, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path


class TestPipelineGenerateModel:
    """Tests for Pipeline.generate_model."""

    def test_generate_model_returns_sql(self, tmp_path):
        csv_path = _write_csv(str(tmp_path), "test.csv", VALID_CSV)
        pipeline = Pipeline()

        sql = pipeline.generate_model(csv_path)

        assert isinstance(sql, str)
        assert "SELECT" in sql
        assert "config" in sql
        assert "target_col1" in sql

    def test_generate_model_invalid_file(self, tmp_path):
        csv_path = _write_csv(str(tmp_path), "bad.csv", "garbage")
        pipeline = Pipeline()

        with pytest.raises(Exception):
            pipeline.generate_model(csv_path)


class TestPipelineValidate:
    """Tests for Pipeline.validate."""

    def test_validate_valid_file(self, tmp_path):
        csv_path = _write_csv(str(tmp_path), "test.csv", VALID_CSV)
        pipeline = Pipeline()

        result = pipeline.validate(csv_path)

        # May have missing sources but should not crash
        assert result is not None

    def test_validate_invalid_file(self, tmp_path):
        csv_path = _write_csv(str(tmp_path), "bad.csv", "not csv")
        pipeline = Pipeline()

        with pytest.raises(Exception):
            pipeline.validate(csv_path)


class TestPipelineBatch:
    """Tests for Pipeline.generate_batch."""

    def test_batch_generates_models(self, tmp_path):
        _write_csv(str(tmp_path), "a.csv", VALID_CSV)
        pipeline = Pipeline()
        context = ProjectContext(
            source_declarations={
                "src_table": SourceInfo(name="src_table", schema="bronze"),
            }
        )

        result = pipeline.generate_batch(str(tmp_path), context)

        assert result.summary.total == 1
        assert result.summary.success_count == 1

    def test_batch_report(self, tmp_path):
        _write_csv(str(tmp_path), "a.csv", VALID_CSV)
        pipeline = Pipeline()
        context = ProjectContext(
            source_declarations={
                "src_table": SourceInfo(name="src_table", schema="bronze"),
            }
        )

        batch_result = pipeline.generate_batch(str(tmp_path), context)
        report = pipeline.generate_report(batch_result)

        assert report is not None
        assert report.summary.total == 1


class TestReviewReportGenerator:
    """Tests for ReviewReportGenerator."""

    def _make_spec(self, name="test_model") -> MappingSpec:
        return MappingSpec(
            name=name,
            layer=Layer.BRONZE_TO_SILVER,
            target=TargetSpec(
                database="db", schema="silver",
                table_name=name, etl_handle="SCD1",
            ),
            inputs=[
                SourceEntry(
                    index=1, source_type=SourceType.PHYSICAL_TABLE,
                    schema="bronze", table_name="src",
                    alias="src", select_fields="*",
                ),
            ],
            mappings=[
                MappingEntry(
                    index=1, target_column="col1",
                    transformation="src.col1", data_type="string",
                    description=None,
                    mapping_rule=MappingRule(pattern=RulePattern.DIRECT_MAP),
                ),
            ],
        )

    def test_generate_report_basic(self):
        spec = self._make_spec()
        batch_result = BatchResult(
            successful=[
                GeneratedModel(
                    model_name="test_model",
                    sql_content="SELECT ...",
                    mapping_spec=spec,
                )
            ],
            summary=BatchSummary(total=1, success_count=1, error_count=0),
        )
        gen = ReviewReportGenerator()

        report = gen.generate(batch_result=batch_result)

        assert len(report.model_details) == 1
        assert report.model_details[0].model_name == "test_model"
        assert "physical_table" in report.model_details[0].patterns_used
        assert report.summary.total == 1

    def test_report_flags_business_logic(self):
        spec = self._make_spec()
        # Replace mapping with business logic
        spec_with_bl = MappingSpec(
            name="bl_model",
            layer=Layer.BRONZE_TO_SILVER,
            target=spec.target,
            inputs=spec.inputs,
            mappings=[
                MappingEntry(
                    index=1, target_column="col1",
                    transformation="CASE WHEN x THEN y END",
                    data_type="string", description=None,
                    mapping_rule=MappingRule(pattern=RulePattern.BUSINESS_LOGIC),
                ),
            ],
        )
        batch_result = BatchResult(
            successful=[
                GeneratedModel(
                    model_name="bl_model",
                    sql_content="SELECT ...",
                    mapping_spec=spec_with_bl,
                )
            ],
            summary=BatchSummary(total=1, success_count=1, error_count=0),
        )
        gen = ReviewReportGenerator()

        report = gen.generate(batch_result=batch_result)

        assert report.model_details[0].has_exceptions is True
        assert any("exception" in a.reason.lower() for a in report.attention_items)

    def test_report_with_test_compliance(self):
        spec = self._make_spec()
        batch_result = BatchResult(
            successful=[
                GeneratedModel(
                    model_name="test_model",
                    sql_content="SELECT ...",
                    mapping_spec=spec,
                )
            ],
            summary=BatchSummary(total=1, success_count=1, error_count=0),
        )
        compliance = [
            TestComplianceReport(
                model_name="test_model",
                is_compliant=False,
                missing_tests=[],
            )
        ]
        gen = ReviewReportGenerator()

        report = gen.generate(
            batch_result=batch_result,
            test_compliance=compliance,
        )

        assert report.model_details[0].test_compliance_status == "non-compliant"

    def test_report_execution_order(self):
        spec = self._make_spec()
        batch_result = BatchResult(
            successful=[
                GeneratedModel(
                    model_name="test_model",
                    sql_content="SELECT ...",
                    mapping_spec=spec,
                )
            ],
            summary=BatchSummary(total=1, success_count=1, error_count=0),
        )
        gen = ReviewReportGenerator()

        report = gen.generate(
            batch_result=batch_result,
            execution_order=["test_model"],
        )

        assert report.execution_order == ["test_model"]

    def test_report_failed_mappings(self):
        from dbt_job_generator.batch.batch_processor import FailedMapping

        batch_result = BatchResult(
            failed=[
                FailedMapping(
                    file_path="bad.csv",
                    model_name="bad_model",
                    error="Parse error",
                )
            ],
            summary=BatchSummary(total=1, success_count=0, error_count=1),
        )
        gen = ReviewReportGenerator()

        report = gen.generate(batch_result=batch_result)

        assert any("failed" in a.reason.lower() for a in report.attention_items)
