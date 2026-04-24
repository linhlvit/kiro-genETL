"""Tests for BatchProcessor."""

from __future__ import annotations

import os
import tempfile

import pytest

from dbt_job_generator.batch.batch_processor import (
    BatchProcessor,
    BatchResult,
    FailedMapping,
    GeneratedModel,
)
from dbt_job_generator.models.validation import ProjectContext, SourceInfo


# Minimal valid CSV content for testing
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

INVALID_CSV = """\
Target
Database,Schema,Table Name,ETL Handle,,,,Description
TEST_DB,silver,test_table,SCD1,,,,Test table
"""


def _write_csv(directory: str, filename: str, content: str) -> str:
    """Write CSV content to a file and return the path."""
    path = os.path.join(directory, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path


class TestBatchProcessorSingle:
    """Tests for process_single."""

    def test_process_single_valid(self, tmp_path):
        csv_path = _write_csv(str(tmp_path), "valid.csv", VALID_CSV)
        processor = BatchProcessor()
        context = ProjectContext(
            source_declarations={
                "src_table": SourceInfo(name="src_table", schema="bronze"),
            }
        )

        model_name, output, spec, warnings = processor.process_single(csv_path, context)

        assert model_name == "test_table"
        assert isinstance(output, str)
        assert "SELECT" in output
        assert "config" in output
        assert spec is not None

    def test_process_single_parse_error(self, tmp_path):
        csv_path = _write_csv(str(tmp_path), "invalid.csv", INVALID_CSV)
        processor = BatchProcessor()
        context = ProjectContext()

        with pytest.raises(Exception):
            processor.process_single(csv_path, context)


class TestBatchProcessorBatch:
    """Tests for process_batch."""

    def test_batch_all_valid(self, tmp_path):
        _write_csv(str(tmp_path), "model_a.csv", VALID_CSV)
        _write_csv(str(tmp_path), "model_b.csv", VALID_CSV)
        processor = BatchProcessor()
        context = ProjectContext(
            source_declarations={
                "src_table": SourceInfo(name="src_table", schema="bronze"),
            }
        )

        result = processor.process_batch(str(tmp_path), context)

        assert result.summary.total == 2
        assert result.summary.success_count == 2
        assert result.summary.error_count == 0
        assert len(result.successful) == 2
        assert len(result.failed) == 0

    def test_batch_mixed_valid_invalid(self, tmp_path):
        _write_csv(str(tmp_path), "good.csv", VALID_CSV)
        _write_csv(str(tmp_path), "bad.csv", INVALID_CSV)
        processor = BatchProcessor()
        context = ProjectContext()

        result = processor.process_batch(str(tmp_path), context)

        assert result.summary.total == 2
        assert result.summary.success_count + result.summary.error_count == 2
        assert result.summary.error_count >= 1
        assert len(result.failed) >= 1

    def test_batch_empty_directory(self, tmp_path):
        processor = BatchProcessor()
        context = ProjectContext()

        result = processor.process_batch(str(tmp_path), context)

        assert result.summary.total == 0
        assert result.summary.success_count == 0
        assert result.summary.error_count == 0

    def test_batch_nonexistent_directory(self):
        processor = BatchProcessor()
        context = ProjectContext()

        result = processor.process_batch("/nonexistent/path", context)

        assert result.summary.error_count >= 1

    def test_batch_all_invalid(self, tmp_path):
        _write_csv(str(tmp_path), "bad1.csv", INVALID_CSV)
        _write_csv(str(tmp_path), "bad2.csv", INVALID_CSV)
        processor = BatchProcessor()
        context = ProjectContext()

        result = processor.process_batch(str(tmp_path), context)

        assert result.summary.total == 2
        assert result.summary.success_count == 0
        assert result.summary.error_count == 2

    def test_batch_consistency(self, tmp_path):
        """success_count + error_count == total."""
        _write_csv(str(tmp_path), "a.csv", VALID_CSV)
        _write_csv(str(tmp_path), "b.csv", INVALID_CSV)
        _write_csv(str(tmp_path), "c.csv", VALID_CSV)
        processor = BatchProcessor()
        context = ProjectContext()

        result = processor.process_batch(str(tmp_path), context)

        assert result.summary.success_count + result.summary.error_count == result.summary.total
