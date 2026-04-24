"""Batch Processor — processes multiple mapping files through the pipeline.

Orchestrates: parse → validate → build CTEs → generate columns → assemble
for each CSV file in a directory. Fail-safe: errors don't stop the batch.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from dbt_job_generator.assembler.config_block import ConfigBlockGenerator
from dbt_job_generator.assembler.from_clause import FromClauseGenerator
from dbt_job_generator.assembler.model_assembler import ModelAssembler
from dbt_job_generator.catalog.rule_catalog import TransformationRuleCatalog
from dbt_job_generator.cte_builder.pipeline_builder import CTEPipelineBuilder
from dbt_job_generator.engine.generator_engine import GeneratorEngine
from dbt_job_generator.models.errors import ErrorResponse, ParseError
from dbt_job_generator.models.mapping import MappingSpec
from dbt_job_generator.models.report import BatchSummary
from dbt_job_generator.models.validation import ProjectContext
from dbt_job_generator.parser.csv_parser import CSVParser
from dbt_job_generator.spark_test_gen.spark_test_generator import SparkTestGenerator
from dbt_job_generator.validator.mapping_validator import MappingValidator


@dataclass
class GeneratedModel:
    """A successfully generated dbt model."""
    model_name: str
    sql_content: str
    mapping_spec: MappingSpec


@dataclass
class FailedMapping:
    """A mapping that failed during processing."""
    file_path: str
    model_name: str
    error: str


@dataclass
class BatchResult:
    """Result of batch processing."""
    successful: list[GeneratedModel] = field(default_factory=list)
    failed: list[FailedMapping] = field(default_factory=list)
    summary: BatchSummary = field(default_factory=BatchSummary)
    test_files: dict[str, str] = field(default_factory=dict)


class BatchProcessor:
    """Processes multiple mapping CSV files through the full pipeline."""

    def __init__(
        self,
        catalog: Optional[TransformationRuleCatalog] = None,
        template: Optional[dict] = None,
        schema_dir: Optional[str] = None,
    ) -> None:
        self._schema_dir = schema_dir
        self._parser = CSVParser()
        self._validator = MappingValidator()
        self._cte_builder = CTEPipelineBuilder()
        self._engine = GeneratorEngine(catalog=catalog)
        self._assembler = ModelAssembler()
        self._config_gen = ConfigBlockGenerator()
        self._from_gen = FromClauseGenerator()
        self._spark_test_gen = SparkTestGenerator()
        self._template = template

    def process_single(
        self, file_path: str, context: ProjectContext
    ) -> tuple[str, str | ErrorResponse, MappingSpec | None, list[str]]:
        """Process a single mapping file.

        Always runs validation. BLOCK errors → fail. WARNING only → continue.
        Returns (model_name, sql_content_or_error, mapping_spec_or_none, warnings).
        """
        # 1. Parse
        mapping = self._parser.parse(file_path)
        model_name = mapping.name

        # 2. Always validate
        validation = self._validator.validate(
            mapping, context, schema_dir=self._schema_dir
        )
        if not validation.is_valid:
            errors = [f"[BLOCK] {be.message}" for be in validation.block_errors]
            return model_name, ErrorResponse(
                error_type="ValidationError",
                message="; ".join(errors),
            ), None, []

        warning_messages = [w.message for w in validation.warnings]

        # 3. Build CTEs
        cte_definitions = self._cte_builder.build(mapping.inputs)

        # 4. Generate SELECT columns
        select_columns, gen_errors = self._engine.generate_select_columns(
            mapping.mappings
        )
        if gen_errors:
            error_msgs = [str(e) for e in gen_errors]
            return model_name, ErrorResponse(
                error_type="GenerationError",
                message="; ".join(error_msgs),
            ), None, []

        # 5. Assemble
        config_block = self._config_gen.generate(mapping, self._template)
        from_clause = self._from_gen.generate(mapping)
        sql_content = self._assembler.assemble(
            config_block=config_block,
            cte_definitions=cte_definitions,
            select_columns=select_columns,
            from_clause=from_clause,
            final_filter=mapping.final_filter,
        )

        return model_name, sql_content, mapping, warning_messages

    def process_batch(
        self, directory: str, context: ProjectContext,
        schema_dir: Optional[str] = None,
    ) -> BatchResult:
        """Process all CSV files in a directory.

        Fail-safe: errors on individual files don't stop the batch.
        If schema_dir is provided, also generates Spark test files.
        """
        result = BatchResult()
        effective_schema_dir = schema_dir or self._schema_dir

        try:
            files = sorted(
                f for f in os.listdir(directory) if f.lower().endswith(".csv")
            )
        except OSError as e:
            result.failed.append(
                FailedMapping(
                    file_path=directory,
                    model_name="",
                    error=f"Cannot read directory: {e}",
                )
            )
            result.summary = BatchSummary(
                total=0, success_count=0, error_count=1,
                errors=[f"Cannot read directory: {e}"],
            )
            return result

        result.summary.total = len(files)

        for filename in files:
            file_path = os.path.join(directory, filename)
            try:
                model_name, output, spec, warnings = self.process_single(
                    file_path, context
                )

                if isinstance(output, ErrorResponse):
                    result.failed.append(
                        FailedMapping(
                            file_path=file_path,
                            model_name=model_name,
                            error=output.message,
                        )
                    )
                    result.summary.error_count += 1
                    result.summary.errors.append(
                        f"{filename}: {output.message}"
                    )
                else:
                    result.successful.append(
                        GeneratedModel(
                            model_name=model_name,
                            sql_content=output,
                            mapping_spec=spec,
                        )
                    )
                    result.summary.success_count += 1
                    if warnings:
                        result.summary.warning_count += 1
                        for w in warnings:
                            result.summary.warnings.append(f"{filename}: {w}")

            except (ParseError, Exception) as e:
                model_name = filename.replace(".csv", "")
                result.failed.append(
                    FailedMapping(
                        file_path=file_path,
                        model_name=model_name,
                        error=str(e),
                    )
                )
                result.summary.error_count += 1
                result.summary.errors.append(f"{filename}: {e}")

        # Always generate Spark test files for successful models
        if result.successful:
            schema_dir_for_tests = effective_schema_dir or "schemas"
            models = [
                (m.mapping_spec, m.sql_content) for m in result.successful
            ]
            result.test_files = self._spark_test_gen.generate_test_batch(
                models, schema_dir_for_tests
            )

        return result
