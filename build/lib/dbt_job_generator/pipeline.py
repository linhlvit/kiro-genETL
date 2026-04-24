"""Pipeline — end-to-end integration of all dbt Job Generator modules.

Wires together: CSVParser → MappingValidator → CTEPipelineBuilder →
GeneratorEngine → ModelAssembler, plus supporting modules.
"""

from __future__ import annotations

from typing import Optional

from dbt_job_generator.assembler.config_block import ConfigBlockGenerator
from dbt_job_generator.assembler.from_clause import FromClauseGenerator
from dbt_job_generator.assembler.model_assembler import ModelAssembler
from dbt_job_generator.batch.batch_processor import BatchProcessor, BatchResult
from dbt_job_generator.catalog.rule_catalog import TransformationRuleCatalog
from dbt_job_generator.cte_builder.pipeline_builder import CTEPipelineBuilder
from dbt_job_generator.dag.dag_builder import DependencyDAGBuilder
from dbt_job_generator.engine.generator_engine import GeneratorEngine
from dbt_job_generator.models.errors import ErrorResponse, GenerationError, ParseError
from dbt_job_generator.models.mapping import MappingSpec
from dbt_job_generator.models.report import ReviewReport
from dbt_job_generator.models.validation import ProjectContext, ValidationResult
from dbt_job_generator.parser.csv_parser import CSVParser
from dbt_job_generator.report.review_report import ReviewReportGenerator
from dbt_job_generator.spark_test_gen.spark_test_generator import SparkTestGenerator
from dbt_job_generator.test_gen.test_generator import TestGenerator
from dbt_job_generator.validator.mapping_validator import MappingValidator


class Pipeline:
    """End-to-end pipeline for generating dbt models from mapping CSV files."""

    def __init__(
        self,
        catalog: Optional[TransformationRuleCatalog] = None,
        template: Optional[dict] = None,
        schema_dir: Optional[str] = None,
    ) -> None:
        self._parser = CSVParser()
        self._validator = MappingValidator()
        self._cte_builder = CTEPipelineBuilder()
        self._engine = GeneratorEngine(catalog=catalog)
        self._assembler = ModelAssembler()
        self._config_gen = ConfigBlockGenerator()
        self._from_gen = FromClauseGenerator()
        self._dag_builder = DependencyDAGBuilder()
        self._test_gen = TestGenerator()
        self._spark_test_gen = SparkTestGenerator()
        self._report_gen = ReviewReportGenerator()
        self._schema_dir = schema_dir
        self._batch_processor = BatchProcessor(
            catalog=catalog, template=template, schema_dir=schema_dir,
        )
        self._template = template

    def parse(self, file_path: str) -> MappingSpec:
        """Parse a mapping CSV file into a MappingSpec."""
        return self._parser.parse(file_path)

    def validate(
        self,
        file_path: str,
        context: Optional[ProjectContext] = None,
        schema_dir: Optional[str] = None,
    ) -> ValidationResult:
        """Validate a mapping CSV file without generating."""
        mapping = self._parser.parse(file_path)
        ctx = context or ProjectContext()
        effective_schema_dir = schema_dir or self._schema_dir
        return self._validator.validate(mapping, ctx, schema_dir=effective_schema_dir)

    def generate_model(
        self,
        file_path: str,
        context: Optional[ProjectContext] = None,
        schema_dir: Optional[str] = None,
    ) -> str:
        """Generate a single dbt model from a mapping file.

        Always validates first. BLOCK errors → raise. WARNING → continue.
        Returns the SQL content string.
        Raises ParseError or GenerationError on failure.
        """
        ctx = context or ProjectContext()
        mapping = self._parser.parse(file_path)
        effective_schema_dir = schema_dir or self._schema_dir

        # Always validate
        validation = self._validator.validate(
            mapping, ctx, schema_dir=effective_schema_dir
        )
        if not validation.is_valid:
            error_msgs = [be.message for be in validation.block_errors]
            raise GenerationError(
                f"Validation blocked: {'; '.join(error_msgs)}"
            )

        # Build CTEs
        cte_definitions = self._cte_builder.build(mapping.inputs)

        # Generate SELECT columns
        select_columns, gen_errors = self._engine.generate_select_columns(
            mapping.mappings
        )
        if gen_errors:
            raise gen_errors[0]

        # Assemble
        config_block = self._config_gen.generate(mapping, self._template)
        from_clause = self._from_gen.generate(mapping)
        sql_content = self._assembler.assemble(
            config_block=config_block,
            cte_definitions=cte_definitions,
            select_columns=select_columns,
            from_clause=from_clause,
            final_filter=mapping.final_filter,
        )

        return sql_content

    def generate_test(
        self,
        mapping: MappingSpec,
        sql_content: str,
        schema_dir: Optional[str] = None,
    ) -> str:
        """Generate a Spark test file for a dbt model.

        Returns the test file content as a string.
        """
        effective_schema_dir = schema_dir or self._schema_dir or "schemas"
        return self._spark_test_gen.generate_test(
            mapping, sql_content, effective_schema_dir
        )

    def generate_batch(
        self,
        directory: str,
        context: Optional[ProjectContext] = None,
    ) -> BatchResult:
        """Generate all dbt models from a directory of mapping files."""
        ctx = context or ProjectContext()
        return self._batch_processor.process_batch(
            directory, ctx, schema_dir=self._schema_dir
        )

    def generate_report(self, batch_result: BatchResult) -> ReviewReport:
        """Generate a review report from batch results."""
        specs = [m.mapping_spec for m in batch_result.successful]
        dag = None
        execution_order = None
        if specs:
            try:
                dag = self._dag_builder.build(specs)
                execution_order = self._dag_builder.topological_sort(dag)
            except Exception:
                pass

        return self._report_gen.generate(
            batch_result=batch_result,
            dag=dag,
            execution_order=execution_order,
        )
