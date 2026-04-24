"""Mapping Validator — verifies workflow validity of mappings."""

from __future__ import annotations

import os
from typing import Optional

from dbt_job_generator.models.enums import Layer, SourceType
from dbt_job_generator.models.errors import ParseError
from dbt_job_generator.models.mapping import MappingSpec, SourceEntry
from dbt_job_generator.models.schema import SchemaValidationError
from dbt_job_generator.models.validation import (
    BatchValidationResult,
    BlockError,
    CTEValidationError,
    MissingPrerequisite,
    MissingSource,
    ProjectContext,
    ValidationResult,
    ValidationWarning,
)
from dbt_job_generator.validator.schema_validator import SchemaValidator


class MappingValidator:
    """Validates mapping specifications against project context."""

    def __init__(self) -> None:
        self._schema_validator = SchemaValidator()

    def validate(
        self,
        mapping: MappingSpec,
        context: ProjectContext,
        schema_dir: Optional[str] = None,
    ) -> ValidationResult:
        """Validate a mapping spec against the project context.

        Checks (WARNING — does not block generation):
        - Source tables exist at the correct layer
        - Prerequisite jobs exist in DAG (for SILVER_TO_GOLD)
        - Missing schema file
        - Downstream impact

        Checks (BLOCK — prevents generation):
        - CTE pipeline dependencies are valid
        - Schema column/data type mismatches

        Aggregates ALL errors — does not stop at the first one.
        """
        result = ValidationResult()

        # 1. Check source tables exist at the correct layer → WARNING
        for entry in mapping.inputs:
            if entry.source_type != SourceType.PHYSICAL_TABLE:
                continue

            if mapping.layer == Layer.BRONZE_TO_SILVER:
                if entry.table_name not in context.source_declarations:
                    ms = MissingSource(
                        table_name=entry.table_name,
                        schema=entry.schema or "",
                        required_layer=Layer.BRONZE_TO_SILVER,
                    )
                    result.missing_sources.append(ms)
                    result.warnings.append(
                        ValidationWarning(
                            message=f"Missing source: {entry.table_name} ({entry.schema or 'bronze'})",
                            warning_type="MISSING_SOURCE",
                            context={"table_name": entry.table_name, "schema": entry.schema or ""},
                        )
                    )

            elif mapping.layer == Layer.SILVER_TO_GOLD:
                if (
                    entry.table_name not in context.existing_models
                    and entry.table_name not in context.source_declarations
                ):
                    ms = MissingSource(
                        table_name=entry.table_name,
                        schema=entry.schema or "",
                        required_layer=Layer.SILVER_TO_GOLD,
                    )
                    result.missing_sources.append(ms)
                    result.warnings.append(
                        ValidationWarning(
                            message=f"Missing source: {entry.table_name} ({entry.schema or 'silver'})",
                            warning_type="MISSING_SOURCE",
                            context={"table_name": entry.table_name, "schema": entry.schema or ""},
                        )
                    )

        # 2. Check prerequisite jobs in DAG (for SILVER_TO_GOLD) → WARNING
        if mapping.layer == Layer.SILVER_TO_GOLD:
            for entry in mapping.inputs:
                if entry.source_type != SourceType.PHYSICAL_TABLE:
                    continue
                if entry.table_name in context.existing_models:
                    model_info = context.existing_models[entry.table_name]
                    if model_info.layer == Layer.BRONZE_TO_SILVER:
                        if entry.table_name not in context.dependency_dag.nodes:
                            mp = MissingPrerequisite(
                                job_name=entry.table_name,
                                required_by=mapping.name,
                            )
                            result.missing_prerequisites.append(mp)
                            result.warnings.append(
                                ValidationWarning(
                                    message=f"Missing prerequisite: {entry.table_name} required by {mapping.name}",
                                    warning_type="MISSING_PREREQUISITE",
                                    context={"job_name": entry.table_name, "required_by": mapping.name},
                                )
                            )

        # 3. CTE pipeline validation → BLOCK
        cte_errors = self.validate_cte_pipeline(mapping.inputs)
        result.cte_errors.extend(cte_errors)
        for ce in cte_errors:
            result.block_errors.append(
                BlockError(
                    error_type="CTE_DEPENDENCY",
                    message=ce.message,
                    context={"alias": ce.alias, "referenced_alias": ce.referenced_alias},
                )
            )

        # 3a. SELECT * check → BLOCK
        #  - Mapping section must not be empty (would produce SELECT *)
        #  - Input select_fields must not be bare "*"
        if not mapping.mappings:
            result.block_errors.append(
                BlockError(
                    error_type="SELECT_STAR",
                    message="Mapping section is empty — would produce SELECT *. All target columns must be explicitly mapped.",
                )
            )
        for entry in mapping.inputs:
            if entry.select_fields.strip() == "*":
                result.block_errors.append(
                    BlockError(
                        error_type="SELECT_STAR",
                        message=f"Input '{entry.alias}' uses SELECT * — all columns must be explicitly listed.",
                        context={"alias": entry.alias, "index": entry.index},
                    )
                )

        # 4. Schema validation (if schema_dir provided)
        if schema_dir is not None:
            schema_errors, schema_found = self._schema_validator.validate(mapping, schema_dir)
            if not schema_found:
                result.warnings.append(
                    ValidationWarning(
                        message=f"No schema file found for target table '{mapping.target.table_name}'",
                        warning_type="MISSING_SCHEMA_FILE",
                        context={"table_name": mapping.target.table_name},
                    )
                )
            else:
                for se in schema_errors:
                    result.block_errors.append(
                        BlockError(
                            error_type=f"SCHEMA_{se.error_type}",
                            message=se.message,
                            context={
                                "column_name": se.column_name,
                                "expected": se.expected,
                                "actual": se.actual,
                            },
                        )
                    )

        # 5. Impact analysis — downstream impact warning
        if mapping.name in context.existing_models:
            dependents = context.dependency_dag.get_dependents(mapping.name)
            if dependents:
                result.warnings.append(
                    ValidationWarning(
                        message=f"Downstream models may be affected: {', '.join(dependents)}",
                        warning_type="DOWNSTREAM_IMPACT",
                        context={"downstream_models": dependents},
                    )
                )

        # Set is_valid based on BLOCK errors only (warnings don't affect it)
        result.is_valid = len(result.block_errors) == 0
        result.has_warnings = len(result.warnings) > 0

        return result

    def validate_cte_pipeline(
        self, inputs: list[SourceEntry]
    ) -> list[CTEValidationError]:
        """Validate CTE pipeline dependencies.

        For each unpivot_cte and derived_cte, checks that table_name
        (the source alias reference) matches an alias from a SourceEntry
        with a smaller index.
        """
        errors: list[CTEValidationError] = []
        known_aliases: list[str] = []

        for entry in inputs:
            if entry.source_type in (SourceType.UNPIVOT_CTE, SourceType.DERIVED_CTE):
                if entry.table_name not in known_aliases:
                    errors.append(
                        CTEValidationError(
                            alias=entry.alias,
                            referenced_alias=entry.table_name,
                            message=(
                                f"CTE '{entry.alias}' references alias "
                                f"'{entry.table_name}' which is not declared "
                                f"by a preceding source entry"
                            ),
                        )
                    )
            known_aliases.append(entry.alias)

        return errors

    def validate_batch(
        self,
        directory: str,
        context: ProjectContext,
        schema_dir: Optional[str] = None,
    ) -> BatchValidationResult:
        """Validate all CSV files in a directory.

        Returns a BatchValidationResult with per-file results.
        """
        from dbt_job_generator.parser.csv_parser import CSVParser

        parser = CSVParser()
        batch_result = BatchValidationResult()

        try:
            files = sorted(
                f for f in os.listdir(directory) if f.lower().endswith(".csv")
            )
        except OSError:
            return batch_result

        batch_result.total = len(files)

        for filename in files:
            file_path = os.path.join(directory, filename)
            try:
                mapping = parser.parse(file_path)
                validation = self.validate(mapping, context, schema_dir=schema_dir)
                batch_result.results[filename] = validation

                if validation.block_errors:
                    batch_result.blocked_count += 1
                elif validation.has_warnings:
                    batch_result.warning_count += 1
                else:
                    batch_result.valid_count += 1

            except (ParseError, Exception) as e:
                # Parse errors are BLOCK
                error_result = ValidationResult(
                    is_valid=False,
                    block_errors=[
                        BlockError(
                            error_type="PARSE_ERROR",
                            message=str(e),
                        )
                    ],
                )
                batch_result.results[filename] = error_result
                batch_result.blocked_count += 1

        return batch_result
