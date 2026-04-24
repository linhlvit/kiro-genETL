"""Schema Validator — validates mapping columns against target schema files."""

from __future__ import annotations

import json
import os
from typing import Optional

from dbt_job_generator.models.mapping import MappingSpec
from dbt_job_generator.models.schema import (
    SchemaColumn,
    SchemaValidationError,
    TargetSchemaFile,
)


class SchemaValidator:
    """Validates mapping specifications against target schema files."""

    def load_schema(
        self, schema_dir: str, schema: str, table_name: str
    ) -> Optional[TargetSchemaFile]:
        """Load a target schema file from disk.

        Looks for {schema_dir}/{schema}/{table_name}.json.
        Returns None if the file does not exist.
        """
        file_path = os.path.join(schema_dir, schema, f"{table_name}.json")
        if not os.path.isfile(file_path):
            return None

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        columns = [
            SchemaColumn(
                name=col["name"],
                data_type=col["data_type"],
                nullable=col.get("nullable", True),
                description=col.get("description"),
            )
            for col in data.get("columns", [])
        ]
        return TargetSchemaFile(
            table_name=data.get("table_name", table_name),
            columns=columns,
        )

    def validate(
        self, mapping: MappingSpec, schema_dir: str
    ) -> tuple[list[SchemaValidationError], bool]:
        """Validate mapping columns against the target schema file.

        Returns (errors, schema_found).
        If schema file not found, returns ([], False).
        If found, checks column names and data types.
        """
        schema = mapping.target.schema
        table_name = mapping.target.table_name.lower().replace(" ", "_")

        target_schema = self.load_schema(schema_dir, schema, table_name)
        if target_schema is None:
            return [], False

        # Build lookup from schema columns
        schema_columns = {col.name: col for col in target_schema.columns}
        errors: list[SchemaValidationError] = []

        for entry in mapping.mappings:
            col_name = entry.target_column

            if col_name not in schema_columns:
                errors.append(
                    SchemaValidationError(
                        error_type="COLUMN_NOT_FOUND",
                        column_name=col_name,
                        expected="",
                        actual=col_name,
                        message=(
                            f"Column '{col_name}' in mapping not found in "
                            f"target schema for '{target_schema.table_name}'"
                        ),
                    )
                )
            else:
                schema_col = schema_columns[col_name]
                if entry.data_type.lower() != schema_col.data_type.lower():
                    errors.append(
                        SchemaValidationError(
                            error_type="DATA_TYPE_MISMATCH",
                            column_name=col_name,
                            expected=schema_col.data_type,
                            actual=entry.data_type,
                            message=(
                                f"Column '{col_name}' data type mismatch: "
                                f"mapping has '{entry.data_type}', "
                                f"schema expects '{schema_col.data_type}'"
                            ),
                        )
                    )

        return errors, True
