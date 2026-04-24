"""PrettyPrinter — converts MappingSpec back to CSV format.

Ensures round-trip property: parse(pretty_print(spec)) == spec
"""

from __future__ import annotations

import csv
from io import StringIO

from dbt_job_generator.models.enums import FinalFilterType
from dbt_job_generator.models.mapping import MappingSpec


# Each row has 11 columns to match the CSV format expected by CSVParser.
_NUM_COLS = 11


def _pad(row: list[str]) -> list[str]:
    """Pad a row to _NUM_COLS with empty strings."""
    return row + [""] * (_NUM_COLS - len(row))


class PrettyPrinter:
    """Convert a MappingSpec back to CSV format."""

    def print(self, mapping: MappingSpec) -> str:
        """Convert a MappingSpec back to CSV format.

        The output is parseable by CSVParser and satisfies the round-trip
        property: ``parse(pretty_print(spec)) == spec``.
        """
        buf = StringIO()
        writer = csv.writer(buf, lineterminator="\n")

        self._write_target(writer, mapping)
        self._write_input(writer, mapping)

        if mapping.relationships:
            self._write_relationship(writer, mapping)

        self._write_mapping(writer, mapping)

        if mapping.final_filter is not None:
            self._write_final_filter(writer, mapping)

        return buf.getvalue()

    # ── Target section ─────────────────────────────────────────────

    @staticmethod
    def _write_target(writer: csv.writer, mapping: MappingSpec) -> None:
        writer.writerow(_pad(["Target", "Bảng đích"]))
        writer.writerow(_pad([
            "Database", "Schema", "Table Name", "ETL Handle",
            "", "", "", "Description", "Last update", "Update by", "Update reason",
        ]))
        row = [""] * _NUM_COLS
        row[0] = mapping.target.database
        row[1] = mapping.target.schema
        row[2] = mapping.target.table_name
        row[3] = mapping.target.etl_handle
        row[7] = mapping.target.description or ""
        writer.writerow(row)

    # ── Input section ──────────────────────────────────────────────

    @staticmethod
    def _write_input(writer: csv.writer, mapping: MappingSpec) -> None:
        writer.writerow(_pad(["Input", "Bảng / CTE nguồn"]))
        writer.writerow(_pad([
            "#", "Source Type", "Schema", "Table Name", "Alias",
            "Select Fields", "Filter", "Description", "Last update",
            "Update by", "Update reason",
        ]))
        for entry in mapping.inputs:
            row = [""] * _NUM_COLS
            row[0] = str(entry.index)
            row[1] = entry.source_type.value
            row[2] = entry.schema or ""
            row[3] = entry.table_name
            row[4] = entry.alias
            row[5] = entry.select_fields
            row[6] = entry.filter or ""
            writer.writerow(row)

    # ── Relationship section ───────────────────────────────────────

    @staticmethod
    def _write_relationship(writer: csv.writer, mapping: MappingSpec) -> None:
        writer.writerow(_pad(["Relationship", "Quan hệ giữa các bảng / CTE nguồn"]))
        writer.writerow(_pad([
            "#", "Main Alias", "Join Type", "Join Alias", "Join On",
            "", "", "Description", "Last update", "Update by", "Update reason",
        ]))
        for i, rel in enumerate(mapping.relationships, start=1):
            row = [""] * _NUM_COLS
            row[0] = str(i)
            row[1] = rel.main_alias
            row[2] = rel.join_type.value
            row[3] = rel.join_alias
            row[4] = rel.join_condition
            writer.writerow(row)

    # ── Mapping section ────────────────────────────────────────────

    @staticmethod
    def _write_mapping(writer: csv.writer, mapping: MappingSpec) -> None:
        writer.writerow(_pad(["Mapping", "Mapping trường nguồn → trường đích"]))
        writer.writerow(_pad([
            "#", "Target Column", "Transformation", "Data Type",
            "", "", "", "Description", "Last update", "Update by", "Update reason",
        ]))
        for entry in mapping.mappings:
            row = [""] * _NUM_COLS
            row[0] = str(entry.index)
            row[1] = entry.target_column
            row[2] = entry.transformation or ""
            row[3] = entry.data_type
            row[7] = entry.description or ""
            writer.writerow(row)

    # ── Final Filter section ───────────────────────────────────────

    @staticmethod
    def _write_final_filter(writer: csv.writer, mapping: MappingSpec) -> None:
        writer.writerow(_pad(["Final Filter", "Điều kiện lọc của Main Transform"]))
        writer.writerow(_pad([
            "#", "Clause Type", "Expression", "", "", "", "",
            "Description", "Last update", "Update by", "Update reason",
        ]))
        ff = mapping.final_filter
        clause_type = (
            "UNION ALL"
            if ff.filter_type == FinalFilterType.UNION_ALL
            else "WHERE"
        )
        row = [""] * _NUM_COLS
        row[0] = "1"
        row[1] = clause_type
        row[2] = ff.expression
        writer.writerow(row)
