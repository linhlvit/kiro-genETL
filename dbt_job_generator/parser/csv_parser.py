"""CSV Parser for dbt Job Generator mapping files."""

from __future__ import annotations

import csv
import os
import re
from io import StringIO
from typing import Optional

from dbt_job_generator.models.enums import (
    FinalFilterType,
    JoinType,
    Layer,
    RulePattern,
    SourceType,
)
from dbt_job_generator.models.mapping import (
    FinalFilter,
    JoinRelationship,
    KeyField,
    MappingEntry,
    MappingMetadata,
    MappingRule,
    MappingSpec,
    SourceEntry,
    TargetSpec,
    TypeValuePair,
    UnpivotFieldSpec,
)
from dbt_job_generator.models.errors import ErrorLocation, ParseError


# Section header keywords used to detect section boundaries
_SECTION_HEADERS = {
    "Target": "Target",
    "Input": "Input",
    "Relationship": "Relationship",
    "Mapping": "Mapping",
    "Final Filter": "Final Filter",
}

_REQUIRED_SECTIONS = {"Target", "Input", "Mapping"}

_JOIN_TYPE_MAP = {
    "LEFT JOIN": JoinType.LEFT_JOIN,
    "INNER JOIN": JoinType.INNER_JOIN,
    "RIGHT JOIN": JoinType.RIGHT_JOIN,
    "FULL OUTER JOIN": JoinType.FULL_OUTER_JOIN,
    "CROSS JOIN": JoinType.CROSS_JOIN,
}

_SOURCE_TYPE_MAP = {
    "physical_table": SourceType.PHYSICAL_TABLE,
    "unpivot_cte": SourceType.UNPIVOT_CTE,
    "derived_cte": SourceType.DERIVED_CTE,
}


def _to_snake_case(name: str) -> str:
    """Convert a table name to snake_case for the mapping name."""
    # Replace spaces with underscores
    s = name.strip().replace(" ", "_")
    # Insert underscore before uppercase letters preceded by lowercase
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


def _strip_or_none(value: str) -> Optional[str]:
    """Return stripped string or None if empty."""
    stripped = value.strip() if value else ""
    return stripped if stripped else None


class CSVParser:
    """Parses CSV mapping files into MappingSpec objects."""

    def parse(self, file_path: str) -> MappingSpec:
        """Parse a single CSV mapping file into a MappingSpec.

        Args:
            file_path: Path to the CSV mapping file.

        Returns:
            A MappingSpec representing the parsed mapping.

        Raises:
            ParseError: If the file is invalid or missing required sections.
        """
        try:
            # Read with BOM handling
            with open(file_path, "r", encoding="utf-8-sig") as f:
                content = f.read()
        except OSError as e:
            raise ParseError(
                f"Cannot read file: {e}",
                ErrorLocation(file=file_path),
            ) from e

        rows = list(csv.reader(StringIO(content)))
        sections = self._split_sections(rows, file_path)

        # Validate required sections
        for section_name in _REQUIRED_SECTIONS:
            if section_name not in sections:
                raise ParseError(
                    f"Missing required section: {section_name}",
                    ErrorLocation(file=file_path, section=section_name),
                )

        target = self._parse_target(sections["Target"], file_path)
        inputs = self._parse_inputs(sections["Input"], file_path)
        relationships = self._parse_relationships(
            sections.get("Relationship", ([], [])), file_path
        )
        mappings = self._parse_mappings(sections["Mapping"], file_path)
        final_filter = self._parse_final_filter(
            sections.get("Final Filter"), file_path
        )

        # Determine layer from target schema
        layer = self._determine_layer(target.schema)
        name = _to_snake_case(target.table_name)

        return MappingSpec(
            name=name,
            layer=layer,
            target=target,
            inputs=inputs,
            relationships=relationships,
            mappings=mappings,
            final_filter=final_filter,
            metadata=MappingMetadata(source_file=file_path),
        )

    def parse_batch(
        self, directory: str
    ) -> list[tuple[str, MappingSpec | ParseError]]:
        """Parse all CSV files in a directory.

        Args:
            directory: Path to directory containing CSV mapping files.

        Returns:
            List of (filename, MappingSpec | ParseError) tuples.
        """
        results: list[tuple[str, MappingSpec | ParseError]] = []
        try:
            files = sorted(
                f
                for f in os.listdir(directory)
                if f.lower().endswith(".csv")
            )
        except OSError as e:
            return [
                (
                    directory,
                    ParseError(
                        f"Cannot read directory: {e}",
                        ErrorLocation(file=directory),
                    ),
                )
            ]

        for filename in files:
            file_path = os.path.join(directory, filename)
            try:
                spec = self.parse(file_path)
                results.append((filename, spec))
            except ParseError as e:
                results.append((filename, e))

        return results

    # ── Section splitting ──────────────────────────────────────────

    def _split_sections(
        self, rows: list[list[str]], file_path: str
    ) -> dict[str, tuple[list[list[str]], int]]:
        """Split CSV rows into named sections.

        Returns a dict mapping section name to (data_rows, start_line).
        Each section has: header_row (section name), column_header_row, then data rows.
        We return only the data rows and the 1-based line number of the section header.
        """
        sections: dict[str, tuple[list[list[str]], int]] = {}
        current_section: Optional[str] = None
        current_rows: list[list[str]] = []
        current_start: int = 0
        skip_column_header = False

        for i, row in enumerate(rows):
            first_cell = row[0].strip() if row else ""

            # Check if this row is a section header
            detected = self._detect_section(first_cell)
            if detected is not None:
                # Save previous section
                if current_section is not None:
                    sections[current_section] = (current_rows, current_start)
                current_section = detected
                current_rows = []
                current_start = i + 1  # 1-based line number
                skip_column_header = True
                continue

            # Skip the column header row right after a section header
            if skip_column_header:
                skip_column_header = False
                continue

            # Skip empty rows
            if self._is_empty_row(row):
                continue

            if current_section is not None:
                current_rows.append(row)

        # Save last section
        if current_section is not None:
            sections[current_section] = (current_rows, current_start)

        return sections

    def _detect_section(self, first_cell: str) -> Optional[str]:
        """Detect if a cell value is a section header."""
        for key in _SECTION_HEADERS:
            if first_cell == key:
                return key
        return None

    @staticmethod
    def _is_empty_row(row: list[str]) -> bool:
        """Check if a row is effectively empty."""
        return all(cell.strip() == "" for cell in row)

    # ── Target section ─────────────────────────────────────────────

    def _parse_target(
        self, section: tuple[list[list[str]], int], file_path: str
    ) -> TargetSpec:
        """Parse the Target section into a TargetSpec."""
        rows, start_line = section
        if not rows:
            raise ParseError(
                "Target section has no data rows",
                ErrorLocation(file=file_path, line=start_line, section="Target"),
            )

        row = rows[0]
        # Columns: Database, Schema, Table Name, ETL Handle, ..., Description
        database = _strip_or_none(row[0]) if len(row) > 0 else None
        schema = _strip_or_none(row[1]) if len(row) > 1 else None
        table_name = _strip_or_none(row[2]) if len(row) > 2 else None
        etl_handle = _strip_or_none(row[3]) if len(row) > 3 else None
        description = _strip_or_none(row[7]) if len(row) > 7 else None

        if not schema:
            raise ParseError(
                "Target section missing required field: schema",
                ErrorLocation(
                    file=file_path,
                    line=start_line + 2,
                    section="Target",
                    field_name="schema",
                ),
            )
        if not table_name:
            raise ParseError(
                "Target section missing required field: table_name",
                ErrorLocation(
                    file=file_path,
                    line=start_line + 2,
                    section="Target",
                    field_name="table_name",
                ),
            )

        return TargetSpec(
            database=database or "",
            schema=schema,
            table_name=table_name,
            etl_handle=etl_handle or "",
            description=description,
        )

    # ── Input section ──────────────────────────────────────────────

    def _parse_inputs(
        self, section: tuple[list[list[str]], int], file_path: str
    ) -> list[SourceEntry]:
        """Parse the Input section into a list of SourceEntry."""
        rows, start_line = section
        entries: list[SourceEntry] = []

        for i, row in enumerate(rows):
            line_num = start_line + 2 + i  # account for header + column header
            entry = self._parse_input_row(row, line_num, file_path)
            entries.append(entry)

        return entries

    def _parse_input_row(
        self, row: list[str], line_num: int, file_path: str
    ) -> SourceEntry:
        """Parse a single Input row into a SourceEntry."""
        # Columns: #, Source Type, Schema, Table Name, Alias, Select Fields, Filter, ...
        def cell(idx: int) -> str:
            return row[idx].strip() if len(row) > idx else ""

        index_str = cell(0)
        source_type_str = cell(1)
        schema = _strip_or_none(cell(2))
        table_name = _strip_or_none(cell(3))
        alias = _strip_or_none(cell(4))
        select_fields = _strip_or_none(cell(5))
        filter_expr = _strip_or_none(cell(6))

        # Validate source_type
        if source_type_str not in _SOURCE_TYPE_MAP:
            raise ParseError(
                f"Invalid source_type: '{source_type_str}'",
                ErrorLocation(
                    file=file_path,
                    line=line_num,
                    section="Input",
                    field_name="source_type",
                ),
            )

        source_type = _SOURCE_TYPE_MAP[source_type_str]

        # Validate required fields
        if not alias:
            raise ParseError(
                "Input row missing required field: alias",
                ErrorLocation(
                    file=file_path,
                    line=line_num,
                    section="Input",
                    field_name="alias",
                ),
            )

        if source_type == SourceType.PHYSICAL_TABLE and not table_name:
            raise ParseError(
                "physical_table row missing required field: table_name",
                ErrorLocation(
                    file=file_path,
                    line=line_num,
                    section="Input",
                    field_name="table_name",
                ),
            )

        try:
            index = int(index_str)
        except (ValueError, TypeError):
            index = i if 'i' in dir() else 0  # noqa: F841

        # For unpivot_cte / derived_cte, table_name is the source alias reference
        return SourceEntry(
            index=int(index_str) if index_str.isdigit() else 0,
            source_type=source_type,
            schema=schema,
            table_name=table_name or "",
            alias=alias,
            select_fields=select_fields or "",
            filter=filter_expr,
        )

    # ── Relationship section ───────────────────────────────────────

    def _parse_relationships(
        self, section: tuple[list[list[str]], int], file_path: str
    ) -> list[JoinRelationship]:
        """Parse the Relationship section into a list of JoinRelationship."""
        rows, start_line = section
        relationships: list[JoinRelationship] = []

        for i, row in enumerate(rows):
            line_num = start_line + 2 + i

            def cell(idx: int) -> str:
                return row[idx].strip() if len(row) > idx else ""

            main_alias = _strip_or_none(cell(1))
            join_type_str = _strip_or_none(cell(2))
            join_alias = _strip_or_none(cell(3))
            join_condition = _strip_or_none(cell(4))

            if not main_alias or not join_type_str or not join_alias or not join_condition:
                continue  # Skip incomplete rows

            join_type_upper = join_type_str.upper()
            if join_type_upper not in _JOIN_TYPE_MAP:
                raise ParseError(
                    f"Invalid join_type: '{join_type_str}'",
                    ErrorLocation(
                        file=file_path,
                        line=line_num,
                        section="Relationship",
                        field_name="join_type",
                    ),
                )

            relationships.append(
                JoinRelationship(
                    main_alias=main_alias,
                    join_type=_JOIN_TYPE_MAP[join_type_upper],
                    join_alias=join_alias,
                    join_condition=join_condition,
                )
            )

        return relationships

    # ── Mapping section ────────────────────────────────────────────

    def _parse_mappings(
        self, section: tuple[list[list[str]], int], file_path: str
    ) -> list[MappingEntry]:
        """Parse the Mapping section into a list of MappingEntry."""
        rows, start_line = section
        entries: list[MappingEntry] = []

        for i, row in enumerate(rows):
            line_num = start_line + 2 + i

            def cell(idx: int) -> str:
                return row[idx].strip() if len(row) > idx else ""

            index_str = cell(0)
            target_column = _strip_or_none(cell(1))
            transformation = _strip_or_none(cell(2))
            data_type = _strip_or_none(cell(3))
            description = _strip_or_none(cell(7))

            if not target_column:
                continue  # Skip rows without target column

            if not data_type:
                raise ParseError(
                    f"Mapping row missing required field: data_type for column '{target_column}'",
                    ErrorLocation(
                        file=file_path,
                        line=line_num,
                        section="Mapping",
                        field_name="data_type",
                    ),
                )

            mapping_rule = self._detect_mapping_rule(transformation, data_type)

            entries.append(
                MappingEntry(
                    index=int(index_str) if index_str.isdigit() else 0,
                    target_column=target_column,
                    transformation=transformation,
                    data_type=data_type,
                    description=description,
                    mapping_rule=mapping_rule,
                )
            )

        return entries

    # ── Final Filter section ───────────────────────────────────────

    def _parse_final_filter(
        self,
        section: Optional[tuple[list[list[str]], int]],
        file_path: str,
    ) -> Optional[FinalFilter]:
        """Parse the Final Filter section into a FinalFilter."""
        if section is None:
            return None

        rows, start_line = section
        if not rows:
            return None

        # First data row: #, Clause Type, Expression, ...
        row = rows[0]

        def cell(idx: int) -> str:
            return row[idx].strip() if len(row) > idx else ""

        clause_type_str = _strip_or_none(cell(1))
        expression = _strip_or_none(cell(2))

        if not clause_type_str or not expression:
            return None

        if clause_type_str.upper() == "UNION ALL":
            filter_type = FinalFilterType.UNION_ALL
        elif clause_type_str.upper() == "WHERE":
            filter_type = FinalFilterType.WHERE_CLAUSE
        else:
            filter_type = FinalFilterType.WHERE_CLAUSE

        return FinalFilter(
            filter_type=filter_type,
            expression=expression,
        )

    # ── Mapping rule detection ─────────────────────────────────────

    @staticmethod
    def _detect_mapping_rule(
        transformation: Optional[str], data_type: str
    ) -> MappingRule:
        """Detect the MappingRule from a transformation expression.

        Rules (in order of precedence):
        1. Empty/None → NULL_MAP
        2. Starts with hash_id( → HASH
        3. Starts with ' and ends with ' → HARDCODE_STRING
        4. Is a number → HARDCODE_NUMERIC
        5. Contains . (alias.column) → DIRECT_MAP
        6. Otherwise → BUSINESS_LOGIC
        """
        if not transformation:
            return MappingRule(pattern=RulePattern.NULL_MAP)

        t = transformation.strip()

        if not t:
            return MappingRule(pattern=RulePattern.NULL_MAP)

        # HASH: starts with hash_id(
        if t.startswith("hash_id("):
            # Extract args from hash_id(...)
            inner = t[len("hash_id("):]
            if inner.endswith(")"):
                inner = inner[:-1]
            args = inner.strip()
            return MappingRule(
                pattern=RulePattern.HASH,
                params={"args": args},
            )

        # HARDCODE_STRING: starts and ends with single quotes
        if t.startswith("'") and t.endswith("'") and len(t) >= 2:
            value = t[1:-1]
            return MappingRule(
                pattern=RulePattern.HARDCODE_STRING,
                params={"value": value},
            )

        # HARDCODE_NUMERIC: is a number
        try:
            float(t)
            return MappingRule(
                pattern=RulePattern.HARDCODE_NUMERIC,
                params={"value": t},
            )
        except ValueError:
            pass

        # DIRECT_MAP: contains a dot (alias.column pattern)
        if "." in t:
            return MappingRule(pattern=RulePattern.DIRECT_MAP)

        # Otherwise: BUSINESS_LOGIC
        return MappingRule(pattern=RulePattern.BUSINESS_LOGIC)

    # ── Unpivot field parsing ──────────────────────────────────────

    @staticmethod
    def _parse_unpivot_fields(select_fields: str) -> UnpivotFieldSpec:
        """Parse unpivot select_fields format into UnpivotFieldSpec.

        Format: "key=source | TYPE:col | passthrough"
        - key=source → KeyField
        - TYPE:col → TypeValuePair
        - plain field → passthrough

        Raises:
            ParseError: If the format is invalid (no key field found).
        """
        parts = [p.strip() for p in select_fields.split("|")]

        key_mapping: Optional[KeyField] = None
        type_value_pairs: list[TypeValuePair] = []
        passthrough_fields: list[str] = []

        for part in parts:
            if not part:
                continue

            if "=" in part:
                # KeyField: target_name=source_name
                pieces = part.split("=", 1)
                key_mapping = KeyField(
                    target_name=pieces[0].strip(),
                    source_name=pieces[1].strip(),
                )
            elif ":" in part:
                # TypeValuePair: TYPE_CODE:source_column
                pieces = part.split(":", 1)
                type_value_pairs.append(
                    TypeValuePair(
                        type_code=pieces[0].strip(),
                        source_column=pieces[1].strip(),
                    )
                )
            else:
                # Passthrough field
                passthrough_fields.append(part.strip())

        if key_mapping is None:
            raise ParseError(
                f"Unpivot select_fields missing key field (expected 'key=source'): '{select_fields}'",
            )

        return UnpivotFieldSpec(
            key_mapping=key_mapping,
            type_value_pairs=type_value_pairs,
            passthrough_fields=passthrough_fields,
        )

    # ── Layer determination ────────────────────────────────────────

    @staticmethod
    def _determine_layer(schema: str) -> Layer:
        """Determine the Layer from the target schema."""
        s = schema.strip().lower()
        if s == "gold":
            return Layer.SILVER_TO_GOLD
        # Default to BRONZE_TO_SILVER for "silver" or any other schema
        return Layer.BRONZE_TO_SILVER
