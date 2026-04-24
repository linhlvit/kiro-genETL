"""CTE Pipeline Builder for dbt Job Generator.

Builds the WITH clause (CTE definitions) from the Input section of a mapping.
Supports three source types: physical_table, unpivot_cte, derived_cte.
"""

from __future__ import annotations

from dbt_job_generator.models.enums import SourceType
from dbt_job_generator.models.generation import CTEDefinition
from dbt_job_generator.models.mapping import SourceEntry
from dbt_job_generator.parser.csv_parser import CSVParser


class PhysicalTableCTEBuilder:
    """Builds CTE for physical_table entries.

    Generates: SELECT select_fields FROM schema.table_name [WHERE filter]
    """

    def build(self, entry: SourceEntry) -> CTEDefinition:
        source_ref = (
            f"{entry.schema}.{entry.table_name}"
            if entry.schema
            else entry.table_name
        )
        sql_parts = [f"SELECT {entry.select_fields}", f"FROM {source_ref}"]
        if entry.filter:
            sql_parts.append(f"WHERE {entry.filter}")

        return CTEDefinition(
            alias=entry.alias,
            sql_body="\n".join(sql_parts),
            source_type=SourceType.PHYSICAL_TABLE,
            depends_on=[],
        )


class UnpivotCTEBuilder:
    """Builds CTE for unpivot_cte entries.

    Parses select_fields using CSVParser._parse_unpivot_fields() to get
    UnpivotFieldSpec, then generates N UNION ALL blocks — one per TypeValuePair.
    """

    def build(self, entry: SourceEntry) -> CTEDefinition:
        spec = CSVParser._parse_unpivot_fields(entry.select_fields)
        source_alias = entry.table_name
        blocks: list[str] = []

        for tvp in spec.type_value_pairs:
            # Build SELECT columns
            cols: list[str] = []
            # Key field mapping: source AS target
            cols.append(f"{spec.key_mapping.source_name} AS {spec.key_mapping.target_name}")
            # Type code literal
            cols.append(f"'{tvp.type_code}' AS type_code")
            # Source column AS address_value
            cols.append(f"{tvp.source_column} AS address_value")
            # Passthrough fields
            for pf in spec.passthrough_fields:
                cols.append(pf)

            select_clause = ",\n    ".join(cols)
            block_parts = [
                f"SELECT\n    {select_clause}",
                f"FROM {source_alias}",
            ]

            # Build WHERE clause: source_column IS NOT NULL
            block_parts.append(f"WHERE {tvp.source_column} IS NOT NULL")

            blocks.append("\n".join(block_parts))

        sql_body = "\nUNION ALL\n".join(blocks)

        return CTEDefinition(
            alias=entry.alias,
            sql_body=sql_body,
            source_type=SourceType.UNPIVOT_CTE,
            depends_on=[source_alias],
        )


class DerivedCTEBuilder:
    """Builds CTE for derived_cte entries.

    Generates: SELECT select_fields FROM source_alias [GROUP BY / filter clause]
    """

    def build(self, entry: SourceEntry) -> CTEDefinition:
        source_alias = entry.table_name
        sql_parts = [f"SELECT {entry.select_fields}", f"FROM {source_alias}"]
        if entry.filter:
            sql_parts.append(entry.filter)

        return CTEDefinition(
            alias=entry.alias,
            sql_body="\n".join(sql_parts),
            source_type=SourceType.DERIVED_CTE,
            depends_on=[source_alias],
        )


class CTEPipelineBuilder:
    """Builds the WITH clause (CTE definitions) from source entries.

    Dispatches each SourceEntry to the appropriate builder based on source_type.
    """

    def __init__(self) -> None:
        self._physical = PhysicalTableCTEBuilder()
        self._unpivot = UnpivotCTEBuilder()
        self._derived = DerivedCTEBuilder()

    def build(self, inputs: list[SourceEntry]) -> list[CTEDefinition]:
        """Build CTE definitions from source entries.

        Args:
            inputs: List of SourceEntry from the Input section.

        Returns:
            List of CTEDefinition in the same order as inputs.
        """
        ctes: list[CTEDefinition] = []
        for entry in inputs:
            if entry.source_type == SourceType.PHYSICAL_TABLE:
                ctes.append(self._physical.build(entry))
            elif entry.source_type == SourceType.UNPIVOT_CTE:
                ctes.append(self._unpivot.build(entry))
            elif entry.source_type == SourceType.DERIVED_CTE:
                ctes.append(self._derived.build(entry))
        return ctes
