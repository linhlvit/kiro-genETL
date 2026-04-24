"""ModelAssembler — assembles a complete dbt model .sql file."""

from __future__ import annotations

from typing import Optional

from dbt_job_generator.models.generation import CTEDefinition, SelectColumn
from dbt_job_generator.models.mapping import FinalFilter
from dbt_job_generator.models.enums import FinalFilterType


class ModelAssembler:
    """Assemble a complete dbt model .sql file from its parts.

    Assembly order:
        1. Config Block
        2. WITH clause (all CTEs)
        3. Main SELECT (all columns with AS alias)
        4. FROM clause (main alias + JOINs)
        5. Final Filter (UNION ALL or WHERE, if present)
    """

    def assemble(
        self,
        config_block: str,
        cte_definitions: list[CTEDefinition],
        select_columns: list[SelectColumn],
        from_clause: str,
        final_filter: Optional[FinalFilter] = None,
    ) -> str:
        """Assemble the complete .sql content.

        Args:
            config_block: The ``{{ config(...) }}`` string.
            cte_definitions: List of CTE definitions for the WITH clause.
            select_columns: List of SELECT column expressions.
            from_clause: The FROM clause (with optional JOINs).
            final_filter: Optional final filter (UNION ALL or WHERE).

        Returns:
            Complete .sql file content as a string.
        """
        parts: list[str] = []

        # 1. Config block
        parts.append(config_block)
        parts.append("")  # blank line

        # 2. WITH clause
        if cte_definitions:
            parts.append(self._build_with_clause(cte_definitions))
            parts.append("")  # blank line

        # 3. Main SELECT + 4. FROM + 5. Final Filter
        if final_filter and final_filter.filter_type == FinalFilterType.UNION_ALL:
            parts.append(self._build_select_block(select_columns, from_clause))
            parts.append(final_filter.expression)
        elif final_filter and final_filter.filter_type == FinalFilterType.WHERE_CLAUSE:
            parts.append(self._build_select_block(select_columns, from_clause))
            parts.append(f"WHERE {final_filter.expression}")
        else:
            parts.append(self._build_select_block(select_columns, from_clause))

        # Trailing semicolon
        parts.append(";")

        return "\n".join(parts) + "\n"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_with_clause(self, ctes: list[CTEDefinition]) -> str:
        """Build the WITH ... clause from CTE definitions."""
        blocks: list[str] = []
        for cte in ctes:
            indented_body = self._indent(cte.sql_body, spaces=4)
            blocks.append(f"{cte.alias} AS (\n{indented_body}\n)")

        # First CTE prefixed with WITH, rest separated by comma + blank line
        result = "WITH " + (",\n\n".join(blocks))
        return result

    def _build_select_block(
        self,
        columns: list[SelectColumn],
        from_clause: str,
    ) -> str:
        """Build the SELECT ... FROM ... block."""
        if not columns:
            return f"SELECT *\n{from_clause}"

        # Find max expression length for alignment
        max_expr_len = max(len(c.expression) for c in columns)

        col_lines: list[str] = []
        for col in columns:
            padding = " " * (max_expr_len - len(col.expression))
            col_lines.append(f"    {col.expression}{padding} AS {col.target_alias}")

        select_part = "SELECT\n" + ",\n".join(col_lines)
        return f"{select_part}\n{from_clause}"

    @staticmethod
    def _indent(text: str, spaces: int = 4) -> str:
        """Indent each line of text by the given number of spaces."""
        prefix = " " * spaces
        lines = text.split("\n")
        return "\n".join(prefix + line if line.strip() else line for line in lines)
