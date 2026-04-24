"""FromClauseGenerator — generates the FROM clause with JOINs."""

from __future__ import annotations

from dbt_job_generator.models.mapping import MappingSpec


class FromClauseGenerator:
    """Generate the FROM clause from a MappingSpec.

    If no relationships: ``FROM {first_input_alias}``
    If relationships exist::

        FROM {main_alias}
            LEFT JOIN {join_alias} ON {condition}
    """

    def generate(self, mapping: MappingSpec) -> str:
        """Generate the FROM clause string.

        Args:
            mapping: The parsed mapping specification.

        Returns:
            A FROM clause string, possibly with JOIN lines.
        """
        if not mapping.relationships:
            # Fall back to the first input alias
            if mapping.inputs:
                return f"FROM {mapping.inputs[0].alias}"
            return "FROM unknown"

        # Main alias comes from the first relationship
        main_alias = mapping.relationships[0].main_alias
        lines = [f"FROM {main_alias}"]

        for rel in mapping.relationships:
            lines.append(
                f"    {rel.join_type.value} {rel.join_alias} "
                f"ON {rel.join_condition}"
            )

        return "\n".join(lines)
