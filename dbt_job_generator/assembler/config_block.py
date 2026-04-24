"""ConfigBlockGenerator — generates the dbt config block string."""

from __future__ import annotations

from typing import Optional

from dbt_job_generator.models.enums import Layer
from dbt_job_generator.models.generation import ConfigBlock
from dbt_job_generator.models.mapping import MappingSpec


class ConfigBlockGenerator:
    """Generate the dbt config block from a MappingSpec.

    Output format:
        {{ config(materialized='table', schema='silver', tags=['SCD4A']) }}

    - schema = 'silver' for BRONZE_TO_SILVER, 'gold' for SILVER_TO_GOLD
    - tags include the etl_handle from target
    - materialization defaults to 'table' but can be overridden by template
    """

    _LAYER_SCHEMA = {
        Layer.BRONZE_TO_SILVER: "silver",
        Layer.SILVER_TO_GOLD: "gold",
    }

    def generate(
        self,
        mapping: MappingSpec,
        template: Optional[dict] = None,
    ) -> str:
        """Generate the config block string.

        Args:
            mapping: The parsed mapping specification.
            template: Optional template dict that can override materialization
                      and add custom config keys.

        Returns:
            A string like ``{{ config(materialized='table', schema='silver', tags=['X']) }}``
        """
        template = template or {}

        materialization = template.get("materialization", "table")
        schema = self._LAYER_SCHEMA.get(mapping.layer, "silver")
        tags = [mapping.target.etl_handle]

        parts = [
            f"materialized='{materialization}'",
            f"schema='{schema}'",
            f"tags=['{tags[0]}']",
        ]

        return "{{ config(" + ", ".join(parts) + ") }}"
