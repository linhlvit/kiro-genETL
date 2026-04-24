"""Change Manager for dbt Job Generator.

Handles incremental change detection between mapping versions
and downstream impact analysis.
"""

from __future__ import annotations

from dbt_job_generator.models.change import (
    InputModification,
    MappingDiff,
    MappingModification,
)
from dbt_job_generator.models.dag import DependencyDAG
from dbt_job_generator.models.mapping import MappingSpec


class ChangeManager:
    """Compares mapping versions and detects downstream impact."""

    def diff(self, old_mapping: MappingSpec, new_mapping: MappingSpec) -> MappingDiff:
        """Compare two MappingSpecs and produce a MappingDiff.

        Detects added/removed/modified inputs and mappings,
        as well as relationship and final_filter changes.
        """
        result = MappingDiff(mapping_name=new_mapping.name)

        # --- Inputs diff (keyed by index) ---
        old_inputs = {e.index: e for e in old_mapping.inputs}
        new_inputs = {e.index: e for e in new_mapping.inputs}

        old_indices = set(old_inputs.keys())
        new_indices = set(new_inputs.keys())

        for idx in sorted(new_indices - old_indices):
            result.added_inputs.append(new_inputs[idx])

        for idx in sorted(old_indices - new_indices):
            result.removed_inputs.append(old_inputs[idx])

        for idx in sorted(old_indices & new_indices):
            if old_inputs[idx] != new_inputs[idx]:
                result.modified_inputs.append(
                    InputModification(
                        index=idx,
                        old_entry=old_inputs[idx],
                        new_entry=new_inputs[idx],
                    )
                )

        # --- Mappings diff (keyed by target_column) ---
        old_mappings = {e.target_column: e for e in old_mapping.mappings}
        new_mappings = {e.target_column: e for e in new_mapping.mappings}

        old_cols = set(old_mappings.keys())
        new_cols = set(new_mappings.keys())

        for col in sorted(new_cols - old_cols):
            result.added_mappings.append(new_mappings[col])

        for col in sorted(old_cols - new_cols):
            result.removed_mappings.append(old_mappings[col])

        for col in sorted(old_cols & new_cols):
            if old_mappings[col] != new_mappings[col]:
                result.modified_mappings.append(
                    MappingModification(
                        target_column=col,
                        old_entry=old_mappings[col],
                        new_entry=new_mappings[col],
                    )
                )

        # --- Relationships changed ---
        result.relationships_changed = (
            old_mapping.relationships != new_mapping.relationships
        )

        # --- Final filter changed ---
        result.final_filter_changed = (
            old_mapping.final_filter != new_mapping.final_filter
        )

        return result

    def detect_downstream_impact(
        self, change: MappingDiff, dag: DependencyDAG
    ) -> list[str]:
        """Find all downstream models affected by a change.

        Traverses the DAG from the changed model to find all direct
        and transitive dependents (models that depend on the changed model).
        """
        start = change.mapping_name
        if start not in dag.nodes:
            return []

        visited: set[str] = set()
        queue: list[str] = [start]

        while queue:
            current = queue.pop(0)
            dependents = dag.get_dependents(current)
            for dep in dependents:
                if dep not in visited:
                    visited.add(dep)
                    queue.append(dep)

        return sorted(visited)
