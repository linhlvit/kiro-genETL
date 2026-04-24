"""Dependency DAG Builder for dbt Job Generator.

Analyzes mapping specifications to build a dependency graph,
detect cycles, and produce topological execution order.
"""

from __future__ import annotations

from dbt_job_generator.models.dag import DAGEdge, DAGNode, DependencyDAG
from dbt_job_generator.models.enums import SourceType
from dbt_job_generator.models.errors import DAGError
from dbt_job_generator.models.mapping import MappingSpec


class DependencyDAGBuilder:
    """Builds a dependency DAG from a list of MappingSpecs."""

    def build(self, mappings: list[MappingSpec]) -> DependencyDAG:
        """Build a DependencyDAG from mapping specifications.

        For each mapping, creates a DAGNode with physical_table sources.
        Creates edges when a physical_table source matches another mapping's name.
        Raises DAGError if a cycle is detected.
        """
        dag = DependencyDAG()

        # Index mapping names for fast lookup
        mapping_names: set[str] = {m.name for m in mappings}

        # Create nodes
        for mapping in mappings:
            physical_sources = [
                entry.table_name
                for entry in mapping.inputs
                if entry.source_type == SourceType.PHYSICAL_TABLE
            ]
            node = DAGNode(
                model_name=mapping.name,
                layer=mapping.layer,
                physical_sources=physical_sources,
            )
            dag.add_node(node)

        # Create edges: if a physical_table source matches another mapping's name
        for mapping in mappings:
            for entry in mapping.inputs:
                if (
                    entry.source_type == SourceType.PHYSICAL_TABLE
                    and entry.table_name in mapping_names
                    and entry.table_name != mapping.name
                ):
                    edge = DAGEdge(
                        from_model=mapping.name,
                        to_model=entry.table_name,
                    )
                    dag.add_edge(edge)

        # Detect cycles and raise if found
        cycles = self.detect_cycles(dag)
        if cycles:
            cycle = cycles[0]
            raise DAGError(
                f"Circular dependency detected: {' -> '.join(cycle)}",
                cycle=cycle,
            )

        return dag

    def detect_cycles(self, dag: DependencyDAG) -> list[list[str]]:
        """Detect all cycles in the DAG using DFS.

        Returns a list of cycles, where each cycle is a list of model names.
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color: dict[str, int] = {name: WHITE for name in dag.nodes}
        # Build adjacency list: node -> list of nodes it depends on
        adj: dict[str, list[str]] = {name: [] for name in dag.nodes}
        for edge in dag.edges:
            if edge.from_model in adj:
                adj[edge.from_model].append(edge.to_model)

        cycles: list[list[str]] = []
        path: list[str] = []

        def dfs(node: str) -> None:
            color[node] = GRAY
            path.append(node)
            for neighbor in adj.get(node, []):
                if neighbor not in color:
                    continue
                if color[neighbor] == GRAY:
                    # Found a cycle — extract it from path
                    cycle_start = path.index(neighbor)
                    cycle = path[cycle_start:] + [neighbor]
                    cycles.append(cycle)
                elif color[neighbor] == WHITE:
                    dfs(neighbor)
            path.pop()
            color[node] = BLACK

        for node in dag.nodes:
            if color[node] == WHITE:
                dfs(node)

        return cycles

    def topological_sort(self, dag: DependencyDAG) -> list[str]:
        """Return models in execution order (dependencies first).

        Raises DAGError if a cycle exists.
        """
        cycles = self.detect_cycles(dag)
        if cycles:
            cycle = cycles[0]
            raise DAGError(
                f"Cannot topologically sort: circular dependency detected: "
                f"{' -> '.join(cycle)}",
                cycle=cycle,
            )

        # Kahn's algorithm
        in_degree: dict[str, int] = {name: 0 for name in dag.nodes}
        adj: dict[str, list[str]] = {name: [] for name in dag.nodes}
        for edge in dag.edges:
            if edge.to_model in in_degree:
                adj[edge.to_model].append(edge.from_model)
            if edge.from_model in in_degree:
                in_degree[edge.from_model] += 1

        queue: list[str] = sorted(
            [name for name, deg in in_degree.items() if deg == 0]
        )
        result: list[str] = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            for dependent in sorted(adj.get(node, [])):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        return result
