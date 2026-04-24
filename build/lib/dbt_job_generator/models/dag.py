"""Dependency DAG data models for dbt Job Generator."""

from __future__ import annotations

from dataclasses import dataclass, field

from dbt_job_generator.models.enums import Layer


@dataclass(frozen=True)
class DAGNode:
    """A node in the dependency DAG."""
    model_name: str
    layer: Layer
    physical_sources: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DAGEdge:
    """An edge in the dependency DAG."""
    from_model: str  # Model that depends
    to_model: str    # Model being depended on


@dataclass
class DependencyDAG:
    """Dependency graph of dbt models."""
    nodes: dict[str, DAGNode] = field(default_factory=dict)
    edges: list[DAGEdge] = field(default_factory=list)

    def add_node(self, node: DAGNode) -> None:
        """Add a node to the DAG."""
        self.nodes[node.model_name] = node

    def add_edge(self, edge: DAGEdge) -> None:
        """Add an edge to the DAG."""
        self.edges.append(edge)

    def get_dependencies(self, model_name: str) -> list[str]:
        """Get all models that the given model depends on."""
        return [e.to_model for e in self.edges if e.from_model == model_name]

    def get_dependents(self, model_name: str) -> list[str]:
        """Get all models that depend on the given model."""
        return [e.from_model for e in self.edges if e.to_model == model_name]
