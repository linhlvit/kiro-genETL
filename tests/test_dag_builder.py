"""Tests for DependencyDAGBuilder."""

import pytest

from dbt_job_generator.dag.dag_builder import DependencyDAGBuilder
from dbt_job_generator.models.dag import DAGEdge, DAGNode, DependencyDAG
from dbt_job_generator.models.enums import (
    FinalFilterType,
    Layer,
    RulePattern,
    SourceType,
)
from dbt_job_generator.models.mapping import (
    FinalFilter,
    MappingEntry,
    MappingMetadata,
    MappingRule,
    MappingSpec,
    SourceEntry,
    TargetSpec,
)
from dbt_job_generator.models.errors import DAGError


def _make_target(name: str = "test_table") -> TargetSpec:
    return TargetSpec(
        database="db",
        schema="silver",
        table_name=name,
        etl_handle="etl_test",
    )


def _make_mapping(
    name: str,
    layer: Layer = Layer.BRONZE_TO_SILVER,
    inputs: list[SourceEntry] | None = None,
) -> MappingSpec:
    return MappingSpec(
        name=name,
        layer=layer,
        target=_make_target(name),
        inputs=inputs or [],
    )


def _physical_input(
    index: int, table_name: str, alias: str, schema: str = "bronze"
) -> SourceEntry:
    return SourceEntry(
        index=index,
        source_type=SourceType.PHYSICAL_TABLE,
        schema=schema,
        table_name=table_name,
        alias=alias,
        select_fields="*",
    )


def _derived_input(index: int, table_name: str, alias: str) -> SourceEntry:
    return SourceEntry(
        index=index,
        source_type=SourceType.DERIVED_CTE,
        schema=None,
        table_name=table_name,
        alias=alias,
        select_fields="col1, col2",
    )


class TestDependencyDAGBuilderBuild:
    """Tests for DependencyDAGBuilder.build()."""

    def test_single_mapping_no_dependencies(self):
        builder = DependencyDAGBuilder()
        m = _make_mapping("model_a", inputs=[
            _physical_input(1, "raw_table", "rt"),
        ])
        dag = builder.build([m])

        assert "model_a" in dag.nodes
        assert len(dag.nodes) == 1
        assert dag.edges == []
        node = dag.nodes["model_a"]
        assert node.physical_sources == ["raw_table"]

    def test_two_independent_mappings(self):
        builder = DependencyDAGBuilder()
        m1 = _make_mapping("model_a", inputs=[
            _physical_input(1, "raw_table_1", "rt1"),
        ])
        m2 = _make_mapping("model_b", inputs=[
            _physical_input(1, "raw_table_2", "rt2"),
        ])
        dag = builder.build([m1, m2])

        assert len(dag.nodes) == 2
        assert dag.edges == []

    def test_dependency_edge_created(self):
        """model_b depends on model_a (physical_table source = model_a)."""
        builder = DependencyDAGBuilder()
        m_a = _make_mapping("model_a", inputs=[
            _physical_input(1, "raw_table", "rt"),
        ])
        m_b = _make_mapping("model_b", layer=Layer.SILVER_TO_GOLD, inputs=[
            _physical_input(1, "model_a", "ma"),
        ])
        dag = builder.build([m_a, m_b])

        assert len(dag.nodes) == 2
        assert len(dag.edges) == 1
        edge = dag.edges[0]
        assert edge.from_model == "model_b"
        assert edge.to_model == "model_a"

    def test_multiple_dependencies(self):
        """model_c depends on both model_a and model_b."""
        builder = DependencyDAGBuilder()
        m_a = _make_mapping("model_a", inputs=[
            _physical_input(1, "raw_1", "r1"),
        ])
        m_b = _make_mapping("model_b", inputs=[
            _physical_input(1, "raw_2", "r2"),
        ])
        m_c = _make_mapping("model_c", layer=Layer.SILVER_TO_GOLD, inputs=[
            _physical_input(1, "model_a", "ma"),
            _physical_input(2, "model_b", "mb"),
        ])
        dag = builder.build([m_a, m_b, m_c])

        assert len(dag.nodes) == 3
        assert len(dag.edges) == 2
        deps = dag.get_dependencies("model_c")
        assert set(deps) == {"model_a", "model_b"}

    def test_only_physical_table_sources_in_node(self):
        """DAGNode.physical_sources should only contain physical_table entries."""
        builder = DependencyDAGBuilder()
        m = _make_mapping("model_a", inputs=[
            _physical_input(1, "raw_table", "rt"),
            _derived_input(2, "rt", "derived_rt"),
        ])
        dag = builder.build([m])

        node = dag.nodes["model_a"]
        assert node.physical_sources == ["raw_table"]

    def test_no_self_dependency_edge(self):
        """A mapping referencing its own name should not create a self-edge."""
        builder = DependencyDAGBuilder()
        m = _make_mapping("model_a", inputs=[
            _physical_input(1, "model_a", "self_ref"),
        ])
        dag = builder.build([m])

        assert dag.edges == []

    def test_circular_dependency_raises_dag_error(self):
        """Two mappings depending on each other should raise DAGError."""
        builder = DependencyDAGBuilder()
        m_a = _make_mapping("model_a", inputs=[
            _physical_input(1, "model_b", "mb"),
        ])
        m_b = _make_mapping("model_b", inputs=[
            _physical_input(1, "model_a", "ma"),
        ])
        with pytest.raises(DAGError) as exc_info:
            builder.build([m_a, m_b])
        assert exc_info.value.cycle is not None

    def test_three_node_cycle_raises_dag_error(self):
        """A -> B -> C -> A should raise DAGError."""
        builder = DependencyDAGBuilder()
        m_a = _make_mapping("a", inputs=[_physical_input(1, "c", "c_alias")])
        m_b = _make_mapping("b", inputs=[_physical_input(1, "a", "a_alias")])
        m_c = _make_mapping("c", inputs=[_physical_input(1, "b", "b_alias")])
        with pytest.raises(DAGError):
            builder.build([m_a, m_b, m_c])

    def test_empty_mappings(self):
        builder = DependencyDAGBuilder()
        dag = builder.build([])
        assert dag.nodes == {}
        assert dag.edges == []

    def test_chain_dependency(self):
        """a -> b -> c (linear chain)."""
        builder = DependencyDAGBuilder()
        m_a = _make_mapping("a", inputs=[_physical_input(1, "raw", "r")])
        m_b = _make_mapping("b", inputs=[_physical_input(1, "a", "a_alias")])
        m_c = _make_mapping("c", inputs=[_physical_input(1, "b", "b_alias")])
        dag = builder.build([m_a, m_b, m_c])

        assert len(dag.edges) == 2
        assert dag.get_dependencies("c") == ["b"]
        assert dag.get_dependencies("b") == ["a"]
        assert dag.get_dependencies("a") == []


class TestDetectCycles:
    """Tests for DependencyDAGBuilder.detect_cycles()."""

    def test_no_cycles_in_acyclic_dag(self):
        builder = DependencyDAGBuilder()
        dag = DependencyDAG()
        dag.add_node(DAGNode("a", Layer.BRONZE_TO_SILVER, []))
        dag.add_node(DAGNode("b", Layer.BRONZE_TO_SILVER, []))
        dag.add_edge(DAGEdge("b", "a"))

        cycles = builder.detect_cycles(dag)
        assert cycles == []

    def test_detects_simple_cycle(self):
        builder = DependencyDAGBuilder()
        dag = DependencyDAG()
        dag.add_node(DAGNode("a", Layer.BRONZE_TO_SILVER, []))
        dag.add_node(DAGNode("b", Layer.BRONZE_TO_SILVER, []))
        dag.add_edge(DAGEdge("a", "b"))
        dag.add_edge(DAGEdge("b", "a"))

        cycles = builder.detect_cycles(dag)
        assert len(cycles) >= 1

    def test_detects_three_node_cycle(self):
        builder = DependencyDAGBuilder()
        dag = DependencyDAG()
        for name in ["a", "b", "c"]:
            dag.add_node(DAGNode(name, Layer.BRONZE_TO_SILVER, []))
        dag.add_edge(DAGEdge("a", "b"))
        dag.add_edge(DAGEdge("b", "c"))
        dag.add_edge(DAGEdge("c", "a"))

        cycles = builder.detect_cycles(dag)
        assert len(cycles) >= 1

    def test_empty_dag_no_cycles(self):
        builder = DependencyDAGBuilder()
        dag = DependencyDAG()
        cycles = builder.detect_cycles(dag)
        assert cycles == []

    def test_single_node_no_cycles(self):
        builder = DependencyDAGBuilder()
        dag = DependencyDAG()
        dag.add_node(DAGNode("a", Layer.BRONZE_TO_SILVER, []))
        cycles = builder.detect_cycles(dag)
        assert cycles == []


class TestTopologicalSort:
    """Tests for DependencyDAGBuilder.topological_sort()."""

    def test_single_node(self):
        builder = DependencyDAGBuilder()
        dag = DependencyDAG()
        dag.add_node(DAGNode("a", Layer.BRONZE_TO_SILVER, []))

        order = builder.topological_sort(dag)
        assert order == ["a"]

    def test_linear_chain(self):
        """a <- b <- c means c depends on b, b depends on a."""
        builder = DependencyDAGBuilder()
        dag = DependencyDAG()
        for name in ["a", "b", "c"]:
            dag.add_node(DAGNode(name, Layer.BRONZE_TO_SILVER, []))
        dag.add_edge(DAGEdge("b", "a"))
        dag.add_edge(DAGEdge("c", "b"))

        order = builder.topological_sort(dag)
        assert order.index("a") < order.index("b")
        assert order.index("b") < order.index("c")

    def test_diamond_dependency(self):
        """a <- b, a <- c, b <- d, c <- d."""
        builder = DependencyDAGBuilder()
        dag = DependencyDAG()
        for name in ["a", "b", "c", "d"]:
            dag.add_node(DAGNode(name, Layer.BRONZE_TO_SILVER, []))
        dag.add_edge(DAGEdge("b", "a"))
        dag.add_edge(DAGEdge("c", "a"))
        dag.add_edge(DAGEdge("d", "b"))
        dag.add_edge(DAGEdge("d", "c"))

        order = builder.topological_sort(dag)
        assert order.index("a") < order.index("b")
        assert order.index("a") < order.index("c")
        assert order.index("b") < order.index("d")
        assert order.index("c") < order.index("d")

    def test_independent_nodes(self):
        builder = DependencyDAGBuilder()
        dag = DependencyDAG()
        for name in ["x", "y", "z"]:
            dag.add_node(DAGNode(name, Layer.BRONZE_TO_SILVER, []))

        order = builder.topological_sort(dag)
        assert set(order) == {"x", "y", "z"}

    def test_cycle_raises_dag_error(self):
        builder = DependencyDAGBuilder()
        dag = DependencyDAG()
        dag.add_node(DAGNode("a", Layer.BRONZE_TO_SILVER, []))
        dag.add_node(DAGNode("b", Layer.BRONZE_TO_SILVER, []))
        dag.add_edge(DAGEdge("a", "b"))
        dag.add_edge(DAGEdge("b", "a"))

        with pytest.raises(DAGError):
            builder.topological_sort(dag)

    def test_empty_dag(self):
        builder = DependencyDAGBuilder()
        dag = DependencyDAG()
        order = builder.topological_sort(dag)
        assert order == []

    def test_all_nodes_present_in_result(self):
        builder = DependencyDAGBuilder()
        dag = DependencyDAG()
        for name in ["a", "b", "c", "d"]:
            dag.add_node(DAGNode(name, Layer.BRONZE_TO_SILVER, []))
        dag.add_edge(DAGEdge("c", "a"))
        dag.add_edge(DAGEdge("d", "b"))

        order = builder.topological_sort(dag)
        assert set(order) == {"a", "b", "c", "d"}
        assert len(order) == 4
