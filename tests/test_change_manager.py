"""Tests for ChangeManager and VersionStore."""

from __future__ import annotations

import pytest

from dbt_job_generator.change_manager import ChangeManager, VersionStore
from dbt_job_generator.models.change import MappingDiff
from dbt_job_generator.models.dag import DAGEdge, DAGNode, DependencyDAG
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
    MappingEntry,
    MappingMetadata,
    MappingRule,
    MappingSpec,
    SourceEntry,
    TargetSpec,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _target() -> TargetSpec:
    return TargetSpec(
        database="lake",
        schema="silver",
        table_name="test_table",
        etl_handle="etl_test",
    )


def _source(index: int = 1, table: str = "src_table", alias: str = "src") -> SourceEntry:
    return SourceEntry(
        index=index,
        source_type=SourceType.PHYSICAL_TABLE,
        schema="bronze",
        table_name=table,
        alias=alias,
        select_fields="*",
    )


def _mapping_entry(
    index: int = 1,
    target_col: str = "col_a",
    transformation: str = "src.col_a",
    data_type: str = "string",
    pattern: RulePattern = RulePattern.DIRECT_MAP,
) -> MappingEntry:
    return MappingEntry(
        index=index,
        target_column=target_col,
        transformation=transformation,
        data_type=data_type,
        description=None,
        mapping_rule=MappingRule(pattern=pattern),
    )


def _spec(
    name: str = "model_a",
    inputs: list[SourceEntry] | None = None,
    mappings: list[MappingEntry] | None = None,
    relationships: list[JoinRelationship] | None = None,
    final_filter: FinalFilter | None = None,
) -> MappingSpec:
    return MappingSpec(
        name=name,
        layer=Layer.BRONZE_TO_SILVER,
        target=_target(),
        inputs=inputs or [_source()],
        mappings=mappings or [_mapping_entry()],
        relationships=relationships or [],
        final_filter=final_filter,
    )


# ---------------------------------------------------------------------------
# ChangeManager.diff tests
# ---------------------------------------------------------------------------

class TestChangeManagerDiff:
    def setup_method(self) -> None:
        self.cm = ChangeManager()

    def test_identical_mappings_produce_empty_diff(self) -> None:
        spec = _spec()
        diff = self.cm.diff(spec, spec)
        assert diff.added_inputs == []
        assert diff.removed_inputs == []
        assert diff.modified_inputs == []
        assert diff.added_mappings == []
        assert diff.removed_mappings == []
        assert diff.modified_mappings == []
        assert diff.relationships_changed is False
        assert diff.final_filter_changed is False

    def test_added_input_detected(self) -> None:
        old = _spec(inputs=[_source(index=1)])
        new_input = _source(index=2, table="new_table", alias="new_src")
        new = _spec(inputs=[_source(index=1), new_input])
        diff = self.cm.diff(old, new)
        assert len(diff.added_inputs) == 1
        assert diff.added_inputs[0].table_name == "new_table"
        assert diff.removed_inputs == []

    def test_removed_input_detected(self) -> None:
        old = _spec(inputs=[_source(index=1), _source(index=2, table="t2", alias="a2")])
        new = _spec(inputs=[_source(index=1)])
        diff = self.cm.diff(old, new)
        assert len(diff.removed_inputs) == 1
        assert diff.removed_inputs[0].table_name == "t2"

    def test_modified_input_detected(self) -> None:
        old_input = _source(index=1, table="old_table", alias="src")
        new_input = _source(index=1, table="new_table", alias="src")
        old = _spec(inputs=[old_input])
        new = _spec(inputs=[new_input])
        diff = self.cm.diff(old, new)
        assert len(diff.modified_inputs) == 1
        assert diff.modified_inputs[0].old_entry.table_name == "old_table"
        assert diff.modified_inputs[0].new_entry.table_name == "new_table"

    def test_added_mapping_detected(self) -> None:
        m1 = _mapping_entry(index=1, target_col="col_a")
        m2 = _mapping_entry(index=2, target_col="col_b")
        old = _spec(mappings=[m1])
        new = _spec(mappings=[m1, m2])
        diff = self.cm.diff(old, new)
        assert len(diff.added_mappings) == 1
        assert diff.added_mappings[0].target_column == "col_b"

    def test_removed_mapping_detected(self) -> None:
        m1 = _mapping_entry(index=1, target_col="col_a")
        m2 = _mapping_entry(index=2, target_col="col_b")
        old = _spec(mappings=[m1, m2])
        new = _spec(mappings=[m1])
        diff = self.cm.diff(old, new)
        assert len(diff.removed_mappings) == 1
        assert diff.removed_mappings[0].target_column == "col_b"

    def test_modified_mapping_detected(self) -> None:
        old_m = _mapping_entry(index=1, target_col="col_a", data_type="string")
        new_m = _mapping_entry(index=1, target_col="col_a", data_type="bigint")
        old = _spec(mappings=[old_m])
        new = _spec(mappings=[new_m])
        diff = self.cm.diff(old, new)
        assert len(diff.modified_mappings) == 1
        assert diff.modified_mappings[0].old_entry.data_type == "string"
        assert diff.modified_mappings[0].new_entry.data_type == "bigint"

    def test_relationship_change_detected(self) -> None:
        rel = JoinRelationship(
            main_alias="a",
            join_type=JoinType.LEFT_JOIN,
            join_alias="b",
            join_condition="a.id = b.id",
        )
        old = _spec(relationships=[])
        new = _spec(relationships=[rel])
        diff = self.cm.diff(old, new)
        assert diff.relationships_changed is True

    def test_final_filter_change_detected(self) -> None:
        ff = FinalFilter(filter_type=FinalFilterType.WHERE_CLAUSE, expression="x > 1")
        old = _spec(final_filter=None)
        new = _spec(final_filter=ff)
        diff = self.cm.diff(old, new)
        assert diff.final_filter_changed is True

    def test_no_change_in_relationships(self) -> None:
        rel = JoinRelationship(
            main_alias="a",
            join_type=JoinType.LEFT_JOIN,
            join_alias="b",
            join_condition="a.id = b.id",
        )
        spec = _spec(relationships=[rel])
        diff = self.cm.diff(spec, spec)
        assert diff.relationships_changed is False


# ---------------------------------------------------------------------------
# ChangeManager.detect_downstream_impact tests
# ---------------------------------------------------------------------------

class TestDetectDownstreamImpact:
    def setup_method(self) -> None:
        self.cm = ChangeManager()

    def _build_dag(self) -> DependencyDAG:
        """Build a simple DAG: A -> B -> C, A -> D."""
        dag = DependencyDAG()
        dag.add_node(DAGNode(model_name="A", layer=Layer.BRONZE_TO_SILVER))
        dag.add_node(DAGNode(model_name="B", layer=Layer.BRONZE_TO_SILVER))
        dag.add_node(DAGNode(model_name="C", layer=Layer.SILVER_TO_GOLD))
        dag.add_node(DAGNode(model_name="D", layer=Layer.SILVER_TO_GOLD))
        # B depends on A, C depends on B, D depends on A
        dag.add_edge(DAGEdge(from_model="B", to_model="A"))
        dag.add_edge(DAGEdge(from_model="C", to_model="B"))
        dag.add_edge(DAGEdge(from_model="D", to_model="A"))
        return dag

    def test_direct_and_transitive_dependents(self) -> None:
        dag = self._build_dag()
        diff = MappingDiff(mapping_name="A")
        result = self.cm.detect_downstream_impact(diff, dag)
        # B and D depend on A directly; C depends on B transitively
        assert result == ["B", "C", "D"]

    def test_leaf_node_has_no_downstream(self) -> None:
        dag = self._build_dag()
        diff = MappingDiff(mapping_name="C")
        result = self.cm.detect_downstream_impact(diff, dag)
        assert result == []

    def test_middle_node_downstream(self) -> None:
        dag = self._build_dag()
        diff = MappingDiff(mapping_name="B")
        result = self.cm.detect_downstream_impact(diff, dag)
        assert result == ["C"]

    def test_unknown_model_returns_empty(self) -> None:
        dag = self._build_dag()
        diff = MappingDiff(mapping_name="UNKNOWN")
        result = self.cm.detect_downstream_impact(diff, dag)
        assert result == []

    def test_single_node_dag(self) -> None:
        dag = DependencyDAG()
        dag.add_node(DAGNode(model_name="X", layer=Layer.BRONZE_TO_SILVER))
        diff = MappingDiff(mapping_name="X")
        result = self.cm.detect_downstream_impact(diff, dag)
        assert result == []


# ---------------------------------------------------------------------------
# VersionStore tests
# ---------------------------------------------------------------------------

class TestVersionStore:
    def setup_method(self) -> None:
        self.store = VersionStore()

    def test_save_and_get_version(self) -> None:
        spec = _spec(name="my_model")
        vid = self.store.save_version("my_model", spec, "initial")
        retrieved = self.store.get_version("my_model", vid)
        assert retrieved == spec

    def test_save_returns_unique_ids(self) -> None:
        spec = _spec()
        v1 = self.store.save_version("m", spec)
        v2 = self.store.save_version("m", spec)
        assert v1 != v2

    def test_list_versions_empty(self) -> None:
        assert self.store.list_versions("nonexistent") == []

    def test_list_versions_ordered(self) -> None:
        spec = _spec()
        v1 = self.store.save_version("m", spec, "first")
        v2 = self.store.save_version("m", spec, "second")
        versions = self.store.list_versions("m")
        assert len(versions) == 2
        assert versions[0].version_id == v1
        assert versions[1].version_id == v2
        assert versions[0].change_description == "first"
        assert versions[1].change_description == "second"

    def test_get_version_not_found_raises(self) -> None:
        with pytest.raises(KeyError):
            self.store.get_version("m", "bad-id")

    def test_get_version_wrong_mapping_raises(self) -> None:
        spec = _spec()
        vid = self.store.save_version("m1", spec)
        with pytest.raises(KeyError):
            self.store.get_version("m2", vid)

    def test_version_info_has_timestamp(self) -> None:
        spec = _spec()
        self.store.save_version("m", spec)
        versions = self.store.list_versions("m")
        assert versions[0].timestamp is not None

    def test_multiple_mappings_isolated(self) -> None:
        s1 = _spec(name="a")
        s2 = _spec(name="b")
        self.store.save_version("a", s1)
        self.store.save_version("b", s2)
        assert len(self.store.list_versions("a")) == 1
        assert len(self.store.list_versions("b")) == 1
