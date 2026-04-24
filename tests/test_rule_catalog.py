"""Unit tests for TransformationRuleCatalog."""

import pytest

from dbt_job_generator.catalog.rule_catalog import TransformationRule, TransformationRuleCatalog
from dbt_job_generator.models.enums import RuleType
from dbt_job_generator.models.errors import CatalogError


def _make_rule(name: str = "test_rule", rule_type: RuleType = RuleType.HASH) -> TransformationRule:
    return TransformationRule(
        name=name,
        type=rule_type,
        sql_template=f"{name}({{{{columns}}}})",
        description=f"Test rule {name}",
    )


class TestTransformationRule:
    def test_frozen_dataclass(self):
        rule = _make_rule()
        with pytest.raises(AttributeError):
            rule.name = "other"  # type: ignore[misc]

    def test_defaults(self):
        rule = TransformationRule(name="r", type=RuleType.HASH, sql_template="t")
        assert rule.is_exception is False
        assert rule.description == ""

    def test_equality(self):
        a = TransformationRule(name="r", type=RuleType.HASH, sql_template="t")
        b = TransformationRule(name="r", type=RuleType.HASH, sql_template="t")
        assert a == b


class TestTransformationRuleCatalog:
    def test_default_hash_function(self):
        catalog = TransformationRuleCatalog()
        assert catalog.get_hash_function() == "hash_id"

    def test_custom_hash_function(self):
        catalog = TransformationRuleCatalog(hash_function="md5")
        assert catalog.get_hash_function() == "md5"

    def test_empty_catalog(self):
        catalog = TransformationRuleCatalog()
        assert catalog.list_rules() == []
        assert catalog.get_rule("missing") is None
        assert catalog.validate_reference("missing") is False

    def test_init_with_rules(self):
        rules = [_make_rule("a"), _make_rule("b", RuleType.BUSINESS_LOGIC)]
        catalog = TransformationRuleCatalog(rules=rules)
        assert len(catalog.list_rules()) == 2
        assert catalog.get_rule("a") == rules[0]
        assert catalog.get_rule("b") == rules[1]

    def test_init_duplicate_raises(self):
        rules = [_make_rule("dup"), _make_rule("dup")]
        with pytest.raises(CatalogError, match="Duplicate rule name: 'dup'"):
            TransformationRuleCatalog(rules=rules)

    def test_add_rule(self):
        catalog = TransformationRuleCatalog()
        rule = _make_rule("new_rule")
        catalog.add_rule(rule)
        assert catalog.get_rule("new_rule") == rule
        assert catalog.validate_reference("new_rule") is True

    def test_add_duplicate_raises(self):
        catalog = TransformationRuleCatalog()
        catalog.add_rule(_make_rule("dup"))
        with pytest.raises(CatalogError, match="Duplicate rule name: 'dup'"):
            catalog.add_rule(_make_rule("dup"))

    def test_validate_reference(self):
        catalog = TransformationRuleCatalog(rules=[_make_rule("exists")])
        assert catalog.validate_reference("exists") is True
        assert catalog.validate_reference("nope") is False

    def test_get_rule_returns_none_for_missing(self):
        catalog = TransformationRuleCatalog(rules=[_make_rule("x")])
        assert catalog.get_rule("y") is None

    def test_list_rules_preserves_insertion_order(self):
        rules = [_make_rule("c"), _make_rule("a"), _make_rule("b")]
        catalog = TransformationRuleCatalog(rules=rules)
        names = [r.name for r in catalog.list_rules()]
        assert names == ["c", "a", "b"]

    def test_exception_rule(self):
        rule = TransformationRule(
            name="manual_logic",
            type=RuleType.BUSINESS_LOGIC,
            sql_template="-- TODO: implement manually",
            is_exception=True,
            description="Requires manual implementation",
        )
        catalog = TransformationRuleCatalog(rules=[rule])
        retrieved = catalog.get_rule("manual_logic")
        assert retrieved is not None
        assert retrieved.is_exception is True

    def test_catalog_error_has_rule_name(self):
        catalog = TransformationRuleCatalog()
        catalog.add_rule(_make_rule("x"))
        with pytest.raises(CatalogError) as exc_info:
            catalog.add_rule(_make_rule("x"))
        assert exc_info.value.rule_name == "x"
