"""Tests for Generator Engine and all handlers."""

import pytest

from dbt_job_generator.catalog.rule_catalog import (
    TransformationRule,
    TransformationRuleCatalog,
)
from dbt_job_generator.engine.generator_engine import GeneratorEngine
from dbt_job_generator.engine.handlers.business_logic import BusinessLogicHandler
from dbt_job_generator.engine.handlers.direct_map import DirectMapHandler
from dbt_job_generator.engine.handlers.hardcode import HardcodeHandler
from dbt_job_generator.engine.handlers.hash_handler import HashHandler
from dbt_job_generator.engine.handlers.null_handler import NullHandler
from dbt_job_generator.engine.handlers.type_cast import TypeCastHandler
from dbt_job_generator.models.enums import RulePattern, RuleType
from dbt_job_generator.models.errors import GenerationError
from dbt_job_generator.models.mapping import MappingEntry, MappingRule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _entry(
    target: str,
    transformation: str | None,
    data_type: str,
    pattern: RulePattern,
    params: dict | None = None,
    index: int = 1,
) -> MappingEntry:
    return MappingEntry(
        index=index,
        target_column=target,
        transformation=transformation,
        data_type=data_type,
        description=None,
        mapping_rule=MappingRule(pattern=pattern, params=params or {}),
    )


# ---------------------------------------------------------------------------
# DirectMapHandler
# ---------------------------------------------------------------------------

class TestDirectMapHandler:
    def test_basic(self):
        handler = DirectMapHandler()
        entry = _entry("target_col", "src.col_name", "string", RulePattern.DIRECT_MAP)
        col = handler.generate(entry)
        assert col.expression == "src.col_name :: string"
        assert col.target_alias == "target_col"

    def test_bigint_type(self):
        handler = DirectMapHandler()
        entry = _entry("amount", "a.amount_raw", "bigint", RulePattern.DIRECT_MAP)
        col = handler.generate(entry)
        assert col.expression == "a.amount_raw :: bigint"
        assert col.target_alias == "amount"


# ---------------------------------------------------------------------------
# TypeCastHandler
# ---------------------------------------------------------------------------

class TestTypeCastHandler:
    def test_currency_amount(self):
        handler = TypeCastHandler()
        entry = _entry(
            "price", "src.price_raw", "decimal",
            RulePattern.CAST,
            params={"cast_type": "currency_amount", "precision": 18, "scale": 2},
        )
        col = handler.generate(entry)
        assert col.expression == "CAST(src.price_raw AS DECIMAL(18,2))"
        assert col.target_alias == "price"

    def test_date(self):
        handler = TypeCastHandler()
        entry = _entry(
            "birth_date", "src.dob", "date",
            RulePattern.CAST,
            params={"cast_type": "date", "format": "yyyy-MM-dd"},
        )
        col = handler.generate(entry)
        assert col.expression == "TO_DATE(src.dob, 'yyyy-MM-dd')"
        assert col.target_alias == "birth_date"

    def test_currency_missing_precision_raises(self):
        handler = TypeCastHandler()
        entry = _entry(
            "price", "src.price_raw", "decimal",
            RulePattern.CAST,
            params={"cast_type": "currency_amount"},
        )
        with pytest.raises(GenerationError, match="missing precision/scale"):
            handler.generate(entry)

    def test_date_missing_format_raises(self):
        handler = TypeCastHandler()
        entry = _entry(
            "event_date", "src.dt", "date",
            RulePattern.CAST,
            params={"cast_type": "date"},
        )
        with pytest.raises(GenerationError, match="missing format"):
            handler.generate(entry)

    def test_generic_cast_fallback(self):
        handler = TypeCastHandler()
        entry = _entry(
            "col", "src.val", "int",
            RulePattern.CAST,
            params={"cast_type": "unknown"},
        )
        col = handler.generate(entry)
        assert col.expression == "src.val :: int"


# ---------------------------------------------------------------------------
# HashHandler
# ---------------------------------------------------------------------------

class TestHashHandler:
    def test_basic(self):
        handler = HashHandler()
        entry = _entry(
            "hash_key",
            "hash_id('SRC', src.id, src.code)",
            "string",
            RulePattern.HASH,
        )
        col = handler.generate(entry)
        assert col.expression == "hash_id('SRC', src.id, src.code)"
        assert col.target_alias == "hash_key"


# ---------------------------------------------------------------------------
# BusinessLogicHandler
# ---------------------------------------------------------------------------

class TestBusinessLogicHandler:
    def test_normal_rule(self):
        handler = BusinessLogicHandler()
        entry = _entry(
            "status",
            "CASE WHEN src.active = 1 THEN 'Y' ELSE 'N' END",
            "string",
            RulePattern.BUSINESS_LOGIC,
        )
        col = handler.generate(entry)
        assert "CASE WHEN" in col.expression
        assert col.expression.endswith(":: string")
        assert col.target_alias == "status"

    def test_exception_rule_with_catalog(self):
        catalog = TransformationRuleCatalog(rules=[
            TransformationRule(
                name="complex_calc",
                type=RuleType.BUSINESS_LOGIC,
                sql_template="",
                is_exception=True,
                description="Needs manual implementation",
            ),
        ])
        handler = BusinessLogicHandler(catalog=catalog)
        entry = _entry(
            "calc_field",
            "complex_calc(src.a, src.b)",
            "decimal",
            RulePattern.BUSINESS_LOGIC,
            params={"rule_name": "complex_calc"},
        )
        col = handler.generate(entry)
        assert col.expression == "NULL :: decimal /* TODO: implement manually */"
        assert col.target_alias == "calc_field"

    def test_non_exception_rule_with_catalog(self):
        catalog = TransformationRuleCatalog(rules=[
            TransformationRule(
                name="simple_rule",
                type=RuleType.BUSINESS_LOGIC,
                sql_template="src.a + src.b",
                is_exception=False,
            ),
        ])
        handler = BusinessLogicHandler(catalog=catalog)
        entry = _entry(
            "total",
            "src.a + src.b",
            "bigint",
            RulePattern.BUSINESS_LOGIC,
            params={"rule_name": "simple_rule"},
        )
        col = handler.generate(entry)
        assert col.expression == "src.a + src.b :: bigint"

    def test_no_catalog(self):
        handler = BusinessLogicHandler(catalog=None)
        entry = _entry(
            "derived", "src.x * 2", "int", RulePattern.BUSINESS_LOGIC
        )
        col = handler.generate(entry)
        assert col.expression == "src.x * 2 :: int"


# ---------------------------------------------------------------------------
# HardcodeHandler
# ---------------------------------------------------------------------------

class TestHardcodeHandler:
    def test_string(self):
        handler = HardcodeHandler()
        entry = _entry(
            "source_system", "'DWH'", "string", RulePattern.HARDCODE_STRING
        )
        col = handler.generate(entry)
        assert col.expression == "'DWH' :: string"
        assert col.target_alias == "source_system"

    def test_numeric(self):
        handler = HardcodeHandler()
        entry = _entry(
            "default_val", "42", "int", RulePattern.HARDCODE_NUMERIC
        )
        col = handler.generate(entry)
        assert col.expression == "42 :: int"
        assert col.target_alias == "default_val"

    def test_string_strips_extra_quotes(self):
        handler = HardcodeHandler()
        entry = _entry(
            "code", "ACTIVE", "string", RulePattern.HARDCODE_STRING
        )
        col = handler.generate(entry)
        assert col.expression == "'ACTIVE' :: string"


# ---------------------------------------------------------------------------
# NullHandler
# ---------------------------------------------------------------------------

class TestNullHandler:
    def test_basic(self):
        handler = NullHandler()
        entry = _entry("unmapped", None, "timestamp", RulePattern.NULL_MAP)
        col = handler.generate(entry)
        assert col.expression == "NULL :: timestamp"
        assert col.target_alias == "unmapped"

    def test_string_type(self):
        handler = NullHandler()
        entry = _entry("empty_col", None, "string", RulePattern.NULL_MAP)
        col = handler.generate(entry)
        assert col.expression == "NULL :: string"


# ---------------------------------------------------------------------------
# GeneratorEngine — dispatch
# ---------------------------------------------------------------------------

class TestGeneratorEngine:
    def test_dispatch_all_patterns(self):
        engine = GeneratorEngine()
        mappings = [
            _entry("col1", "src.a", "string", RulePattern.DIRECT_MAP),
            _entry("col2", "hash_id('S', src.id)", "string", RulePattern.HASH),
            _entry("col3", "'VAL'", "string", RulePattern.HARDCODE_STRING),
            _entry("col4", "99", "int", RulePattern.HARDCODE_NUMERIC),
            _entry("col5", None, "timestamp", RulePattern.NULL_MAP),
            _entry("col6", "src.x + 1", "bigint", RulePattern.BUSINESS_LOGIC),
        ]
        columns, errors = engine.generate_select_columns(mappings)
        assert len(columns) == 6
        assert len(errors) == 0
        assert columns[0].target_alias == "col1"
        assert columns[1].target_alias == "col2"
        assert columns[2].target_alias == "col3"
        assert columns[3].target_alias == "col4"
        assert columns[4].target_alias == "col5"
        assert columns[5].target_alias == "col6"

    def test_cast_error_collected(self):
        engine = GeneratorEngine()
        mappings = [
            _entry("ok", "src.a", "string", RulePattern.DIRECT_MAP),
            _entry(
                "bad_cast", "src.price", "decimal",
                RulePattern.CAST,
                params={"cast_type": "currency_amount"},  # missing precision/scale
            ),
        ]
        columns, errors = engine.generate_select_columns(mappings)
        assert len(columns) == 1
        assert len(errors) == 1
        assert "missing precision/scale" in str(errors[0])

    def test_engine_with_catalog(self):
        catalog = TransformationRuleCatalog(rules=[
            TransformationRule(
                name="exc_rule",
                type=RuleType.BUSINESS_LOGIC,
                sql_template="",
                is_exception=True,
            ),
        ])
        engine = GeneratorEngine(catalog=catalog)
        mappings = [
            _entry(
                "exc_col", "exc_rule(src.a)", "string",
                RulePattern.BUSINESS_LOGIC,
                params={"rule_name": "exc_rule"},
            ),
        ]
        columns, errors = engine.generate_select_columns(mappings)
        assert len(columns) == 1
        assert "TODO" in columns[0].expression

    def test_empty_mappings(self):
        engine = GeneratorEngine()
        columns, errors = engine.generate_select_columns([])
        assert columns == []
        assert errors == []
