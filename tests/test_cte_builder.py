"""Unit tests for CTE Pipeline Builder."""

from __future__ import annotations

import pytest

from dbt_job_generator.cte_builder.pipeline_builder import (
    CTEPipelineBuilder,
    DerivedCTEBuilder,
    PhysicalTableCTEBuilder,
    UnpivotCTEBuilder,
)
from dbt_job_generator.models.enums import SourceType
from dbt_job_generator.models.mapping import SourceEntry


# ── PhysicalTableCTEBuilder ───────────────────────────────────────


class TestPhysicalTableCTEBuilder:
    def setup_method(self) -> None:
        self.builder = PhysicalTableCTEBuilder()

    def test_basic_with_filter(self) -> None:
        entry = SourceEntry(
            index=1,
            source_type=SourceType.PHYSICAL_TABLE,
            schema="bronze",
            table_name="THONG_TIN_DK_THUE",
            alias="th_ti_dk_th",
            select_fields="id, email_kd, fax_kd, 'DCST' AS source_system_code",
            filter="data_date = to_date('{{ var(\"etl_date\") }}', 'yyyy-MM-dd')",
        )
        cte = self.builder.build(entry)

        assert cte.alias == "th_ti_dk_th"
        assert cte.source_type == SourceType.PHYSICAL_TABLE
        assert cte.depends_on == []
        assert "SELECT id, email_kd, fax_kd, 'DCST' AS source_system_code" in cte.sql_body
        assert "FROM bronze.THONG_TIN_DK_THUE" in cte.sql_body
        assert "WHERE data_date = to_date" in cte.sql_body

    def test_no_filter(self) -> None:
        entry = SourceEntry(
            index=1,
            source_type=SourceType.PHYSICAL_TABLE,
            schema="bronze",
            table_name="MY_TABLE",
            alias="my_tbl",
            select_fields="col1, col2",
            filter=None,
        )
        cte = self.builder.build(entry)

        assert cte.alias == "my_tbl"
        assert "WHERE" not in cte.sql_body
        assert "FROM bronze.MY_TABLE" in cte.sql_body

    def test_no_schema(self) -> None:
        entry = SourceEntry(
            index=1,
            source_type=SourceType.PHYSICAL_TABLE,
            schema=None,
            table_name="RAW_TABLE",
            alias="raw_tbl",
            select_fields="*",
            filter=None,
        )
        cte = self.builder.build(entry)

        assert "FROM RAW_TABLE" in cte.sql_body
        assert "." not in cte.sql_body.split("FROM")[1].split("\n")[0]


# ── UnpivotCTEBuilder ─────────────────────────────────────────────


class TestUnpivotCTEBuilder:
    def setup_method(self) -> None:
        self.builder = UnpivotCTEBuilder()

    def test_involved_party_electronic_address(self) -> None:
        """Test with the real sample: involved-party-electronic-address pattern."""
        entry = SourceEntry(
            index=2,
            source_type=SourceType.UNPIVOT_CTE,
            schema=None,
            table_name="th_ti_dk_th",
            alias="leg_th_ti_dk_th",
            select_fields="ip_code=id | EMAIL:email_kd | FAX:fax_kd | FAX:fax | PHONE:dien_thoai_kd | PHONE:dien_thoai | source_system_code",
            filter="address_value IS NOT NULL",
        )
        cte = self.builder.build(entry)

        assert cte.alias == "leg_th_ti_dk_th"
        assert cte.source_type == SourceType.UNPIVOT_CTE
        assert cte.depends_on == ["th_ti_dk_th"]

        # Should have 5 UNION ALL blocks (5 TypeValuePairs)
        blocks = cte.sql_body.split("UNION ALL")
        assert len(blocks) == 5

        # First block: EMAIL
        assert "id AS ip_code" in blocks[0]
        assert "'EMAIL' AS type_code" in blocks[0]
        assert "email_kd AS address_value" in blocks[0]
        assert "source_system_code" in blocks[0]
        assert "FROM th_ti_dk_th" in blocks[0]
        assert "WHERE email_kd IS NOT NULL" in blocks[0]

        # Second block: FAX (fax_kd)
        assert "'FAX' AS type_code" in blocks[1]
        assert "fax_kd AS address_value" in blocks[1]
        assert "WHERE fax_kd IS NOT NULL" in blocks[1]

        # Fourth block: PHONE (dien_thoai_kd)
        assert "'PHONE' AS type_code" in blocks[3]
        assert "dien_thoai_kd AS address_value" in blocks[3]
        assert "WHERE dien_thoai_kd IS NOT NULL" in blocks[3]

    def test_single_type_value_pair(self) -> None:
        """Unpivot with only 1 TypeValuePair — no UNION ALL."""
        entry = SourceEntry(
            index=2,
            source_type=SourceType.UNPIVOT_CTE,
            schema=None,
            table_name="src_cte",
            alias="unpivot_cte",
            select_fields="key=id | TYPE1:col1 | passthrough_col",
            filter=None,
        )
        cte = self.builder.build(entry)

        assert "UNION ALL" not in cte.sql_body
        assert "id AS key" in cte.sql_body
        assert "'TYPE1' AS type_code" in cte.sql_body
        assert "col1 AS address_value" in cte.sql_body
        assert "passthrough_col" in cte.sql_body
        assert "WHERE col1 IS NOT NULL" in cte.sql_body
        assert cte.depends_on == ["src_cte"]

    def test_no_passthrough_fields(self) -> None:
        """Unpivot with no passthrough fields."""
        entry = SourceEntry(
            index=2,
            source_type=SourceType.UNPIVOT_CTE,
            schema=None,
            table_name="src",
            alias="unpivot",
            select_fields="out_key=in_key | A:col_a | B:col_b",
            filter=None,
        )
        cte = self.builder.build(entry)

        blocks = cte.sql_body.split("UNION ALL")
        assert len(blocks) == 2
        # Verify no extra trailing commas from empty passthrough
        assert "in_key AS out_key" in blocks[0]
        assert "'A' AS type_code" in blocks[0]
        assert "col_a AS address_value" in blocks[0]


# ── DerivedCTEBuilder ─────────────────────────────────────────────


class TestDerivedCTEBuilder:
    def setup_method(self) -> None:
        self.builder = DerivedCTEBuilder()

    def test_with_group_by(self) -> None:
        """Test derived CTE with GROUP BY (fund-management-company pattern)."""
        entry = SourceEntry(
            index=3,
            source_type=SourceType.DERIVED_CTE,
            schema=None,
            table_name="fu_bu_raw",
            alias="fu_bu",
            select_fields="fundid, array_agg(buid) AS business_type_codes",
            filter="GROUP BY fundid",
        )
        cte = self.builder.build(entry)

        assert cte.alias == "fu_bu"
        assert cte.source_type == SourceType.DERIVED_CTE
        assert cte.depends_on == ["fu_bu_raw"]
        assert "SELECT fundid, array_agg(buid) AS business_type_codes" in cte.sql_body
        assert "FROM fu_bu_raw" in cte.sql_body
        assert "GROUP BY fundid" in cte.sql_body

    def test_without_group_by(self) -> None:
        """Derived CTE without GROUP BY — just a transformation."""
        entry = SourceEntry(
            index=2,
            source_type=SourceType.DERIVED_CTE,
            schema=None,
            table_name="base_cte",
            alias="derived",
            select_fields="col1, col2, col1 + col2 AS total",
            filter=None,
        )
        cte = self.builder.build(entry)

        assert cte.alias == "derived"
        assert "FROM base_cte" in cte.sql_body
        assert "GROUP BY" not in cte.sql_body
        assert cte.depends_on == ["base_cte"]


# ── CTEPipelineBuilder (integration) ──────────────────────────────


class TestCTEPipelineBuilder:
    def setup_method(self) -> None:
        self.builder = CTEPipelineBuilder()

    def test_mixed_pipeline(self) -> None:
        """Test pipeline with physical_table → unpivot_cte → derived_cte."""
        inputs = [
            SourceEntry(
                index=1,
                source_type=SourceType.PHYSICAL_TABLE,
                schema="bronze",
                table_name="THONG_TIN_DK_THUE",
                alias="th_ti_dk_th",
                select_fields="id, email_kd, source_system_code",
                filter="data_date = to_date('{{ var(\"etl_date\") }}', 'yyyy-MM-dd')",
            ),
            SourceEntry(
                index=2,
                source_type=SourceType.UNPIVOT_CTE,
                schema=None,
                table_name="th_ti_dk_th",
                alias="leg_th_ti_dk_th",
                select_fields="ip_code=id | EMAIL:email_kd | source_system_code",
                filter=None,
            ),
            SourceEntry(
                index=3,
                source_type=SourceType.DERIVED_CTE,
                schema=None,
                table_name="leg_th_ti_dk_th",
                alias="agg_cte",
                select_fields="ip_code, count(*) AS cnt",
                filter="GROUP BY ip_code",
            ),
        ]

        ctes = self.builder.build(inputs)

        assert len(ctes) == 3

        # Physical table CTE
        assert ctes[0].alias == "th_ti_dk_th"
        assert ctes[0].source_type == SourceType.PHYSICAL_TABLE
        assert ctes[0].depends_on == []

        # Unpivot CTE
        assert ctes[1].alias == "leg_th_ti_dk_th"
        assert ctes[1].source_type == SourceType.UNPIVOT_CTE
        assert ctes[1].depends_on == ["th_ti_dk_th"]

        # Derived CTE
        assert ctes[2].alias == "agg_cte"
        assert ctes[2].source_type == SourceType.DERIVED_CTE
        assert ctes[2].depends_on == ["leg_th_ti_dk_th"]

    def test_empty_inputs(self) -> None:
        """Empty input list returns empty CTE list."""
        assert self.builder.build([]) == []

    def test_single_physical_table(self) -> None:
        """Single physical_table entry."""
        inputs = [
            SourceEntry(
                index=1,
                source_type=SourceType.PHYSICAL_TABLE,
                schema="silver",
                table_name="FUND_COMPANY",
                alias="fu_co",
                select_fields="id, name",
                filter=None,
            ),
        ]
        ctes = self.builder.build(inputs)
        assert len(ctes) == 1
        assert ctes[0].alias == "fu_co"
        assert "FROM silver.FUND_COMPANY" in ctes[0].sql_body
