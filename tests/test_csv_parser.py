"""Tests for CSVParser."""

import os
import tempfile
import textwrap

import pytest

from dbt_job_generator.models.enums import (
    FinalFilterType,
    JoinType,
    Layer,
    RulePattern,
    SourceType,
)
from dbt_job_generator.models.errors import ParseError
from dbt_job_generator.models.mapping import (
    KeyField,
    TypeValuePair,
    UnpivotFieldSpec,
)
from dbt_job_generator.parser.csv_parser import CSVParser


@pytest.fixture
def parser():
    return CSVParser()


def _write_csv(content: str) -> str:
    """Write CSV content to a temp file and return the path."""
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    )
    f.write(textwrap.dedent(content))
    f.close()
    return f.name


SAMPLE_FULL_CSV = """\
Target,Bảng đích,,,,,,,,,
Database,Schema,Table Name,ETL Handle,,,,Description,Last update,Update by,Update reason
,silver,Involved Party Electronic Address,SCD4A,,,,Description text,,,
Input,Bảng / CTE nguồn,,,,,,,,,
#,Source Type,Schema,Table Name,Alias,Select Fields,Filter,Description,Last update,Update by,Update reason
1,physical_table,bronze,TABLE_NAME,alias,"field1, field2","filter_expr",,,,
2,unpivot_cte,,alias,new_alias,"ip_code=id | EMAIL:email_kd | FAX:fax",address_value IS NOT NULL,,,,
3,derived_cte,,alias,agg_alias,"fundid, array_agg(buid) AS codes",GROUP BY fundid,,,,
Relationship,Quan hệ giữa các bảng / CTE nguồn,,,,,,,,,
#,Main Alias,Join Type,Join Alias,Join On,,,Description,Last update,Update by,Update reason
1,alias,LEFT JOIN,agg_alias,alias.id = agg_alias.fundid,,,,,,
Mapping,Mapping trường nguồn → trường đích,,,,,,,,,
#,Target Column,Transformation,Data Type,,,,Description,Last update,Update by,Update reason
1,target_col,alias.source_col,string,,,,,,,
2,hash_col,"hash_id('SRC', alias.id)",string,,,,,,,
3,hardcode_col,'HARDCODED_VALUE',string,,,,,,,
4,null_col,,date,,,,,,,
5,num_col,42,bigint,,,,,,,
6,logic_col,"CASE WHEN x THEN y END",string,,,,,,,
Final Filter,Điều kiện lọc của Main Transform,,,,,,,,,
#,Clause Type,Expression,,,,,Description,Last update,Update by,Update reason
1,UNION ALL,"SELECT * FROM cte1 UNION ALL SELECT * FROM cte2",,,,,Description,,,
"""


class TestCSVParserParse:
    """Tests for CSVParser.parse()."""

    def test_parse_full_csv(self, parser):
        path = _write_csv(SAMPLE_FULL_CSV)
        try:
            spec = parser.parse(path)

            # Target
            assert spec.target.schema == "silver"
            assert spec.target.table_name == "Involved Party Electronic Address"
            assert spec.target.etl_handle == "SCD4A"
            assert spec.target.description == "Description text"
            assert spec.layer == Layer.BRONZE_TO_SILVER
            assert spec.name == "involved_party_electronic_address"

            # Inputs
            assert len(spec.inputs) == 3
            assert spec.inputs[0].source_type == SourceType.PHYSICAL_TABLE
            assert spec.inputs[0].schema == "bronze"
            assert spec.inputs[0].table_name == "TABLE_NAME"
            assert spec.inputs[0].alias == "alias"
            assert spec.inputs[0].select_fields == "field1, field2"
            assert spec.inputs[0].filter == "filter_expr"

            assert spec.inputs[1].source_type == SourceType.UNPIVOT_CTE
            assert spec.inputs[1].table_name == "alias"
            assert spec.inputs[1].alias == "new_alias"

            assert spec.inputs[2].source_type == SourceType.DERIVED_CTE
            assert spec.inputs[2].alias == "agg_alias"

            # Relationships
            assert len(spec.relationships) == 1
            assert spec.relationships[0].main_alias == "alias"
            assert spec.relationships[0].join_type == JoinType.LEFT_JOIN
            assert spec.relationships[0].join_alias == "agg_alias"

            # Mappings
            assert len(spec.mappings) == 6
            assert spec.mappings[0].mapping_rule.pattern == RulePattern.DIRECT_MAP
            assert spec.mappings[1].mapping_rule.pattern == RulePattern.HASH
            assert spec.mappings[1].mapping_rule.params["args"] == "'SRC', alias.id"
            assert spec.mappings[2].mapping_rule.pattern == RulePattern.HARDCODE_STRING
            assert spec.mappings[2].mapping_rule.params["value"] == "HARDCODED_VALUE"
            assert spec.mappings[3].mapping_rule.pattern == RulePattern.NULL_MAP
            assert spec.mappings[4].mapping_rule.pattern == RulePattern.HARDCODE_NUMERIC
            assert spec.mappings[4].mapping_rule.params["value"] == "42"
            assert spec.mappings[5].mapping_rule.pattern == RulePattern.BUSINESS_LOGIC

            # Final Filter
            assert spec.final_filter is not None
            assert spec.final_filter.filter_type == FinalFilterType.UNION_ALL

            # Metadata
            assert spec.metadata.source_file == path
        finally:
            os.unlink(path)

    def test_parse_gold_layer(self, parser):
        csv_content = """\
Target,,,,,,,,,,,
Database,Schema,Table Name,ETL Handle,,,,Description,Last update,Update by,Update reason
,gold,My Gold Table,HANDLE,,,,,,,,
Input,,,,,,,,,,,
#,Source Type,Schema,Table Name,Alias,Select Fields,Filter,Description,Last update,Update by,Update reason
1,physical_table,silver,SRC_TABLE,src,"col1, col2",,,,,
Mapping,,,,,,,,,,,
#,Target Column,Transformation,Data Type,,,,Description,Last update,Update by,Update reason
1,col1,src.col1,string,,,,,,,
"""
        path = _write_csv(csv_content)
        try:
            spec = parser.parse(path)
            assert spec.layer == Layer.SILVER_TO_GOLD
            assert spec.name == "my_gold_table"
        finally:
            os.unlink(path)

    def test_parse_no_relationship_no_final_filter(self, parser):
        csv_content = """\
Target,,,,,,,,,,,
Database,Schema,Table Name,ETL Handle,,,,Description,Last update,Update by,Update reason
,silver,Simple Table,ETL1,,,,,,,,
Input,,,,,,,,,,,
#,Source Type,Schema,Table Name,Alias,Select Fields,Filter,Description,Last update,Update by,Update reason
1,physical_table,bronze,SRC,src,"col1",,,,,
Mapping,,,,,,,,,,,
#,Target Column,Transformation,Data Type,,,,Description,Last update,Update by,Update reason
1,col1,src.col1,string,,,,,,,
"""
        path = _write_csv(csv_content)
        try:
            spec = parser.parse(path)
            assert spec.relationships == []
            assert spec.final_filter is None
        finally:
            os.unlink(path)


class TestCSVParserErrors:
    """Tests for error handling."""

    def test_missing_target_section(self, parser):
        csv_content = """\
Input,,,,,,,,,,,
#,Source Type,Schema,Table Name,Alias,Select Fields,Filter,Description,Last update,Update by,Update reason
1,physical_table,bronze,SRC,src,"col1",,,,,
Mapping,,,,,,,,,,,
#,Target Column,Transformation,Data Type,,,,Description,Last update,Update by,Update reason
1,col1,src.col1,string,,,,,,,
"""
        path = _write_csv(csv_content)
        try:
            with pytest.raises(ParseError, match="Missing required section: Target"):
                parser.parse(path)
        finally:
            os.unlink(path)

    def test_missing_input_section(self, parser):
        csv_content = """\
Target,,,,,,,,,,,
Database,Schema,Table Name,ETL Handle,,,,Description,Last update,Update by,Update reason
,silver,Table,ETL,,,,,,,,
Mapping,,,,,,,,,,,
#,Target Column,Transformation,Data Type,,,,Description,Last update,Update by,Update reason
1,col1,src.col1,string,,,,,,,
"""
        path = _write_csv(csv_content)
        try:
            with pytest.raises(ParseError, match="Missing required section: Input"):
                parser.parse(path)
        finally:
            os.unlink(path)

    def test_missing_mapping_section(self, parser):
        csv_content = """\
Target,,,,,,,,,,,
Database,Schema,Table Name,ETL Handle,,,,Description,Last update,Update by,Update reason
,silver,Table,ETL,,,,,,,,
Input,,,,,,,,,,,
#,Source Type,Schema,Table Name,Alias,Select Fields,Filter,Description,Last update,Update by,Update reason
1,physical_table,bronze,SRC,src,"col1",,,,,
"""
        path = _write_csv(csv_content)
        try:
            with pytest.raises(ParseError, match="Missing required section: Mapping"):
                parser.parse(path)
        finally:
            os.unlink(path)

    def test_invalid_source_type(self, parser):
        csv_content = """\
Target,,,,,,,,,,,
Database,Schema,Table Name,ETL Handle,,,,Description,Last update,Update by,Update reason
,silver,Table,ETL,,,,,,,,
Input,,,,,,,,,,,
#,Source Type,Schema,Table Name,Alias,Select Fields,Filter,Description,Last update,Update by,Update reason
1,invalid_type,bronze,SRC,src,"col1",,,,,
Mapping,,,,,,,,,,,
#,Target Column,Transformation,Data Type,,,,Description,Last update,Update by,Update reason
1,col1,src.col1,string,,,,,,,
"""
        path = _write_csv(csv_content)
        try:
            with pytest.raises(ParseError, match="Invalid source_type"):
                parser.parse(path)
        finally:
            os.unlink(path)

    def test_physical_table_missing_table_name(self, parser):
        csv_content = """\
Target,,,,,,,,,,,
Database,Schema,Table Name,ETL Handle,,,,Description,Last update,Update by,Update reason
,silver,Table,ETL,,,,,,,,
Input,,,,,,,,,,,
#,Source Type,Schema,Table Name,Alias,Select Fields,Filter,Description,Last update,Update by,Update reason
1,physical_table,bronze,,src,"col1",,,,,
Mapping,,,,,,,,,,,
#,Target Column,Transformation,Data Type,,,,Description,Last update,Update by,Update reason
1,col1,src.col1,string,,,,,,,
"""
        path = _write_csv(csv_content)
        try:
            with pytest.raises(ParseError, match="missing required field: table_name"):
                parser.parse(path)
        finally:
            os.unlink(path)

    def test_input_missing_alias(self, parser):
        csv_content = """\
Target,,,,,,,,,,,
Database,Schema,Table Name,ETL Handle,,,,Description,Last update,Update by,Update reason
,silver,Table,ETL,,,,,,,,
Input,,,,,,,,,,,
#,Source Type,Schema,Table Name,Alias,Select Fields,Filter,Description,Last update,Update by,Update reason
1,physical_table,bronze,SRC,,"col1",,,,,
Mapping,,,,,,,,,,,
#,Target Column,Transformation,Data Type,,,,Description,Last update,Update by,Update reason
1,col1,src.col1,string,,,,,,,
"""
        path = _write_csv(csv_content)
        try:
            with pytest.raises(ParseError, match="missing required field: alias"):
                parser.parse(path)
        finally:
            os.unlink(path)

    def test_nonexistent_file(self, parser):
        with pytest.raises(ParseError, match="Cannot read file"):
            parser.parse("/nonexistent/path/file.csv")


class TestMappingRuleDetection:
    """Tests for _detect_mapping_rule."""

    def test_null_map(self):
        rule = CSVParser._detect_mapping_rule(None, "string")
        assert rule.pattern == RulePattern.NULL_MAP

    def test_null_map_empty_string(self):
        rule = CSVParser._detect_mapping_rule("", "string")
        assert rule.pattern == RulePattern.NULL_MAP

    def test_hash(self):
        rule = CSVParser._detect_mapping_rule("hash_id('SRC', col1, col2)", "string")
        assert rule.pattern == RulePattern.HASH
        assert rule.params["args"] == "'SRC', col1, col2"

    def test_hardcode_string(self):
        rule = CSVParser._detect_mapping_rule("'MY_VALUE'", "string")
        assert rule.pattern == RulePattern.HARDCODE_STRING
        assert rule.params["value"] == "MY_VALUE"

    def test_hardcode_numeric_int(self):
        rule = CSVParser._detect_mapping_rule("42", "bigint")
        assert rule.pattern == RulePattern.HARDCODE_NUMERIC
        assert rule.params["value"] == "42"

    def test_hardcode_numeric_float(self):
        rule = CSVParser._detect_mapping_rule("3.14", "decimal")
        assert rule.pattern == RulePattern.HARDCODE_NUMERIC
        assert rule.params["value"] == "3.14"

    def test_direct_map(self):
        rule = CSVParser._detect_mapping_rule("alias.column_name", "string")
        assert rule.pattern == RulePattern.DIRECT_MAP

    def test_business_logic(self):
        rule = CSVParser._detect_mapping_rule(
            "CASE WHEN x > 0 THEN 'yes' ELSE 'no' END", "string"
        )
        assert rule.pattern == RulePattern.BUSINESS_LOGIC


class TestUnpivotFieldParsing:
    """Tests for _parse_unpivot_fields."""

    def test_full_unpivot(self):
        spec = CSVParser._parse_unpivot_fields(
            "ip_code=id | EMAIL:email_kd | FAX:fax_kd | source_system_code"
        )
        assert spec.key_mapping == KeyField(target_name="ip_code", source_name="id")
        assert len(spec.type_value_pairs) == 2
        assert spec.type_value_pairs[0] == TypeValuePair(
            type_code="EMAIL", source_column="email_kd"
        )
        assert spec.type_value_pairs[1] == TypeValuePair(
            type_code="FAX", source_column="fax_kd"
        )
        assert spec.passthrough_fields == ["source_system_code"]

    def test_single_type_value_pair(self):
        spec = CSVParser._parse_unpivot_fields("key=src | TYPE:col")
        assert spec.key_mapping == KeyField(target_name="key", source_name="src")
        assert len(spec.type_value_pairs) == 1
        assert spec.passthrough_fields == []

    def test_only_passthrough(self):
        spec = CSVParser._parse_unpivot_fields("key=src | field1 | field2")
        assert spec.key_mapping == KeyField(target_name="key", source_name="src")
        assert spec.type_value_pairs == []
        assert spec.passthrough_fields == ["field1", "field2"]

    def test_missing_key_field(self):
        with pytest.raises(ParseError, match="missing key field"):
            CSVParser._parse_unpivot_fields("EMAIL:email_kd | FAX:fax_kd")


class TestParseBatch:
    """Tests for parse_batch."""

    def test_batch_with_valid_files(self, parser):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv1 = """\
Target,,,,,,,,,,,
Database,Schema,Table Name,ETL Handle,,,,Description,Last update,Update by,Update reason
,silver,Table One,ETL1,,,,,,,,
Input,,,,,,,,,,,
#,Source Type,Schema,Table Name,Alias,Select Fields,Filter,Description,Last update,Update by,Update reason
1,physical_table,bronze,SRC1,src1,"col1",,,,,
Mapping,,,,,,,,,,,
#,Target Column,Transformation,Data Type,,,,Description,Last update,Update by,Update reason
1,col1,src1.col1,string,,,,,,,
"""
            csv2 = """\
Target,,,,,,,,,,,
Database,Schema,Table Name,ETL Handle,,,,Description,Last update,Update by,Update reason
,silver,Table Two,ETL2,,,,,,,,
Input,,,,,,,,,,,
#,Source Type,Schema,Table Name,Alias,Select Fields,Filter,Description,Last update,Update by,Update reason
1,physical_table,bronze,SRC2,src2,"col2",,,,,
Mapping,,,,,,,,,,,
#,Target Column,Transformation,Data Type,,,,Description,Last update,Update by,Update reason
1,col2,src2.col2,string,,,,,,,
"""
            with open(os.path.join(tmpdir, "a.csv"), "w") as f:
                f.write(csv1)
            with open(os.path.join(tmpdir, "b.csv"), "w") as f:
                f.write(csv2)

            results = parser.parse_batch(tmpdir)
            assert len(results) == 2
            assert all(not isinstance(r[1], ParseError) for r in results)

    def test_batch_with_invalid_file(self, parser):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Valid file
            csv_valid = """\
Target,,,,,,,,,,,
Database,Schema,Table Name,ETL Handle,,,,Description,Last update,Update by,Update reason
,silver,Table,ETL,,,,,,,,
Input,,,,,,,,,,,
#,Source Type,Schema,Table Name,Alias,Select Fields,Filter,Description,Last update,Update by,Update reason
1,physical_table,bronze,SRC,src,"col1",,,,,
Mapping,,,,,,,,,,,
#,Target Column,Transformation,Data Type,,,,Description,Last update,Update by,Update reason
1,col1,src.col1,string,,,,,,,
"""
            # Invalid file (missing Mapping section)
            csv_invalid = """\
Target,,,,,,,,,,,
Database,Schema,Table Name,ETL Handle,,,,Description,Last update,Update by,Update reason
,silver,Table,ETL,,,,,,,,
Input,,,,,,,,,,,
#,Source Type,Schema,Table Name,Alias,Select Fields,Filter,Description,Last update,Update by,Update reason
1,physical_table,bronze,SRC,src,"col1",,,,,
"""
            with open(os.path.join(tmpdir, "valid.csv"), "w") as f:
                f.write(csv_valid)
            with open(os.path.join(tmpdir, "invalid.csv"), "w") as f:
                f.write(csv_invalid)

            results = parser.parse_batch(tmpdir)
            assert len(results) == 2
            # One should be a ParseError
            errors = [r for r in results if isinstance(r[1], ParseError)]
            successes = [r for r in results if not isinstance(r[1], ParseError)]
            assert len(errors) == 1
            assert len(successes) == 1

    def test_batch_nonexistent_directory(self, parser):
        results = parser.parse_batch("/nonexistent/dir")
        assert len(results) == 1
        assert isinstance(results[0][1], ParseError)


class TestBOMHandling:
    """Test that BOM-encoded files are handled correctly."""

    def test_utf8_bom(self, parser):
        csv_content = """\
Target,,,,,,,,,,,
Database,Schema,Table Name,ETL Handle,,,,Description,Last update,Update by,Update reason
,silver,BOM Table,ETL,,,,,,,,
Input,,,,,,,,,,,
#,Source Type,Schema,Table Name,Alias,Select Fields,Filter,Description,Last update,Update by,Update reason
1,physical_table,bronze,SRC,src,"col1",,,,,
Mapping,,,,,,,,,,,
#,Target Column,Transformation,Data Type,,,,Description,Last update,Update by,Update reason
1,col1,src.col1,string,,,,,,,
"""
        f = tempfile.NamedTemporaryFile(
            mode="wb", suffix=".csv", delete=False
        )
        # Write BOM + content
        f.write(b"\xef\xbb\xbf")
        f.write(csv_content.encode("utf-8"))
        f.close()
        try:
            spec = parser.parse(f.name)
            assert spec.target.table_name == "BOM Table"
        finally:
            os.unlink(f.name)
