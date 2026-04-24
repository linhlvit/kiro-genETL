# Kế hoạch Triển khai: dbt Job Generator

## Tổng quan

Triển khai công cụ CLI dbt Job Generator bằng Python, tự động sinh dbt model (Spark SQL) từ file mapping CSV đã duyệt. Kiến trúc pipeline: CSV Parser → Mapping Validator → CTE Pipeline Builder → Generator Engine → Model Assembler, cùng các module hỗ trợ (DAG Builder, Test Generator, Change Manager, Review Report). Sử dụng Hypothesis cho property-based testing.

## Tasks

- [ ] 1. Thiết lập cấu trúc dự án và định nghĩa Data Models
  - [ ] 1.1 Tạo cấu trúc thư mục dự án và cấu hình package
    - Tạo thư mục `dbt_job_generator/` với các sub-packages: `parser/`, `validator/`, `cte_builder/`, `engine/`, `assembler/`, `dag/`, `test_gen/`, `change_manager/`, `report/`, `batch/`, `catalog/`, `models/`
    - Tạo `pyproject.toml` hoặc `setup.py` với dependencies (hypothesis, pytest, click/typer cho CLI)
    - _Requirements: Tất cả_

  - [ ] 1.2 Định nghĩa tất cả Data Models (Enums, Dataclasses)
    - Tạo file `models/enums.py`: `Layer`, `SourceType`, `JoinType`, `RulePattern`, `FinalFilterType`, `Severity`, `ChangeType`
    - Tạo file `models/mapping.py`: `TargetSpec`, `SourceEntry`, `UnpivotFieldSpec`, `KeyField`, `TypeValuePair`, `JoinRelationship`, `MappingEntry`, `MappingRule`, `FinalFilter`, `MappingSpec`, `MappingMetadata`
    - Tạo file `models/generation.py`: `CTEDefinition`, `ConfigBlock`, `SelectColumn`, `GenerationResult`
    - Tạo file `models/dag.py`: `DAGNode`, `DAGEdge`, `DependencyDAG`
    - Tạo file `models/validation.py`: `ValidationResult`, `ValidationError`, `ValidationWarning`, `MissingSource`, `MissingPrerequisite`, `CTEValidationError`, `ProjectContext`, `ModelInfo`, `SourceInfo`
    - Tạo file `models/change.py`: `MappingDiff`, `InputModification`, `MappingModification`, `MappingChangeRequest`, `ChangeResult`, `MappingVersion`
    - Tạo file `models/testing.py`: `SchemaTestConfig`, `RelationshipTest`, `UnitTestConfig`, `TestRequirement`, `TestComplianceReport`
    - Tạo file `models/report.py`: `ReviewReport`, `ModelReviewDetail`, `AttentionItem`, `BatchSummary`
    - Tạo file `models/errors.py`: `ParseError`, `GenerationError`, `CatalogError`, `DAGError`, `ChangeError`, `UnpivotParseError`, `ErrorResponse`, `ErrorLocation`
    - Implement `__eq__` và `__hash__` cho các model cần so sánh (MappingSpec, SourceEntry, MappingEntry, v.v.)
    - _Requirements: 1.1, 1.2, 1.4, 14.4_

  - [ ]* 1.3 Viết unit tests cho Data Models
    - Test khởi tạo, equality, serialization cho các dataclass chính
    - Test enum values và conversions
    - _Requirements: 1.1, 1.2_

- [ ] 2. Triển khai CSV Parser và PrettyPrinter
  - [ ] 2.1 Triển khai CSV Parser — đọc và phân tích File Mapping
    - Tạo file `parser/csv_parser.py` với class `CSVParser`
    - Implement `parse(file_path: str) -> Result[MappingSpec, ParseError]`: đọc CSV, nhận diện 5 section (Target, Input, Relationship, Mapping, Final Filter), parse từng section thành data model tương ứng
    - Implement parsing logic cho `SourceEntry` với 3 source types: `physical_table`, `unpivot_cte`, `derived_cte`
    - Implement parsing unpivot `select_fields` format: `key=source | TYPE:col | passthrough` thành `UnpivotFieldSpec`
    - Implement `parse_batch(directory: str) -> list[Result[MappingSpec, ParseError]]`
    - Trả về `ParseError` với vị trí (dòng/cột) và mô tả lỗi khi file không hợp lệ
    - _Requirements: 1.1, 1.2, 1.3_

  - [ ] 2.2 Triển khai PrettyPrinter — chuyển MappingSpec về CSV
    - Tạo file `parser/pretty_printer.py` với class `PrettyPrinter`
    - Implement `print(mapping: MappingSpec) -> str`: chuyển MappingSpec thành chuỗi CSV theo đúng format gốc
    - Đảm bảo round-trip: `parse(pretty_print(spec)) == spec`
    - _Requirements: 1.4_

  - [ ]* 2.3 Viết property test cho Parser round-trip
    - **Property 1: Parser round-trip**
    - Xây dựng Hypothesis generator cho `MappingSpec` hợp lệ (bao gồm tất cả source types, relationships, mapping rules, final filter)
    - Kiểm chứng: `parse(pretty_print(spec)) == spec` cho mọi MappingSpec ngẫu nhiên
    - **Validates: Requirements 1.1, 1.2, 1.4**

  - [ ]* 2.4 Viết property test cho Parser báo lỗi
    - **Property 2: Parser báo lỗi chính xác cho input không hợp lệ**
    - Xây dựng Hypothesis generator cho CSV strings không hợp lệ (thiếu section, sai source_type, unpivot format lỗi)
    - Kiểm chứng: Parser trả về lỗi với vị trí và mô tả, KHÔNG trả về MappingSpec hợp lệ
    - **Validates: Requirements 1.3**

  - [ ]* 2.5 Viết unit tests cho CSV Parser với mapping mẫu thực tế
    - Test parse file mapping mẫu involved-party-electronic-address (physical_table → unpivot_cte → UNION ALL)
    - Test parse file mapping mẫu fund-management-company (physical_table → derived_cte → LEFT JOIN)
    - Test edge cases: file rỗng, thiếu section, unpivot với 1 TypeValuePair, chỉ passthrough fields
    - _Requirements: 1.1, 1.2, 1.3_

- [ ] 3. Checkpoint — Đảm bảo Parser hoạt động đúng
  - Đảm bảo tất cả tests pass, hỏi user nếu có thắc mắc.

- [ ] 4. Triển khai Mapping Validator
  - [ ] 4.1 Triển khai Mapping Validator — xác minh tính hợp lệ workflow
    - Tạo file `validator/mapping_validator.py` với class `MappingValidator`
    - Implement `validate(mapping: MappingSpec, context: ProjectContext) -> ValidationResult`
    - Kiểm tra source tables tồn tại ở layer trước: Bronze sources cho B→S, Silver models cho S→G
    - Kiểm tra prerequisite jobs tồn tại trong DependencyDAG (cho S→G mappings)
    - Implement `validate_cte_pipeline(inputs: list[SourceEntry]) -> list[CTEValidationError]`: kiểm tra `unpivot_cte` và `derived_cte` tham chiếu alias đã khai báo trước đó (index nhỏ hơn)
    - Thu thập TẤT CẢ lỗi (không dừng ở lỗi đầu tiên) — error aggregation
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

  - [ ]* 4.2 Viết property test cho Validator — source tables thiếu
    - **Property 3: Validator phát hiện đúng source tables thiếu theo layer**
    - Xây dựng Hypothesis generators cho `MappingSpec` và `ProjectContext`
    - Kiểm chứng: tập missing sources == tập physical_table entries không tồn tại trong context
    - **Validates: Requirements 2.1, 2.2, 2.3**

  - [ ]* 4.3 Viết property test cho Validator — prerequisite jobs thiếu
    - **Property 4: Validator phát hiện đúng prerequisite jobs thiếu**
    - Kiểm chứng: tập missing prerequisites == tập B→S jobs được tham chiếu mà chưa tồn tại trong DAG
    - **Validates: Requirements 2.4, 2.5**

  - [ ]* 4.4 Viết property test cho CTE pipeline dependency
    - **Property 5: CTE pipeline dependency hợp lệ**
    - Kiểm chứng: mỗi unpivot_cte/derived_cte tham chiếu alias có index nhỏ hơn; Validator phát hiện lỗi khi tham chiếu alias chưa khai báo
    - **Validates: Requirements 1.1, 1.2**

  - [ ]* 4.5 Viết unit tests cho Mapping Validator
    - Test validation với mapping mẫu thực tế (valid và invalid)
    - Test edge cases: CTE tham chiếu alias chưa khai báo, circular CTE dependency
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

- [ ] 5. Triển khai Transformation Rule Catalog
  - [ ] 5.1 Triển khai Transformation Rule Catalog
    - Tạo file `catalog/rule_catalog.py` với class `TransformationRuleCatalog`
    - Implement `get_hash_function() -> str`: trả về hàm hash chuẩn (hash_id)
    - Implement `get_rule(rule_name: str) -> Optional[TransformationRule]`
    - Implement `validate_reference(rule_name: str) -> bool`
    - Implement `list_rules() -> list[TransformationRule]`
    - Định nghĩa `TransformationRule` dataclass với `name`, `type` (HASH/BUSINESS_LOGIC/DERIVED), `sql_template`, `is_exception`, `description`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6_

- [ ] 6. Triển khai CTE Pipeline Builder
  - [ ] 6.1 Triển khai CTE Pipeline Builder — xây dựng WITH clause
    - Tạo file `cte_builder/pipeline_builder.py` với class `CTEPipelineBuilder`
    - Implement `build(inputs: list[SourceEntry]) -> list[CTEDefinition]`
    - Triển khai `PhysicalTableCTEBuilder`: sinh `SELECT select_fields FROM schema.table_name WHERE filter`
    - Triển khai `UnpivotCTEBuilder`: sinh N khối `SELECT ... UNION ALL` từ UnpivotFieldSpec — mỗi khối có key field mapping, type_code literal, source column AS value, passthrough fields, WHERE source_column IS NOT NULL
    - Triển khai `DerivedCTEBuilder`: sinh `SELECT select_fields FROM source_alias [GROUP BY ...]`
    - _Requirements: 1.1, 7.1, 7.2_

  - [ ]* 6.2 Viết property test cho Unpivot CTE
    - **Property 14: Unpivot CTE sinh đúng số khối UNION ALL với đúng type codes**
    - Kiểm chứng: N TypeValuePairs → đúng N khối SELECT UNION ALL, mỗi khối có key field, type_code, value column, passthrough fields, WHERE IS NOT NULL
    - **Validates: Requirements 7.1, 7.2**

  - [ ]* 6.3 Viết property test cho Derived CTE
    - **Property 15: Derived CTE sinh đúng aggregation từ source CTE**
    - Kiểm chứng: DerivedCTE sinh SQL với select_fields, FROM source alias, GROUP BY nếu có
    - **Validates: Requirements 1.1**

  - [ ]* 6.4 Viết unit tests cho CTE Pipeline Builder
    - Test physical_table CTE với filter và không filter
    - Test unpivot CTE với mapping mẫu involved-party-electronic-address
    - Test derived CTE với mapping mẫu fund-management-company
    - Test edge cases: unpivot 1 TypeValuePair, chỉ passthrough fields, derived không GROUP BY
    - _Requirements: 1.1, 7.1, 7.2_

- [ ] 7. Checkpoint — Đảm bảo Parser, Validator, CTE Builder hoạt động đúng
  - Đảm bảo tất cả tests pass, hỏi user nếu có thắc mắc.

- [ ] 8. Triển khai Generator Engine và Rule Handlers
  - [ ] 8.1 Triển khai DirectMapHandler và TypeCastHandler
    - Tạo file `engine/handlers/direct_map.py`: sinh `alias.col :: data_type AS target_col`
    - Tạo file `engine/handlers/type_cast.py`: sinh `expression :: data_type AS target_col` (sử dụng `::` thay vì `CAST()`)
    - Xử lý CAST đặc biệt: Currency Amount → `CAST(col AS DECIMAL(p,s))`, Date → `TO_DATE(col, format)`
    - Báo lỗi khi CAST thiếu precision/scale hoặc format
    - _Requirements: 3.1, 3.2, 4.1, 4.2, 4.3_

  - [ ]* 8.2 Viết property tests cho DIRECT_MAP và CAST
    - **Property 6: DIRECT_MAP sinh đúng biểu thức với type casting**
    - **Property 7: Type casting sử dụng cú pháp :: nhất quán**
    - **Property 8: CAST báo lỗi khi thiếu tham số**
    - **Validates: Requirements 3.1, 3.2, 4.1, 4.2, 4.3**

  - [ ] 8.3 Triển khai HashHandler và BusinessLogicHandler
    - Tạo file `engine/handlers/hash_handler.py`: sinh `hash_id(col1, col2, ...) AS target_col` từ catalog
    - Tạo file `engine/handlers/business_logic.py`: tra cứu catalog, sinh SQL expression hoặc placeholder comment cho exception rules
    - Báo lỗi khi rule tham chiếu không tồn tại trong catalog
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.6_

  - [ ]* 8.4 Viết property tests cho HASH và Business Logic
    - **Property 9: HASH sinh đúng hàm hash từ catalog và đảm bảo nhất quán**
    - **Property 10: Business Logic sinh đúng SQL hoặc placeholder từ catalog**
    - **Property 11: Generator báo lỗi khi tham chiếu rule không tồn tại trong catalog**
    - **Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.6**

  - [ ] 8.5 Triển khai HardcodeHandler và NullHandler
    - Tạo file `engine/handlers/hardcode.py`: sinh `'value' :: string AS target_col` (string) hoặc `value :: type AS target_col` (numeric)
    - Tạo file `engine/handlers/null_handler.py`: sinh `NULL :: data_type AS target_col` cho unmapped fields
    - _Requirements: 6.1, 6.2, 3.1_

  - [ ]* 8.6 Viết property tests cho HARDCODE và NULL_MAP
    - **Property 12: HARDCODE sinh đúng giá trị với type casting**
    - **Property 13: NULL_MAP sinh đúng NULL với type casting**
    - **Validates: Requirements 6.1, 6.2, 3.1**

  - [ ] 8.7 Triển khai Generator Engine chính — tổng hợp tất cả handlers
    - Tạo file `engine/generator_engine.py` với class `GeneratorEngine`
    - Implement `generate_sql(mapping: MappingSpec, catalog: TransformationRuleCatalog) -> GenerationResult`
    - Dispatch từng MappingEntry đến handler phù hợp dựa trên `MappingRule.pattern`
    - Tổng hợp `cte_definitions`, `select_columns`, `from_clause`, `final_filter`
    - Thu thập errors và warnings
    - _Requirements: 3.1, 3.2, 4.1, 4.2, 5.1, 5.3, 6.1, 6.2, 12.2_

- [ ] 9. Triển khai Model Assembler
  - [ ] 9.1 Triển khai ConfigBlockGenerator và FromClauseGenerator
    - Tạo file `assembler/config_block.py`: sinh `{{ config(materialized='...', schema='...', tags=['...']) }}` — schema tương ứng layer (Silver cho B→S, Gold cho S→G)
    - Tạo file `assembler/from_clause.py`: sinh `FROM main_alias LEFT JOIN join_alias ON condition` từ Relationship section
    - _Requirements: 9.1, 9.2, 9.3, 12.1_

  - [ ]* 9.2 Viết property tests cho Config Block và FROM clause
    - **Property 19: Config Block schema tương ứng với layer**
    - **Property 18: JOIN clause sinh đúng từ Relationship section**
    - **Validates: Requirements 9.1, 9.2, 9.3, 12.1**

  - [ ] 9.3 Triển khai Model Assembler chính
    - Tạo file `assembler/model_assembler.py` với class `ModelAssembler`
    - Implement `assemble(config, cte_definitions, select_columns, from_clause, final_filter, template) -> str`
    - Thứ tự assembly: Config Block → WITH clause → Main SELECT → FROM + JOINs → Final Filter
    - Xử lý UNION ALL final filter: sinh N khối Main SELECT nối bằng UNION ALL
    - _Requirements: 12.1, 12.2, 12.3_

  - [ ]* 9.4 Viết property tests cho Model Assembly
    - **Property 16: Filter condition sinh đúng tại đúng vị trí**
    - **Property 17: Final Filter UNION ALL sinh đúng cấu trúc merge**
    - **Property 23: Model assembly đúng thứ tự và đầy đủ thành phần**
    - **Validates: Requirements 8.1, 8.2, 12.1, 12.2**

  - [ ]* 9.5 Viết property test cho Source Reference
    - **Property 20: Source Reference macro tương ứng với loại source**
    - Kiểm chứng: external source → `source()`, internal dbt model → `ref()`
    - **Validates: Requirements 10.1, 10.2**

  - [ ]* 9.6 Viết unit tests cho Model Assembler với mapping mẫu thực tế
    - Test assembly end-to-end với involved-party-electronic-address pattern
    - Test assembly end-to-end với fund-management-company pattern
    - So sánh output SQL với expected SQL
    - _Requirements: 12.1, 12.2, 12.3_

- [ ] 10. Checkpoint — Đảm bảo core pipeline (Parser → Validator → CTE Builder → Engine → Assembler) hoạt động đúng
  - Đảm bảo tất cả tests pass, hỏi user nếu có thắc mắc.

- [ ] 11. Triển khai Dependency DAG Builder
  - [ ] 11.1 Triển khai DAG Builder — phân tích và sinh dependency graph
    - Tạo file `dag/dag_builder.py` với class `DependencyDAGBuilder`
    - Implement `build(mappings: list[MappingSpec]) -> Result[DependencyDAG, DAGError]`: phân tích physical_table sources để xác định edges
    - Implement `detect_cycles(dag: DependencyDAG) -> list[list[str]]`: phát hiện circular dependency
    - Implement `topological_sort(dag: DependencyDAG) -> list[str]`: sinh thứ tự thực thi
    - _Requirements: 11.1, 11.2, 11.3, 11.4_

  - [ ]* 11.2 Viết property tests cho DAG Builder
    - **Property 21: Dependency DAG phản ánh đúng quan hệ phụ thuộc**
    - **Property 22: Phát hiện circular dependency trong DAG**
    - **Property 32: Thứ tự thực thi là topological sort hợp lệ**
    - **Validates: Requirements 11.1, 11.2, 11.3, 11.4, 16.3**

  - [ ]* 11.3 Viết unit tests cho DAG Builder
    - Test DAG với mapping mẫu thực tế (nhiều models phụ thuộc nhau)
    - Test edge cases: DAG 1 node, circular dependency, model phụ thuộc nhiều sources
    - _Requirements: 11.1, 11.2, 11.3, 11.4_

- [ ] 12. Triển khai Test Generator
  - [ ] 12.1 Triển khai Test Generator — sinh schema tests và kiểm tra unit test compliance
    - Tạo file `test_gen/test_generator.py` với class `TestGenerator`
    - Implement `generate_schema_tests(mapping: MappingSpec) -> SchemaTestConfig`: sinh not_null, unique, relationship tests từ field constraints
    - Implement `check_required_tests(model, config: UnitTestConfig) -> TestComplianceReport`: kiểm tra unit tests bắt buộc theo config
    - Đánh dấu model "chưa đạt tiêu chuẩn" khi thiếu required tests
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5, 15.6_

  - [ ]* 12.2 Viết property tests cho Test Generator
    - **Property 29: Test generation phản ánh đúng field constraints**
    - **Property 30: Phát hiện đúng unit tests bắt buộc còn thiếu**
    - **Validates: Requirements 15.1, 15.2, 15.3, 15.5, 15.6**

  - [ ]* 12.3 Viết unit tests cho Test Generator
    - Test schema test generation với mapping có NOT NULL, unique key, relationships
    - Test compliance check với UnitTestConfig mẫu
    - _Requirements: 15.1, 15.2, 15.3, 15.5, 15.6_

- [ ] 13. Triển khai Change Manager
  - [ ] 13.1 Triển khai Change Manager — xử lý incremental changes
    - Tạo file `change_manager/change_manager.py` với class `ChangeManager`
    - Implement `diff(old_mapping, new_mapping) -> MappingDiff`: so sánh 2 MappingSpec, phát hiện inputs/mappings thêm/xóa/sửa, relationships/final_filter thay đổi
    - Implement `process_change(request: MappingChangeRequest, context: ProjectContext) -> ChangeResult`: xử lý UPDATE_RULE, ADD_FIELD, NEW_MAPPING
    - Implement `detect_downstream_impact(change: MappingDiff, dag: DependencyDAG) -> list[str]`: phát hiện downstream models bị ảnh hưởng
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5_

  - [ ] 13.2 Triển khai Version Store — lưu lịch sử phiên bản mapping
    - Tạo file `change_manager/version_store.py` với class `VersionStore`
    - Implement `save_version(mapping_name, mapping) -> str`: lưu version mới
    - Implement `get_version(mapping_name, version_id) -> MappingSpec`: truy xuất version
    - Implement `list_versions(mapping_name) -> list[VersionInfo]`: liệt kê versions
    - _Requirements: 14.6_

  - [ ]* 13.3 Viết property tests cho Change Manager
    - **Property 25: Diff detection chính xác khi mapping thay đổi**
    - **Property 26: Thêm trường mới không ảnh hưởng trường hiện có**
    - **Property 27: Downstream impact detection**
    - **Property 28: Version store round-trip**
    - **Validates: Requirements 14.1, 14.2, 14.4, 14.5, 14.6**

  - [ ]* 13.4 Viết unit tests cho Change Manager
    - Test diff với mapping thay đổi rule, thêm trường, thêm mapping mới
    - Test downstream impact detection với DAG mẫu
    - Test version store save/load round-trip
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5, 14.6_

- [ ] 14. Checkpoint — Đảm bảo DAG Builder, Test Generator, Change Manager hoạt động đúng
  - Đảm bảo tất cả tests pass, hỏi user nếu có thắc mắc.

- [ ] 15. Triển khai Batch Processor
  - [ ] 15.1 Triển khai Batch Processor — xử lý nhiều file mapping
    - Tạo file `batch/batch_processor.py` với class `BatchProcessor`
    - Implement `process(directory: str, context: ProjectContext) -> BatchResult`
    - Điều phối pipeline: parse → validate → generate → assemble cho từng file
    - Fail-safe: file lỗi không dừng batch, ghi nhận lỗi và tiếp tục
    - Sinh `BatchSummary` với total, success_count, error_count, errors
    - _Requirements: 13.1, 13.2, 13.3_

  - [ ]* 15.2 Viết property test cho Batch Processor
    - **Property 24: Batch processing đảm bảo tính nhất quán kết quả**
    - Kiểm chứng: success_count + error_count == total, mọi valid mapping có model, mọi invalid có error
    - **Validates: Requirements 13.1, 13.2, 13.3**

  - [ ]* 15.3 Viết unit tests cho Batch Processor
    - Test batch với mix valid/invalid files
    - Test edge cases: tất cả files lỗi, tất cả files thành công, thư mục rỗng
    - _Requirements: 13.1, 13.2, 13.3_

- [ ] 16. Triển khai Review Report Generator
  - [ ] 16.1 Triển khai Review Report Generator — sinh báo cáo review
    - Tạo file `report/review_report.py` với class `ReviewReportGenerator`
    - Implement `generate(models, dag, test_compliance, batch_result) -> ReviewReport`
    - Liệt kê patterns sử dụng (physical_table, unpivot_cte, derived_cte, JOIN, UNION ALL)
    - Đánh dấu models cần review kỹ (business logic exception, logic phức tạp)
    - Liệt kê thứ tự chạy theo topological sort của DAG
    - _Requirements: 16.1, 16.2, 16.3_

  - [ ]* 16.2 Viết property test cho Review Report
    - **Property 31: Review report đầy đủ và đánh dấu đúng**
    - Kiểm chứng: report chứa entry cho mọi model, patterns đúng, flagging đúng cho exception/complex logic
    - **Validates: Requirements 16.1, 16.2**

  - [ ]* 16.3 Viết unit tests cho Review Report Generator
    - Test report generation với batch mẫu
    - Test flagging logic cho complex models
    - _Requirements: 16.1, 16.2, 16.3_

- [ ] 17. Tích hợp toàn bộ pipeline và CLI
  - [ ] 17.1 Tích hợp pipeline end-to-end
    - Tạo file `pipeline.py`: kết nối CSVParser → MappingValidator → CTEPipelineBuilder → GeneratorEngine → ModelAssembler
    - Implement `generate_model(file_path: str, context: ProjectContext, catalog: TransformationRuleCatalog) -> Result[str, list[ErrorResponse]]`
    - Implement `generate_batch(directory: str, context: ProjectContext, catalog: TransformationRuleCatalog) -> BatchResult`
    - Tích hợp ChangeManager, TestGenerator, ReviewReportGenerator vào pipeline
    - _Requirements: 12.1, 12.2, 12.3, 13.1_

  - [ ] 17.2 Triển khai CLI interface
    - Tạo file `cli.py` với các commands: `generate` (single file), `batch` (directory), `validate` (check only), `diff` (compare mappings), `report` (generate review report)
    - Sử dụng click hoặc typer cho CLI framework
    - _Requirements: Tất cả_

  - [ ]* 17.3 Viết integration tests end-to-end
    - Test pipeline hoàn chỉnh: file mapping CSV → dbt model .sql output
    - So sánh output với expected SQL từ mapping mẫu thực tế (involved-party-electronic-address, fund-management-company)
    - Test batch processing end-to-end
    - Test change management workflow hoàn chỉnh
    - _Requirements: 12.1, 12.2, 12.3, 13.1, 14.1_

- [ ] 18. Checkpoint cuối — Đảm bảo toàn bộ hệ thống hoạt động đúng
  - Đảm bảo tất cả tests pass, hỏi user nếu có thắc mắc.

## Ghi chú

- Các task đánh dấu `*` là tùy chọn và có thể bỏ qua để đẩy nhanh MVP
- Mỗi task tham chiếu đến requirements cụ thể để đảm bảo truy vết
- Checkpoints đảm bảo kiểm tra tính đúng đắn theo từng giai đoạn
- Property tests kiểm chứng các thuộc tính phổ quát (sử dụng Hypothesis)
- Unit tests kiểm chứng các trường hợp cụ thể và edge cases
- Ngôn ngữ triển khai: Python (phù hợp với tech stack và Hypothesis cho PBT)
