# Kế hoạch Triển khai: dbt Job Generator

## Tổng quan

Triển khai công cụ CLI dbt Job Generator bằng Python, tự động sinh dbt model (Spark SQL) từ file mapping CSV đã duyệt. Kiến trúc pipeline: CSV Parser → Mapping Validator → CTE Pipeline Builder → Generator Engine → Model Assembler, cùng các module hỗ trợ (DAG Builder, Test Generator, Change Manager, Review Report). Sử dụng Hypothesis cho property-based testing.

## Tasks

- [ ] 1. Thiết lập cấu trúc dự án và định nghĩa Data Models
  - [x] 1.1 Tạo cấu trúc thư mục dự án và cấu hình package
    - Tạo thư mục `dbt_job_generator/` với các sub-packages: `parser/`, `validator/`, `cte_builder/`, `engine/`, `assembler/`, `dag/`, `test_gen/`, `change_manager/`, `report/`, `batch/`, `catalog/`, `models/`
    - Tạo `pyproject.toml` hoặc `setup.py` với dependencies (hypothesis, pytest, click/typer cho CLI)
    - _Requirements: Tất cả_

  - [x] 1.2 Định nghĩa tất cả Data Models (Enums, Dataclasses)
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

  - [ ] 1.3 Viết unit tests cho Data Models
    - Test khởi tạo, equality, serialization cho các dataclass chính
    - Test enum values và conversions
    - _Requirements: 1.1, 1.2_

- [ ] 2. Triển khai CSV Parser và PrettyPrinter
  - [x] 2.1 Triển khai CSV Parser — đọc và phân tích File Mapping
    - Tạo file `parser/csv_parser.py` với class `CSVParser`
    - Implement `parse(file_path: str) -> Result[MappingSpec, ParseError]`: đọc CSV, nhận diện 5 section (Target, Input, Relationship, Mapping, Final Filter), parse từng section thành data model tương ứng
    - Implement parsing logic cho `SourceEntry` với 3 source types: `physical_table`, `unpivot_cte`, `derived_cte`
    - Implement parsing unpivot `select_fields` format: `key=source | TYPE:col | passthrough` thành `UnpivotFieldSpec`
    - Implement `parse_batch(directory: str) -> list[Result[MappingSpec, ParseError]]`
    - Trả về `ParseError` với vị trí (dòng/cột) và mô tả lỗi khi file không hợp lệ
    - _Requirements: 1.1, 1.2, 1.3_

  - [x] 2.2 Triển khai PrettyPrinter — chuyển MappingSpec về CSV
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

- [x] 3. Checkpoint — Đảm bảo Parser hoạt động đúng
  - Đảm bảo tất cả tests pass, hỏi user nếu có thắc mắc.

- [ ] 4. Triển khai Mapping Validator
  - [x] 4.1 Triển khai Mapping Validator — xác minh tính hợp lệ workflow
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

- [x] 5. Triển khai Transformation Rule Catalog
  - [x] 5.1 Triển khai Transformation Rule Catalog
    - Tạo file `catalog/rule_catalog.py` với class `TransformationRuleCatalog`
    - Implement `get_hash_function() -> str`: trả về hàm hash chuẩn (hash_id)
    - Implement `get_rule(rule_name: str) -> Optional[TransformationRule]`
    - Implement `validate_reference(rule_name: str) -> bool`
    - Implement `list_rules() -> list[TransformationRule]`
    - Định nghĩa `TransformationRule` dataclass với `name`, `type` (HASH/BUSINESS_LOGIC/DERIVED), `sql_template`, `is_exception`, `description`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6_

- [ ] 6. Triển khai CTE Pipeline Builder
  - [x] 6.1 Triển khai CTE Pipeline Builder — xây dựng WITH clause
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

- [x] 7. Checkpoint — Đảm bảo Parser, Validator, CTE Builder hoạt động đúng
  - Đảm bảo tất cả tests pass, hỏi user nếu có thắc mắc.

- [ ] 8. Triển khai Generator Engine và Rule Handlers
  - [x] 8.1 Triển khai DirectMapHandler và TypeCastHandler
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

  - [x] 8.3 Triển khai HashHandler và BusinessLogicHandler
    - Tạo file `engine/handlers/hash_handler.py`: sinh `hash_id(col1, col2, ...) AS target_col` từ catalog
    - Tạo file `engine/handlers/business_logic.py`: tra cứu catalog, sinh SQL expression hoặc placeholder comment cho exception rules
    - Báo lỗi khi rule tham chiếu không tồn tại trong catalog
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.6_

  - [ ]* 8.4 Viết property tests cho HASH và Business Logic
    - **Property 9: HASH sinh đúng hàm hash từ catalog và đảm bảo nhất quán**
    - **Property 10: Business Logic sinh đúng SQL hoặc placeholder từ catalog**
    - **Property 11: Generator báo lỗi khi tham chiếu rule không tồn tại trong catalog**
    - **Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.6**

  - [x] 8.5 Triển khai HardcodeHandler và NullHandler
    - Tạo file `engine/handlers/hardcode.py`: sinh `'value' :: string AS target_col` (string) hoặc `value :: type AS target_col` (numeric)
    - Tạo file `engine/handlers/null_handler.py`: sinh `NULL :: data_type AS target_col` cho unmapped fields
    - _Requirements: 6.1, 6.2, 3.1_

  - [ ]* 8.6 Viết property tests cho HARDCODE và NULL_MAP
    - **Property 12: HARDCODE sinh đúng giá trị với type casting**
    - **Property 13: NULL_MAP sinh đúng NULL với type casting**
    - **Validates: Requirements 6.1, 6.2, 3.1**

  - [x] 8.7 Triển khai Generator Engine chính — tổng hợp tất cả handlers
    - Tạo file `engine/generator_engine.py` với class `GeneratorEngine`
    - Implement `generate_sql(mapping: MappingSpec, catalog: TransformationRuleCatalog) -> GenerationResult`
    - Dispatch từng MappingEntry đến handler phù hợp dựa trên `MappingRule.pattern`
    - Tổng hợp `cte_definitions`, `select_columns`, `from_clause`, `final_filter`
    - Thu thập errors và warnings
    - _Requirements: 3.1, 3.2, 4.1, 4.2, 5.1, 5.3, 6.1, 6.2, 12.2_

- [ ] 9. Triển khai Model Assembler
  - [x] 9.1 Triển khai ConfigBlockGenerator và FromClauseGenerator
    - Tạo file `assembler/config_block.py`: sinh `{{ config(materialized='...', schema='...', tags=['...']) }}` — schema tương ứng layer (Silver cho B→S, Gold cho S→G)
    - Tạo file `assembler/from_clause.py`: sinh `FROM main_alias LEFT JOIN join_alias ON condition` từ Relationship section
    - _Requirements: 9.1, 9.2, 9.3, 12.1_

  - [ ]* 9.2 Viết property tests cho Config Block và FROM clause
    - **Property 19: Config Block schema tương ứng với layer**
    - **Property 18: JOIN clause sinh đúng từ Relationship section**
    - **Validates: Requirements 9.1, 9.2, 9.3, 12.1**

  - [x] 9.3 Triển khai Model Assembler chính
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

- [x] 10. Checkpoint — Đảm bảo core pipeline (Parser → Validator → CTE Builder → Engine → Assembler) hoạt động đúng
  - Đảm bảo tất cả tests pass, hỏi user nếu có thắc mắc.

- [ ] 11. Triển khai Dependency DAG Builder
  - [x] 11.1 Triển khai DAG Builder — phân tích và sinh dependency graph
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
  - [x] 12.1 Triển khai Test Generator — sinh schema tests và kiểm tra unit test compliance
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
  - [x] 13.1 Triển khai Change Manager — xử lý incremental changes
    - Tạo file `change_manager/change_manager.py` với class `ChangeManager`
    - Implement `diff(old_mapping, new_mapping) -> MappingDiff`: so sánh 2 MappingSpec, phát hiện inputs/mappings thêm/xóa/sửa, relationships/final_filter thay đổi
    - Implement `process_change(request: MappingChangeRequest, context: ProjectContext) -> ChangeResult`: xử lý UPDATE_RULE, ADD_FIELD, NEW_MAPPING
    - Implement `detect_downstream_impact(change: MappingDiff, dag: DependencyDAG) -> list[str]`: phát hiện downstream models bị ảnh hưởng
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5_

  - [x] 13.2 Triển khai Version Store — lưu lịch sử phiên bản mapping
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

- [x] 14. Checkpoint — Đảm bảo DAG Builder, Test Generator, Change Manager hoạt động đúng
  - Đảm bảo tất cả tests pass, hỏi user nếu có thắc mắc.

- [ ] 15. Triển khai Batch Processor
  - [x] 15.1 Triển khai Batch Processor — xử lý nhiều file mapping
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
  - [x] 16.1 Triển khai Review Report Generator — sinh báo cáo review
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
  - [x] 17.1 Tích hợp pipeline end-to-end
    - Tạo file `pipeline.py`: kết nối CSVParser → MappingValidator → CTEPipelineBuilder → GeneratorEngine → ModelAssembler
    - Implement `generate_model(file_path: str, context: ProjectContext, catalog: TransformationRuleCatalog) -> Result[str, list[ErrorResponse]]`
    - Implement `generate_batch(directory: str, context: ProjectContext, catalog: TransformationRuleCatalog) -> BatchResult`
    - Tích hợp ChangeManager, TestGenerator, ReviewReportGenerator vào pipeline
    - _Requirements: 12.1, 12.2, 12.3, 13.1_

  - [x] 17.2 Triển khai CLI interface
    - Tạo file `cli.py` với các commands: `generate` (single file), `batch` (directory), `validate` (check only), `diff` (compare mappings), `report` (generate review report)
    - Sử dụng click hoặc typer cho CLI framework
    - _Requirements: Tất cả_

  - [ ]* 17.3 Viết integration tests end-to-end
    - Test pipeline hoàn chỉnh: file mapping CSV → dbt model .sql output
    - So sánh output với expected SQL từ mapping mẫu thực tế (involved-party-electronic-address, fund-management-company)
    - Test batch processing end-to-end
    - Test change management workflow hoàn chỉnh
    - _Requirements: 12.1, 12.2, 12.3, 13.1, 14.1_

- [x] 18. Checkpoint cuối — Đảm bảo toàn bộ hệ thống hoạt động đúng
  - Đảm bảo tất cả tests pass, hỏi user nếu có thắc mắc.

- [ ] 19. Cập nhật Data Models cho Validation Enhancement
  - [ ] 19.1 Cập nhật models/validation.py — thêm BlockError, cập nhật ValidationWarning, ValidationResult, thêm BatchValidationResult
    - Thêm dataclass `BlockError` với các trường: `error_type: str` (PARSE_ERROR, CTE_DEPENDENCY, SCHEMA_COLUMN_NOT_FOUND, SCHEMA_DATA_TYPE_MISMATCH), `message: str`, `location: Optional[ErrorLocation]`, `context: dict`
    - Cập nhật dataclass `ValidationWarning` — thêm trường `warning_type: str` (MISSING_SOURCE, MISSING_PREREQUISITE, MISSING_SCHEMA_FILE) và `context: dict`
    - Cập nhật dataclass `ValidationResult` — thêm `has_warnings: bool`, `block_errors: list[BlockError]`, cập nhật `warnings: list[ValidationWarning]`. `is_valid` giờ chỉ False khi có block_errors (WARNING không ảnh hưởng is_valid)
    - Thêm dataclass `BatchValidationResult` với: `total: int`, `valid_count: int`, `warning_count: int`, `blocked_count: int`, `results: dict[str, ValidationResult]`
    - _Requirements: 2.1-2.11, 3.5_

  - [ ] 19.2 Thêm models/schema.py — TargetSchemaFile, SchemaColumn, SchemaValidationError
    - Tạo file `dbt_job_generator/models/schema.py`
    - Thêm dataclass `SchemaColumn` với: `name: str`, `data_type: str`, `nullable: bool`, `description: Optional[str]`
    - Thêm dataclass `TargetSchemaFile` với: `table_name: str`, `columns: list[SchemaColumn]`
    - Thêm dataclass `SchemaValidationError` với: `error_type: str` (COLUMN_NOT_FOUND, DATA_TYPE_MISMATCH), `column_name: str`, `expected: str`, `actual: str`, `message: str`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.6_

  - [ ] 19.3 Cập nhật models/enums.py — thêm ValidationSeverity enum
    - Thêm enum `ValidationSeverity` với hai giá trị: `BLOCK = "BLOCK"`, `WARNING = "WARNING"`
    - _Requirements: 2.1-2.8_

- [ ] 20. Tạo sample Target Schema File cho fund_management_company
  - [ ] 20.1 Tạo thư mục schemas/silver/ và file fund_management_company.json
    - Tạo file `schemas/silver/fund_management_company.json`
    - Schema file chứa 28 columns từ mapping CSV fund_management_company_FIMS_FUNDCOMPANY:
      - fund_management_company_id (string), fund_management_company_code (string), source_system_code (string), fund_management_company_name (string), fund_management_company_short_name (string), fund_management_company_english_name (string), practice_status_code (string), charter_capital_amount (decimal(23,2)), dorf_indicator (string), license_decision_number (string), license_decision_date (date), active_date (date), stop_date (date), business_type_codes (string), created_by (string), created_timestamp (timestamp), updated_timestamp (timestamp), country_of_registration_id (string), country_of_registration_code (string), life_cycle_status_code (string), director_name (string), depository_certificate_number (string), company_type_codes (string), description (string), company_type_code (string), fund_type_code (string), business_license_number (string), website (string)
    - Mỗi column: `name`, `data_type`, `nullable`, `description`
    - Format JSON theo chuẩn TargetSchemaFile: `{"table_name": "fund_management_company", "columns": [...]}`
    - _Requirements: 3.6_

- [ ] 21. Triển khai Schema Validator
  - [ ] 21.1 Tạo validator/schema_validator.py với class SchemaValidator
    - Tạo file `dbt_job_generator/validator/schema_validator.py`
    - Implement `load_schema(schema_dir: str, schema: str, table_name: str) -> Optional[TargetSchemaFile]`: đọc JSON file từ `{schema_dir}/{schema}/{table_name}.json`, trả về None nếu file không tồn tại
    - Implement `validate(mapping: MappingSpec, schema_dir: str) -> list[SchemaValidationError]`: kiểm tra tất cả target columns trong Mapping section so với TargetSchemaFile
    - Lỗi `COLUMN_NOT_FOUND`: cột trong mapping không có trong schema file → trả về lỗi chỉ rõ tên cột không khớp
    - Lỗi `DATA_TYPE_MISMATCH`: data type trong mapping không tương thích với schema file → trả về lỗi chỉ rõ actual vs expected
    - Nếu schema file không tồn tại → trả về list rỗng (caller sẽ xử lý WARNING)
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.6_

  - [ ]* 21.2 Viết property tests cho Schema Validator
    - **Property 33: Schema validation phát hiện đúng column name không khớp**
    - **Property 34: Schema validation phát hiện đúng data type không tương thích**
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4**

  - [ ]* 21.3 Viết unit tests cho Schema Validator
    - Test load_schema với file tồn tại và không tồn tại
    - Test validate: mapping khớp hoàn toàn với schema → không có lỗi
    - Test validate: mapping có cột không tồn tại trong schema → COLUMN_NOT_FOUND
    - Test validate: mapping có data type không tương thích → DATA_TYPE_MISMATCH
    - Test validate: schema file rỗng (không có cột)
    - Test với mapping mẫu fund_management_company và schema file thực tế
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

- [ ] 22. Cập nhật Mapping Validator — WARNING/BLOCK severity classification
  - [ ] 22.1 Refactor MappingValidator.validate() — trả về ValidationResult mới với block_errors và warnings
    - Cập nhật `validate()` signature: thêm `schema_dir: Optional[str] = None`
    - Missing source tables → `ValidationWarning(warning_type="MISSING_SOURCE", ...)` — KHÔNG ngăn generate
    - Missing prerequisite jobs → `ValidationWarning(warning_type="MISSING_PREREQUISITE", ...)` — KHÔNG ngăn generate
    - CTE pipeline errors → `BlockError(error_type="CTE_DEPENDENCY", ...)` — ngăn generate
    - Tích hợp SchemaValidator: nếu schema_dir được cung cấp, gọi SchemaValidator.validate()
      - Schema validation errors (COLUMN_NOT_FOUND, DATA_TYPE_MISMATCH) → `BlockError`
      - Schema file không tồn tại → `ValidationWarning(warning_type="MISSING_SCHEMA_FILE", ...)`
    - `is_valid = True` khi không có block_errors (WARNING không ảnh hưởng)
    - `has_warnings = True` khi có bất kỳ warning nào
    - Giữ backward compatibility: các trường cũ (missing_sources, missing_prerequisites, cte_errors) vẫn được populate
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.11, 3.1, 3.2, 3.3, 3.4, 3.5_

  - [ ] 22.2 Thêm validate_batch() method vào MappingValidator
    - Implement `validate_batch(directory: str, context: ProjectContext, schema_dir: Optional[str] = None) -> BatchValidationResult`
    - Parse tất cả CSV files trong thư mục, validate từng file
    - Sinh `BatchValidationResult` với: total, valid_count (không BLOCK, không WARNING), warning_count (có WARNING, không BLOCK), blocked_count (có BLOCK)
    - Mỗi file có ValidationResult riêng trong `results` dict
    - Fail-safe: parse error ở một file không dừng batch
    - _Requirements: 2.9, 2.10_

  - [ ]* 22.3 Viết property tests cho WARNING/BLOCK classification
    - **Property 35: Phân loại WARNING vs BLOCK đúng theo severity rules**
    - **Property 36: Batch validation result đếm nhất quán**
    - **Validates: Requirements 2.1-2.10**

  - [ ]* 22.4 Viết unit tests cho WARNING/BLOCK classification và validate_batch
    - Test mapping chỉ có WARNING (missing sources) → is_valid = True, has_warnings = True
    - Test mapping có BLOCK (CTE lỗi) → is_valid = False
    - Test mapping có cả WARNING và BLOCK → is_valid = False, has_warnings = True
    - Test mapping không có lỗi → is_valid = True, has_warnings = False
    - Test schema validation tích hợp: schema lỗi → BLOCK, thiếu schema file → WARNING
    - Test validate_batch: thư mục rỗng, mix valid/warning/blocked files
    - Test validate_batch: báo cáo tổng hợp đếm đúng
    - _Requirements: 2.1-2.11, 3.1-3.6_

- [ ] 23. Cập nhật BatchProcessor và Pipeline — bỏ skip_validation, luôn chạy validation
  - [ ] 23.1 Cập nhật BatchProcessor — bỏ skip_validation, luôn chạy validation trước generate
    - Xóa parameter `skip_validation` khỏi `__init__()` và `process_single()`
    - Thêm parameter `schema_dir: Optional[str] = None` vào `__init__()` và `process_single()`
    - Trong `process_single()`: luôn gọi `MappingValidator.validate()` trước khi generate
    - Nếu có BLOCK errors → không generate, ghi vào `FailedMapping` với chi tiết block errors
    - Nếu chỉ có WARNING → tiếp tục generate, ghi warnings vào báo cáo (thêm warnings vào `GeneratedModel` hoặc `BatchSummary`)
    - Cập nhật `BatchSummary` — thêm `warning_count` và `warnings: list[str]`
    - _Requirements: 13.4, 2.11_

  - [ ] 23.2 Cập nhật Pipeline — generate_model() luôn validate, thêm schema_dir parameter
    - Thêm `schema_dir: Optional[str] = None` vào `Pipeline.__init__()` và `generate_model()`
    - Trong `generate_model()`: luôn gọi validation trước khi generate
    - Nếu có BLOCK errors → raise exception với chi tiết lỗi
    - Nếu chỉ có WARNING → tiếp tục generate, trả về SQL content (warnings có thể log hoặc trả kèm)
    - Xóa `skip_validation` parameter khỏi `Pipeline.__init__()`
    - Cập nhật `generate_batch()` — truyền `schema_dir` cho `BatchProcessor`
    - _Requirements: 13.4, 2.11_

  - [ ]* 23.3 Viết unit tests cho validation integration trong BatchProcessor và Pipeline
    - Test BatchProcessor: mapping có BLOCK → failed, mapping chỉ WARNING → successful
    - Test Pipeline.generate_model(): BLOCK → exception, WARNING → success với warnings
    - Test Pipeline: không còn skip_validation option
    - Test batch: mix BLOCK/WARNING/valid files → đúng phân loại
    - _Requirements: 13.4, 2.11_

- [ ] 24. Thêm Impact Analysis Warning cho validate flow
  - [ ] 24.1 Tích hợp ChangeManager.detect_downstream_impact() vào validate flow
    - Khi validate một mapping đã tồn tại trong `ProjectContext.existing_models` (update scenario):
      - So sánh mapping mới với version cũ (nếu có trong VersionStore hoặc context)
      - Nếu có thay đổi, gọi `ChangeManager.detect_downstream_impact()` với DAG từ context
      - Thêm `ValidationWarning(warning_type="DOWNSTREAM_IMPACT", ...)` liệt kê danh sách downstream models bị ảnh hưởng trực tiếp
    - Impact analysis chỉ là WARNING, không ngăn generate
    - _Requirements: 15.5_

  - [ ]* 24.2 Viết unit tests cho impact analysis warning
    - Test validate mapping mới (không có version cũ) → không có downstream warning
    - Test validate mapping đã tồn tại, có thay đổi, có downstream models → WARNING liệt kê models
    - Test validate mapping đã tồn tại, có thay đổi, không có downstream models → không có WARNING
    - _Requirements: 15.5_

- [ ] 25. Cập nhật CLI — thêm validate-batch, schema-dir option
  - [ ] 25.1 Thêm command validate-batch vào CLI
    - Thêm command `validate-batch` nhận argument `directory`
    - Gọi `MappingValidator.validate_batch()` và hiển thị báo cáo tổng hợp
    - Hiển thị: total files, valid count, warning count, blocked count
    - Liệt kê chi tiết WARNING và BLOCK cho từng file
    - Exit code 0 nếu không có BLOCK, exit code 1 nếu có BLOCK
    - _Requirements: 2.9, 2.10_

  - [ ] 25.2 Thêm --schema-dir option cho generate, batch, validate, validate-batch
    - Thêm `--schema-dir` option (type=click.Path) cho tất cả commands: `generate`, `batch`, `validate`, `validate-batch`
    - Truyền `schema_dir` xuống Pipeline và MappingValidator
    - _Requirements: 3.6_

  - [ ] 25.3 Cập nhật output format — hiển thị WARNING/BLOCK rõ ràng
    - Cập nhật command `validate`: hiển thị phân loại WARNING vs BLOCK
    - Cập nhật command `generate`: hiển thị warnings nếu có (nhưng vẫn generate)
    - Cập nhật command `batch`: hiển thị summary với warning_count, liệt kê warnings
    - Xóa `--skip-validation` flag khỏi command `batch`
    - _Requirements: 2.1-2.11_

  - [ ]* 25.4 Viết unit tests cho CLI commands mới
    - Test `validate-batch` với thư mục có mix valid/warning/blocked files
    - Test `--schema-dir` option truyền đúng cho tất cả commands
    - Test output format hiển thị WARNING/BLOCK đúng
    - Test `batch` command không còn `--skip-validation` flag
    - _Requirements: 2.9, 2.10, 3.6_

- [ ] 26. Checkpoint — Đảm bảo validation enhancement hoạt động đúng
  - Đảm bảo tất cả tests pass, hỏi user nếu có thắc mắc.

- [ ] 27. Cập nhật README.md
  - [ ] 27.1 Cập nhật workflow section — mô tả validate-batch, schema validation
    - Thêm mô tả về WARNING/BLOCK severity classification
    - Thêm mô tả về validate-batch command và use case
    - Cập nhật workflow: generate luôn chạy validation (không còn skip)
    - _Requirements: 2.1-2.11_

  - [ ] 27.2 Thêm section về Target Schema File — format, thư mục, cách tạo
    - Mô tả format JSON của Target_Schema_File
    - Mô tả quy ước đường dẫn: `schemas/{schema}/{table_name}.json`
    - Ví dụ với fund_management_company.json
    - _Requirements: 3.6_

  - [ ] 27.3 Cập nhật CLI commands — thêm validate-batch, --schema-dir
    - Thêm mô tả command `validate-batch <directory> [--schema-dir <path>]`
    - Thêm mô tả `--schema-dir` option cho generate, batch, validate
    - Cập nhật mô tả command `batch` — bỏ `--skip-validation`
    - _Requirements: 2.9, 2.10, 3.6_

- [x] 28. Thêm SELECT * validation rule
  - [x] 28.1 Thêm BLOCK rule SELECT_STAR vào MappingValidator
  - [x] 28.2 Cập nhật unit tests cho SELECT * validation

- [x] 29. Tạo Bronze schema files và Spark integration tests
  - [x] 29.1 Tạo schemas/bronze/ với FUNDCOMPANY.json, FUNDCOMBUSINES.json, FUNDCOMTYPE.json
  - [x] 29.2 Tạo hash_id UDF (dbt_job_generator/udfs/hash_id.py)
  - [x] 29.3 Tạo Spark test infrastructure (conftest.py, sql_test_helper.py, fake_data_factory.py)
  - [x] 29.4 Tạo Spark integration test cho fund_management_company

- [x] 30. Triển khai Spark Test Generator — tự động sinh file pytest cho mỗi dbt model
  - [x] 30.1 Tạo module spark_test_gen/test_generator.py với class SparkTestGenerator
    - Implement `generate_test(mapping: MappingSpec, sql_content: str, schema_dir: str) -> str` (nội dung file test)
    - Đọc Bronze schema files để xác định cấu trúc bảng nguồn
    - Sinh fake data rows (1-3 rows/table)
    - Sinh file pytest với: imports, schemas, fake data, SQL constant, expected columns, 3 test methods (`test_sql_executes`, `test_correct_columns`, `test_has_rows`)
    - Xử lý SQL: strip config, replace Jinja vars, convert `::` thành `CAST()`, replace `hash_id` bằng `sha2(concat_ws('|', ...), 256)`
    - Nếu Source_Schema_File không tồn tại cho bảng nguồn → sinh file test với TODO comment chỉ rõ bảng cần bổ sung schema
    - Sử dụng hạ tầng test Spark hiện có: `fake_data_factory.register_fake_table`, `sql_test_helper.prepare_sql`
    - _Requirements: 18.1, 18.2, 18.3, 18.4, 18.5_
  - [x] 30.2 Tích hợp SparkTestGenerator vào Pipeline và BatchProcessor
    - Khi `generate_model()` thành công, đồng thời gọi `SparkTestGenerator.generate_test()` để sinh test file
    - Implement `generate_test_batch(models: list[GeneratedModel], schema_dir: str) -> dict[str, str]` cho batch
    - Khi batch, sinh Spark_Test_File cho tất cả dbt_Model thành công
    - Lưu test files vào thư mục output tests tương ứng (mặc định: `output/tests/`)
    - _Requirements: 18.1, 18.6_
  - [x] 30.3 Cập nhật CLI — thêm option sinh test files
    - Thêm `--test-output-dir` option cho `generate` và `batch` commands
    - Mặc định: `output/tests/`
    - Khi option được chỉ định, sinh Spark_Test_File song song với dbt_Model
    - _Requirements: 18.1, 18.6_
  - [ ]* 30.4 Viết unit tests cho SparkTestGenerator
    - Test `generate_test()` với mapping mẫu fund_management_company → file test hợp lệ
    - Test file sinh ra là Python hợp lệ (compile bằng `ast.parse`)
    - Test file sinh ra chứa đúng 3 test methods
    - Test expected columns khớp với Mapping section
    - Test xử lý SQL: strip config, replace vars, convert `::`, replace `hash_id`
    - Test khi Source_Schema_File thiếu → TODO comment trong file test
    - Test `generate_test_batch()` sinh test files cho tất cả models thành công
    - **Property 38: Spark Test File hợp lệ và đầy đủ test methods**
    - _Requirements: 18.1, 18.2, 18.3, 18.4, 18.5, 18.6_

- [x] 31. Chuyển Transformation Rule Catalog sang CSV format
  - [x] 31.1 Tạo file `config/transformation_rules.csv` với format: Rule Name, Rule Type, SQL Template, Is Exception, Description
  - [x] 31.2 Tạo CSV reader cho TransformationRuleCatalog

- [x] 32. Triển khai Generation Log — phát hiện mapping thay đổi/thêm mới
  - [x] 32.1 Tạo module generation_log.py
  - [x] 32.2 Tích hợp Generation Log vào BatchProcessor

## Ghi chú

- Các task đánh dấu `*` là tùy chọn và có thể bỏ qua để đẩy nhanh MVP
- Mỗi task tham chiếu đến requirements cụ thể để đảm bảo truy vết
- Checkpoints đảm bảo kiểm tra tính đúng đắn theo từng giai đoạn
- Property tests kiểm chứng các thuộc tính phổ quát (sử dụng Hypothesis)
- Unit tests kiểm chứng các trường hợp cụ thể và edge cases
- Ngôn ngữ triển khai: Python (phù hợp với tech stack và Hypothesis cho PBT)
