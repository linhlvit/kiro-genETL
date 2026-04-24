# dbt Job Generator

Công cụ CLI tự động sinh dbt model (Spark SQL) từ file mapping CSV đã được duyệt, phục vụ kiến trúc Data Lakehouse theo mô hình Medallion.

## Mục đích

Trong luồng Data Lakehouse theo Medallion Architecture (Bronze → Silver → Gold), việc viết tay các dbt model cho hàng trăm bảng là tốn thời gian và dễ sai sót. dbt Job Generator giải quyết vấn đề này bằng cách:

- Đọc file mapping CSV (đã được duyệt bởi team) chứa thông tin source → target, data type, transformation rule
- Tự động sinh file `.sql` dbt model hoàn chỉnh, sẵn sàng deploy sau khi review
- Hỗ trợ xử lý hàng loạt (batch) toàn bộ thư mục mapping
- Sinh dependency DAG đảm bảo thứ tự chạy đúng
- Sinh báo cáo review để Data Engineer Lead duyệt trước khi deploy
- Tự động sinh Spark integration test (pytest) cho mỗi dbt model

**Tech stack tích hợp:** Airflow (Orchestration) · dbt (Transformation Modeling) · EMR on Spark (ETL Engine)

## Cài đặt

Yêu cầu Python >= 3.10.

```bash
# Clone repo
git clone <repo-url>
cd dbt-job-generator

# Cài đặt (editable mode)
pip install -e .

# Cài thêm dev dependencies (pytest, hypothesis) nếu cần chạy test
pip install -e ".[dev]"
```

Sau khi cài đặt, lệnh `dbt-job-gen` sẽ có sẵn trong terminal.

## Cấu trúc dự án

```
dbt_job_generator/
├── cli.py                  # CLI entry point (click)
├── pipeline.py             # Pipeline tích hợp end-to-end
├── parser/
│   ├── csv_parser.py       # Đọc file mapping CSV → MappingSpec
│   └── pretty_printer.py   # Chuyển MappingSpec → CSV (round-trip)
├── validator/
│   ├── mapping_validator.py  # Xác minh workflow (source tồn tại, dependency)
│   └── schema_validator.py   # Xác minh schema bảng đích (tên cột, data type)
├── cte_builder/
│   └── pipeline_builder.py   # Sinh WITH clause (physical_table, unpivot_cte, derived_cte)
├── engine/
│   ├── generator_engine.py   # Dispatch mapping rules → SQL fragments
│   └── handlers/             # DIRECT_MAP, CAST, HASH, HARDCODE, NULL, Business Logic
├── assembler/
│   ├── config_block.py       # Sinh {{ config(...) }}
│   ├── from_clause.py        # Sinh FROM + JOINs
│   └── model_assembler.py    # Tổng hợp file .sql hoàn chỉnh
├── dag/
│   └── dag_builder.py        # Phân tích dependency, phát hiện cycle, topological sort
├── catalog/
│   └── rule_catalog.py       # Quản lý transformation rules (đọc từ CSV)
├── batch/
│   └── batch_processor.py    # Xử lý hàng loạt nhiều file mapping
├── test_gen/
│   └── test_generator.py     # Sinh schema tests, kiểm tra unit test compliance
├── spark_test_gen/
│   └── spark_test_generator.py  # Tự động sinh file pytest cho mỗi dbt model
├── change_manager/
│   ├── change_manager.py     # Diff mapping, phát hiện downstream impact
│   └── version_store.py      # Lưu lịch sử phiên bản mapping
├── report/
│   └── review_report.py      # Sinh báo cáo review cho con người
├── udfs/
│   └── hash_id.py            # hash_id UDF — SHA-256 surrogate key generator
├── generation_log.py         # Generation log — phát hiện mapping thay đổi/thêm mới
└── models/                   # Data models (dataclasses, enums, errors)
```

## Quy trình hoạt động (Workflow)

Quy trình sử dụng tool gồm 5 bước, trong đó bước Validate là tùy chọn nhưng được khuyến nghị khi làm việc trong dự án có nhiều mapping phụ thuộc nhau.

```
                                    ┌─────────────────────────────────┐
                                    │  1. Chuẩn bị file mapping CSV  │
                                    └──────────────┬──────────────────┘
                                                   │
                                                   ▼
                              ┌──────────────────────────────────────────┐
                              │  2. Validate (tùy chọn, khuyến nghị)    │
                              │     dbt-job-gen validate mapping.csv    │
                              │                                          │
                              │  Kiểm tra:                               │
                              │  • Cú pháp CSV hợp lệ                   │
                              │  • CTE pipeline đúng thứ tự dependency  │
                              │  • Source tables đã khai báo (nếu có    │
                              │    project context)                      │
                              └──────────────┬───────────────────────────┘
                                             │
                                             ▼
                              ┌──────────────────────────────────────────┐
                              │  3. Generate                             │
                              │     dbt-job-gen generate mapping.csv     │
                              │     dbt-job-gen batch mappings/ -o out/  │
                              │                                          │
                              │  Pipeline nội bộ:                        │
                              │  CSV Parser → CTE Builder →              │
                              │  Generator Engine → Model Assembler      │
                              └──────────────┬───────────────────────────┘
                                             │
                                             ▼
                              ┌──────────────────────────────────────────┐
                              │  4. Review output                        │
                              │     Kiểm tra file .sql sinh ra           │
                              │     Spot check business logic phức tạp   │
                              └──────────────┬───────────────────────────┘
                                             │
                                             ▼
                              ┌──────────────────────────────────────────┐
                              │  5. Deploy vào dbt project               │
                              │     Copy .sql → dbt project → dbt run    │
                              └──────────────────────────────────────────┘
```

### Bước 1: Chuẩn bị file mapping CSV

Data Engineer tạo file mapping CSV theo cấu trúc 5 section. File này thường được review và duyệt bởi team trước khi đưa vào tool.

| Section | Mô tả | Bắt buộc |
|---------|--------|----------|
| **Target** | Thông tin bảng đích: schema, table name, ETL handle | Có |
| **Input** | Danh sách source entries: `physical_table`, `unpivot_cte`, `derived_cte` | Có |
| **Relationship** | Quan hệ JOIN giữa các CTE (LEFT JOIN, INNER JOIN, ...) | Không |
| **Mapping** | Ánh xạ từng cột: target column, transformation expression, data type | Có |
| **Final Filter** | Điều kiện cuối: UNION ALL (merge nhiều luồng) hoặc WHERE | Không |

Tổ chức thư mục khuyến nghị:
```
mapping/
├── silver/          # Mapping Bronze → Silver
│   ├── involved_party_electronic_address.csv
│   └── fund_management_company_FIMS_FUNDCOMPANY.csv
└── gold/            # Mapping Silver → Gold
    └── dim_fund_management_company.csv
```

### Bước 2: Validate (tùy chọn)

Lệnh `validate` kiểm tra tính hợp lệ của file mapping mà không sinh code. Kết quả validation được phân loại thành hai mức:

- **BLOCK** — lỗi nghiêm trọng, ngăn generate (CTE dependency lỗi, schema không khớp, SELECT *)
- **WARNING** — ghi nhận nhưng không ngăn generate (missing sources, missing prerequisites, thiếu schema file)

```bash
# Validate 1 file
dbt-job-gen validate mapping/silver/fund_management_company_FIMS_FUNDCOMPANY.csv

# Validate 1 file với schema validation
dbt-job-gen validate mapping/silver/fund_management_company_FIMS_FUNDCOMPANY.csv --schema-dir schemas/

# Validate hàng loạt cả thư mục
dbt-job-gen validate-batch mapping/silver/ --schema-dir schemas/
```

Khi chạy `validate-batch`, tool sinh báo cáo tổng hợp: số file hợp lệ, số file có WARNING, số file bị BLOCK, và chi tiết lỗi từng file.

### Bước 3: Generate

Đây là bước chính — sinh 3 loại output từ mapping CSV:
- File `.sql` dbt model
- File `test_*_spark.py` Spark integration test (bắt buộc)
- File `review_report.md` báo cáo review (khi chạy batch)

Tool luôn chạy validation trước khi generate:
- Nếu có lỗi BLOCK → dừng và báo lỗi
- Nếu chỉ có WARNING → tiếp tục generate, hiển thị warnings

```bash
# Sinh 1 file (SQL + test)
dbt-job-gen generate mapping/silver/fund_management_company_FIMS_FUNDCOMPANY.csv \
  --schema-dir schemas/ -o output/silver/fund_management_company.sql

# Sinh hàng loạt (SQL + tests + review report)
dbt-job-gen batch mapping/silver/ -o output/silver/ --schema-dir schemas/

# Chỉ sinh các job chưa tạo (mặc định, skip file đã tồn tại)
dbt-job-gen batch mapping/silver/ -o output/silver/ --schema-dir schemas/

# Generate lại tất cả từ đầu
dbt-job-gen batch mapping/silver/ -o output/silver/ --schema-dir schemas/ --force
```

Khi chạy `generate` hoặc `batch`, tool thực hiện pipeline nội bộ:
1. **CSV Parser** — đọc file CSV, tách 5 section, parse thành cấu trúc dữ liệu nội bộ
2. **CTE Pipeline Builder** — sinh WITH clause cho từng source entry (physical_table, unpivot_cte, derived_cte)
3. **Generator Engine** — áp dụng mapping rules (DIRECT_MAP, HASH, HARDCODE, NULL, Business Logic) để sinh SQL fragments
4. **Model Assembler** — tổng hợp config block + WITH clause + SELECT + FROM/JOINs + Final Filter thành file `.sql` hoàn chỉnh

### Bước 4: Review output

Tool sinh ra cho mỗi lần chạy:
- `{output_dir}/{model_name}.sql` — dbt model
- `{output_dir}/tests/test_{model_name}_spark.py` — Spark integration test
- `{output_dir}/review_report.md` — báo cáo review (khi batch)

Các điểm cần review (xem `review_report.md`):
- Business logic phức tạp (CASE WHEN, nhiều JOIN)
- Trường đánh dấu exception (placeholder `/* TODO: implement manually */`)
- Thứ tự chạy giữa các model (dependency)
- Test compliance status

### Bước 5: Deploy vào dbt project

Sau khi review xong, copy các file `.sql` vào thư mục models của dbt project:

```bash
# Copy output vào dbt project
cp output/silver/*.sql /path/to/dbt_project/models/silver/

# Chạy dbt
cd /path/to/dbt_project
dbt run --models silver.*
```

## Các lệnh sử dụng chính

### `generate` — Sinh 1 dbt model + test

```bash
dbt-job-gen generate <file_path> [--output/-o <output_path>] [--schema-dir <path>] [--test-output-dir <path>]
```

| Tham số | Mô tả |
|---------|--------|
| `file_path` | Đường dẫn đến file mapping CSV |
| `--output`, `-o` | Đường dẫn file output `.sql`. Nếu không chỉ định, in ra stdout |
| `--schema-dir` | Đường dẫn thư mục chứa schema files (mặc định: `schemas/`) |
| `--test-output-dir` | Thư mục cho test files (mặc định: `{output_dir}/tests/`) |

Tool luôn sinh cả file `.sql` và file `test_*_spark.py`. Test file mặc định lưu vào thư mục `tests/` cùng cấp với output SQL.

Ví dụ:
```bash
# Sinh SQL + test (output: output/silver/model.sql + output/silver/tests/test_model_spark.py)
dbt-job-gen generate mapping/silver/fund_management_company_FIMS_FUNDCOMPANY.csv \
  --schema-dir schemas/ -o output/silver/fund_management_company.sql
```

### `batch` — Sinh hàng loạt từ thư mục

```bash
dbt-job-gen batch <directory> [--output-dir/-o <output_directory>] [--schema-dir <path>] [--test-output-dir <path>] [--force]
```

| Tham số | Mô tả |
|---------|--------|
| `directory` | Thư mục chứa các file mapping CSV |
| `--output-dir`, `-o` | Thư mục output (mặc định: `output/`) |
| `--schema-dir` | Đường dẫn thư mục chứa schema files (mặc định: `schemas/`) |
| `--test-output-dir` | Thư mục cho test files (mặc định: `{output_dir}/tests/`) |
| `--rules-csv` | Đường dẫn file CSV chứa transformation rules (mặc định: không dùng) |
| `--force` | Generate lại tất cả từ đầu. Mặc định: chỉ sinh mapping mới/thay đổi (dựa trên generation log) |

Ví dụ:
```bash
# Sinh hàng loạt (chỉ các job chưa tạo)
dbt-job-gen batch mapping/silver/ -o output/silver/ --schema-dir schemas/

# Generate lại tất cả từ đầu
dbt-job-gen batch mapping/silver/ -o output/silver/ --schema-dir schemas/ --force
```

Output:
```
Total: 2, Success: 2, Errors: 0, Warnings: 2
  WARNING: fund_management_company.csv: Missing source: FUNDCOMPANY (bronze)
Output written to: output/silver/
  (0 new, 0 changed, 2 unchanged skipped)
Test files written to: output/silver/tests/
Review report: output/silver/review_report.md
```

Khi chạy batch, tool sử dụng generation log (`.generation_log.json`) để phát hiện mapping thay đổi:
- Mapping mới (chưa có trong log) → generate
- Mapping thay đổi (checksum khác) → generate lại
- Mapping không đổi → skip
- `--force` bỏ qua log, generate tất cả

Mỗi file mapping thành công tạo 3 outputs:
- `{output_dir}/{model_name}.sql` — dbt model
- `{output_dir}/tests/test_{model_name}_spark.py` — Spark test
- `{output_dir}/review_report.md` — báo cáo review tổng hợp

### `validate` — Kiểm tra mapping mà không sinh code

```bash
dbt-job-gen validate <file_path> [--schema-dir <path>]
```

Ví dụ:
```bash
dbt-job-gen validate mappings/involved-party-electronic-address.csv --schema-dir schemas/
```

Output khi hợp lệ:
```
Validation passed.
```

Output khi có warnings (vẫn pass):
```
Validation passed with warnings:
  WARNING [MISSING_SOURCE]: Missing source: UNKNOWN_TABLE (bronze)
```

Output khi có lỗi BLOCK:
```
Validation failed (BLOCK errors found):
  BLOCK [CTE_DEPENDENCY]: CTE 'unpivot_a' references alias 'nonexistent' which is not declared
  BLOCK [SELECT_STAR]: Mapping section is empty — would produce SELECT *
```

### `validate-batch` — Kiểm tra hàng loạt cả thư mục

```bash
dbt-job-gen validate-batch <directory> [--schema-dir <path>]
```

| Tham số | Mô tả |
|---------|--------|
| `directory` | Thư mục chứa các file mapping CSV |
| `--schema-dir` | Đường dẫn thư mục chứa schema files |

Ví dụ:
```bash
dbt-job-gen validate-batch mappings/silver/ --schema-dir schemas/
```

Output:
```
Total: 10, Valid: 7, Warnings: 2, Blocked: 1

  bad_cte.csv: BLOCKED
    BLOCK [CTE_DEPENDENCY]: CTE 'u1' references alias 'missing'

  no_schema.csv: WARNING
    WARNING [MISSING_SCHEMA_FILE]: No schema file found for target table 'some_table'
```

Exit code 0 nếu không có BLOCK, exit code 1 nếu có BLOCK.

## Mapping Patterns được hỗ trợ

| Pattern | Transformation trong CSV | SQL sinh ra |
|---------|--------------------------|-------------|
| DIRECT_MAP | `alias.column` | `alias.column :: data_type AS target_col` |
| HASH | `hash_id('SRC', alias.id)` | `hash_id('SRC', alias.id) AS target_col` |
| HARDCODE (string) | `'FIXED_VALUE'` | `'FIXED_VALUE' :: string AS target_col` |
| HARDCODE (numeric) | `42` | `42 :: data_type AS target_col` |
| NULL (unmapped) | *(để trống)* | `NULL :: data_type AS target_col` |
| Business Logic | `CASE WHEN x THEN y END` | `CASE WHEN x THEN y END :: data_type AS target_col` |

## Source Types trong Input section

| Source Type | Mô tả | SQL sinh ra |
|-------------|--------|-------------|
| `physical_table` | Bảng vật lý từ Bronze/Silver | `SELECT fields FROM schema.table WHERE filter` |
| `unpivot_cte` | Unpivot từ CTE trước đó | N khối `SELECT ... UNION ALL` (1 khối/type code) |
| `derived_cte` | Aggregation/transformation | `SELECT fields FROM source_alias GROUP BY ...` |

## Cấu trúc SQL Output

```sql
{{ config(materialized='table', schema='silver', tags=['SCD4A']) }}

WITH cte_1 AS (
    SELECT id, name, email
    FROM bronze.SOURCE_TABLE
    WHERE data_date = to_date('{{ var("etl_date") }}', 'yyyy-MM-dd')
),

cte_2 AS (
    SELECT id AS key, 'EMAIL' AS type_code, email AS value
    FROM cte_1
    WHERE email IS NOT NULL
    UNION ALL
    SELECT id AS key, 'PHONE' AS type_code, phone AS value
    FROM cte_1
    WHERE phone IS NOT NULL
)

SELECT
    hash_id(cte_2.type_code, cte_2.key) AS surrogate_id,
    cte_2.key :: string                  AS entity_code,
    cte_2.type_code :: string            AS type_code,
    cte_2.value :: string                AS value
FROM cte_2
;
```

## Target Schema File

Target Schema File định nghĩa schema của bảng đích, do Database Designer maintain. Dùng để validate mapping trước khi generate — đảm bảo tên cột và data type khớp với thiết kế.

### Format

File JSON theo quy ước đường dẫn: `schemas/{schema}/{table_name}.json`

```json
{
  "table_name": "fund_management_company",
  "columns": [
    {"name": "fund_management_company_id", "data_type": "string", "nullable": false, "description": "Surrogate key"},
    {"name": "charter_capital_amount", "data_type": "decimal(23,2)", "nullable": true, "description": "Vốn điều lệ"}
  ]
}
```

### Cấu trúc thư mục

```
schemas/
├── silver/
│   ├── fund_management_company.json
│   └── involved_party_electronic_address.json
└── gold/
    └── dim_fund_management_company.json
```

Sử dụng `--schema-dir schemas/` khi chạy validate hoặc generate để kích hoạt schema validation.

## Schema Files

Dự án sử dụng schema files làm single source of truth cho cấu trúc bảng, chia thành hai tầng:

### Cấu trúc thư mục

```
schemas/
├── bronze/                    # Schema bảng nguồn (quản lý bởi source system owners)
│   ├── FUNDCOMPANY.json
│   ├── FUNDCOMBUSINES.json
│   ├── FUNDCOMTYPE.json
│   ├── THONG_TIN_DK_THUE.json
│   └── TTKDT_NGUOI_DAI_DIEN.json
└── silver/                    # Schema bảng đích (quản lý bởi Database Designers)
    ├── fund_management_company.json
    └── involved_party_electronic_address.json
```

### Bronze Schema Files

File JSON định nghĩa schema của bảng nguồn Bronze, cùng format với Silver schema files:

```json
{
  "table_name": "FUNDCOMPANY",
  "columns": [
    {"name": "id", "data_type": "bigint", "nullable": false, "description": "PK công ty quản lý quỹ"},
    {"name": "name", "data_type": "string", "nullable": true, "description": "Tên công ty"}
  ]
}
```

Bronze schema files được sử dụng trong Spark integration tests để sinh fake data với đúng cấu trúc bảng nguồn.

### Silver Schema Files (Target Schema Files)

Dùng để validate mapping trước khi generate — đảm bảo tên cột và data type khớp với thiết kế. Xem mục "Target Schema File" ở trên.

## Spark Integration Tests

Spark integration tests kiểm chứng SQL sinh ra có thể chạy được trên Spark thực tế với fake data.

### Prerequisites

- Python 3.13
- PySpark 3.5+ (classic mode, `pip install pyspark`)
- Java 17+ (khuyến nghị Java 21)

### Cách chạy

```bash
# Chạy Spark integration tests (viết tay)
python -m pytest tests/spark/ -v

# Chạy Spark integration tests (tự động sinh)
python -m pytest output/silver/tests/ -v
```

### Cách hoạt động

1. Đọc Bronze schema files (`schemas/bronze/*.json`) làm single source of truth cho cấu trúc bảng nguồn
2. Sinh fake data từ schema files bằng pure SQL (UNION ALL of SELECT literals), đăng ký thành temp views trên SparkSession local
3. Chạy SQL sinh ra (sau khi strip dbt directives, chuyển `::` thành `CAST()`, thay `hash_id` bằng `sha2`) trên PySpark classic mode `master("local[1]")`
4. Kiểm chứng: SQL chạy không lỗi, output có đúng columns, output có đúng số rows
5. Sử dụng `result._jdf.count()` thay vì `result.count()` để tránh lỗi Python worker trên Windows

### Cấu trúc

```
tests/spark/                                 # Test viết tay (reference)
├── conftest.py                              # SparkSession fixture (PySpark classic, local[1])
├── sql_test_helper.py                       # Strip dbt directives, replace schema refs, convert casts
├── fake_data_factory.py                     # Đọc Bronze schema files, build Spark schemas, register temp views
└── test_fund_management_company_spark.py    # Integration tests cho fund_management_company

output/silver/tests/                         # Test tự động sinh (self-contained)
├── conftest.py                              # Copy từ tests/spark/
├── sql_test_helper.py                       # Copy từ tests/spark/
├── fake_data_factory.py                     # Copy từ tests/spark/
├── test_fund_management_company_spark.py    # Tự động sinh
└── test_involved_party_electronic_address_spark.py  # Tự động sinh
```

Test files tự động sinh là **self-contained** — generator tự copy các helper files (`conftest.py`, `fake_data_factory.py`, `sql_test_helper.py`) vào thư mục test output. Không cần phụ thuộc vào package `tests.spark`.

## Tự động sinh Spark Test

Tool bắt buộc sinh file pytest cho mỗi dbt model khi generate. Test file mặc định lưu vào `{output_dir}/tests/`:

```bash
dbt-job-gen generate mapping/silver/fund_management_company_FIMS_FUNDCOMPANY.csv \
  --schema-dir schemas/ -o output/silver/fund_management_company.sql
```

Output bắt buộc:
- `output/silver/fund_management_company.sql` — dbt model
- `output/silver/tests/test_fund_management_company_spark.py` — Spark integration test
- `output/silver/tests/conftest.py` — SparkSession fixture (copy tự động)
- `output/silver/tests/fake_data_factory.py` — Fake data factory (copy tự động)
- `output/silver/tests/sql_test_helper.py` — SQL helper (copy tự động)

Mỗi test file sinh ra chứa 3 test cases:
1. SQL chạy không lỗi trên Spark
2. Output có đúng danh sách columns
3. Output có rows (filter hoạt động đúng)

Test file sử dụng **local imports** (`from fake_data_factory import register_fake_table`) — không phụ thuộc vào package `tests.spark`. Generator tự copy các helper files vào thư mục test output để test files có thể chạy độc lập:

```bash
# Chạy test trực tiếp từ output directory
python -m pytest output/silver/tests/ -v
```

Test file sử dụng fake data sinh từ Bronze schema files (`schemas/bronze/*.json`).
Nếu Bronze schema file chưa tồn tại cho bảng nguồn, test file sẽ chứa TODO comment.

## Transformation Rules CSV

Transformation rules dùng chung (HASH, Business Logic, Derived) được định nghĩa trong file CSV tại `config/transformation_rules.csv`, do Data Engineer Lead maintain.

### Format

```csv
Rule Name,Rule Type,SQL Template,Is Exception,Description
hash_id,HASH,"sha2(concat_ws('|', {args}), 256)",false,Surrogate key bằng SHA-256
complex_calc,BUSINESS_LOGIC,"CASE WHEN {col1} > 0 THEN {col2} ELSE 0 END",true,Cần review thủ công
```

| Cột | Mô tả |
|-----|--------|
| Rule Name | Tên rule (unique) |
| Rule Type | HASH / BUSINESS_LOGIC / DERIVED |
| SQL Template | Template SQL với placeholders `{args}`, `{col1}`, ... |
| Is Exception | `true` nếu cần code thủ công, `false` nếu tự động |
| Description | Mô tả rule |

### Sử dụng

```bash
dbt-job-gen generate mapping.csv --rules-csv config/transformation_rules.csv -o output/model.sql
dbt-job-gen batch mapping/silver/ --rules-csv config/transformation_rules.csv -o output/silver/
```

## Generation Log

Tool sử dụng generation log để phát hiện mapping thay đổi/thêm mới khi chạy batch, tránh generate lại mapping không đổi.

### Cách hoạt động

1. Mỗi lần generate thành công, tool lưu MD5 checksum của file mapping CSV vào `.generation_log.json`
2. Lần chạy tiếp theo, tool so sánh checksum hiện tại với log:
   - Mapping mới (chưa có trong log) → generate
   - Mapping thay đổi (checksum khác) → generate lại
   - Mapping không đổi (checksum giống) → skip
3. `--force` bỏ qua log, generate tất cả từ đầu

### File log

Lưu tại `{output_dir}/.generation_log.json`:

```json
{
  "fund_management_company": {
    "mapping_hash": "7ab5d6423e3e2d9ac6dd2e66a71b9db1",
    "generated_at": "2024-04-24T09:01:07+00:00",
    "sql_file": "fund_management_company.sql",
    "test_file": "test_fund_management_company_spark.py"
  }
}
```

## Chạy tests

```bash
# Chạy toàn bộ unit test suite (221 tests)
python -m pytest tests/ -v --ignore=tests/spark

# Chạy Spark integration tests viết tay (yêu cầu Java 17+)
python -m pytest tests/spark/ -v

# Chạy Spark integration tests tự động sinh (self-contained)
python -m pytest output/silver/tests/ -v

# Chạy tất cả tests (unit + Spark)
python -m pytest tests/ -v
```

> **Lưu ý:** Spark integration tests yêu cầu Java 17+ (khuyến nghị Java 21) được cài đặt và có trong PATH. Test files tự động sinh trong `output/*/tests/` là self-contained — đã bao gồm `conftest.py` và helper files, có thể chạy độc lập mà không cần project root.

## License

Internal tool — không phân phối công khai.
