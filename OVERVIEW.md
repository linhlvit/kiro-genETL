# dbt Job Generator — Tổng quan hệ thống

Tài liệu tổng hợp phục vụ trình bày cho team và lãnh đạo trong dự án Tối ưu quy trình phát triển.

## 1. Mục tiêu

Tự động sinh dbt model (Spark SQL) từ file mapping CSV đã duyệt, giảm thiểu lỗi thủ công và tăng tốc quá trình phát triển ETL trong kiến trúc Data Lakehouse (Medallion: Bronze → Silver → Gold).

## 2. Quy trình hoạt động (Workflow)

```
Chuẩn bị mapping CSV → Validate → Generate → Review → Deploy
```

| Bước | Người thực hiện | Công cụ | Output |
|------|----------------|---------|--------|
| 1. Chuẩn bị mapping CSV | Data Engineer | Excel/CSV editor | File mapping CSV |
| 2. Validate | Data Engineer | `dbt-job-gen validate` | Báo cáo WARNING/BLOCK |
| 3. Generate | Data Engineer | `dbt-job-gen generate/batch` | File .sql dbt model + file test_*.py + helper files (self-contained) |
| 4. Review | DE Lead / Senior DE | Manual review | Sign-off |
| 5. Deploy | DevOps / DE | `dbt run` trên Airflow/EMR | Production models |

## 3. Danh sách file Input

| File | Đường dẫn | Format | Owner | Mô tả |
|------|-----------|--------|-------|-------|
| Mapping CSV | `mapping/{layer}/*.csv` | CSV (5 sections: Target, Input, Relationship, Mapping, Final Filter) | Data Engineer | Input chính. Mỗi file = 1 dbt model. Chứa source→target, data type, transformation rule |
| Transformation Rules | `config/transformation_rules.csv` | CSV | Data Engineer Lead | Danh mục transformation rules dùng chung (HASH, Business Logic, Derived) |
| Bronze Schema | `schemas/bronze/{TABLE_NAME}.json` | JSON | Source System Owner | Schema bảng nguồn. Single source of truth cho cấu trúc bảng Bronze |
| Silver Schema | `schemas/silver/{table_name}.json` | JSON | Database Designer | Schema bảng đích. Dùng để validate mapping (tên cột, data type) |
| Gold Schema | `schemas/gold/{table_name}.json` | JSON | Database Designer | Schema bảng đích tầng Gold |

### Cấu trúc file Mapping CSV

```
Target          → Thông tin bảng đích (schema, table name, ETL handle)
Input           → Danh sách source entries (physical_table, unpivot_cte, derived_cte)
Relationship    → Quan hệ JOIN giữa các CTE
Mapping         → Ánh xạ từng cột: target column, transformation, data type
Final Filter    → UNION ALL hoặc WHERE clause cuối
```

### Cấu trúc file Schema (Bronze/Silver/Gold)

```json
{
  "table_name": "fund_management_company",
  "columns": [
    {"name": "fund_management_company_id", "data_type": "string", "nullable": false, "description": "Surrogate key"},
    {"name": "charter_capital_amount", "data_type": "decimal(23,2)", "nullable": true, "description": "Vốn điều lệ"}
  ]
}
```

## 4. Danh sách file Output

| File | Đường dẫn | Format | Sinh bởi | Mô tả |
|------|-----------|--------|----------|-------|
| dbt Model | `output/{layer}/{model_name}.sql` | Spark SQL | `dbt-job-gen generate/batch` | File .sql hoàn chỉnh, sẵn deploy sau review |
| Spark Test | `{output_dir}/tests/test_{model_name}_spark.py` | pytest + PySpark | `dbt-job-gen generate/batch` | File pytest tự động sinh (self-contained), validate SQL chạy được trên Spark với fake data từ Bronze schema |
| Spark Test Helpers | `{output_dir}/tests/conftest.py`, `fake_data_factory.py`, `sql_test_helper.py` | Python | `dbt-job-gen generate/batch` | Copy tự động từ tests/spark/, cho phép test chạy độc lập |
| Review Report | Terminal + `review_report.md` | Markdown | `dbt-job-gen batch` | Báo cáo tổng hợp: success/error/warning count, chi tiết models |
| Validation Report | Terminal output | Text | `dbt-job-gen validate/validate-batch` | Báo cáo WARNING/BLOCK cho từng file |
| Generation Log | `{output_dir}/.generation_log.json` | JSON | `dbt-job-gen batch` | Checksum MD5 mapping CSV, phát hiện thay đổi/thêm mới |

### Cấu trúc file dbt Model sinh ra

```sql
{{ config(materialized='table', schema='silver', tags=['SCD4A']) }}

WITH cte_1 AS (
    SELECT ... FROM bronze.TABLE WHERE data_date = ...
),
cte_2 AS (
    SELECT ... FROM cte_1 GROUP BY ...
)

SELECT
    hash_id('SRC', col1)     AS surrogate_key,
    alias.col :: string      AS target_col,
    'HARDCODE' :: string     AS fixed_col,
    NULL :: date             AS unmapped_col
FROM cte_1
    LEFT JOIN cte_2 ON cte_1.id = cte_2.id
;
```

## 5. Quy tắc Validate (Validation Rules)

### BLOCK — Ngăn generate, bắt buộc sửa

| Rule | Loại lỗi | Mô tả |
|------|----------|-------|
| Parse Error | `PARSE_ERROR` | CSV format lỗi, thiếu section bắt buộc (Target, Input, Mapping) |
| CTE Dependency | `CTE_DEPENDENCY` | unpivot_cte/derived_cte tham chiếu alias chưa khai báo |
| Schema Column | `SCHEMA_COLUMN_NOT_FOUND` | Cột trong mapping không tồn tại trong Target Schema File |
| Schema Data Type | `SCHEMA_DATA_TYPE_MISMATCH` | Data type trong mapping không khớp với Target Schema File |
| SELECT * | `SELECT_STAR` | Mapping section rỗng hoặc Input có select_fields="*" |
| Catalog Error | `CATALOG_ERROR` | Rule tham chiếu không tồn tại trong Transformation Rule Catalog |
| Generation Error | `GENERATION_ERROR` | Thiếu tham số (CAST thiếu precision/format) |
| DAG Error | `DAG_ERROR` | Circular dependency giữa các models |

### WARNING — Ghi nhận, không ngăn generate

| Rule | Loại cảnh báo | Mô tả |
|------|--------------|-------|
| Missing Source | `MISSING_SOURCE` | Source table chưa tồn tại ở layer trước |
| Missing Prerequisite | `MISSING_PREREQUISITE` | Prerequisite job chưa tồn tại trong DAG |
| Missing Schema File | `MISSING_SCHEMA_FILE` | Không tìm thấy Target Schema File cho bảng đích |
| Downstream Impact | `DOWNSTREAM_IMPACT` | Thay đổi mapping ảnh hưởng đến downstream models |

## 6. Mapping Patterns được hỗ trợ

| Pattern | Ví dụ trong CSV | SQL sinh ra | Mức tự động |
|---------|----------------|-------------|-------------|
| DIRECT_MAP | `alias.column` | `alias.column :: type AS target` | Cao — tự động |
| HASH | `hash_id('SRC', alias.id)` | `hash_id('SRC', alias.id) AS target` | Cao — tự động |
| HARDCODE (string) | `'FIXED_VALUE'` | `'FIXED_VALUE' :: string AS target` | Cao — tự động |
| HARDCODE (numeric) | `42` | `42 :: type AS target` | Cao — tự động |
| NULL (unmapped) | *(để trống)* | `NULL :: type AS target` | Cao — tự động |
| CAST | `CAST(col AS DECIMAL(18,2))` | `CAST(col AS DECIMAL(18,2)) AS target` | Cao — tự động |
| UNPIVOT | (trong Input section) | N khối SELECT UNION ALL | Cao — pattern lặp lại |
| Business Logic | `CASE WHEN x THEN y END` | `CASE WHEN x THEN y END :: type AS target` | Cao — sinh từ rule |
| Exception | *(đánh dấu exception)* | `NULL :: type /* TODO: implement manually */` | Thủ công — cần review |

## 7. Source Types trong Input section

| Source Type | Mô tả | SQL sinh ra |
|-------------|--------|-------------|
| `physical_table` | Bảng vật lý từ Bronze/Silver | `SELECT fields FROM schema.table WHERE filter` |
| `unpivot_cte` | Unpivot từ CTE trước đó | N khối `SELECT ... UNION ALL` |
| `derived_cte` | Aggregation/transformation | `SELECT fields FROM source GROUP BY ...` |

## 8. Ownership Matrix (RACI)

| Hoạt động | Data Engineer | DE Lead | Database Designer | Source System Owner | DevOps |
|-----------|:---:|:---:|:---:|:---:|:---:|
| Tạo/cập nhật Mapping CSV | R | A | C | I | - |
| Maintain Bronze Schema | I | I | - | R | - |
| Maintain Silver/Gold Schema | C | A | R | - | - |
| Chạy validate | R | I | - | - | - |
| Chạy generate | R | I | - | - | - |
| Review output SQL | C | R | C | - | - |
| Deploy dbt models | I | A | - | - | R |
| Maintain Transformation Rules | C | R | - | - | - |

R = Responsible, A = Accountable, C = Consulted, I = Informed

## 9. Tech Stack

| Component | Công nghệ | Vai trò |
|-----------|-----------|---------|
| Orchestration | Apache Airflow | Điều phối ETL jobs |
| Transformation | dbt (data build tool) | Mô hình hóa transformation |
| ETL Engine | EMR on Spark | Thực thi SQL trên cluster |
| Code Generator | dbt Job Generator (Python CLI) | Sinh dbt models từ mapping |
| Testing | PySpark 3.5 (classic mode) + pytest | Spark integration tests (self-contained) |
| Hash Function | SHA-256 (`hash_id`) | Sinh surrogate keys |

## 10. Tham chiếu tài liệu chi tiết

| Tài liệu | Đường dẫn | Nội dung |
|-----------|-----------|----------|
| Requirements | `.kiro/specs/dbt-job-generator/requirements.md` | 17 yêu cầu chi tiết với acceptance criteria |
| Design | `.kiro/specs/dbt-job-generator/design.md` | Kiến trúc, components, data models, 37 correctness properties |
| Tasks | `.kiro/specs/dbt-job-generator/tasks.md` | 30 tasks triển khai (đã hoàn thành) |
| README | `README.md` | Hướng dẫn cài đặt, sử dụng, CLI commands |
| Sample Mapping | `mapping/silver/*.csv` | File mapping mẫu thực tế |
| Sample Schema | `schemas/bronze/*.json`, `schemas/silver/*.json` | Schema files mẫu |
| Sample Output | `output/silver/*.sql` | dbt model sinh ra từ mapping mẫu |
