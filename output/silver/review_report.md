# Review Report

Total: 2, Success: 2, Errors: 0, Warnings: 2


## Thứ tự thực thi

1. fund_management_company
2. involved_party_electronic_address


## Chi tiết Models

| Model | Patterns | Complex | Exception | CTEs | Test Status |
|-------|----------|---------|-----------|------|-------------|
| fund_management_company | physical_table, derived_cte, JOIN | Yes | No | 5 | not-checked |
| involved_party_electronic_address | physical_table, unpivot_cte, UNION_ALL | Yes | No | 4 | not-checked |


## Điểm cần chú ý

- **fund_management_company**: Complex transformation logic
  - Model uses unpivot, derived CTEs, or multiple JOINs.
- **involved_party_electronic_address**: Complex transformation logic
  - Model uses unpivot, derived CTEs, or multiple JOINs.
