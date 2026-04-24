# Tài liệu Yêu cầu — dbt Job Generator

## Giới thiệu

Công cụ dbt Job Generator tự động sinh dbt model (Spark SQL) từ file mapping đã được duyệt trong kiến trúc Data Lakehouse theo mô hình Medallion. Công cụ hỗ trợ các phép biến đổi Bronze → Silver và Silver → Gold, sử dụng Airflow để điều phối, dbt để mô hình hóa transformation, và EMR on Spark làm ETL Engine. Mục tiêu là tự động hóa tối đa việc sinh code dbt model, giảm thiểu lỗi thủ công, đồng thời vẫn đảm bảo quy trình review và duyệt bởi con người trước khi deploy.

## Bảng thuật ngữ

- **Generator**: Công cụ dbt Job Generator — hệ thống chính chịu trách nhiệm đọc file mapping và sinh ra dbt model cùng dependency DAG.
- **File_Mapping**: File mapping đã duyệt — chứa thông tin source → target, data type, mapping rule cho từng trường. Mỗi file mapping tương ứng với một dbt model.
- **dbt_Model**: File SQL được sinh ra bởi Generator, chứa config block, source reference, và SQL transform theo chuẩn Spark SQL. Mỗi file .sql tương ứng với một mapping.
- **Dependency_DAG**: File cấu hình dependency giữa các dbt model, xác định thứ tự chạy dựa trên danh sách source tables trong mapping.
- **dbt_Project_Template**: Cấu trúc chuẩn của một dbt model bao gồm config block, source reference, SQL transform, và test.
- **Mapping_Rule**: Quy tắc ánh xạ dữ liệu từ source sang target, bao gồm các pattern: DIRECT_MAP, CAST, HASH, HARDCODE, UNPIVOT, Filter, Business Logic, và Dependency.
- **Medallion_Architecture**: Kiến trúc Data Lakehouse gồm ba tầng: Bronze (raw data), Silver (cleansed data), Gold (business-level aggregates).
- **Config_Block**: Phần cấu hình đầu file dbt model, chứa metadata như materialization strategy, tags, schema.
- **Source_Reference**: Khai báo tham chiếu đến bảng nguồn trong dbt model, sử dụng macro source() hoặc ref().
- **Parser**: Module của Generator chịu trách nhiệm đọc và phân tích cú pháp File_Mapping thành cấu trúc dữ liệu nội bộ.
- **Pretty_Printer**: Module của Generator chịu trách nhiệm chuyển đổi cấu trúc dữ liệu nội bộ của mapping trở lại định dạng file mapping gốc.

## Yêu cầu

### Yêu cầu 1: Đọc và phân tích File Mapping

**User Story:** Là một Data Engineer, tôi muốn Generator tự động đọc và phân tích file mapping đã duyệt, để trích xuất thông tin source-target, data type và mapping rule cho từng trường.

#### Tiêu chí chấp nhận

1. WHEN một File_Mapping hợp lệ được cung cấp, THE Parser SHALL phân tích file và trích xuất danh sách các trường bao gồm source column, target column, data type, và Mapping_Rule cho từng trường.
2. WHEN một File_Mapping chứa nhiều source tables, THE Parser SHALL trích xuất danh sách tất cả source tables để phục vụ việc sinh Dependency_DAG.
3. IF một File_Mapping có định dạng không hợp lệ hoặc thiếu trường bắt buộc, THEN THE Parser SHALL trả về thông báo lỗi mô tả rõ vị trí và loại lỗi.
4. FOR ALL File_Mapping hợp lệ, việc phân tích rồi in lại (parse → pretty print → parse) SHALL tạo ra đối tượng mapping tương đương với đối tượng ban đầu (round-trip property).

### Yêu cầu 2: Sinh dbt Model theo pattern DIRECT_MAP

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh câu lệnh SELECT với alias khi mapping rule là DIRECT_MAP, để đổi tên cột từ source sang target mà không cần viết tay.

#### Tiêu chí chấp nhận

1. WHEN một Mapping_Rule có pattern DIRECT_MAP, THE Generator SHALL sinh câu lệnh `SELECT source_col AS target_col` trong dbt_Model.
2. WHEN nhiều trường trong cùng một File_Mapping có pattern DIRECT_MAP, THE Generator SHALL sinh tất cả các câu lệnh SELECT tương ứng trong cùng một dbt_Model.

### Yêu cầu 3: Sinh dbt Model theo pattern CAST

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh câu lệnh CAST cho các trường cần chuyển đổi kiểu dữ liệu, để đảm bảo data type chính xác theo target schema.

#### Tiêu chí chấp nhận

1. WHEN một Mapping_Rule có pattern CAST với kiểu Currency Amount, THE Generator SHALL sinh câu lệnh `CAST(source_col AS DECIMAL(precision, scale)) AS target_col` với precision và scale lấy từ File_Mapping.
2. WHEN một Mapping_Rule có pattern CAST với kiểu Date, THE Generator SHALL sinh câu lệnh `TO_DATE(source_col, '<format>') AS target_col` với format lấy từ File_Mapping.
3. IF một Mapping_Rule có pattern CAST nhưng thiếu thông tin format hoặc precision, THEN THE Generator SHALL báo lỗi và chỉ rõ trường thiếu thông tin.

### Yêu cầu 4: Sinh dbt Model theo pattern HASH (Surrogate Key)

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh hash function cho surrogate key từ business key, để đảm bảo tính nhất quán của surrogate key trên toàn hệ thống.

#### Tiêu chí chấp nhận

1. WHEN một Mapping_Rule có pattern HASH, THE Generator SHALL sinh câu lệnh hash sử dụng hàm hash chuẩn của dự án với danh sách business key columns lấy từ File_Mapping.
2. THE Generator SHALL sử dụng cùng một hàm hash chuẩn cho tất cả các surrogate key trong toàn bộ dbt project.

### Yêu cầu 5: Sinh dbt Model theo pattern HARDCODE

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh giá trị hardcode cho các trường cố định, để giảm thiểu lỗi nhập liệu thủ công.

#### Tiêu chí chấp nhận

1. WHEN một Mapping_Rule có pattern HARDCODE, THE Generator SHALL sinh câu lệnh `SELECT '<value>' AS target_col` với value lấy từ File_Mapping.
2. WHEN giá trị hardcode là kiểu số, THE Generator SHALL sinh câu lệnh `SELECT <numeric_value> AS target_col` mà không bọc trong dấu nháy đơn.

### Yêu cầu 6: Sinh dbt Model theo pattern UNPIVOT

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh câu lệnh UNION ALL cho các trường cần unpivot, để xử lý shared entity pattern một cách nhất quán.

#### Tiêu chí chấp nhận

1. WHEN một Mapping_Rule có pattern UNPIVOT, THE Generator SHALL sinh câu lệnh UNION ALL cho mỗi source column kèm theo Type Code tương ứng lấy từ File_Mapping.
2. WHEN một UNPIVOT có N source columns, THE Generator SHALL sinh đúng N khối SELECT được nối bằng UNION ALL.

### Yêu cầu 7: Sinh Filter Condition

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh WHERE clause từ filter rule trong mapping, để đảm bảo dữ liệu được lọc đúng theo yêu cầu nghiệp vụ.

#### Tiêu chí chấp nhận

1. WHEN một Mapping_Rule có filter condition, THE Generator SHALL sinh WHERE clause trong dbt_Model theo đúng biểu thức filter được định nghĩa trong File_Mapping.
2. WHEN một File_Mapping không có filter condition, THE Generator SHALL sinh dbt_Model mà không chứa WHERE clause.

### Yêu cầu 8: Sinh dbt Model theo pattern Business Logic / Derived

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh SQL expression cho các trường có business logic phức tạp, để tăng tốc quá trình phát triển và giảm lỗi sao chép.

#### Tiêu chí chấp nhận

1. WHEN một Mapping_Rule có pattern Business Logic chứa SQL expression, THE Generator SHALL sinh đúng SQL expression đó vào dbt_Model.
2. IF một Mapping_Rule có pattern Business Logic được đánh dấu là exception (cần code thủ công), THEN THE Generator SHALL sinh dbt_Model với placeholder comment chỉ rõ vị trí cần bổ sung logic thủ công.


### Yêu cầu 9: Sinh Config Block theo dbt Project Template

**User Story:** Là một Data Engineer, tôi muốn mỗi dbt model được sinh ra đều có config block chuẩn theo template dự án, để đảm bảo tính nhất quán và tuân thủ chuẩn dbt project.

#### Tiêu chí chấp nhận

1. THE Generator SHALL sinh Config_Block ở đầu mỗi dbt_Model bao gồm materialization strategy, tags, và schema theo dbt_Project_Template.
2. WHEN File_Mapping chỉ định tầng Bronze → Silver, THE Generator SHALL gán schema tương ứng với tầng Silver trong Config_Block.
3. WHEN File_Mapping chỉ định tầng Silver → Gold, THE Generator SHALL gán schema tương ứng với tầng Gold trong Config_Block.

### Yêu cầu 10: Sinh Source Reference

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh source reference sử dụng macro source() hoặc ref() của dbt, để đảm bảo lineage tracking và dependency resolution chính xác.

#### Tiêu chí chấp nhận

1. WHEN source table nằm ngoài dbt project (raw data từ Bronze), THE Generator SHALL sinh Source_Reference sử dụng macro `source()`.
2. WHEN source table là một dbt model khác trong cùng project, THE Generator SHALL sinh Source_Reference sử dụng macro `ref()`.

### Yêu cầu 11: Sinh Dependency DAG

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh dependency DAG từ danh sách source tables trong mapping, để đảm bảo thứ tự chạy các model đúng logic phụ thuộc.

#### Tiêu chí chấp nhận

1. WHEN một tập hợp File_Mapping được cung cấp, THE Generator SHALL phân tích danh sách source tables của từng mapping và sinh Dependency_DAG thể hiện quan hệ phụ thuộc giữa các dbt_Model.
2. THE Generator SHALL đảm bảo Dependency_DAG không chứa circular dependency.
3. IF Dependency_DAG chứa circular dependency, THEN THE Generator SHALL báo lỗi và liệt kê danh sách các model tham gia vào vòng lặp phụ thuộc.
4. WHEN một dbt_Model phụ thuộc vào nhiều source tables, THE Generator SHALL thể hiện tất cả các dependency trong Dependency_DAG.

### Yêu cầu 12: Tổng hợp dbt Model hoàn chỉnh

**User Story:** Là một Data Engineer, tôi muốn Generator tổng hợp tất cả các thành phần (config block, source reference, SQL transform, filter) thành một file dbt model hoàn chỉnh, để có thể deploy trực tiếp sau khi review.

#### Tiêu chí chấp nhận

1. THE Generator SHALL sinh mỗi dbt_Model dưới dạng một file .sql hoàn chỉnh bao gồm Config_Block, Source_Reference, và SQL transform theo đúng thứ tự chuẩn của dbt_Project_Template.
2. WHEN một File_Mapping chứa nhiều Mapping_Rule khác nhau (DIRECT_MAP, CAST, HASH, HARDCODE, UNPIVOT, Business Logic), THE Generator SHALL kết hợp tất cả các rule vào cùng một dbt_Model.
3. FOR ALL dbt_Model được sinh ra, THE Generator SHALL đảm bảo SQL hợp lệ theo cú pháp Spark SQL.

### Yêu cầu 13: Xử lý hàng loạt (Batch Processing)

**User Story:** Là một Data Engineer, tôi muốn Generator xử lý nhiều file mapping cùng lúc, để sinh toàn bộ dbt model cho một batch transformation thay vì xử lý từng file một.

#### Tiêu chí chấp nhận

1. WHEN một thư mục chứa nhiều File_Mapping được cung cấp, THE Generator SHALL xử lý tất cả các file và sinh dbt_Model tương ứng cho từng file.
2. IF một File_Mapping trong batch bị lỗi, THEN THE Generator SHALL tiếp tục xử lý các file còn lại và báo cáo tổng hợp danh sách lỗi sau khi hoàn thành.
3. WHEN xử lý batch hoàn tất, THE Generator SHALL sinh báo cáo tổng hợp bao gồm số lượng model sinh thành công, số lượng lỗi, và danh sách lỗi chi tiết.

### Yêu cầu 14: Sinh dbt Test

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh các test cơ bản cho mỗi dbt model, để đảm bảo chất lượng dữ liệu sau transformation.

#### Tiêu chí chấp nhận

1. WHEN một dbt_Model được sinh ra, THE Generator SHALL sinh schema test cho các trường được đánh dấu NOT NULL trong File_Mapping.
2. WHEN một trường trong File_Mapping được đánh dấu là unique key, THE Generator SHALL sinh unique test cho trường đó.
3. WHEN một trường trong File_Mapping có relationship với bảng khác, THE Generator SHALL sinh relationship test tương ứng.

### Yêu cầu 15: Báo cáo Review cho con người

**User Story:** Là một Data Engineer Lead, tôi muốn Generator sinh báo cáo review tóm tắt cho mỗi batch, để hỗ trợ quá trình glance review và spot check hiệu quả.

#### Tiêu chí chấp nhận

1. WHEN một batch dbt_Model được sinh xong, THE Generator SHALL sinh báo cáo review bao gồm danh sách model, mapping pattern được sử dụng, và các điểm cần chú ý (business logic phức tạp, nhiều JOIN, UNION ALL).
2. THE Generator SHALL đánh dấu các dbt_Model chứa business logic phức tạp hoặc exception cần review kỹ bởi con người.
3. THE Generator SHALL liệt kê thứ tự chạy các model theo Dependency_DAG để con người xác nhận trước khi deploy.
