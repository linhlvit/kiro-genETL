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
- **Transformation_Rule_Catalog**: Danh mục quản lý tập trung các transformation rule dùng chung (HASH, Business Logic, Derived) và các rule phức tạp cần mô tả riêng, đảm bảo tính nhất quán khi áp dụng trên toàn bộ dbt project.
- **Medallion_Architecture**: Kiến trúc Data Lakehouse gồm ba tầng: Bronze (raw data), Silver (cleansed data), Gold (business-level aggregates).
- **Config_Block**: Phần cấu hình đầu file dbt model, chứa metadata như materialization strategy, tags, schema.
- **Source_Reference**: Khai báo tham chiếu đến bảng nguồn trong dbt model, sử dụng macro source() hoặc ref().
- **Parser**: Module của Generator chịu trách nhiệm đọc và phân tích cú pháp File_Mapping thành cấu trúc dữ liệu nội bộ.
- **Pretty_Printer**: Module của Generator chịu trách nhiệm chuyển đổi cấu trúc dữ liệu nội bộ của mapping trở lại định dạng file mapping gốc.
- **Mapping_Validator**: Module của Generator chịu trách nhiệm xác minh tính hợp lệ của File_Mapping về mặt workflow — bao gồm kiểm tra source tables đã được khai báo ở layer trước và kiểm tra ràng buộc phụ thuộc giữa các job.
- **Mapping_Change_Request**: Yêu cầu thay đổi mapping — mô tả việc cập nhật rule, thêm mới trường, hoặc thêm mới File_Mapping trong quá trình triển khai dự án (incremental change management).
- **Unit_Test_Config**: Cấu hình quản lý tập trung các unit test case bắt buộc cho mỗi dbt_Model, quy định loại test, ngưỡng chấp nhận, và danh sách test case phải có để đảm bảo tính nhất quán.
- **Target_Schema_File**: File định nghĩa schema của bảng đích, quản lý riêng theo từng bảng, do Database Designer maintain. Chứa tên bảng, danh sách cột (tên cột, data type, nullable, description).

## Yêu cầu

### Yêu cầu 1: Đọc và phân tích File Mapping

**User Story:** Là một Data Engineer, tôi muốn Generator tự động đọc và phân tích file mapping đã duyệt, để trích xuất thông tin source-target, data type và mapping rule cho từng trường.

#### Tiêu chí chấp nhận

1. WHEN một File_Mapping hợp lệ được cung cấp, THE Parser SHALL phân tích file và trích xuất danh sách các trường bao gồm source column, target column, data type, và Mapping_Rule cho từng trường.
2. WHEN một File_Mapping chứa nhiều source tables, THE Parser SHALL trích xuất danh sách tất cả source tables để phục vụ việc sinh Dependency_DAG.
3. IF một File_Mapping có định dạng không hợp lệ hoặc thiếu trường bắt buộc, THEN THE Parser SHALL trả về thông báo lỗi mô tả rõ vị trí và loại lỗi.
4. FOR ALL File_Mapping hợp lệ, việc phân tích rồi in lại (parse → pretty print → parse) SHALL tạo ra đối tượng mapping tương đương với đối tượng ban đầu (round-trip property).

### Yêu cầu 2: Xác minh File Mapping (Mapping Validation)

**User Story:** Là một Data Engineer, tôi muốn Generator tự động xác minh tính hợp lệ của file mapping về mặt workflow, để đảm bảo các bảng nguồn đã được khai báo ở layer trước và các ràng buộc phụ thuộc giữa các job được thỏa mãn trước khi sinh code.

#### Phân loại mức độ nghiêm trọng (Severity Classification)

Mapping_Validator phân loại kết quả validation thành hai mức:

**WARNING** — ghi nhận nhưng KHÔNG ngăn generate:
- Missing source tables (khi chạy standalone không có project context, không thể biết source đã tồn tại hay chưa)
- Missing prerequisite jobs / downstream impact

**BLOCK** — ngăn generate, bắt buộc phải sửa:
- Parse errors (CSV format lỗi, thiếu section bắt buộc)
- CTE pipeline dependency lỗi (unpivot_cte/derived_cte tham chiếu alias chưa khai báo)
- Schema validation lỗi (mapping không khớp với Target_Schema_File của bảng đích — xem Yêu cầu 3)

#### Tiêu chí chấp nhận — WARNING rules

1. WHEN một File_Mapping tầng Silver → Gold được cung cấp, THE Mapping_Validator SHALL kiểm tra tất cả source tables đã tồn tại dưới dạng dbt_Model hoặc source declaration ở tầng Silver, và ghi nhận các source tables thiếu dưới dạng WARNING.
2. WHEN một File_Mapping tầng Bronze → Silver được cung cấp, THE Mapping_Validator SHALL kiểm tra tất cả source tables đã được khai báo trong source configuration của tầng Bronze, và ghi nhận các source tables thiếu dưới dạng WARNING.
3. IF một source table trong File_Mapping chưa được khai báo ở layer trước, THEN THE Mapping_Validator SHALL ghi nhận WARNING và liệt kê danh sách source tables bị thiếu kèm layer yêu cầu, nhưng KHÔNG ngăn quá trình generate.
4. WHEN một File_Mapping tầng Silver → Gold phụ thuộc vào job Bronze → Silver, THE Mapping_Validator SHALL xác minh job Bronze → Silver tương ứng đã tồn tại trong Dependency_DAG, và ghi nhận các job thiếu dưới dạng WARNING.
5. IF một job phụ thuộc (prerequisite job) chưa tồn tại, THEN THE Mapping_Validator SHALL ghi nhận WARNING và liệt kê danh sách job cần được tạo trước khi chạy job hiện tại, nhưng KHÔNG ngăn quá trình generate.

#### Tiêu chí chấp nhận — BLOCK rules

6. IF một File_Mapping có lỗi parse (CSV format lỗi, thiếu section bắt buộc), THEN THE Mapping_Validator SHALL trả về lỗi BLOCK và KHÔNG cho phép generate.
7. IF một unpivot_cte hoặc derived_cte trong Input section tham chiếu đến alias chưa được khai báo bởi SourceEntry có index nhỏ hơn, THEN THE Mapping_Validator SHALL trả về lỗi BLOCK chỉ rõ alias bị thiếu và KHÔNG cho phép generate.
8. IF mapping không khớp với Target_Schema_File của bảng đích (xem Yêu cầu 3), THEN THE Mapping_Validator SHALL trả về lỗi BLOCK và KHÔNG cho phép generate.

#### Tiêu chí chấp nhận — Batch Validation

9. WHEN một thư mục chứa nhiều File_Mapping được cung cấp, THE Mapping_Validator SHALL xác minh tất cả các file và sinh báo cáo tổng hợp validation phân loại theo WARNING và BLOCK cho từng file.
10. WHEN batch validation hoàn tất, THE Mapping_Validator SHALL sinh báo cáo tổng hợp bao gồm số lượng file hợp lệ, số lượng file có WARNING, số lượng file bị BLOCK, và danh sách chi tiết lỗi theo từng file.

#### Tiêu chí chấp nhận — Chung

11. THE Mapping_Validator SHALL thực hiện xác minh trước khi Generator bắt đầu sinh dbt_Model, đảm bảo chỉ các File_Mapping không có lỗi BLOCK mới được xử lý tiếp.

### Yêu cầu 3: Xác minh Schema bảng đích (Schema Validation)

**User Story:** Là một Data Engineer, tôi muốn Generator kiểm tra mapping có phù hợp với schema của bảng đích không (tên cột, data type), để đảm bảo dbt model sinh ra tương thích với data model đã được thiết kế.

#### Tiêu chí chấp nhận

1. WHEN một Target_Schema_File tồn tại cho bảng đích của File_Mapping, THE Mapping_Validator SHALL kiểm tra tất cả tên cột trong Mapping section có khớp với danh sách cột trong Target_Schema_File.
2. WHEN một Target_Schema_File tồn tại cho bảng đích của File_Mapping, THE Mapping_Validator SHALL kiểm tra data type của từng cột trong Mapping section có tương thích với data type được định nghĩa trong Target_Schema_File.
3. IF tên cột trong File_Mapping không tồn tại trong Target_Schema_File, THEN THE Mapping_Validator SHALL trả về lỗi BLOCK chỉ rõ tên cột không khớp và danh sách cột hợp lệ từ Target_Schema_File.
4. IF data type của cột trong File_Mapping không tương thích với data type trong Target_Schema_File, THEN THE Mapping_Validator SHALL trả về lỗi BLOCK chỉ rõ tên cột, data type trong mapping, và data type yêu cầu từ Target_Schema_File.
5. IF không có Target_Schema_File cho bảng đích của File_Mapping, THEN THE Mapping_Validator SHALL ghi nhận WARNING rằng schema validation bị bỏ qua do thiếu Target_Schema_File, và KHÔNG ngăn quá trình generate.
6. THE Mapping_Validator SHALL đọc Target_Schema_File từ đường dẫn được cấu hình, trong đó mỗi bảng đích có một Target_Schema_File riêng do Database Designer maintain.

### Yêu cầu 4: Sinh dbt Model theo pattern DIRECT_MAP

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh câu lệnh SELECT với alias khi mapping rule là DIRECT_MAP, để đổi tên cột từ source sang target mà không cần viết tay.

#### Tiêu chí chấp nhận

1. WHEN một Mapping_Rule có pattern DIRECT_MAP, THE Generator SHALL sinh câu lệnh `SELECT source_col AS target_col` trong dbt_Model.
2. WHEN nhiều trường trong cùng một File_Mapping có pattern DIRECT_MAP, THE Generator SHALL sinh tất cả các câu lệnh SELECT tương ứng trong cùng một dbt_Model.

### Yêu cầu 5: Sinh dbt Model theo pattern CAST

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh câu lệnh CAST cho các trường cần chuyển đổi kiểu dữ liệu, để đảm bảo data type chính xác theo target schema.

#### Tiêu chí chấp nhận

1. WHEN một Mapping_Rule có pattern CAST với kiểu Currency Amount, THE Generator SHALL sinh câu lệnh `CAST(source_col AS DECIMAL(precision, scale)) AS target_col` với precision và scale lấy từ File_Mapping.
2. WHEN một Mapping_Rule có pattern CAST với kiểu Date, THE Generator SHALL sinh câu lệnh `TO_DATE(source_col, '<format>') AS target_col` với format lấy từ File_Mapping.
3. IF một Mapping_Rule có pattern CAST nhưng thiếu thông tin format hoặc precision, THEN THE Generator SHALL báo lỗi và chỉ rõ trường thiếu thông tin.

### Yêu cầu 6: Quản lý danh mục Transformation Rule

**User Story:** Là một Data Engineer, tôi muốn Generator quản lý tập trung các transformation rule dùng chung (HASH, Business Logic, Derived) trong một danh mục thống nhất, để đảm bảo tính nhất quán khi áp dụng rule trên toàn bộ dbt project và dễ dàng mô tả các rule phức tạp.

#### Tiêu chí chấp nhận

1. WHEN một Mapping_Rule có pattern HASH, THE Generator SHALL sinh câu lệnh hash sử dụng hàm hash chuẩn được định nghĩa trong Transformation_Rule_Catalog với danh sách business key columns lấy từ File_Mapping.
2. THE Generator SHALL sử dụng cùng một hàm hash chuẩn từ Transformation_Rule_Catalog cho tất cả các surrogate key trong toàn bộ dbt project.
3. WHEN một Mapping_Rule có pattern Business Logic chứa SQL expression, THE Generator SHALL tra cứu Transformation_Rule_Catalog và sinh đúng SQL expression tương ứng vào dbt_Model.
4. IF một Mapping_Rule có pattern Business Logic được đánh dấu là exception (cần code thủ công), THEN THE Generator SHALL sinh dbt_Model với placeholder comment chỉ rõ vị trí cần bổ sung logic thủ công.
5. WHEN một transformation rule được sử dụng bởi nhiều File_Mapping, THE Generator SHALL đảm bảo rule đó được định nghĩa một lần duy nhất trong Transformation_Rule_Catalog và tham chiếu nhất quán từ tất cả các dbt_Model liên quan.
6. IF một Mapping_Rule tham chiếu đến rule chưa tồn tại trong Transformation_Rule_Catalog, THEN THE Generator SHALL báo lỗi và chỉ rõ tên rule bị thiếu cùng File_Mapping liên quan.

### Yêu cầu 7: Sinh dbt Model theo pattern HARDCODE

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh giá trị hardcode cho các trường cố định, để giảm thiểu lỗi nhập liệu thủ công.

#### Tiêu chí chấp nhận

1. WHEN một Mapping_Rule có pattern HARDCODE, THE Generator SHALL sinh câu lệnh `SELECT '<value>' AS target_col` với value lấy từ File_Mapping.
2. WHEN giá trị hardcode là kiểu số, THE Generator SHALL sinh câu lệnh `SELECT <numeric_value> AS target_col` mà không bọc trong dấu nháy đơn.

### Yêu cầu 8: Sinh dbt Model theo pattern UNPIVOT

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh câu lệnh UNION ALL cho các trường cần unpivot, để xử lý shared entity pattern một cách nhất quán.

#### Tiêu chí chấp nhận

1. WHEN một Mapping_Rule có pattern UNPIVOT, THE Generator SHALL sinh câu lệnh UNION ALL cho mỗi source column kèm theo Type Code tương ứng lấy từ File_Mapping.
2. WHEN một UNPIVOT có N source columns, THE Generator SHALL sinh đúng N khối SELECT được nối bằng UNION ALL.

### Yêu cầu 9: Sinh Filter Condition

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh WHERE clause từ filter rule trong mapping, để đảm bảo dữ liệu được lọc đúng theo yêu cầu nghiệp vụ.

#### Tiêu chí chấp nhận

1. WHEN một Mapping_Rule có filter condition, THE Generator SHALL sinh WHERE clause trong dbt_Model theo đúng biểu thức filter được định nghĩa trong File_Mapping.
2. WHEN một File_Mapping không có filter condition, THE Generator SHALL sinh dbt_Model mà không chứa WHERE clause.

### Yêu cầu 10: Sinh Config Block theo dbt Project Template

**User Story:** Là một Data Engineer, tôi muốn mỗi dbt model được sinh ra đều có config block chuẩn theo template dự án, để đảm bảo tính nhất quán và tuân thủ chuẩn dbt project.

#### Tiêu chí chấp nhận

1. THE Generator SHALL sinh Config_Block ở đầu mỗi dbt_Model bao gồm materialization strategy, tags, và schema theo dbt_Project_Template.
2. WHEN File_Mapping chỉ định tầng Bronze → Silver, THE Generator SHALL gán schema tương ứng với tầng Silver trong Config_Block.
3. WHEN File_Mapping chỉ định tầng Silver → Gold, THE Generator SHALL gán schema tương ứng với tầng Gold trong Config_Block.

### Yêu cầu 11: Sinh Source Reference

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh source reference sử dụng macro source() hoặc ref() của dbt, để đảm bảo lineage tracking và dependency resolution chính xác.

#### Tiêu chí chấp nhận

1. WHEN source table nằm ngoài dbt project (raw data từ Bronze), THE Generator SHALL sinh Source_Reference sử dụng macro `source()`.
2. WHEN source table là một dbt model khác trong cùng project, THE Generator SHALL sinh Source_Reference sử dụng macro `ref()`.

### Yêu cầu 12: Sinh Dependency DAG

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh dependency DAG từ danh sách source tables trong mapping, để đảm bảo thứ tự chạy các model đúng logic phụ thuộc.

#### Tiêu chí chấp nhận

1. WHEN một tập hợp File_Mapping được cung cấp, THE Generator SHALL phân tích danh sách source tables của từng mapping và sinh Dependency_DAG thể hiện quan hệ phụ thuộc giữa các dbt_Model.
2. THE Generator SHALL đảm bảo Dependency_DAG không chứa circular dependency.
3. IF Dependency_DAG chứa circular dependency, THEN THE Generator SHALL báo lỗi và liệt kê danh sách các model tham gia vào vòng lặp phụ thuộc.
4. WHEN một dbt_Model phụ thuộc vào nhiều source tables, THE Generator SHALL thể hiện tất cả các dependency trong Dependency_DAG.

### Yêu cầu 13: Tổng hợp dbt Model hoàn chỉnh

**User Story:** Là một Data Engineer, tôi muốn Generator tổng hợp tất cả các thành phần (config block, source reference, SQL transform, filter) thành một file dbt model hoàn chỉnh, để có thể deploy trực tiếp sau khi review.

#### Tiêu chí chấp nhận

1. THE Generator SHALL sinh mỗi dbt_Model dưới dạng một file .sql hoàn chỉnh bao gồm Config_Block, Source_Reference, và SQL transform theo đúng thứ tự chuẩn của dbt_Project_Template.
2. WHEN một File_Mapping chứa nhiều Mapping_Rule khác nhau (DIRECT_MAP, CAST, HASH, HARDCODE, UNPIVOT, Business Logic), THE Generator SHALL kết hợp tất cả các rule vào cùng một dbt_Model.
3. FOR ALL dbt_Model được sinh ra, THE Generator SHALL đảm bảo SQL hợp lệ theo cú pháp Spark SQL.
4. THE Generator SHALL luôn chạy validation (Yêu cầu 2, Yêu cầu 3) trước khi sinh code. Nếu có lỗi BLOCK, THE Generator SHALL dừng và báo lỗi chi tiết. Nếu chỉ có WARNING, THE Generator SHALL tiếp tục sinh code và ghi nhận warnings trong báo cáo.

### Yêu cầu 14: Xử lý hàng loạt (Batch Processing)

**User Story:** Là một Data Engineer, tôi muốn Generator xử lý nhiều file mapping cùng lúc, để sinh toàn bộ dbt model cho một batch transformation thay vì xử lý từng file một.

#### Tiêu chí chấp nhận

1. WHEN một thư mục chứa nhiều File_Mapping được cung cấp, THE Generator SHALL xử lý tất cả các file và sinh dbt_Model tương ứng cho từng file.
2. IF một File_Mapping trong batch bị lỗi, THEN THE Generator SHALL tiếp tục xử lý các file còn lại và báo cáo tổng hợp danh sách lỗi sau khi hoàn thành.
3. WHEN xử lý batch hoàn tất, THE Generator SHALL sinh báo cáo tổng hợp bao gồm số lượng model sinh thành công, số lượng lỗi, và danh sách lỗi chi tiết.

### Yêu cầu 15: Xử lý thay đổi Mapping (Change Management)

**User Story:** Là một Data Engineer, tôi muốn Generator hỗ trợ xử lý khi mapping thay đổi (cập nhật rule, thêm trường mới, hoặc thêm mới mapping), để quản lý quá trình thay đổi incremental trong triển khai dự án mà không cần sinh lại toàn bộ.

#### Tiêu chí chấp nhận

1. WHEN một Mapping_Change_Request cập nhật rule cho File_Mapping đã tồn tại, THE Generator SHALL so sánh mapping mới với mapping cũ và chỉ sinh lại phần dbt_Model bị ảnh hưởng bởi thay đổi.
2. WHEN một Mapping_Change_Request thêm trường mới vào File_Mapping đã tồn tại, THE Generator SHALL cập nhật dbt_Model tương ứng bằng cách bổ sung các câu lệnh SQL cho trường mới mà không ảnh hưởng đến các trường hiện có.
3. WHEN một Mapping_Change_Request thêm mới một File_Mapping hoàn toàn, THE Generator SHALL sinh dbt_Model mới và cập nhật Dependency_DAG để phản ánh quan hệ phụ thuộc mới.
4. WHEN một Mapping_Change_Request được xử lý, THE Generator SHALL sinh báo cáo diff mô tả chi tiết các thay đổi giữa phiên bản cũ và phiên bản mới của dbt_Model.
5. IF một Mapping_Change_Request gây ra thay đổi ảnh hưởng đến các dbt_Model phụ thuộc (downstream models), THEN THE Generator SHALL cảnh báo và liệt kê danh sách model bị ảnh hưởng.
6. THE Generator SHALL lưu lại lịch sử phiên bản của mỗi File_Mapping để hỗ trợ việc truy vết và rollback khi cần thiết.

### Yêu cầu 16: Sinh dbt Test

**User Story:** Là một Data Engineer, tôi muốn Generator tự động sinh các test cơ bản cho mỗi dbt model và quản lý cấu hình unit test bắt buộc, để đảm bảo chất lượng dữ liệu sau transformation và duy trì tính nhất quán của quy trình kiểm thử.

#### Tiêu chí chấp nhận

1. WHEN một dbt_Model được sinh ra, THE Generator SHALL sinh schema test cho các trường được đánh dấu NOT NULL trong File_Mapping.
2. WHEN một trường trong File_Mapping được đánh dấu là unique key, THE Generator SHALL sinh unique test cho trường đó.
3. WHEN một trường trong File_Mapping có relationship với bảng khác, THE Generator SHALL sinh relationship test tương ứng.
4. THE Generator SHALL đọc Unit_Test_Config để xác định danh sách các loại unit test bắt buộc cho từng loại dbt_Model (Bronze → Silver, Silver → Gold).
5. WHEN một dbt_Model được sinh ra, THE Generator SHALL kiểm tra dbt_Model đó có đầy đủ các unit test bắt buộc theo Unit_Test_Config và báo cáo danh sách test còn thiếu.
6. IF một dbt_Model thiếu unit test bắt buộc theo Unit_Test_Config, THEN THE Generator SHALL cảnh báo trong báo cáo review và đánh dấu dbt_Model đó là chưa đạt tiêu chuẩn kiểm thử.
7. THE Generator SHALL hỗ trợ cập nhật Unit_Test_Config để thêm mới hoặc chỉnh sửa quy định về unit test bắt buộc mà không ảnh hưởng đến các test đã sinh trước đó.

### Yêu cầu 17: Báo cáo Review cho con người

**User Story:** Là một Data Engineer Lead, tôi muốn Generator sinh báo cáo review tóm tắt cho mỗi batch, để hỗ trợ quá trình glance review và spot check hiệu quả.

#### Tiêu chí chấp nhận

1. WHEN một batch dbt_Model được sinh xong, THE Generator SHALL sinh báo cáo review bao gồm danh sách model, mapping pattern được sử dụng, và các điểm cần chú ý (business logic phức tạp, nhiều JOIN, UNION ALL).
2. THE Generator SHALL đánh dấu các dbt_Model chứa business logic phức tạp hoặc exception cần review kỹ bởi con người.
3. THE Generator SHALL liệt kê thứ tự chạy các model theo Dependency_DAG để con người xác nhận trước khi deploy.
