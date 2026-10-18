# Data Quality và phân tích OLAP trên LakehouseCopy-Item query_analytics.py benchmark/analytics_baseline.py

## 1. Mục tiêu và phạm vi

Hoàn thiện việc kiểm tra chất lượng metadata ảnh tại tầng Transform và xây dựng các truy vấn phân tích bằng DuckDB trên Delta Lake.

Phần triển khai gồm:
- Kiểm chứng 7 nhóm quy tắc Data Quality đã có.
- Bổ sung kiểm thử dữ liệu hợp lệ, dữ liệu lỗi và các tình huống biên.
- Bổ sung truy vấn tổng hợp dung lượng và thống kê lỗi.
- Đo thời gian truy vấn theo quy mô dữ liệu.
- So sánh bảng Delta có và không phân vùng theo category.

Các quy tắc kiểm tra metadata; chưa kiểm tra nội dung hoặc sự tồn tại của file ảnh thực tế.

## 2. Môi trường thực nghiệm

| Thành phần | Cấu hình |
|---|---|
| Hệ điều hành | Windows, môi trường win32 |
| CPU | Intel Core i7-1255U, 10 nhân, 12 luồng |
| RAM hệ thống ghi nhận | 15,73 GB |
| Python | 3.13.5 |
| DuckDB | 1.5.5 |
| deltalake | 1.6.3 |
| PyArrow | 25.0.1 |
| Pandas | 3.0.5 |
| pytest | 7.4.0 |

Thí nghiệm so sánh phân vùng cấu hình DuckDB dùng 4 luồng.
Thí nghiệm tăng quy mô sử dụng cấu hình luồng mặc định, không đặt rõ trong script.

## 3. Kiểm tra Data Quality tại Transform

### 3.1. Các quy tắc

Logic kiểm tra nằm tại `pipeline/validation.py`.

| Mã | Điều kiện hợp lệ | Mã lỗi |
|---|---|---|
| DQ-01 | image_id không rỗng | NULL_IMAGE_ID |
| DQ-02 | file_size > 0 | INVALID_FILE_SIZE |
| DQ-03 | width > 0 và height > 0 | INVALID_WIDTH; INVALID_HEIGHT |
| DQ-04 | category thuộc danh mục cho phép | INVALID_CATEGORY |
| DQ-05 | format thuộc jpg, png, webp | INVALID_FORMAT |
| DQ-06 | created_at hợp lệ và không ở tương lai | INVALID_CREATED_AT; FUTURE_CREATED_AT |
| DQ-07 | image_id không trùng trong chunk hoặc tập seen_ids được truyền vào | DUPLICATE_IMAGE_ID |

Danh mục hợp lệ: san_pham, chan_dung, phong_canh, do_an,
dong_vat, kien_truc.

### 3.2. Nguyên tắc xử lý

Hàm `validate_and_clean()` trả về hai DataFrame: clean và errors.

- Không vi phạm quy tắc: bản ghi thuộc clean.
- Vi phạm ít nhất một quy tắc: bản ghi thuộc errors.
- Một bản ghi có nhiều lỗi được ghi đủ nguyên nhân trong error_reason,
  ngăn cách bằng dấu chấm phẩy.
- Một dòng có nhiều nguyên nhân vẫn chỉ được tính là một bản ghi lỗi.
- Tổng số dòng sạch và lỗi phải bằng số dòng đầu vào.
- Hàm kiểm tra không thay đổi DataFrame đầu vào trong tình huống đã kiểm thử.

Dữ liệu sạch được Transform bổ sung size_mb, aspect_ratio và ingested_at.
Dữ liệu lỗi được tích hợp vào cơ chế DLQ hiện có.

Tập seen_ids do bên gọi quản lý. Test truyền seen_ids mô phỏng ID đã gặp
ở chunk trước, chưa tự chứng minh toàn bộ cơ chế quản lý tập này trong pipeline.

### 3.3. Kết quả kiểm thử

Đã bổ sung 37 ca trong `tests/test_validation.py`:

| Nhóm kiểm thử | Số ca |
|---|---:|
| ID rỗng | 1 |
| Dung lượng không hợp lệ | 3 |
| Kích thước không hợp lệ | 7 |
| Danh mục hợp lệ và không hợp lệ | 9 |
| Định dạng hợp lệ và không hợp lệ | 6 |
| Thời gian hợp lệ và không hợp lệ | 4 |
| ID duy nhất và trùng lặp | 3 |
| Một bản ghi có nhiều lỗi | 1 |
| Đầu vào rỗng | 1 |
| Tất cả bản ghi đều lỗi | 1 |
| Không thay đổi dữ liệu đầu vào | 1 |
| Tổng | 37 |

Các ca đã chạy PASS. Phạm vi này chưa bao phủ mọi dạng sai kiểu dữ liệu,
ID thập phân hoặc các trường hợp thời gian có múi giờ.

## 4. Truy vấn OLAP bằng DuckDB

Triển khai trong `query_analytics.py`.

Dữ liệu sạch được đăng ký với DuckDB thông qua
`DeltaTable(...).to_pyarrow_dataset()`, sử dụng snapshot Delta hiện hành.

| Kết quả/hàm | Nội dung |
|---|---|
| storage_overview | Số ảnh, tổng và trung bình dung lượng |
| storage_by_category | Số ảnh, tổng và trung bình dung lượng theo danh mục |
| storage_by_month | Số ảnh và tổng dung lượng theo năm, tháng |
| storage_by_format | Số ảnh, tổng và trung bình dung lượng theo định dạng |
| errors_by_reason | Số lượt vi phạm theo từng nguyên nhân trong DLQ |
| query_formats_by_period | Thống kê định dạng trong một danh mục và khoảng thời gian |

Các kết quả thống kê cơ bản có sẵn được giữ lại.

Dung lượng được tính bằng byte. Với truy vấn tổng quan, bảng rỗng có
tổng dung lượng bằng 0 và dung lượng trung bình là NULL/None.

Truy vấn lọc sử dụng tham số, với khoảng thời gian:
created_at >= start_date và created_at < end_date.

Thống kê DLQ tách error_reason theo dấu chấm phẩy. Do một dòng có thể có
nhiều lỗi, tổng số lượt vi phạm có thể lớn hơn số bản ghi lỗi.

### 4.1. Kiểm thử OLAP

`tests/test_analytics.py` có 4 ca:

1. Đếm bản ghi sạch và lỗi.
2. Kiểm tra các phép tổng hợp dung lượng.
3. Đếm từng nguyên nhân lỗi DLQ.
4. Lọc theo danh mục và khoảng thời gian, gồm các mốc biên.

Cả 4 ca PASS. Kết quả tổng hợp được đối chiếu với dữ liệu nhỏ có đáp án
tính trước.

## 5. Thực nghiệm hiệu năng

### 5.1. Phương pháp

Triển khai tại `benchmark/run_benchmarks.py`.

- Dữ liệu sinh tự động với seed=9, error_ratio=0.
- Tạo dữ liệu và ghi Delta trước khi đo.
- Đăng ký Arrow Dataset trước khi đo.
- Đo thời gian thực thi và lấy đầy đủ kết quả bằng fetchall().
- Mỗi truy vấn có một lần đầu và 5 lần lặp.
- Báo cáo trung vị của 5 lần lặp.
- Kiểm tra kết quả không đổi giữa các lần chạy.

Thời gian không bao gồm tạo dữ liệu, ghi Delta, mở kết nối và đăng ký dataset.
Lần đầu không được coi là phép đo đọc nguội vì không kiểm soát cache hệ điều hành.

### 5.2. Tăng quy mô dữ liệu

Bảng Delta phân vùng theo category.

| Số bản ghi | Theo danh mục (ms) | Theo tháng (ms) | Lọc kết hợp (ms) |
|---:|---:|---:|---:|
| 100.000 | 7,969 | 10,046 | 6,640 |
| 500.000 | 15,830 | 19,215 | 13,775 |
| 1.000.000 | 22,572 | 21,983 | 16,613 |

Truy vấn lọc chọn category='do_an' và khoảng thời gian
[2025-01-01, 2026-01-01), sau đó tổng hợp theo format.

Khi quy mô tăng 10 lần từ 100.000 lên 1.000.000 dòng, thời gian trung vị
tăng khoảng 2,19–2,83 lần trong phép thử này.

### 5.3. Kế hoạch thực thi

EXPLAIN ANALYZE ở mức 100.000 dòng cho thấy:

- TABLE_SCAN sử dụng ARROW_SCAN.
- Điều kiện category và created_at xuất hiện tại bước scan.
- Scan xuất các cột file_size và format phục vụ tổng hợp.
- Scan trả ra 4.555 dòng sau lọc.
- HASH_GROUP_BY tổng hợp thành 3 nhóm định dạng.
- ORDER_BY sắp xếp kết quả.
- Tổng thời gian của lần chạy có profiling: 9,2 ms.

Kết quả thể hiện filter pushdown ở bước đọc.
Số 4.555 là số dòng đầu ra của scan, không chứng minh chỉ có
4.555 dòng được đọc vật lý. Kế hoạch chưa thể hiện rõ số file bị loại bỏ.

### 5.4. So sánh bố trí phân vùng

Cùng một triệu dòng dữ liệu, cùng SQL và tham số, DuckDB dùng 4 luồng.
Thứ tự thực thi hai bố trí được đảo luân phiên.
Kết quả hai bên được kiểm tra bằng nhau.

| Bố trí | Số file dữ liệu | Trung vị 5 lần lặp (ms) |
|---|---:|---:|
| Không phân vùng | 1 | 54,993 |
| Phân vùng category | 6 | 15,454 |

Trong thí nghiệm này, bố trí phân vùng nhanh hơn khoảng 3,56 lần,
tương đương giảm khoảng 71,9% thời gian.

Đây là so sánh hai bố trí bảng được tạo, bao gồm ảnh hưởng của số file;
không phải phép đo tách riêng tác động của phân vùng.

## 6. Bằng chứng và tái thực hiện

Các file kết quả:

- results/delta_query_benchmark.csv
- results/delta_query_scale_benchmark.csv
- results/delta_partition_comparison.csv
- results/duckdb_filtered_explain.txt

Chạy kiểm thử:

```powershell
python -m pytest tests -q
```

Kết quả toàn dự án gần nhất: 67 passed trong 6,84 giây.
Trong đó gồm 37 ca DQ mới và tổng cộng 4 ca analytics.

Chạy benchmark mặc định:

```powershell
python -m benchmark.run_benchmarks
```

Chạy lại thí nghiệm tăng quy mô và lưu kết quả:

```powershell
python -c "import os; import pandas as pd; import config; from benchmark.run_benchmarks import benchmark_delta_queries; results = pd.DataFrame([item for rows in [100_000, 500_000, 1_000_000] for item in benchmark_delta_queries(rows)]); results.to_csv(os.path.join(config.LOCAL_RESULTS_DIR, 'delta_query_scale_benchmark.csv'), index=False); print(results.to_string(index=False))"
```

Chạy lại phép so sánh phân vùng:

```powershell
python -c "import os; import pandas as pd; import config; from benchmark.run_benchmarks import benchmark_partitioning; results = pd.DataFrame(benchmark_partitioning(1_000_000)); results.to_csv(os.path.join(config.LOCAL_RESULTS_DIR, 'delta_partition_comparison.csv'), index=False); print(results.to_string(index=False))"
```

## 7. Giới hạn và kết luận

Dữ liệu thử nghiệm là metadata sinh tự động, tối đa một triệu dòng.
Chưa đánh giá truy vấn đồng thời, dữ liệu trên Object Storage,
tải vượt RAM hoặc nhiều lần thực nghiệm độc lập để tính độ biến thiên.

Benchmark tạo DataFrame trong RAM; kết quả không đánh giá khả năng
tiết kiệm bộ nhớ của pipeline đọc theo chunk.

Trong môi trường đã ghi nhận, ba truy vấn trên bảng Delta phân vùng đạt
thời gian trung vị khoảng 16,61–22,57 ms ở một triệu dòng.
Bố trí phân vùng category cải thiện thời gian của truy vấn lọc được chọn
so với bố trí không phân vùng trong phép so sánh riêng.

Các kết luận chỉ áp dụng trong phạm vi dữ liệu, truy vấn và điều kiện
thực nghiệm đã mô tả.

## 8. Tối ưu hàm tổng hợp OLAP

### 8.1. Thay đổi

Trước tối ưu, run_queries() thực hiện 8 câu SQL trên dữ liệu sạch.
Một số truy vấn đếm lặp lại thông tin đã có trong kết quả tổng hợp dung lượng.

Sau tối ưu, hàm thực hiện 4 câu SQL trên dữ liệu sạch, sau đó lấy lại
các kết quả đếm từ storage_overview, storage_by_category,
storage_by_format và storage_by_month.

Các trường kết quả cũ được giữ nguyên.

### 8.2. Phương pháp đánh giá

- Cùng bảng Delta phân vùng category, gồm một triệu bản ghi.
- Dữ liệu sinh với seed=9, error_ratio=0.
- DLQ mẫu gồm hai bản ghi với nhiều nguyên nhân lỗi.
- Hai phiên bản chạy luân phiên, mỗi phiên bản một lần đầu và 5 lần lặp.
- Đo toàn bộ lời gọi run_queries(), gồm mở kết nối, đăng ký dataset,
  thực thi truy vấn, lấy kết quả và đóng kết nối.
- Kiểm tra toàn bộ dictionary kết quả bằng nhau giữa hai phiên bản.
- Sử dụng cấu hình luồng mặc định của DuckDB.

### 8.3. Kết quả

| Phiên bản | Trung vị 5 lần lặp |
|---|---:|
| Trước tối ưu | 179,271 ms |
| Sau tối ưu | 112,737 ms |

Trong phép thử, thời gian giảm khoảng 37,1%, tương đương tăng tốc
khoảng 1,59 lần. Kết quả hai phiên bản bằng nhau.

Bằng chứng:
- benchmark/analytics_baseline.py
- results/analytics_optimization_comparison.csv

Kết quả toàn bộ kiểm thử sau tối ưu: 67 passed.

Phép đo này đánh giá toàn bộ hàm tổng hợp, khác với phép đo thời gian
từng câu SQL ở phần trước. Kết luận áp dụng cho điều kiện thử nghiệm
đã mô tả, chưa đại diện cho mọi tải dữ liệu.