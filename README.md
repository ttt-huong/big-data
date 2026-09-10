# P4: Lakehouse trên Object Storage cho dữ liệu ảnh quy mô lớn

Stack: **MinIO + Parquet + Delta Lake (delta-rs) + DuckDB** — không dùng Spark, không dùng VM.

## Yêu cầu
- Windows/macOS/Linux + Docker Desktop
- Python 3.9+

## Cài đặt

```bash
# 1. Khởi động MinIO
docker compose up -d

# 2. Cài thư viện Python
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Mở http://localhost:9001 (user/pass: `minioadmin` / `minioadmin`) để xem giao diện MinIO.
Không cần tự tạo bucket tay — script `00_setup_minio.py` sẽ tự tạo.

## Chạy lần lượt theo đúng 5 thí nghiệm

Tất cả lệnh chạy từ thư mục gốc project.

```bash
# Bước 0: Tạo bucket + upload ảnh tối giản (chứng minh Object Storage)
python 00_setup_minio.py

# Bước 1: Sinh metadata mẫu (dùng cho TN4, TN5)
python 01_generate_metadata.py --n 1000000

# TN1: CSV vs Parquet
python 02_tn1_csv_vs_parquet.py --n 1000000

# TN2: Partition vs không partition
python 03_tn2_partitioning.py --n 1000000

# TN3: Benchmark theo quy mô tăng dần (chạy TN1 lặp lại ở nhiều mức)
python 04_tn3_scale_benchmark.py

# TN4: Truy vấn phân tích bằng DuckDB
python 05_tn4_query_duckdb.py --n 1000000

# TN5: Delta Lake - ACID, time travel, schema evolution
python 06_tn5_delta_lake.py --n 100000

# Vẽ biểu đồ tổng hợp cho báo cáo/slide
python 07_plot_results.py
```

## Lưu ý quan trọng

- **Quy mô dữ liệu**: mặc định chạy tới 5 triệu dòng (`config.SCALE_LEVELS`). Nếu máy yếu,
  sửa file `config.py`, để `SCALE_LEVELS = [100_000, 1_000_000]` là đủ thuyết phục,
  không bắt buộc phải lên 10 triệu.
- **TN5 nên chạy với quy mô nhỏ** (100K) vì thao tác UPDATE/append trên Delta table
  chưa được tối ưu tốc độ như benchmark thuần Parquet.
- **Kết quả** (số liệu CSV + biểu đồ PNG) nằm trong thư mục `results/` — dùng trực tiếp
  để chèn vào báo cáo Word và slide thuyết trình.
- **Ảnh trong MinIO**: chỉ là ảnh tối giản (ô màu ngẫu nhiên), đúng theo phạm vi đề cương —
  không cần và không nên thu thập ảnh thật số lượng lớn.

## Bảng ánh xạ Thí nghiệm ↔ File kết quả

| Thí nghiệm | Script | File kết quả |
|---|---|---|
| TN1 - CSV vs Parquet | `02_tn1_csv_vs_parquet.py` | `results/tn1_csv_vs_parquet.csv` |
| TN2 - Partition | `03_tn2_partitioning.py` | `results/tn2_partitioning.csv`, `chart_tn2_partition.png` |
| TN3 - Tăng quy mô | `04_tn3_scale_benchmark.py` | `results/tn3_scale_benchmark.csv`, `chart_tn3_scale.png` |
| TN4 - Query | `05_tn4_query_duckdb.py` | in trực tiếp ra màn hình, chụp ảnh làm bằng chứng |
| TN5 - Delta Lake | `06_tn5_delta_lake.py` | in trực tiếp ra màn hình, chụp ảnh làm bằng chứng |

## Dừng MinIO khi làm xong
```bash
docker compose down
```
