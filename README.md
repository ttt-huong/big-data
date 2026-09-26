# Local ETL/Lakehouse Prototype

Project học phần chạy trên **một máy Windows**, tập trung vào một pipeline ETL vừa phải và có thể demo end-to-end. Core pipeline không phụ thuộc MinIO, Docker hay distributed processing.

## Kiến trúc

```text
Generator
  -> raw/*.parquet
  -> Extract theo chunk
  -> Transform + Validation
  -> data/errors/*.parquet (DLQ)
  -> local Delta Lake: data/lakehouse/clean_metadata
  -> DuckDB analytics
  -> benchmark CSV
```

Công nghệ sử dụng:

- Python, Pandas, NumPy
- PyArrow và Parquet
- Delta Lake local (`deltalake`/delta-rs)
- DuckDB
- Pytest
- Matplotlib cho phần benchmark mở rộng

MinIO không còn là dependency bắt buộc. Registry image MinIO trước đây không ổn định, trong khi mục tiêu project là một demo local chạy được. Delta Lake local vẫn cung cấp transaction log, versioning và MERGE/upsert mà không cần object storage ngoài.

## Cấu trúc chính

```text
big-data/
├── data/
│   ├── raw/                 # source Parquet được sinh khi chạy
│   ├── processed/           # file trung gian/benchmark
│   ├── errors/              # DLQ theo từng batch Parquet
│   └── lakehouse/           # local Delta table
├── generator/
│   └── data_generator.py
├── pipeline/
│   ├── extract.py           # CSV/Parquet và chunk reader
│   ├── transform.py         # enrichment và DLQ writer
│   ├── validation.py        # data quality rules
│   ├── load.py              # local Delta MERGE/upsert
│   └── logging_utils.py
├── benchmark/
│   └── run_benchmarks.py
├── tests/
├── main_pipeline.py
├── query_analytics.py
├── 01_generate_metadata.py
├── config.py
├── requirements.txt
└── README.md
```

## Cài đặt

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

## Chạy tests

```powershell
pytest tests/ -v
```

Test kiểm tra generator, validation, duplicate, chunk reader, Full/Incremental watermark, Delta MERGE/idempotency và DuckDB analytics.

## Chạy pipeline

### Sinh source Parquet

```powershell
python 01_generate_metadata.py --rows 100000 --error-ratio 0.05
```

### Full Load

```powershell
python main_pipeline.py --mode full --rows 100000 --error-ratio 0.05 --chunk-size 25000
```

Full Load tạo source Parquet, validate dữ liệu, lưu record lỗi vào `data/errors/`, ghi clean data vào Delta Lake local và cập nhật watermark.

### Incremental Load

```powershell
python main_pipeline.py --mode incremental --rows 20000 --error-ratio 0.02 --chunk-size 5000
```

Incremental dùng `data/watermark.json` và chỉ đọc record có `image_id` lớn hơn watermark. Target dùng Delta MERGE theo `image_id`, nên chạy lại cùng input không tạo duplicate.

### Backfill demo

Backfill ở mức demo dùng source có ID cũ hơn một mốc chỉ định:

```powershell
python main_pipeline.py --mode backfill --rows 10000 --backfill-before-id 5000 --chunk-size 2500
```

Backfill không cập nhật watermark chính.

### DuckDB analytics

```powershell
python query_analytics.py
```

Analytics gồm tổng clean records, phân bố category/format, phân bố year/month và tổng error records trong DLQ.

## Benchmark

Chạy các benchmark local, không tạo số liệu giả:

```powershell
python -m benchmark.run_benchmarks
```

Kết quả được ghi vào:

- `results/storage_benchmark.csv`: CSV vs Parquet size/read/write.
- `results/chunk_benchmark.csv`: full read vs chunk read.
- `results/query_benchmark.csv`: DuckDB query time.

Benchmark mặc định dùng các mức `10K`, `50K`, `100K`. Có thể thay đổi trong `config.py`.

## Data quality rules

Record hợp lệ cần có:

- `image_id` không null.
- `file_size > 0`.
- `width > 0`, `height > 0`.
- `category` thuộc danh sách cho phép.
- `format` thuộc danh sách cho phép.
- `created_at` parse được và không ở tương lai.
- `image_id` không duplicate trong cùng batch.

Record lỗi được lưu riêng với `error_reason` trong `data/errors/`.

## Ghi chú phạm vi

Project không sử dụng Spark, Hadoop, Kafka, Airflow, Kubernetes, Dask cluster, Ray cluster hoặc hệ thống nhiều máy. Docker/MinIO không cần thiết cho core demo; pipeline local là đường chạy chính để bảo đảm reproducibility trên máy cá nhân.
