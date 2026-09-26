"""Configuration for the local, single-machine ETL demo."""

import os
import sys

# Đảm bảo in tiếng Việt không bị UnicodeEncodeError trên console Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LOCAL_DATA_DIR = os.path.join(PROJECT_ROOT, "data")
RAW_DATA_DIR = os.path.join(LOCAL_DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(LOCAL_DATA_DIR, "processed")
ERROR_DATA_DIR = os.path.join(LOCAL_DATA_DIR, "errors")
LAKEHOUSE_DIR = os.path.join(LOCAL_DATA_DIR, "lakehouse")
LOCAL_RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")
LOCAL_LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")

for directory in (RAW_DATA_DIR, PROCESSED_DATA_DIR, ERROR_DATA_DIR, LAKEHOUSE_DIR, LOCAL_RESULTS_DIR, LOCAL_LOGS_DIR):
    os.makedirs(directory, exist_ok=True)

WATERMARK_FILE = os.path.join(LOCAL_DATA_DIR, "watermark.json")
TARGET_DELTA_PATH = os.path.join(LAKEHOUSE_DIR, "clean_metadata")

# --- Thiết kế metadata ảnh ---
# image_id, filename, file_size, width, height, format, category, created_at, year, month
CATEGORIES = ["san_pham", "chan_dung", "phong_canh", "do_an", "dong_vat", "kien_truc"]
FORMATS = ["jpg", "png", "webp"]

# Khoảng thời gian giả lập ảnh được tạo ra (để có cột year/month cho partition)
DATE_RANGE_START = "2023-01-01"
DATE_RANGE_END = "2026-08-31"

# Các mốc quy mô dữ liệu dùng cho TN3 (tăng dần)
# Có thể giảm bớt nếu máy yếu, ví dụ chỉ chạy [100_000, 1_000_000]
DEFAULT_ROWS = 100_000
DEFAULT_ERROR_RATIO = 0.05
BENCHMARK_ROWS = [10_000, 50_000, 100_000]
