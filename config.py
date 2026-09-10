"""
Cấu hình dùng chung cho toàn bộ project P4: Lakehouse trên Object Storage.
"""

# --- Kết nối MinIO (object storage) ---
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

BUCKET_IMAGES = "lakehouse-images"      # chứa ảnh (thật/tối giản)
BUCKET_METADATA = "lakehouse-metadata"  # chứa Parquet / Delta table metadata

# storage_options dùng cho thư viện deltalake (delta-rs) khi ghi lên MinIO
DELTA_STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

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

# --- Thư mục local dùng làm nơi trung chuyển trước khi upload ---
LOCAL_DATA_DIR = "./data"
LOCAL_RESULTS_DIR = "./results"

# Tự động tạo thư mục dữ liệu và kết quả nếu chưa có
os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
os.makedirs(LOCAL_RESULTS_DIR, exist_ok=True)

# --- Thiết kế metadata ảnh ---
# image_id, filename, file_size, width, height, format, category, created_at, year, month
CATEGORIES = ["san_pham", "chan_dung", "phong_canh", "do_an", "dong_vat", "kien_truc"]
FORMATS = ["jpg", "png", "webp"]

# Khoảng thời gian giả lập ảnh được tạo ra (để có cột year/month cho partition)
DATE_RANGE_START = "2023-01-01"
DATE_RANGE_END = "2026-08-31"

# Các mốc quy mô dữ liệu dùng cho TN3 (tăng dần)
# Có thể giảm bớt nếu máy yếu, ví dụ chỉ chạy [100_000, 1_000_000]
SCALE_LEVELS = [100_000, 1_000_000, 5_000_000]  # thêm 10_000_000 nếu máy đủ mạnh

# Số lượng ảnh thật/tối giản upload lên MinIO để minh họa Object Storage
# (không cần khớp với số dòng metadata, chỉ cần đủ để chứng minh)
NUM_REAL_IMAGES = 500
