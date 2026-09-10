"""
BƯỚC 0: Chuẩn bị Object Storage
- Tạo 2 bucket trên MinIO: lakehouse-images, lakehouse-metadata
- Sinh một số lượng ảnh tối giản (ô màu ngẫu nhiên 64x64) và upload lên MinIO
  để chứng minh Object Storage lưu được dữ liệu ảnh thật.

Chạy: python 00_setup_minio.py
"""

import sys
import io
import random
import boto3
from PIL import Image
from botocore.exceptions import ClientError, EndpointConnectionError

import config

s3 = boto3.client(
    "s3",
    endpoint_url=config.MINIO_ENDPOINT,
    aws_access_key_id=config.MINIO_ACCESS_KEY,
    aws_secret_access_key=config.MINIO_SECRET_KEY,
)


def ensure_bucket(name: str):
    try:
        s3.head_bucket(Bucket=name)
        print(f"Bucket '{name}' đã tồn tại.")
    except ClientError:
        s3.create_bucket(Bucket=name)
        print(f"Đã tạo bucket '{name}'.")
    except (EndpointConnectionError, Exception) as e:
        print(f"\n[LỖI KẾT NỐI] Không thể kết nối tới MinIO tại {config.MINIO_ENDPOINT}.")
        print("Vui lòng đảm bảo Docker Desktop đã bật và chạy lệnh: docker compose up -d\n")
        sys.exit(1)


def make_dummy_image(width=64, height=64) -> bytes:
    """Sinh 1 ảnh tối giản: ô màu ngẫu nhiên, để làm 'ảnh giả lập' theo đúng phạm vi đề cương."""
    color = (
        random.randint(0, 255),
        random.randint(0, 255),
        random.randint(0, 255),
    )
    img = Image.new("RGB", (width, height), color=color)
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def upload_dummy_images(n: int):
    for i in range(n):
        key = f"img_{i:07d}.jpg"
        data = make_dummy_image()
        s3.put_object(Bucket=config.BUCKET_IMAGES, Key=key, Body=data)
        if (i + 1) % 100 == 0:
            print(f"  ...đã upload {i + 1}/{n} ảnh")
    print(f"Hoàn tất upload {n} ảnh vào bucket '{config.BUCKET_IMAGES}'.")


if __name__ == "__main__":
    print("=== Thiết lập Object Storage (MinIO) ===")
    ensure_bucket(config.BUCKET_IMAGES)
    ensure_bucket(config.BUCKET_METADATA)

    print(f"\nSinh và upload {config.NUM_REAL_IMAGES} ảnh tối giản...")
    upload_dummy_images(config.NUM_REAL_IMAGES)

    print("\nXong. Kiểm tra tại http://localhost:9001 (user/pass: minioadmin/minioadmin)")
