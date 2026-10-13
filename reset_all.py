"""Reset local runtime data for a fresh demo run.

This project uses local Parquet/Delta paths.  The old version still mentioned
MinIO/S3 and tried to write an empty Delta table before deleting the local
folders, which made reset unreliable on a clean machine.
"""

import os
import shutil

import config


def reset_object_storage():
    """Xóa toàn bộ object dữ liệu trong bucket SeaweedFS hiện tại."""
    backend = str(getattr(config, "STORAGE_BACKEND", "local")).lower()
    if backend not in {"seaweedfs", "s3", "object_storage"}:
        return

    try:
        import boto3
    except ImportError as error:
        print(f"  Bỏ qua SeaweedFS vì thiếu boto3: {error}")
        return

    client = boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT,
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
        region_name=getattr(config, "S3_REGION", "us-east-1"),
    )

    try:
        client.head_bucket(Bucket=config.S3_BUCKET)
    except Exception:
        print(f"  Không tìm thấy bucket SeaweedFS: {config.S3_BUCKET}")
        return

    paginator = client.get_paginator("list_objects_v2")
    keys = [
        item["Key"]
        for page in paginator.paginate(Bucket=config.S3_BUCKET)
        for item in page.get("Contents", [])
    ]
    for start in range(0, len(keys), 1000):
        batch = keys[start : start + 1000]
        client.delete_objects(
            Bucket=config.S3_BUCKET,
            Delete={"Objects": [{"Key": key} for key in batch]},
        )
    print(f"  Đã dọn dẹp SeaweedFS: {len(keys)} object")


def reset_all_layers():
    print("Đang dọn dẹp dữ liệu local (Bronze, Silver, Gold, DLQ, kết quả và log)...")
    for folder in [config.LOCAL_DATA_DIR, config.LOCAL_RESULTS_DIR, config.LOCAL_LOGS_DIR]:
        if os.path.exists(folder):
            for item in os.listdir(folder):
                item_path = os.path.join(folder, item)
                try:
                    if os.path.isfile(item_path) or os.path.islink(item_path):
                        os.unlink(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                except Exception as e:
                    print(f"     Không thể xóa {item_path}: {e}")
            print(f"  Đã dọn dẹp: {folder}")

    print("Hoàn tất reset dữ liệu local.")
    reset_object_storage()

if __name__ == "__main__":
    reset_all_layers()
