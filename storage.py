"""Object-storage helpers for the optional SeaweedFS S3 mirror.

The pipeline keeps local artifacts as a safe fallback and mirrors them to the
S3-compatible endpoint when ``STORAGE_BACKEND=seaweedfs``.
"""

from __future__ import annotations

import os
from pathlib import Path

import config


def enabled() -> bool:
    return str(getattr(config, "STORAGE_BACKEND", "local")).lower() in {
        "seaweedfs",
        "s3",
        "object_storage",
    }


def _client():
    try:
        import boto3
    except ImportError as error:
        raise RuntimeError("Cần cài boto3 để ghi dữ liệu lên SeaweedFS") from error

    return boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT,
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
        region_name=getattr(config, "S3_REGION", "us-east-1"),
    )


def _ensure_bucket(client) -> None:
    try:
        client.head_bucket(Bucket=config.S3_BUCKET)
    except Exception:
        client.create_bucket(Bucket=config.S3_BUCKET)


def upload_file(local_path: str, object_key: str) -> str | None:
    """Upload one local file and return its S3 URI.

    When object storage is disabled this is a no-op, preserving local mode.
    """
    if not enabled() or not os.path.isfile(local_path):
        return None
    client = _client()
    _ensure_bucket(client)
    key = object_key.replace("\\", "/")
    client.upload_file(local_path, config.S3_BUCKET, key)
    return f"s3://{config.S3_BUCKET}/{key}"


def upload_directory(local_dir: str, object_prefix: str) -> list[str]:
    """Mirror all files under a local directory to an S3 prefix."""
    if not enabled() or not os.path.isdir(local_dir):
        return []
    uploaded = []
    root = Path(local_dir)
    for path in root.rglob("*"):
        if path.is_file():
            relative = path.relative_to(root).as_posix()
            uri = upload_file(str(path), f"{object_prefix.rstrip('/')}/{relative}")
            if uri:
                uploaded.append(uri)
    return uploaded


def list_objects(object_prefix: str = "") -> list[str]:
    """Liệt kê object trong bucket theo prefix."""
    if not enabled():
        return []
    client = _client()
    _ensure_bucket(client)
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=object_prefix):
        keys.extend(
            item["Key"]
            for item in page.get("Contents", [])
            if item.get("Key")
        )
    return keys


def download_object(object_key: str, cache_dir: str | None = None) -> str:
    """Tải một object về cache local và trả về đường dẫn file."""
    if not enabled():
        raise RuntimeError("Object storage đang tắt")
    if not object_key or object_key.startswith(("/", "\\")) or ".." in Path(object_key).parts:
        raise ValueError(f"Object key không an toàn: {object_key}")

    root = Path(cache_dir or os.path.join(config.LOCAL_DATA_DIR, ".object_cache"))
    local_path = root.joinpath(*Path(object_key.replace("\\", "/")).parts)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    client = _client()
    remote_size = int(client.head_object(Bucket=config.S3_BUCKET, Key=object_key).get("ContentLength", -1))
    if not local_path.exists() or (remote_size >= 0 and local_path.stat().st_size != remote_size):
        client.download_file(config.S3_BUCKET, object_key, str(local_path))
    return str(local_path)


def download_objects(object_prefix: str, suffix: str = ".parquet") -> list[str]:
    """Tải các object phù hợp prefix về cache local."""
    keys = [key for key in list_objects(object_prefix) if key.lower().endswith(suffix)]
    return [download_object(key) for key in keys]


def mirror_pipeline_outputs(
    *,
    raw_file: str | None = None,
    silver_file: str | None = None,
    gold_file: str | None = None,
    error_file: str | None = None,
    delta_dir: str | None = None,
) -> dict[str, object]:
    """Mirror pipeline artifacts and return uploaded object URIs."""
    if not enabled():
        return {"enabled": False, "objects": []}

    objects: list[str] = []
    for local_path, prefix in (
        (raw_file, "bronze"),
        (silver_file, "silver"),
        (gold_file, "gold"),
        (error_file, "errors"),
    ):
        if local_path and os.path.isfile(local_path):
            uri = upload_file(local_path, f"{prefix}/{os.path.basename(local_path)}")
            if uri:
                objects.append(uri)
    if delta_dir:
        objects.extend(upload_directory(delta_dir, "lakehouse/clean_metadata"))
    return {"enabled": True, "objects": objects}
