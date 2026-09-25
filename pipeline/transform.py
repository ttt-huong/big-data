"""
Transform Layer cho ETL Pipeline.
Chịu trách nhiệm gọi validation, làm giàu dữ liệu (enrichment), ép kiểu
và ghi nhận dữ liệu lỗi ra error_records.parquet.
"""

import os
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import config
from pipeline.validation import validate_and_clean
from pipeline.logging_utils import logger, PipelineMetrics


def transform_data(df: pd.DataFrame, metrics: PipelineMetrics) -> pd.DataFrame:
    """
    Thực hiện Transform dữ liệu:
    1. Kiểm tra validation & tách bản ghi hợp lệ / lỗi
    2. Enrich dữ liệu sạch (tạo aspect_ratio, size_mb, ingested_at)
    3. Lưu lại bản ghi lỗi vào ERROR_RECORDS_PATH
    4. Cập nhật thống kê vào metrics
    """
    t0 = time.time()
    
    if df.empty:
        metrics.transform_time_s = round(time.time() - t0, 3)
        return df

    # Validate & Clean
    clean_df, error_df = validate_and_clean(df)

    metrics.valid_records += len(clean_df)
    metrics.error_records += len(error_df)

    # Nếu có bản ghi lỗi -> Lưu vào error_records.parquet
    if not error_df.empty:
        save_error_records(error_df)
        logger.warning(f"[Transform] Đã phát hiện và lưu {len(error_df):,} bản ghi LỖI vào {config.ERROR_RECORDS_PATH}")

    # Enrich Clean Data
    if not clean_df.empty:
        clean_df["aspect_ratio"] = (clean_df["width"] / clean_df["height"]).round(2)
        clean_df["size_mb"] = (clean_df["file_size"] / (1024 ** 2)).round(3)
        clean_df["ingested_at"] = pd.Timestamp.now()

        # Late-Arriving Data Detection (Xử lý dữ liệu muộn so với Watermark chỉ trong chế độ incremental)
        clean_df["is_late_arriving"] = False
        if metrics and metrics.mode == "incremental" and "created_at" in clean_df.columns:
            from pipeline.extract import get_watermark
            last_ts = getattr(metrics, "initial_watermark_ts", None) or get_watermark().get("last_watermark_ts")
            if last_ts:
                wm_dt = pd.to_datetime(last_ts)
                clean_dt = pd.to_datetime(clean_df["created_at"], errors="coerce")
                late_mask = (clean_dt < wm_dt) & clean_dt.notna()
                clean_df["is_late_arriving"] = late_mask.fillna(False).astype(bool)
                num_late = int(late_mask.sum())
                if num_late > 0:
                    metrics.late_records += num_late
                    logger.info(f"[Transform] Phát hiện {num_late:,} bản ghi DỮ LIỆU MUỘN (Late-Arriving Data < {last_ts}).")

    metrics.transform_time_s = round(time.time() - t0, 3)
    logger.info(f"[Transform] Xử lý xong: {len(clean_df):,} bản ghi SẠCH, {len(error_df):,} bản ghi LỖI ({metrics.transform_time_s:.3f}s)")
    return clean_df


def save_error_records(error_df: pd.DataFrame):
    """Append error records efficiently by writing each batch to a separate Parquet file."""
    error_df = error_df.copy()
    error_df["logged_at"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    # Normalize datetime and object columns for Parquet compatibility
    for col in error_df.columns:
        if pd.api.types.is_datetime64_any_dtype(error_df[col]):
            error_df[col] = error_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
        elif error_df[col].dtype == "object":
            error_df[col] = error_df[col].astype(str)

    table = pa.Table.from_pandas(error_df)

    # Write each batch into a dedicated folder to avoid costly read‑modify‑write cycles.
    error_dir = os.path.splitext(config.ERROR_RECORDS_PATH)[0]  # e.g. ./data/error_records
    os.makedirs(error_dir, exist_ok=True)

    # Unique filename based on timestamp to guarantee ordering.
    ts = pd.Timestamp.now().strftime("%Y%m%d%H%M%S%f")
    file_path = os.path.join(error_dir, f"error_{ts}.parquet")

    try:
        pq.write_table(table, file_path)
        logger.info(f"[Transform] Ghi {len(error_df):,} bản ghi lỗi vào {file_path}")
    except Exception as e:
        logger.error(f"Lỗi khi ghi error record batch: {e}")
        raise

