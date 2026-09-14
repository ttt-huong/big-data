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

    metrics.transform_time_s = round(time.time() - t0, 3)
    logger.info(f"[Transform] Xử lý xong: {len(clean_df):,} bản ghi SẠCH, {len(error_df):,} bản ghi LỖI ({metrics.transform_time_s:.3f}s)")
    return clean_df


def save_error_records(error_df: pd.DataFrame):
    """Ghi bổ sung (append) các bản ghi lỗi vào file parquet chứa error records."""
    error_df = error_df.copy()
    error_df["logged_at"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Chuẩn hóa cột datetime và object dạng string ISO để tránh lệch kiểu PyArrow timestamp[s] vs timestamp[ms]
    for col in error_df.columns:
        if pd.api.types.is_datetime64_any_dtype(error_df[col]):
            error_df[col] = error_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
        elif error_df[col].dtype == "object":
            error_df[col] = error_df[col].astype(str)

    table = pa.Table.from_pandas(error_df)
    if os.path.exists(config.ERROR_RECORDS_PATH):
        try:
            existing_table = pq.read_table(config.ERROR_RECORDS_PATH)
            combined_table = pa.concat_tables([existing_table, table], promote_options="default")
            pq.write_table(combined_table, config.ERROR_RECORDS_PATH)
        except Exception as e:
            logger.error(f"Lỗi khi append error records: {e}")
            pq.write_table(table, config.ERROR_RECORDS_PATH)
    else:
        pq.write_table(table, config.ERROR_RECORDS_PATH)
