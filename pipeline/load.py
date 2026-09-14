"""
Load Layer cho ETL Pipeline.
Ghi dữ liệu sạch vào Target Storage (Delta Lake trên MinIO hoặc Parquet Local).
Hỗ trợ Retry khi gặp lỗi nạp dữ liệu tạm thời và tự động cập nhật Watermark.
"""

import os
import time
import pandas as pd
from deltalake import write_deltalake

import config
from pipeline.extract import save_watermark
from pipeline.logging_utils import logger, PipelineMetrics


def load_data(
    clean_df: pd.DataFrame,
    metrics: PipelineMetrics,
    target_uri: str = config.TARGET_DELTA_URI,
    max_retries: int = 3
) -> bool:
    """
    Nạp dữ liệu sạch vào Delta Lake / Parquet Target Storage:
    - mode: 'append' nếu có dữ liệu mới
    - Có cơ chế retry nếu gặp lỗi tạm thời
    - Cập nhật Watermark sau khi ghi thành công
    """
    t0 = time.time()

    if clean_df.empty:
        logger.info("[Load] Không có dữ liệu sạch để load.")
        metrics.load_time_s = round(time.time() - t0, 3)
        return True

    success = False
    last_exception = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"[Load] Ghi {len(clean_df):,} bản ghi vào Target: {target_uri} (Lần thử {attempt}/{max_retries})")
            
            # Ghi vào Delta Table trên MinIO (S3)
            write_deltalake(
                target_uri,
                clean_df,
                mode="append",
                schema_mode="merge",
                storage_options=config.DELTA_STORAGE_OPTIONS,
                partition_by=["category"],
            )
            success = True
            break
        except Exception as e:
            last_exception = e
            metrics.retries += 1
            logger.warning(f"[Load] Thất bại lần {attempt}: {e}. Đang thử lại...")
            time.sleep(1)

    # Nếu Delta Lake MinIO gặp sự cố kết nối, fallback ghi ra Parquet Dataset local
    if not success:
        logger.error(f"[Load] Ghi Delta Table thất bại sau {max_retries} lần thử: {last_exception}")
        fallback_path = os.path.join(config.LOCAL_DATA_DIR, "clean_dataset_fallback")
        try:
            logger.info(f"[Load Fallback] Đang ghi dữ liệu sạch vào local Parquet fallback: {fallback_path}")
            clean_df.to_parquet(fallback_path, partition_cols=["category"], index=False)
            success = True
        except Exception as fallback_err:
            logger.critical(f"[Load Fallback Failed] Không thể ghi fallback: {fallback_err}")
            metrics.finish(status="FAILED", error_message=str(fallback_err))
            return False

    load_time = time.time() - t0
    metrics.loaded_records += len(clean_df)
    metrics.load_time_s = round(load_time, 3)

    # Cập nhật Watermark sau khi ghi thành công
    if "image_id" in clean_df.columns:
        max_id = clean_df["image_id"].max()
        max_ts = clean_df["created_at"].max() if "created_at" in clean_df.columns else None
        save_watermark(last_watermark_ts=max_ts, last_image_id=max_id)

    logger.info(f"[Load] Hoàn tất nạp {len(clean_df):,} bản ghi ({load_time:.3f}s)")
    return True
