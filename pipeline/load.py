"""
Load Layer cho ETL Pipeline.
Ghi dữ liệu sạch vào Target Storage (Delta Lake trên MinIO hoặc Parquet Local).
Hỗ trợ Retry khi gặp lỗi nạp dữ liệu tạm thời và tự động cập nhật Watermark.
"""

import os
import time
import pandas as pd
from deltalake import DeltaTable, write_deltalake

import config
from pipeline.extract import save_watermark
from pipeline.logging_utils import logger, PipelineMetrics


def load_data(
    clean_df: pd.DataFrame,
    metrics: PipelineMetrics,
    target_uri: str = config.TARGET_DELTA_URI,
    write_mode: str = "merge",
    max_retries: int = 3
) -> bool:
    """
    Nạp dữ liệu sạch vào Delta Lake / Parquet Target Storage:
    - write_mode: 'merge' (Idempotent Upsert theo image_id), 'append', hoặc 'overwrite'
    - Có cơ chế retry khi gặp lỗi tạm thời
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
            logger.info(f"[Load] Nạp {len(clean_df):,} bản ghi vào Target ({write_mode.upper()}): {target_uri} (Lần thử {attempt}/{max_retries})")
            
            # Xác định storage_options dựa trên URI (chỉ dùng S3 options khi URI bắt đầu bằng s3://)
            storage_opts = config.DELTA_STORAGE_OPTIONS if target_uri.startswith("s3://") else None

            is_delta_table = False
            try:
                if storage_opts:
                    is_delta_table = DeltaTable.is_deltatable(target_uri, storage_options=storage_opts)
                else:
                    is_delta_table = DeltaTable.is_deltatable(target_uri)
            except Exception as check_err:
                logger.warning(f"[Load Check Warning] Lỗi kiểm tra Delta Table: {check_err}")
                is_delta_table = False

            if write_mode == "merge" and is_delta_table:
                if storage_opts:
                    dt = DeltaTable(target_uri, storage_options=storage_opts)
                else:
                    dt = DeltaTable(target_uri)
                (
                    dt.merge(
                        source=clean_df,
                        predicate="target.image_id = source.image_id",
                        source_alias="source",
                        target_alias="target",
                    )
                    .when_matched_update_all()
                    .when_not_matched_insert_all()
                    .execute()
                )
                logger.info(f"[Load] Đã thực hiện MERGE INTO (Upsert) thành công trên Delta Table v{dt.version()}")
            else:
                mode_to_use = "append" if write_mode == "merge" else write_mode
                if storage_opts:
                    write_deltalake(
                        target_uri,
                        clean_df,
                        mode=mode_to_use,
                        schema_mode="merge",
                        storage_options=storage_opts,
                        partition_by=["category"],
                    )
                else:
                    write_deltalake(
                        target_uri,
                        clean_df,
                        mode=mode_to_use,
                        schema_mode="merge",
                        partition_by=["category"],
                    )
                logger.info(f"[Load] Đã ghi thành công Delta Table (mode={mode_to_use})")

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

    # Cập nhật Watermark sau khi ghi thành công (Bỏ qua nếu là chế độ Backfill)
    if metrics.mode != "backfill" and "image_id" in clean_df.columns:
        max_id = clean_df["image_id"].max()
        max_ts = clean_df["created_at"].max() if "created_at" in clean_df.columns else None
        save_watermark(last_watermark_ts=max_ts, last_image_id=max_id)
    elif metrics.mode == "backfill":
        logger.info("[Load Backfill] Giữ nguyên mốc Watermark hiện tại (không ghi đè watermark bằng dữ liệu backfill).")


    logger.info(f"[Load] Hoàn tất nạp {len(clean_df):,} bản ghi ({load_time:.3f}s)")
    return True
