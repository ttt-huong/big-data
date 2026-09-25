"""
Extract Layer cho ETL Pipeline.
Hỗ trợ Full Load và Incremental Load (dựa trên Watermark Timestamp/ID trong watermark.json).
"""

import os
import json
import time
from typing import Optional, Dict, Any
import pandas as pd

import config
from pipeline.logging_utils import logger, PipelineMetrics, retry_operation


def get_watermark() -> Dict[str, Any]:
    """Đọc thông tin watermark từ file config.WATERMARK_FILE."""
    if os.path.exists(config.WATERMARK_FILE):
        try:
            with open(config.WATERMARK_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Không thể đọc file watermark: {e}. Tạo watermark mới.")
    return {"last_watermark_ts": None, "last_image_id": 0}


def save_watermark(last_watermark_ts: Optional[str], last_image_id: int):
    """Lưu thông tin watermark mới sau khi extract/load thành công."""
    watermark_data = {
        "last_watermark_ts": str(last_watermark_ts) if last_watermark_ts else None,
        "last_image_id": int(last_image_id),
        "updated_at": str(pd.Timestamp.now())
    }
    with open(config.WATERMARK_FILE, "w", encoding="utf-8") as f:
        json.dump(watermark_data, f, indent=2)
    logger.info(f"[Extract] Đã cập nhật Watermark mới: last_image_id={last_image_id}, last_ts={last_watermark_ts}")


@retry_operation(max_retries=3, delay=0.5, backoff=2.0)
def extract_data(
    source_input: Any,
    mode: str = "full",
    metrics: Optional[PipelineMetrics] = None,
    batch_size: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> pd.DataFrame:
    """
    Trích xuất dữ liệu từ nguồn:
    - source_input: có thể là đường dẫn file (Parquet/CSV) hoặc trực tiếp là pd.DataFrame
    - mode: 'full' (đọc tất cả), 'incremental' (đọc từ watermark) hoặc 'backfill' (lọc theo dải ngày quá khứ)
    """
    t0 = time.time()
    
    # 1. Đọc dữ liệu thô
    if isinstance(source_input, str):
        if source_input.endswith(".parquet"):
            raw_df = pd.read_parquet(source_input)
        elif source_input.endswith(".csv"):
            raw_df = pd.read_csv(source_input)
        else:
            raise ValueError(f"Định dạng nguồn không hỗ trợ: {source_input}")
    elif isinstance(source_input, pd.DataFrame):
        raw_df = source_input.copy()
    else:
        raise TypeError(f"Nguồn dữ liệu không hợp lệ: {type(source_input)}")

    if raw_df.empty:
        if metrics:
            metrics.extract_time_s = round(time.time() - t0, 3)
        return raw_df

    # 2. Xử lý theo mode FULL, INCREMENTAL hoặc BACKFILL
    if mode == "incremental":
        watermark = get_watermark()
        last_id = watermark.get("last_image_id", 0)
        last_ts = watermark.get("last_watermark_ts")

        logger.info(f"[Extract] Chế độ INCREMENTAL LOAD. Watermark hiện tại: last_id={last_id}, last_ts={last_ts}")

        # Ưu tiên lọc theo last_image_id nếu có cột image_id
        if "image_id" in raw_df.columns and last_id > 0:
            extracted_df = raw_df[raw_df["image_id"] > last_id].copy()
        elif "created_at" in raw_df.columns and last_ts:
            extracted_df = raw_df[pd.to_datetime(raw_df["created_at"]) > pd.to_datetime(last_ts)].copy()
        else:
            logger.info("[Extract] Chưa có watermark cũ -> Chuyển sang đọc toàn bộ dữ liệu.")
            extracted_df = raw_df.copy()
    elif mode == "backfill":
        logger.info(f"[Extract] Chế độ BACKFILL LOAD. Lọc dữ liệu từ {start_date} đến {end_date}.")
        extracted_df = raw_df.copy()
        if "created_at" in extracted_df.columns:
            extracted_df["_dt"] = pd.to_datetime(extracted_df["created_at"], errors="coerce")
            if start_date:
                extracted_df = extracted_df[extracted_df["_dt"] >= pd.to_datetime(start_date)]
            if end_date:
                extracted_df = extracted_df[extracted_df["_dt"] <= pd.to_datetime(end_date)]
            extracted_df.drop(columns=["_dt"], inplace=True)
    else:
        logger.info(f"[Extract] Chế độ FULL LOAD. Đọc toàn bộ dataset ({len(raw_df):,} dòng).")
        extracted_df = raw_df.copy()

    # Nếu quy định batch_size -> Lấy batch đầu tiên
    if batch_size and len(extracted_df) > batch_size:
        logger.info(f"[Extract] Áp dụng batch_size={batch_size:,} trên tổng {len(extracted_df):,} bản ghi.")
        extracted_df = extracted_df.head(batch_size).copy()

    extract_time = time.time() - t0
    if metrics:
        metrics.extracted_records = len(extracted_df)
        metrics.extract_time_s = round(extract_time, 3)
        metrics.mode = mode

    logger.info(f"[Extract] Đã trích xuất {len(extracted_df):,} bản ghi ({extract_time:.3f}s)")
    return extracted_df


def _filter_by_mode(
    df: pd.DataFrame,
    mode: str = "full",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> pd.DataFrame:
    """Helper lọc dữ liệu theo mode incremental / backfill."""
    if df.empty:
        return df
    if mode == "incremental":
        watermark = get_watermark()
        last_id = watermark.get("last_image_id", 0)
        last_ts = watermark.get("last_watermark_ts")
        if "image_id" in df.columns and last_id > 0:
            return df[(df["image_id"].isna()) | (df["image_id"] > last_id)].copy()
        elif "created_at" in df.columns and last_ts:
            return df[pd.to_datetime(df["created_at"], errors="coerce") > pd.to_datetime(last_ts)].copy()
    elif mode == "backfill":
        if "created_at" in df.columns:
            extracted_df = df.copy()
            extracted_df["_dt"] = pd.to_datetime(extracted_df["created_at"], errors="coerce")
            if start_date:
                extracted_df = extracted_df[extracted_df["_dt"] >= pd.to_datetime(start_date)]
            if end_date:
                extracted_df = extracted_df[extracted_df["_dt"] <= pd.to_datetime(end_date)]
            extracted_df.drop(columns=["_dt"], inplace=True)
            return extracted_df
    return df


def extract_data_chunks(
    source_input: Any,
    chunk_size: int = 50_000,
    mode: str = "full",
    metrics: Optional[PipelineMetrics] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """
    Generator trích xuất dữ liệu theo từng Chunk để tối ưu hóa bộ nhớ RAM (True Streaming Reader).
    Đối với file/thư mục Parquet/CSV, đọc dạng Stream từng batch thay vì load toàn bộ dataset vào RAM.
    """
    t0 = time.time()
    total_yielded = 0

    if isinstance(source_input, str) and os.path.exists(source_input):
        if source_input.endswith(".parquet"):
            import pyarrow.dataset as ds
            dataset = ds.dataset(source_input)
            for batch in dataset.to_batches(batch_size=chunk_size):
                chunk_df = batch.to_pandas()
                filtered_chunk = _filter_by_mode(chunk_df, mode=mode, start_date=start_date, end_date=end_date)
                if not filtered_chunk.empty:
                    total_yielded += len(filtered_chunk)
                    yield filtered_chunk
        elif source_input.endswith(".csv"):
            for chunk_df in pd.read_csv(source_input, chunksize=chunk_size):
                filtered_chunk = _filter_by_mode(chunk_df, mode=mode, start_date=start_date, end_date=end_date)
                if not filtered_chunk.empty:
                    total_yielded += len(filtered_chunk)
                    yield filtered_chunk
        else:
            # Nếu là đường dẫn tới thư mục dataset pyarrow hỗ trợ
            import pyarrow.dataset as ds
            try:
                dataset = ds.dataset(source_input)
                for batch in dataset.to_batches(batch_size=chunk_size):
                    chunk_df = batch.to_pandas()
                    filtered_chunk = _filter_by_mode(chunk_df, mode=mode, start_date=start_date, end_date=end_date)
                    if not filtered_chunk.empty:
                        total_yielded += len(filtered_chunk)
                        yield filtered_chunk
            except Exception:
                full_df = extract_data(source_input, mode=mode, metrics=metrics, start_date=start_date, end_date=end_date)
                for i in range(0, len(full_df), chunk_size):
                    chunk = full_df.iloc[i : i + chunk_size].copy()
                    total_yielded += len(chunk)
                    yield chunk
    else:
        full_df = extract_data(source_input, mode=mode, metrics=metrics, start_date=start_date, end_date=end_date)
        if full_df.empty:
            yield full_df
        else:
            for i in range(0, len(full_df), chunk_size):
                chunk = full_df.iloc[i : i + chunk_size].copy()
                total_yielded += len(chunk)
                yield chunk

    if metrics:
        metrics.extracted_records = total_yielded
        metrics.extract_time_s = round(time.time() - t0, 3)


