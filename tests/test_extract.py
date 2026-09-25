"""
Unit tests cho pipeline.extract module.
"""

import os
import json
import pytest
import pandas as pd

import config
from pipeline.extract import extract_data, extract_data_chunks, get_watermark, save_watermark
from pipeline.logging_utils import PipelineMetrics


def test_get_and_save_watermark(tmp_path):
    """Kiểm tra đọc và ghi file watermark."""
    test_wm_file = tmp_path / "watermark.json"
    original_wm = config.WATERMARK_FILE
    config.WATERMARK_FILE = str(test_wm_file)

    try:
        # Khi chưa có file
        wm = get_watermark()
        assert wm == {"last_watermark_ts": None, "last_image_id": 0}

        # Lưu watermark mới
        save_watermark(last_watermark_ts="2024-05-01 00:00:00", last_image_id=100)

        # Đọc lại
        wm_updated = get_watermark()
        assert wm_updated["last_image_id"] == 100
        assert wm_updated["last_watermark_ts"] == "2024-05-01 00:00:00"
    finally:
        config.WATERMARK_FILE = original_wm


def test_extract_data_full():
    """Kiểm tra trích xuất ở chế độ FULL LOAD."""
    metrics = PipelineMetrics(mode="full")
    df = pd.DataFrame([{"image_id": 1}, {"image_id": 2}, {"image_id": 3}])
    
    extracted_df = extract_data(df, mode="full", metrics=metrics)
    assert len(extracted_df) == 3
    assert metrics.extracted_records == 3


def test_extract_data_incremental(tmp_path):
    """Kiểm tra trích xuất ở chế độ INCREMENTAL LOAD dựa trên watermark."""
    test_wm_file = tmp_path / "watermark.json"
    original_wm = config.WATERMARK_FILE
    config.WATERMARK_FILE = str(test_wm_file)

    try:
        save_watermark(last_watermark_ts=None, last_image_id=5)

        metrics = PipelineMetrics(mode="incremental")
        df = pd.DataFrame([
            {"image_id": 4},
            {"image_id": 5},
            {"image_id": 6},
            {"image_id": 7}
        ])

        # Chỉ các bản ghi có image_id > 5 mới được trích xuất
        extracted_df = extract_data(df, mode="incremental", metrics=metrics)
        assert len(extracted_df) == 2
        assert extracted_df["image_id"].tolist() == [6, 7]
    finally:
        config.WATERMARK_FILE = original_wm


def test_extract_data_chunks():
    """Kiểm tra generator extract_data_chunks chia đều dữ liệu thành các batch nhỏ."""
    df = pd.DataFrame([{"image_id": i} for i in range(1, 105)])  # 104 bản ghi
    chunks = list(extract_data_chunks(df, chunk_size=50))
    
    assert len(chunks) == 3  # 50 + 50 + 4
    assert len(chunks[0]) == 50
    assert len(chunks[1]) == 50
    assert len(chunks[2]) == 4


def test_extract_data_chunks_file_streaming(tmp_path):
    """Kiểm tra extract_data_chunks đọc stream thực sự từ file Parquet và CSV."""
    df = pd.DataFrame([{"image_id": i, "val": f"v_{i}"} for i in range(1, 105)])
    parquet_file = str(tmp_path / "stream_test.parquet")
    csv_file = str(tmp_path / "stream_test.csv")
    df.to_parquet(parquet_file, index=False)
    df.to_csv(csv_file, index=False)

    # Parquet streaming
    p_chunks = list(extract_data_chunks(parquet_file, chunk_size=40))
    assert len(p_chunks) == 3  # 40 + 40 + 24
    assert len(p_chunks[0]) == 40
    assert len(p_chunks[2]) == 24

    # CSV streaming
    c_chunks = list(extract_data_chunks(csv_file, chunk_size=40))
    assert len(c_chunks) == 3
    assert len(c_chunks[0]) == 40
    assert len(c_chunks[2]) == 24



def test_extract_data_backfill():
    """Kiểm tra trích xuất ở chế độ BACKFILL LOAD theo dải ngày."""
    metrics = PipelineMetrics(mode="backfill")
    df = pd.DataFrame([
        {"image_id": 1, "created_at": "2026-01-10 10:00:00"},
        {"image_id": 2, "created_at": "2026-01-20 10:00:00"},
        {"image_id": 3, "created_at": "2026-02-05 10:00:00"}
    ])

    extracted_df = extract_data(df, mode="backfill", start_date="2026-01-01", end_date="2026-01-31", metrics=metrics)
    assert len(extracted_df) == 2
    assert extracted_df["image_id"].tolist() == [1, 2]


