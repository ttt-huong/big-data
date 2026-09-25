"""
Unit tests cho pipeline.load module (Idempotency & Delta Lake MERGE).
"""

import os
import pytest
import pandas as pd
from deltalake import DeltaTable

from pipeline.load import load_data
from pipeline.logging_utils import PipelineMetrics


def test_load_data_idempotent_merge(tmp_path):
    """Kiểm tra load_data sử dụng MERGE INTO để tránh lặp dữ liệu khi nạp trùng."""
    target_dir = str(tmp_path / "delta_test_table")
    
    # 1. Lần nạp 1: Nạp 2 bản ghi
    df_batch1 = pd.DataFrame([
        {
            "image_id": 1,
            "filename": "img_1.jpg",
            "file_size": 100,
            "category": "san_pham",
            "created_at": pd.Timestamp("2024-01-01 10:00:00")
        },
        {
            "image_id": 2,
            "filename": "img_2.jpg",
            "file_size": 200,
            "category": "phong_canh",
            "created_at": pd.Timestamp("2024-01-01 11:00:00")
        }
    ])

    metrics1 = PipelineMetrics(mode="full")
    success1 = load_data(df_batch1, metrics=metrics1, target_uri=target_dir, write_mode="merge")
    assert success1 is True

    dt1 = DeltaTable(target_dir)
    assert len(dt1.to_pyarrow_table()) == 2

    # 2. Lần nạp 2: Nạp lại ID 1 (cập nhật file_size thành 999) + thêm ID 3 mới
    df_batch2 = pd.DataFrame([
        {
            "image_id": 1,
            "filename": "img_1_updated.jpg",
            "file_size": 999,
            "category": "san_pham",
            "created_at": pd.Timestamp("2024-01-01 10:00:00")
        },
        {
            "image_id": 3,
            "filename": "img_3.jpg",
            "file_size": 300,
            "category": "do_an",
            "created_at": pd.Timestamp("2024-01-01 12:00:00")
        }
    ])

    metrics2 = PipelineMetrics(mode="incremental")
    success2 = load_data(df_batch2, metrics=metrics2, target_uri=target_dir, write_mode="merge")
    assert success2 is True

    dt2 = DeltaTable(target_dir)
    res_df = dt2.to_pyarrow_table().to_pandas()
    
    # Tổng số bản ghi phải là 3 (không bị trùng ID 1 thành 4 bản ghi)
    assert len(res_df) == 3
    assert set(res_df["image_id"].tolist()) == {1, 2, 3}

    # Kiểm tra ID 1 đã được UPDATE file_size = 999
    row_1 = res_df[res_df["image_id"] == 1].iloc[0]
    assert row_1["file_size"] == 999
