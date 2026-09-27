import glob
import json
import os
import duckdb
import pandas as pd
import pytest
from deltalake import DeltaTable

import config
from main_pipeline import run_pipeline
from pipeline.extract import get_watermark, save_watermark
from pipeline.transform import compute_is_late, transform_data


def test_1_different_day_is_late_true():
    """TEST 1 — Different day:
    created_at: 2026-09-20, source_arrived_at: 2026-09-23 -> is_late = True"""
    df = pd.DataFrame({
        "image_id": [1],
        "created_at": ["2026-09-20"],
        "source_arrived_at": ["2026-09-23"],
    })
    result = compute_is_late(df)
    assert bool(result.iloc[0]) is True


def test_2_same_day_is_late_false():
    """TEST 2 — Same day:
    created_at: 2026-09-23, source_arrived_at: 2026-09-23 -> is_late = False"""
    df = pd.DataFrame({
        "image_id": [1],
        "created_at": ["2026-09-23"],
        "source_arrived_at": ["2026-09-23"],
    })
    result = compute_is_late(df)
    assert bool(result.iloc[0]) is False


def test_3_same_day_different_hour_is_late_false():
    """TEST 3 — Same day, different hour:
    created_at: 2026-09-23 08:00, source_arrived_at: 2026-09-23 18:00 -> is_late = False"""
    df = pd.DataFrame({
        "image_id": [1],
        "created_at": ["2026-09-23 08:00:00"],
        "source_arrived_at": ["2026-09-23 18:00:00"],
    })
    result = compute_is_late(df)
    assert bool(result.iloc[0]) is False


def test_4_5_6_7_mixed_batch_late_record_monotonic_watermark_and_rerun(tmp_path, monkeypatch):
    """TEST 4, 5, 6, 7 — Comprehensive E2E test:
    - Mixed batch with Normal (105) and Late (95) records
    - Watermark advances from 102 to 105 (never decreases)
    - Late record 95 is not dropped and exists in Delta with is_late=True
    - Incremental rerun skips processed source without duplicates
    """
    watermark_file = str(tmp_path / "watermark.json")
    target_table = str(tmp_path / "delta_table")
    error_dir = str(tmp_path / "errors")
    os.makedirs(error_dir, exist_ok=True)

    monkeypatch.setattr(config, "WATERMARK_FILE", watermark_file)
    monkeypatch.setattr(config, "TARGET_DELTA_PATH", target_table)
    monkeypatch.setattr(config, "ERROR_DATA_DIR", error_dir)

    # 1. Batch 1: IDs 100, 101, 102 (created & arrived on 2026-09-20)
    batch1_file = tmp_path / "batch_1.parquet"
    batch1_df = pd.DataFrame({
        "image_id": [100, 101, 102],
        "filename": ["img_0000100.jpg", "img_0000101.jpg", "img_0000102.jpg"],
        "file_size": [100000, 200000, 300000],
        "width": [512, 512, 512],
        "height": [512, 512, 512],
        "format": ["jpg", "jpg", "jpg"],
        "category": ["san_pham", "do_an", "chan_dung"],
        "created_at": ["2026-09-20 10:00:00", "2026-09-20 11:00:00", "2026-09-20 12:00:00"],
        "source_arrived_at": ["2026-09-20 12:00:00", "2026-09-20 12:00:00", "2026-09-20 12:00:00"],
    })
    batch1_df.to_parquet(batch1_file, index=False)

    m1 = run_pipeline(str(batch1_file), mode="full", chunk_size=10)
    assert m1.status == "SUCCESS"
    assert m1.loaded_records == 3

    # Check watermark after Batch 1
    state1 = get_watermark()
    assert state1["last_image_id"] == 102
    assert "batch_1.parquet" in state1["processed_sources"]

    # 2. Batch 2:
    # ID 105: normal (created 2026-09-23, arrived 2026-09-23)
    # ID 95: late (created 2026-09-20, arrived 2026-09-23)
    batch2_file = tmp_path / "batch_2.parquet"
    batch2_df = pd.DataFrame({
        "image_id": [105, 95],
        "filename": ["img_0000105.jpg", "img_0000095.jpg"],
        "file_size": [150000, 250000],
        "width": [256, 1024],
        "height": [256, 1024],
        "format": ["jpg", "png"],
        "category": ["kien_truc", "phong_canh"],
        "created_at": ["2026-09-23 09:00:00", "2026-09-20 08:00:00"],
        "source_arrived_at": ["2026-09-23 15:00:00", "2026-09-23 15:00:00"],
    })
    batch2_df.to_parquet(batch2_file, index=False)

    # Run Incremental
    m2 = run_pipeline(str(batch2_file), mode="incremental", chunk_size=10)
    assert m2.status == "SUCCESS"
    assert m2.loaded_records == 2
    assert m2.late_records == 1

    # Check watermark after Batch 2 (TEST 4 & TEST 6: 102 -> 105, never decreases to 95)
    state2 = get_watermark()
    assert state2["last_image_id"] == 105
    assert set(state2["processed_sources"]) == {"batch_1.parquet", "batch_2.parquet"}

    # Verify records in Delta Table (TEST 5: late record 95 is loaded, not dropped)
    dt = DeltaTable(target_table).to_pyarrow_table().to_pandas()
    assert len(dt) == 5
    assert set(dt["image_id"]) == {100, 101, 102, 105, 95}

    row_105 = dt[dt["image_id"] == 105].iloc[0]
    assert bool(row_105["is_late"]) is False

    row_95 = dt[dt["image_id"] == 95].iloc[0]
    assert bool(row_95["is_late"]) is True

    # TEST 7 — Incremental rerun
    m2_rerun = run_pipeline(str(batch2_file), mode="incremental", chunk_size=10)
    assert m2_rerun.status == "SUCCESS"
    assert m2_rerun.extracted_records == 0
    assert m2_rerun.loaded_records == 0

    # Ensure no duplicates in Delta or state
    dt_after = DeltaTable(target_table).to_pyarrow_table().to_pandas()
    assert len(dt_after) == 5
    assert get_watermark()["last_image_id"] == 105


def test_6_watermark_monotonicity_with_only_late_records(tmp_path, monkeypatch):
    """TEST 6 — Batch containing ONLY late-arriving records with IDs < watermark:
    Watermark must remain strictly unchanged and not rollback."""
    watermark_file = str(tmp_path / "watermark.json")
    target_table = str(tmp_path / "delta_table")
    error_dir = str(tmp_path / "errors")
    os.makedirs(error_dir, exist_ok=True)

    monkeypatch.setattr(config, "WATERMARK_FILE", watermark_file)
    monkeypatch.setattr(config, "TARGET_DELTA_PATH", target_table)
    monkeypatch.setattr(config, "ERROR_DATA_DIR", error_dir)

    save_watermark(500, processed_sources=["initial.parquet"])

    late_only_file = tmp_path / "batch_late_only.parquet"
    late_only_df = pd.DataFrame({
        "image_id": [50, 60],
        "filename": ["img_0000050.jpg", "img_0000060.jpg"],
        "file_size": [100000, 200000],
        "width": [512, 512],
        "height": [512, 512],
        "format": ["jpg", "jpg"],
        "category": ["san_pham", "do_an"],
        "created_at": ["2026-09-10 10:00:00", "2026-09-11 10:00:00"],
        "source_arrived_at": ["2026-09-24 10:00:00", "2026-09-24 10:00:00"],
    })
    late_only_df.to_parquet(late_only_file, index=False)

    m = run_pipeline(str(late_only_file), mode="incremental", chunk_size=10)
    assert m.status == "SUCCESS"
    assert m.loaded_records == 2

    # Watermark must still be 500
    state = get_watermark()
    assert state["last_image_id"] == 500
    assert "batch_late_only.parquet" in state["processed_sources"]


def test_8_invalid_late_record_routed_to_dlq(tmp_path, monkeypatch):
    """TEST 8 — Invalid late record:
    Create a new source batch containing a late record with validation error.
    Expected:
    - is_late = True
    - record goes to DLQ
    - deterministic error identity
    - rerun does not duplicate DLQ
    """
    target_table = str(tmp_path / "delta_table")
    error_dir = str(tmp_path / "errors")
    watermark_file = str(tmp_path / "watermark.json")
    os.makedirs(error_dir, exist_ok=True)

    monkeypatch.setattr(config, "WATERMARK_FILE", watermark_file)
    monkeypatch.setattr(config, "TARGET_DELTA_PATH", target_table)
    monkeypatch.setattr(config, "ERROR_DATA_DIR", error_dir)

    invalid_late_file = tmp_path / "batch_invalid_late.parquet"
    invalid_late_df = pd.DataFrame({
        "image_id": [85],
        "filename": ["img_0000085.jpg"],
        "file_size": [-500],  # Invalid file size -> goes to DLQ
        "width": [512],
        "height": [512],
        "format": ["jpg"],
        "category": ["san_pham"],
        "created_at": ["2026-09-20 10:00:00"],
        "source_arrived_at": ["2026-09-24 10:00:00"],
    })
    invalid_late_df.to_parquet(invalid_late_file, index=False)

    # First run
    m1 = run_pipeline(str(invalid_late_file), mode="incremental", chunk_size=10)
    assert m1.status == "SUCCESS"
    assert m1.error_records == 1
    assert m1.loaded_records == 0

    # Query DLQ with DuckDB
    con = duckdb.connect()
    pattern = os.path.join(error_dir, "*.parquet").replace("\\", "/")
    dlq_df = con.sql(f"SELECT * FROM read_parquet('{pattern}')").df()
    assert len(dlq_df) == 1
    assert dlq_df["image_id"].iloc[0] == 85
    assert bool(dlq_df["is_late"].iloc[0]) is True
    assert "INVALID_FILE_SIZE" in dlq_df["error_reason"].iloc[0]
    initial_error_id = dlq_df["error_id"].iloc[0]

    # Rerun with fresh state to force re-processing of identical batch
    save_watermark(0, processed_sources=[])
    m2 = run_pipeline(str(invalid_late_file), mode="incremental", chunk_size=10)
    assert m2.status == "SUCCESS"

    dlq_df_rerun = con.sql(f"SELECT * FROM read_parquet('{pattern}')").df()
    assert len(dlq_df_rerun) == 1  # DLQ is completely idempotent! No duplicates!
    assert dlq_df_rerun["error_id"].iloc[0] == initial_error_id
