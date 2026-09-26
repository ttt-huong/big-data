import json
import glob
import os
import duckdb
import pandas as pd
from deltalake import DeltaTable

import config
from generator.data_generator import generate_metadata
from main_pipeline import run_pipeline
from pipeline.extract import read_chunks, get_watermark, save_watermark
from pipeline.transform import transform_data


def test_backfill_filter_only_records_before_specified_id(tmp_path):
    """1. Backfill filter selects only records where image_id < backfill_before_id."""
    source = tmp_path / "source.parquet"
    # IDs 1 to 20
    generate_metadata(20, seed=1).to_parquet(source, index=False)

    chunks = list(read_chunks(str(source), chunk_size=5, mode="backfill", backfill_before_id=12))
    all_extracted = pd.concat(chunks, ignore_index=True)

    assert len(all_extracted) == 11
    assert (all_extracted["image_id"] < 12).all()
    assert all_extracted["image_id"].max() == 11


def test_backfill_loads_clean_records_into_delta(tmp_path, monkeypatch):
    """2. Backfill extracts, validates, and loads clean records into Delta Lake."""
    target_table = str(tmp_path / "table")
    source_file = tmp_path / "backfill_source.parquet"
    monkeypatch.setattr(config, "WATERMARK_FILE", str(tmp_path / "watermark.json"))
    monkeypatch.setattr(config, "TARGET_DELTA_PATH", target_table)
    monkeypatch.setattr(config, "ERROR_DATA_DIR", str(tmp_path / "errors"))
    (tmp_path / "errors").mkdir()

    frame = generate_metadata(15, error_ratio=0.0, seed=10)
    frame.to_parquet(source_file, index=False)

    metrics = run_pipeline(str(source_file), mode="backfill", chunk_size=5, backfill_before_id=10)

    assert metrics.status == "SUCCESS"
    assert metrics.extracted_records == 9
    assert metrics.loaded_records == 9

    table_df = DeltaTable(target_table).to_pyarrow_table().to_pandas()
    assert len(table_df) == 9
    assert set(table_df["image_id"]) == set(range(1, 10))


def test_backfill_does_not_update_watermark(tmp_path, monkeypatch):
    """3. Backfill execution must NEVER advance or modify the watermark."""
    watermark_path = str(tmp_path / "watermark.json")
    monkeypatch.setattr(config, "WATERMARK_FILE", watermark_path)
    monkeypatch.setattr(config, "TARGET_DELTA_PATH", str(tmp_path / "table"))
    monkeypatch.setattr(config, "ERROR_DATA_DIR", str(tmp_path / "errors"))
    (tmp_path / "errors").mkdir()

    # Pre-set watermark to a higher ID, e.g. 5000
    save_watermark(5000)
    assert get_watermark()["last_image_id"] == 5000

    source_file = tmp_path / "historical_source.parquet"
    # Generate historical records starting at id 1
    frame = generate_metadata(20, error_ratio=0.0, seed=5, start_id=1)
    frame.to_parquet(source_file, index=False)

    # Run backfill for records before id 10
    metrics = run_pipeline(str(source_file), mode="backfill", chunk_size=5, backfill_before_id=10)
    assert metrics.status == "SUCCESS"

    # Watermark must remain strictly unchanged at 5000
    watermark_after = get_watermark()["last_image_id"]
    assert watermark_after == 5000


def test_backfill_rerun_does_not_duplicate_delta(tmp_path, monkeypatch):
    """4. Running the same backfill twice must not produce duplicate records in Delta table."""
    target_table = str(tmp_path / "table")
    source_file = tmp_path / "source.parquet"
    monkeypatch.setattr(config, "WATERMARK_FILE", str(tmp_path / "watermark.json"))
    monkeypatch.setattr(config, "TARGET_DELTA_PATH", target_table)
    monkeypatch.setattr(config, "ERROR_DATA_DIR", str(tmp_path / "errors"))
    (tmp_path / "errors").mkdir()

    frame = generate_metadata(25, error_ratio=0.0, seed=7)
    frame.to_parquet(source_file, index=False)

    # First run
    run1 = run_pipeline(str(source_file), mode="backfill", chunk_size=10, backfill_before_id=15)
    assert run1.status == "SUCCESS"
    assert run1.loaded_records == 14

    count_after_run1 = len(DeltaTable(target_table).to_pyarrow_table())
    assert count_after_run1 == 14

    # Second run (exact same input and mode)
    run2 = run_pipeline(str(source_file), mode="backfill", chunk_size=10, backfill_before_id=15)
    assert run2.status == "SUCCESS"

    count_after_run2 = len(DeltaTable(target_table).to_pyarrow_table())
    assert count_after_run2 == 14  # Idempotent: exactly 14 records, 0 duplicates


def test_dlq_rerun_does_not_duplicate_error_records(tmp_path, monkeypatch):
    """5. Rerunning backfill with rejected records does not duplicate rows in DLQ."""
    target_table = str(tmp_path / "table")
    source_file = tmp_path / "source_with_errors.parquet"
    error_dir = tmp_path / "errors"
    error_dir.mkdir()
    monkeypatch.setattr(config, "WATERMARK_FILE", str(tmp_path / "watermark.json"))
    monkeypatch.setattr(config, "TARGET_DELTA_PATH", target_table)
    monkeypatch.setattr(config, "ERROR_DATA_DIR", str(error_dir))

    # Generate records with deliberate errors
    frame = generate_metadata(30, error_ratio=0.3, seed=42)
    frame.to_parquet(source_file, index=False)

    # Run 1
    run1 = run_pipeline(str(source_file), mode="backfill", chunk_size=15, backfill_before_id=20)
    assert run1.status == "SUCCESS"
    assert run1.error_records > 0

    # Count errors via DuckDB
    con = duckdb.connect()
    pattern = os.path.join(str(error_dir), "*.parquet").replace("\\", "/")
    dlq_count_run1 = con.sql(f"SELECT COUNT(*) FROM read_parquet('{pattern}')").fetchone()[0]
    assert dlq_count_run1 == run1.error_records

    # Run 2 (Rerun identical backfill)
    run2 = run_pipeline(str(source_file), mode="backfill", chunk_size=15, backfill_before_id=20)
    assert run2.status == "SUCCESS"

    dlq_count_run2 = con.sql(f"SELECT COUNT(*) FROM read_parquet('{pattern}')").fetchone()[0]
    # Idempotent DLQ: total error records in DLQ must be identical to Run 1
    assert dlq_count_run2 == dlq_count_run1


def test_invalid_backfill_record_persisted_with_stable_identity(tmp_path):
    """6. Invalid backfill record is routed to DLQ with stable error_id and error_batch_id."""
    error_dir = tmp_path / "dlq"
    bad_data = pd.DataFrame({
        "image_id": [None, 999],
        "filename": ["bad_img.jpg", "invalid_cat.jpg"],
        "file_size": [-100, 5000],
        "width": [0, 800],
        "height": [0, 600],
        "format": ["invalid_fmt", "jpg"],
        "category": ["san_pham", "not_a_category"],
        "created_at": ["not-a-date", "2025-01-01"],
    })

    clean, errors = transform_data(bad_data, batch_id="test_batch_001", error_dir=str(error_dir))

    assert clean.empty
    assert len(errors) == 2
    assert "error_id" in errors.columns
    assert "error_batch_id" in errors.columns
    assert (errors["error_batch_id"] == "test_batch_001").all()

    # Verify file on disk
    expected_file = error_dir / "errors_test_batch_001.parquet"
    assert expected_file.exists()

    saved_df = pd.read_parquet(expected_file)
    assert len(saved_df) == 2
    assert "error_reason" in saved_df.columns
    assert set(saved_df["error_id"]) == set(errors["error_id"])
