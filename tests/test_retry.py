import pytest
import pandas as pd
from deltalake import DeltaTable
from unittest.mock import MagicMock

import config
from generator.data_generator import generate_metadata
from main_pipeline import run_pipeline
from pipeline.load import load_data, RETRYABLE_EXCEPTIONS, _write_to_delta


def test_retry_scenario_a_transient_failure_then_success(tmp_path, monkeypatch):
    """Scenario A: Attempt 1 fails with transient OSError, Attempt 2 succeeds.
    Pipeline retries, finishes with SUCCESS, loads all data without duplicates."""
    target_table = str(tmp_path / "table")
    source_file = tmp_path / "source.parquet"
    monkeypatch.setattr(config, "WATERMARK_FILE", str(tmp_path / "watermark.json"))
    monkeypatch.setattr(config, "TARGET_DELTA_PATH", target_table)
    monkeypatch.setattr(config, "ERROR_DATA_DIR", str(tmp_path / "errors"))
    (tmp_path / "errors").mkdir()

    # Generate 10 clean records
    frame = generate_metadata(10, error_ratio=0.0, seed=1)
    frame.to_parquet(source_file, index=False)

    call_count = 0
    original_write = _write_to_delta

    def flaky_write(clean, target_path):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise OSError("Simulated temporary I/O or lock failure")
        return original_write(clean, target_path)

    monkeypatch.setattr("pipeline.load._write_to_delta", flaky_write)

    metrics = run_pipeline(
        str(source_file),
        mode="full",
        chunk_size=10,
        max_attempts=3,
        retry_delay=0.01,
    )

    assert metrics.status == "SUCCESS"
    assert metrics.retry_count == 1
    assert call_count == 2
    assert metrics.loaded_records == 10

    # Verify Delta table has exactly 10 records and no duplicates
    result_df = DeltaTable(target_table).to_pyarrow_table().to_pandas()
    assert len(result_df) == 10
    assert result_df["image_id"].nunique() == 10


def test_retry_scenario_b_exhausted_attempts_raises_failure(tmp_path, monkeypatch):
    """Scenario B: All attempts fail with transient error.
    Pipeline stops with failure, raises exception, records FAILED status."""
    target_table = str(tmp_path / "table")
    source_file = tmp_path / "source.parquet"
    monkeypatch.setattr(config, "WATERMARK_FILE", str(tmp_path / "watermark.json"))
    monkeypatch.setattr(config, "TARGET_DELTA_PATH", target_table)
    monkeypatch.setattr(config, "ERROR_DATA_DIR", str(tmp_path / "errors"))
    (tmp_path / "errors").mkdir()

    frame = generate_metadata(10, error_ratio=0.0, seed=2)
    frame.to_parquet(source_file, index=False)

    call_count = 0

    def persistent_failure(clean, target_path):
        nonlocal call_count
        call_count += 1
        raise OSError(f"Simulated persistent I/O failure on attempt {call_count}")

    monkeypatch.setattr("pipeline.load._write_to_delta", persistent_failure)

    with pytest.raises(OSError) as exc_info:
        run_pipeline(
            str(source_file),
            mode="full",
            chunk_size=10,
            max_attempts=3,
            retry_delay=0.01,
        )

    assert "Simulated persistent I/O failure on attempt 3" in str(exc_info.value)
    assert call_count == 3


def test_retry_non_retryable_exception_fails_immediately(tmp_path, monkeypatch):
    """Non-transient exceptions (e.g. ValueError) should not be retried indiscriminately."""
    target = str(tmp_path / "table")
    clean = pd.DataFrame({"image_id": [1], "category": ["san_pham"]})

    call_count = 0

    def fail_with_value_error(clean, target_path):
        nonlocal call_count
        call_count += 1
        raise ValueError("Invalid schema or corrupted logic")

    monkeypatch.setattr("pipeline.load._write_to_delta", fail_with_value_error)

    with pytest.raises(ValueError):
        load_data(clean, target_path=target, max_attempts=3, retry_delay=0.01)

    # Must fail immediately on attempt 1 without retrying
    assert call_count == 1


def test_retry_merge_idempotency_preserves_integrity(tmp_path, monkeypatch):
    """Verify that retrying a Delta MERGE operation maintains idempotency and data integrity."""
    target = str(tmp_path / "table")
    first_batch = pd.DataFrame({
        "image_id": [1, 2],
        "category": ["san_pham", "do_an"],
        "format": ["jpg", "png"],
        "created_at": pd.to_datetime(["2025-01-01", "2025-01-02"]),
        "year": [2025, 2025],
        "month": [1, 1],
        "file_size": [1000, 2000],
        "width": [800, 1024],
        "height": [600, 768],
    })
    # Initial load succeeds
    assert load_data(first_batch, target) == 2

    # Second batch updates image_id 2 and inserts image_id 3
    second_batch = pd.DataFrame({
        "image_id": [2, 3],
        "category": ["do_an", "phong_canh"],
        "format": ["png", "webp"],
        "created_at": pd.to_datetime(["2025-01-02", "2025-01-03"]),
        "year": [2025, 2025],
        "month": [1, 1],
        "file_size": [2500, 3000],
        "width": [1024, 1920],
        "height": [768, 1080],
    })

    call_count = 0
    original_write = _write_to_delta

    def flaky_merge(clean, target_path):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise OSError("Lock conflict during Delta MERGE")
        return original_write(clean, target_path)

    monkeypatch.setattr("pipeline.load._write_to_delta", flaky_merge)

    retries_recorded = []
    loaded = load_data(
        second_batch,
        target_path=target,
        max_attempts=3,
        retry_delay=0.01,
        on_retry=lambda attempt, exc: retries_recorded.append(attempt),
    )

    assert loaded == 2
    assert retries_recorded == [1]
    assert call_count == 2

    # Verify final Delta table has exactly 3 records, image_id 2 is updated, image_id 3 is inserted
    df = DeltaTable(target).to_pyarrow_table().to_pandas()
    assert len(df) == 3
    assert set(df["image_id"]) == {1, 2, 3}
    assert df.loc[df["image_id"] == 2, "file_size"].iloc[0] == 2500
