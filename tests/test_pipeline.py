import json

import pandas as pd
from deltalake import DeltaTable

import config
from generator.data_generator import generate_metadata
from main_pipeline import run_pipeline
from pipeline.extract import read_chunks
from pipeline.load import load_data
from pipeline.logging_utils import PipelineMetrics


def test_chunk_reader_returns_all_rows(tmp_path):
    source = tmp_path / "source.parquet"
    generate_metadata(105, seed=2).to_parquet(source, index=False)
    chunks = list(read_chunks(str(source), chunk_size=50))
    assert [len(chunk) for chunk in chunks] == [50, 50, 5]


def test_backfill_reads_records_before_id(tmp_path):
    source = tmp_path / "source.parquet"
    generate_metadata(5, seed=2).to_parquet(source, index=False)
    chunks = list(read_chunks(str(source), chunk_size=10, mode="backfill", backfill_before_id=4))
    assert sum(len(chunk) for chunk in chunks) == 3


def test_delta_upsert_is_idempotent(tmp_path):
    target = str(tmp_path / "table")
    first = pd.DataFrame({"image_id": [1, 2], "value": [10, 20], "category": ["san_pham", "do_an"]})
    second = pd.DataFrame({"image_id": [1, 3], "value": [99, 30], "category": ["san_pham", "chan_dung"]})
    assert load_data(first, target) == 2
    assert load_data(second, target) == 2
    assert load_data(second, target) == 2
    result = DeltaTable(target).to_pyarrow_table().to_pandas()
    assert len(result) == 3
    assert result.loc[result["image_id"] == 1, "value"].iloc[0] == 99


def test_full_then_incremental_updates_watermark(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "WATERMARK_FILE", str(tmp_path / "watermark.json"))
    monkeypatch.setattr(config, "TARGET_DELTA_PATH", str(tmp_path / "table"))
    monkeypatch.setattr(config, "ERROR_DATA_DIR", str(tmp_path / "errors"))
    config.ERROR_DATA_DIR = str(tmp_path / "errors")
    config.ERROR_DATA_DIR and (tmp_path / "errors").mkdir()
    full_source = tmp_path / "full.parquet"
    incremental_source = tmp_path / "incremental.parquet"
    generate_metadata(10, seed=3).to_parquet(full_source, index=False)
    generate_metadata(3, seed=4, start_id=11).to_parquet(incremental_source, index=False)

    full = run_pipeline(str(full_source), mode="full", chunk_size=4)
    incremental = run_pipeline(str(incremental_source), mode="incremental", chunk_size=2)
    assert full.status == "SUCCESS"
    assert incremental.status == "SUCCESS"
    assert json.loads((tmp_path / "watermark.json").read_text())["last_image_id"] == 13
    assert len(DeltaTable(str(tmp_path / "table")).to_pyarrow_table()) == 13
