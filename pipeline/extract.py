"""Read local CSV/Parquet sources with a stable per-run watermark."""

import json
import os
from typing import Any, Dict, Iterator, Optional

import pandas as pd

import config


def get_watermark() -> Dict[str, Any]:
    if not os.path.exists(config.WATERMARK_FILE):
        return {"last_image_id": 0}
    with open(config.WATERMARK_FILE, "r", encoding="utf-8") as stream:
        return json.load(stream)


def save_watermark(last_image_id: int) -> None:
    temporary_path = f"{config.WATERMARK_FILE}.tmp"
    with open(temporary_path, "w", encoding="utf-8") as stream:
        json.dump({"last_image_id": int(last_image_id)}, stream, indent=2)
    os.replace(temporary_path, config.WATERMARK_FILE)


def _filter_frame(frame: pd.DataFrame, mode: str, watermark: int, backfill_before_id: Optional[int]) -> pd.DataFrame:
    if mode == "incremental":
        return frame[frame["image_id"] > watermark].copy()
    if mode == "backfill" and backfill_before_id is not None:
        return frame[frame["image_id"] < backfill_before_id].copy()
    return frame.copy()


def read_chunks(
    source_path: str,
    chunk_size: int = 25_000,
    mode: str = "full",
    watermark: int = 0,
    backfill_before_id: Optional[int] = None,
) -> Iterator[pd.DataFrame]:
    """Yield source chunks while keeping the run's watermark constant."""
    if source_path.endswith(".csv"):
        chunks = pd.read_csv(source_path, chunksize=chunk_size)
    elif source_path.endswith(".parquet"):
        import pyarrow.dataset as dataset
        chunks = (batch.to_pandas() for batch in dataset.dataset(source_path).to_batches(batch_size=chunk_size))
    else:
        raise ValueError(f"Unsupported source format: {source_path}")

    for frame in chunks:
        filtered = _filter_frame(frame, mode, watermark, backfill_before_id)
        if not filtered.empty:
            yield filtered


def extract_data(source_input: Any, mode: str = "full", watermark: int = 0) -> pd.DataFrame:
    """Small compatibility helper for DataFrame or file inputs."""
    if isinstance(source_input, pd.DataFrame):
        frame = source_input.copy()
    elif isinstance(source_input, str):
        if source_input.endswith(".csv"):
            frame = pd.read_csv(source_input)
        elif source_input.endswith(".parquet"):
            frame = pd.read_parquet(source_input)
        else:
            raise ValueError(f"Unsupported source format: {source_input}")
    else:
        raise TypeError(f"Unsupported source input: {type(source_input)}")
    return _filter_frame(frame, mode, watermark, None)
