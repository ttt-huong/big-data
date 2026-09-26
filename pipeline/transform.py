"""Enrich valid metadata and persist rejected records in the local DLQ."""

import os
import uuid

import pandas as pd

import config
from pipeline.validation import validate_and_clean


def transform_data(frame: pd.DataFrame, seen_ids: set[int] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    clean, errors = validate_and_clean(frame, seen_ids=seen_ids)
    if not clean.empty:
        clean = clean.copy()
        clean["size_mb"] = (clean["file_size"] / (1024**2)).round(3)
        clean["aspect_ratio"] = (clean["width"] / clean["height"]).round(3)
        clean["ingested_at"] = pd.Timestamp.now()
    if not errors.empty:
        errors = errors.copy()
        batch_id = str(uuid.uuid4())
        errors["error_batch_id"] = batch_id
        errors["logged_at"] = pd.Timestamp.now()
        errors.to_parquet(os.path.join(config.ERROR_DATA_DIR, f"errors_{batch_id}.parquet"), index=False)
    return clean, errors
