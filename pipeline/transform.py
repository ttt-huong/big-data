"""Enrich valid metadata and persist rejected records in the local DLQ."""

import hashlib
import os

import pandas as pd

import config
from pipeline.validation import validate_and_clean


def _generate_error_id(row: pd.Series) -> str:
    image_id_val = str(row.get("image_id", ""))
    filename_val = str(row.get("filename", ""))
    reason_val = str(row.get("error_reason", ""))
    created_at_val = str(row.get("created_at", ""))
    raw_key = f"{image_id_val}|{filename_val}|{reason_val}|{created_at_val}"
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:16]


def _generate_error_batch_id(errors: pd.DataFrame) -> str:
    if "error_id" in errors.columns:
        keys = errors["error_id"].astype(str).tolist()
    else:
        keys = [
            f"{r.get('image_id')}_{r.get('filename')}_{r.get('error_reason')}"
            for _, r in errors.iterrows()
        ]
    content_sig = hashlib.sha256("".join(sorted(keys)).encode("utf-8")).hexdigest()[:16]
    return f"batch_{content_sig}"


def compute_is_late(frame: pd.DataFrame) -> pd.Series:
    if frame.empty or "created_at" not in frame.columns:
        return pd.Series(False, index=frame.index, dtype=bool)

    created_dt = pd.to_datetime(frame["created_at"], format="mixed", errors="coerce")
    if "source_arrived_at" in frame.columns:
        arrived_dt = pd.to_datetime(frame["source_arrived_at"], format="mixed", errors="coerce")
    else:
        arrived_dt = created_dt

    valid_mask = created_dt.notna() & arrived_dt.notna()
    is_late_series = pd.Series(False, index=frame.index, dtype=bool)
    if valid_mask.any():
        is_late_series[valid_mask] = created_dt[valid_mask].dt.date < arrived_dt[valid_mask].dt.date
    return is_late_series


def transform_data(
    frame: pd.DataFrame,
    seen_ids: set[int] | None = None,
    batch_id: str | None = None,
    error_dir: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if frame.empty:
        return frame.copy(), pd.DataFrame(columns=[*frame.columns, "error_reason"])

    working_frame = frame.copy()
    if "is_late" not in working_frame.columns:
        working_frame["is_late"] = compute_is_late(working_frame)
    if "source_arrived_at" not in working_frame.columns and "created_at" in working_frame.columns:
        working_frame["source_arrived_at"] = working_frame["created_at"]

    clean, errors = validate_and_clean(working_frame, seen_ids=seen_ids)
    if not clean.empty:
        clean = clean.copy()
        clean["size_mb"] = (clean["file_size"] / (1024**2)).round(3)
        clean["aspect_ratio"] = (clean["width"] / clean["height"]).round(3)
        clean["ingested_at"] = pd.Timestamp.now()
        clean["is_late"] = clean["is_late"].astype(bool)
    if not errors.empty:
        errors = errors.copy()
        errors["error_id"] = errors.apply(_generate_error_id, axis=1)
        resolved_batch_id = batch_id or _generate_error_batch_id(errors)
        errors["error_batch_id"] = resolved_batch_id
        errors["logged_at"] = pd.Timestamp.now().floor("s")
        errors["is_late"] = errors["is_late"].astype(bool)
        target_dir = error_dir or config.ERROR_DATA_DIR
        os.makedirs(target_dir, exist_ok=True)
        errors.to_parquet(os.path.join(target_dir, f"errors_{resolved_batch_id}.parquet"), index=False)
    return clean, errors
