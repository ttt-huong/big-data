"""Data quality rules for metadata records."""

import numpy as np
import pandas as pd

import config


def validate_and_clean(frame: pd.DataFrame, seen_ids: set[int] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    if frame.empty:
        return frame.copy(), pd.DataFrame(columns=[*frame.columns, "error_reason"])

    data = frame.copy()
    reasons = pd.Series("", index=data.index, dtype="string")
    reasons[data["image_id"].isna()] += "NULL_IMAGE_ID;"
    reasons[data["file_size"].fillna(0) <= 0] += "INVALID_FILE_SIZE;"
    reasons[data["width"].fillna(0) <= 0] += "INVALID_WIDTH;"
    reasons[data["height"].fillna(0) <= 0] += "INVALID_HEIGHT;"
    reasons[~data["category"].isin(config.CATEGORIES)] += "INVALID_CATEGORY;"
    reasons[~data["format"].isin(config.FORMATS)] += "INVALID_FORMAT;"

    parsed_dates = pd.to_datetime(data["created_at"], errors="coerce")
    reasons[parsed_dates.isna()] += "INVALID_CREATED_AT;"
    reasons[parsed_dates > pd.Timestamp.now()] += "FUTURE_CREATED_AT;"

    duplicate_mask = data["image_id"].notna() & data.duplicated("image_id", keep="first")
    if seen_ids:
        duplicate_mask |= data["image_id"].isin(seen_ids)
    reasons[duplicate_mask] += "DUPLICATE_IMAGE_ID;"

    invalid = reasons != ""
    clean = data[~invalid].copy()
    errors = data[invalid].copy()
    errors["error_reason"] = reasons[invalid].str.rstrip(";")

    if not clean.empty:
        clean["image_id"] = clean["image_id"].astype(np.int64)
        clean["file_size"] = clean["file_size"].astype(np.int64)
        clean["created_at"] = pd.to_datetime(clean["created_at"])
        clean["year"] = clean["created_at"].dt.year
        clean["month"] = clean["created_at"].dt.month
    return clean, errors
