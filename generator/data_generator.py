"""Generate reproducible metadata for the local ETL demo."""

import numpy as np
import pandas as pd

import config


def generate_metadata(rows: int, error_ratio: float = 0.0, seed: int = 42, start_id: int = 1) -> pd.DataFrame:
    if rows < 1:
        raise ValueError("rows must be greater than zero")
    if not 0 <= error_ratio <= 1:
        raise ValueError("error_ratio must be between 0 and 1")

    rng = np.random.default_rng(seed)
    image_ids = np.arange(start_id, start_id + rows)
    formats = rng.choice(config.FORMATS, size=rows)
    created_at = pd.to_datetime(
        rng.integers(
            pd.Timestamp(config.DATE_RANGE_START).value // 10**9,
            pd.Timestamp(config.DATE_RANGE_END).value // 10**9,
            size=rows,
        ),
        unit="s",
    )
    frame = pd.DataFrame({
        "image_id": image_ids,
        "filename": [f"img_{image_id:07d}.{fmt}" for image_id, fmt in zip(image_ids, formats)],
        "file_size": rng.integers(10_000, 5_000_000, size=rows),
        "width": rng.choice([64, 128, 256, 512, 1024], size=rows),
        "height": rng.choice([64, 128, 256, 512, 1024], size=rows),
        "format": formats,
        "category": rng.choice(config.CATEGORIES, size=rows),
        "created_at": created_at,
    })
    frame["year"] = frame["created_at"].dt.year
    frame["month"] = frame["created_at"].dt.month

    bad_count = int(rows * error_ratio)
    if bad_count == 0:
        return frame

    selected = rng.choice(rows, size=bad_count, replace=False)
    groups = np.array_split(selected, 5)
    frame.loc[groups[0], "image_id"] = np.nan
    frame.loc[groups[1], "file_size"] = 0
    frame.loc[groups[2], "category"] = "unknown"
    frame.loc[groups[3], "format"] = "exe"
    if len(groups[4]):
        frame.loc[groups[4], "image_id"] = rng.choice(image_ids, size=len(groups[4]), replace=True)
    return frame


def generate_clean_metadata(rows: int, seed: int = 42, start_id: int = 1) -> pd.DataFrame:
    return generate_metadata(rows, seed=seed, start_id=start_id)


def generate_dirty_metadata(rows: int, error_ratio: float = 0.05, seed: int = 42, start_id: int = 1) -> pd.DataFrame:
    return generate_metadata(rows, error_ratio=error_ratio, seed=seed, start_id=start_id)
