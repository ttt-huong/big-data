"""
Data Generator cho dự án ETL/ELT.
Hỗ trợ sinh dữ liệu sạch (Clean Data) và dữ liệu bẩn (Dirty Data) chứa các dạng lỗi thực tế:
- Null / missing values
- Sai format / sai category
- Giá trị âm hoặc không hợp lệ (file_size, width, height)
- Bản ghi bị lặp lại (duplicate)
"""

import time
import numpy as np
import pandas as pd
import config


def generate_clean_metadata(n: int, seed: int = 42, start_id: int = 1) -> pd.DataFrame:
    """Sinh dữ liệu chuẩn (clean metadata) dạng vectorized với quy mô n bản ghi."""
    rng = np.random.default_rng(seed)

    image_id = np.arange(start_id, start_id + n)
    fmt = rng.choice(config.FORMATS, size=n)
    filename = np.array([f"img_{img_id:07d}.{f}" for img_id, f in zip(image_id, fmt)])

    file_size = rng.integers(10_000, 5_000_000, size=n)
    width = rng.choice([64, 128, 256, 512, 1024], size=n)
    height = rng.choice([64, 128, 256, 512, 1024], size=n)
    category = rng.choice(config.CATEGORIES, size=n)

    start_ts = pd.Timestamp(config.DATE_RANGE_START).value // 10**9
    end_ts = pd.Timestamp(config.DATE_RANGE_END).value // 10**9
    created_at_unix = rng.integers(start_ts, end_ts, size=n)
    created_at = pd.to_datetime(created_at_unix, unit="s")

    df = pd.DataFrame({
        "image_id": image_id,
        "filename": filename,
        "file_size": file_size,
        "width": width,
        "height": height,
        "format": fmt,
        "category": category,
        "created_at": created_at,
    })
    df["year"] = df["created_at"].dt.year
    df["month"] = df["created_at"].dt.month
    return df


def generate_dirty_metadata(n: int, error_ratio: float = 0.05, seed: int = 42, start_id: int = 1) -> pd.DataFrame:
    """
    Sinh dữ liệu có chứa tỷ lệ lỗi (error_ratio) để kiểm thử tầng Transform & Validation của ETL.
    Các dạng lỗi cố ý chèn vào bao gồm 100% kịch bản kiểm thử của tầng validation:
    1. image_id bị Null (NULL_IMAGE_ID)
    2. file_size <= 0 (INVALID_FILE_SIZE)
    3. width/height <= 0 (INVALID_WIDTH, INVALID_HEIGHT)
    4. category không thuộc danh mục cho phép (INVALID_CATEGORY)
    5. format không hợp lệ .exe, .pdf (INVALID_FORMAT)
    6. created_at trỏ về tương lai hoặc Null (FUTURE_CREATED_AT, NULL_CREATED_AT)
    7. Bản ghi trùng lặp image_id (DUPLICATE_IMAGE_ID)
    """
    df = generate_clean_metadata(n, seed=seed, start_id=start_id)
    rng = np.random.default_rng(seed)

    num_errors = int(n * error_ratio)
    if num_errors == 0:
        return df

    error_indices = rng.choice(n, size=num_errors, replace=False)
    
    # Chia danh sách lỗi thành 7 phần
    split_size = max(1, num_errors // 7)
    idx_null_id = error_indices[:split_size]
    idx_negative_size = error_indices[split_size:split_size * 2]
    idx_invalid_dim = error_indices[split_size * 2:split_size * 3]
    idx_invalid_cat = error_indices[split_size * 3:split_size * 4]
    idx_invalid_fmt = error_indices[split_size * 4:split_size * 5]
    idx_future_or_null_date = error_indices[split_size * 5:split_size * 6]
    idx_duplicates = error_indices[split_size * 6:]

    # Convert image_id to float type to allow NaN without dtype error
    df["image_id"] = df["image_id"].astype("float64")

    # 1. Chèn lỗi Null image_id
    df.loc[idx_null_id, "image_id"] = np.nan

    # 2. Chèn lỗi file_size âm hoặc bằng 0
    df.loc[idx_negative_size, "file_size"] = rng.choice([-500, 0, -1024], size=len(idx_negative_size))

    # 3. Chèn lỗi width/height âm hoặc bằng 0
    if len(idx_invalid_dim) > 0:
        half_dim = max(1, len(idx_invalid_dim) // 2)
        df.loc[idx_invalid_dim[:half_dim], "width"] = rng.choice([-100, 0], size=half_dim)
        df.loc[idx_invalid_dim[half_dim:], "height"] = rng.choice([-100, 0], size=len(idx_invalid_dim) - half_dim)

    # 4. Chèn lỗi category sai chuẩn
    df.loc[idx_invalid_cat, "category"] = rng.choice(["unknown_cat", "racy", "corrupted"], size=len(idx_invalid_cat))

    # 5. Chèn lỗi format lạ
    df.loc[idx_invalid_fmt, "format"] = rng.choice(["exe", "pdf", "docx"], size=len(idx_invalid_fmt))

    # 6. Chèn lỗi created_at tương lai hoặc Null
    if len(idx_future_or_null_date) > 0:
        half_date = max(1, len(idx_future_or_null_date) // 2)
        df.loc[idx_future_or_null_date[:half_date], "created_at"] = pd.Timestamp("2099-01-01 00:00:00")
        df.loc[idx_future_or_null_date[half_date:], "created_at"] = pd.NaT

    # 7. Chèn lỗi Duplicate image_id
    if len(idx_duplicates) > 0:
        dup_target_ids = rng.choice(df.loc[~df.index.isin(idx_null_id), "image_id"].values, size=len(idx_duplicates))
        df.loc[idx_duplicates, "image_id"] = dup_target_ids

    return df


if __name__ == "__main__":
    print("Testing Data Generator...")
    df_clean = generate_clean_metadata(1000)
    print(f"Clean dataset shape: {df_clean.shape}")
    
    df_dirty = generate_dirty_metadata(1000, error_ratio=0.1)
    print(f"Dirty dataset shape: {df_dirty.shape}")
    print("Sample dirty records:")
    print(df_dirty[df_dirty["category"].isin(["unknown_cat", "racy"])].head())
