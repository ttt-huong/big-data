"""
Validation & Quality Control cho ETL Pipeline.
Thực hiện lọc dữ liệu trùng, kiểm tra quy tắc toàn vẹn (data quality rules),
tách riêng Clean Records và Error Records kèm lý do lỗi.
"""

import pandas as pd
import numpy as np
import config


def validate_and_clean(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Thực hiện các quy tắc kiểm soát chất lượng dữ liệu:
    1. Kiểm tra Null / NaN ở các cột bắt buộc (image_id)
    2. Kiểm tra giá trị hợp lệ: file_size > 0, width > 0, height > 0
    3. Kiểm tra danh mục hợp lệ (category in config.CATEGORIES)
    4. Kiểm tra định dạng hợp lệ (format in config.FORMATS)
    5. Kiểm tra và loại bỏ bản ghi trùng lặp (duplicate image_id)

    Trả về:
    - clean_df: Dữ liệu sạch đã chuẩn hóa
    - error_df: Các bản ghi lỗi kèm cột 'error_reason'
    """
    if df.empty:
        return df.copy(), pd.DataFrame()

    df = df.copy()
    error_reasons = pd.Series("", index=df.index, dtype=str)

    # 1. Kiểm tra Null image_id
    null_id_mask = df["image_id"].isna()
    error_reasons[null_id_mask] += "NULL_IMAGE_ID; "

    # 2. Kiểm tra file_size <= 0
    invalid_size_mask = df["file_size"].fillna(0) <= 0
    error_reasons[invalid_size_mask] += "INVALID_FILE_SIZE; "

    # 3. Kiểm tra category không hợp lệ
    invalid_cat_mask = ~df["category"].isin(config.CATEGORIES)
    error_reasons[invalid_cat_mask] += "INVALID_CATEGORY; "

    # 4. Kiểm tra format không hợp lệ
    invalid_fmt_mask = ~df["format"].isin(config.FORMATS)
    error_reasons[invalid_fmt_mask] += "INVALID_FORMAT; "

    # 5. Kiểm tra bản ghi trùng lặp image_id (chỉ giữ bản ghi đầu tiên, các bản ghi sau báo lỗi DUPLICATE)
    valid_id_df = df[~null_id_mask]
    duplicate_mask = valid_id_df.duplicated(subset=["image_id"], keep="first")
    duplicate_indices = valid_id_df[duplicate_mask].index
    error_reasons[duplicate_indices] += "DUPLICATE_IMAGE_ID; "

    # Tách bản ghi lỗi và bản ghi hợp lệ
    is_error = error_reasons != ""
    
    clean_df = df[~is_error].copy()
    error_df = df[is_error].copy()
    error_df["error_reason"] = error_reasons[is_error].str.rstrip("; ")

    # Chuẩn hóa kiểu dữ liệu cho clean_df
    if not clean_df.empty:
        clean_df["image_id"] = clean_df["image_id"].astype(np.int64)
        clean_df["file_size"] = clean_df["file_size"].astype(np.int64)
        clean_df["created_at"] = pd.to_datetime(clean_df["created_at"])
        clean_df["year"] = clean_df["created_at"].dt.year
        clean_df["month"] = clean_df["created_at"].dt.month

    return clean_df, error_df
