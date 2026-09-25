"""
Unit tests cho pipeline.validation module.
"""

import pytest
import pandas as pd
from pipeline.validation import validate_and_clean


def test_validate_and_clean_empty():
    """Kiểm tra xử lý khi DataFrame rỗng."""
    empty_df = pd.DataFrame()
    clean_df, error_df = validate_and_clean(empty_df)
    assert clean_df.empty
    assert error_df.empty


def test_validate_and_clean_sample(sample_raw_data):
    """Kiểm tra tách bản ghi sạch và bản ghi lỗi từ sample fixture."""
    clean_df, error_df = validate_and_clean(sample_raw_data)

    # Từ 7 bản ghi đầu vào:
    # Bản ghi ID 1, ID 2 là hợp lệ -> clean_df có 2 bản ghi
    # Các bản ghi còn lại (Null ID, Size 0, Cat/Fmt không hợp lệ, Dup ID 1, Future Date) là lỗi -> error_df có 5 bản ghi
    assert len(clean_df) == 2
    assert len(error_df) == 5

    # Kiểm tra các ID trong clean_df
    assert set(clean_df["image_id"].tolist()) == {1, 2}

    # Kiểm tra cột year/month được sinh tự động trong clean_df
    assert "year" in clean_df.columns
    assert "month" in clean_df.columns
    assert clean_df["year"].tolist() == [2024, 2024]


def test_error_reasons(sample_raw_data):
    """Kiểm tra đúng mã lỗi được gán cho từng bản ghi."""
    _, error_df = validate_and_clean(sample_raw_data)
    
    # Kiểm tra presence của các error_reason cụ thể
    reasons_text = " ".join(error_df["error_reason"].tolist())
    assert "NULL_IMAGE_ID" in reasons_text
    assert "INVALID_FILE_SIZE" in reasons_text
    assert "INVALID_CATEGORY" in reasons_text
    assert "INVALID_FORMAT" in reasons_text
    assert "DUPLICATE_IMAGE_ID" in reasons_text
    assert "FUTURE_CREATED_AT" in reasons_text


def test_dirty_data_generator_full_error_coverage():
    """Kiểm tra generate_dirty_metadata sinh đầy đủ 100% các dạng lỗi tầng validation quy định."""
    from generator.data_generator import generate_dirty_metadata
    dirty_df = generate_dirty_metadata(700, error_ratio=0.7, seed=42)
    _, error_df = validate_and_clean(dirty_df)

    assert not error_df.empty
    reasons_text = " ".join(error_df["error_reason"].tolist())

    assert "NULL_IMAGE_ID" in reasons_text
    assert "INVALID_FILE_SIZE" in reasons_text
    assert ("INVALID_WIDTH" in reasons_text) or ("INVALID_HEIGHT" in reasons_text)
    assert "INVALID_CATEGORY" in reasons_text
    assert "INVALID_FORMAT" in reasons_text
    assert "DUPLICATE_IMAGE_ID" in reasons_text
    assert "FUTURE_CREATED_AT" in reasons_text
    assert "NULL_CREATED_AT" in reasons_text


def test_invalid_created_at_routing():
    """Kiểm tra bản ghi chứa created_at không đúng định dạng ngày tháng được chuyển vào DLQ mà không gây crash."""
    raw_df = pd.DataFrame([
        {
            "image_id": 1,
            "filename": "img_001.jpg",
            "file_size": 1024,
            "width": 100,
            "height": 100,
            "format": "jpg",
            "category": "san_pham",
            "created_at": "not-a-valid-date"
        }
    ])
    clean_df, error_df = validate_and_clean(raw_df)
    assert clean_df.empty
    assert len(error_df) == 1
    assert "INVALID_CREATED_AT" in error_df["error_reason"].iloc[0]

