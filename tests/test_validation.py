import pandas as pd

import pytest

from pipeline.validation import validate_and_clean


# DQ-01: image_id phải là số nguyên dương.
def test_missing_image_id_is_rejected():
    # Arrange: chuẩn bị 2 bản ghi, chỉ dòng thứ hai thiếu ID.
    frame = pd.DataFrame({
        "image_id": [1, None],
        "filename": ["anh_1.jpg", "anh_2.jpg"],
        "file_size": [50000, 30000],
        "width": [640, 640],
        "height": [480, 480],
        "category": ["do_an", "do_an"],
        "format": ["jpg", "jpg"],
        "created_at": ["2025-01-01", "2025-01-01"],
    })

    # Act: gọi hàm kiểm tra thực tế của dự án.
    clean, errors = validate_and_clean(frame)

    # Assert: đối chiếu kết quả với kỳ vọng.
    assert len(clean) == 1
    assert len(errors) == 1

    assert clean.iloc[0]["image_id"] == 1
    assert errors.iloc[0]["filename"] == "anh_2.jpg"
    assert errors.iloc[0]["error_reason"] == "NULL_IMAGE_ID"


# DQ-02: file_size phải lớn hơn 0.
@pytest.mark.parametrize("invalid_size", [0, -100, None])
def test_invalid_file_size_is_rejected(invalid_size):
    frame = pd.DataFrame({
        "image_id": [1],
        "filename": ["anh_1.jpg"],
        "file_size": [invalid_size],
        "width": [640],
        "height": [480],
        "category": ["do_an"],
        "format": ["jpg"],
        "created_at": ["2025-01-01"],
    })

    clean, errors = validate_and_clean(frame)

    assert clean.empty
    assert len(errors) == 1
    assert errors.iloc[0]["image_id"] == 1
    assert errors.iloc[0]["error_reason"] == "INVALID_FILE_SIZE"   


# DQ-03: width, height phải lớn hơn 0.
@pytest.mark.parametrize(
    "width, height, expected_reason",
    [
        (0, 480, "INVALID_WIDTH"),
        (-100, 480, "INVALID_WIDTH"),
        (None, 480, "INVALID_WIDTH"),
        (640, 0, "INVALID_HEIGHT"),
        (640, -100, "INVALID_HEIGHT"),
        (640, None, "INVALID_HEIGHT"),
        (0, 0, "INVALID_WIDTH;INVALID_HEIGHT"),
    ],
)
def test_invalid_dimensions_are_rejected(width, height, expected_reason):
    frame = pd.DataFrame({
        "image_id": [1],
        "filename": ["anh_1.jpg"],
        "file_size": [50000],
        "width": [width],
        "height": [height],
        "category": ["do_an"],
        "format": ["jpg"],
        "created_at": ["2025-01-01"],
    })

    clean, errors = validate_and_clean(frame)

    assert clean.empty
    assert len(errors) == 1
    assert errors.iloc[0]["image_id"] == 1
    assert errors.iloc[0]["error_reason"] == expected_reason


# DQ-04: category phải thuộc san_pham, chan_dung, phong_canh, do_an, dong_vat, kien_truc
@pytest.mark.parametrize(
    "category, is_valid",
    [
        ("san_pham", True),
        ("chan_dung", True),
        ("phong_canh", True),
        ("do_an", True),
        ("dong_vat", True),
        ("kien_truc", True),
        ("unknown", False),
        ("", False),
        (None, False),
    ],
)
def test_category_validation(category, is_valid):
    frame = pd.DataFrame({
        "image_id": [1],
        "filename": ["anh_1.jpg"],
        "file_size": [50000],
        "width": [640],
        "height": [480],
        "category": [category],
        "format": ["jpg"],
        "created_at": ["2025-01-01"],
    })

    clean, errors = validate_and_clean(frame)

    if is_valid:
        assert len(clean) == 1
        assert errors.empty
        assert clean.iloc[0]["category"] == category
    else:
        assert clean.empty
        assert len(errors) == 1
        assert errors.iloc[0]["error_reason"] == "INVALID_CATEGORY"


# DQ-05: định dạng phải thuộc jpg, png, webp
@pytest.mark.parametrize(
    "image_format, is_valid",
    [
        ("jpg", True),
        ("png", True),
        ("webp", True),
        ("exe", False),
        ("", False),
        (None, False),
    ],
)
def test_format_validation(image_format, is_valid):
    frame = pd.DataFrame({
        "image_id": [1],
        "filename": ["anh_1"],
        "file_size": [50000],
        "width": [640],
        "height": [480],
        "category": ["do_an"],
        "format": [image_format],
        "created_at": ["2025-01-01"],
    })

    clean, errors = validate_and_clean(frame)

    if is_valid:
        assert len(clean) == 1
        assert errors.empty
        assert clean.iloc[0]["format"] == image_format
    else:
        assert clean.empty
        assert len(errors) == 1
        assert errors.iloc[0]["error_reason"] == "INVALID_FORMAT"


# DQ-06: created_at phải là thời gian hợp lệ và không nằm trong tương lai.
@pytest.mark.parametrize(
    "date_case, expected_reason",
    [
        ("past", None),
        ("invalid", "INVALID_CREATED_AT"),
        ("missing", "INVALID_CREATED_AT"),
        ("future", "FUTURE_CREATED_AT"),
    ],
)
def test_created_at_validation(date_case, expected_reason):
    now = pd.Timestamp.now()

    date_values = {
        "past": now - pd.Timedelta(days=1),
        "invalid": "not-a-date",
        "missing": None,
        "future": now + pd.Timedelta(days=1),
    }

    frame = pd.DataFrame({
        "image_id": [1],
        "filename": ["anh_1.jpg"],
        "file_size": [50000],
        "width": [640],
        "height": [480],
        "category": ["do_an"],
        "format": ["jpg"],
        "created_at": [date_values[date_case]],
    })

    clean, errors = validate_and_clean(frame)

    if expected_reason is None:
        assert len(clean) == 1
        assert errors.empty
        assert clean.iloc[0]["created_at"] == date_values[date_case]
    else:
        assert clean.empty
        assert len(errors) == 1
        assert errors.iloc[0]["error_reason"] == expected_reason


# DQ-07: phát hiện image_id trùng, gồm trùng trong một chunk và trùng với ID đã gặp ở chunk trước.
@pytest.mark.parametrize(
    "image_ids, seen_ids, expected_clean_files, expected_error_files",
    [
        (
            [1, 2],
            set(),
            ["anh_a.jpg", "anh_b.jpg"],
            [],
        ),
        (
            [1, 1],
            set(),
            ["anh_a.jpg"],
            ["anh_b.jpg"],
        ),
        (
            [1, 2],
            {1},
            ["anh_b.jpg"],
            ["anh_a.jpg"],
        ),
    ],
    ids=["unique_ids", "duplicate_in_chunk", "duplicate_from_previous_chunk"],
)
def test_duplicate_image_id_validation(
    image_ids,
    seen_ids,
    expected_clean_files,
    expected_error_files,
):
    frame = pd.DataFrame({
        "image_id": image_ids,
        "filename": ["anh_a.jpg", "anh_b.jpg"],
        "file_size": [50000, 30000],
        "width": [640, 640],
        "height": [480, 480],
        "category": ["do_an", "do_an"],
        "format": ["jpg", "jpg"],
        "created_at": ["2025-01-01", "2025-01-01"],
    })

    clean, errors = validate_and_clean(frame, seen_ids=seen_ids)

    assert clean["filename"].tolist() == expected_clean_files
    assert errors["filename"].tolist() == expected_error_files

    if expected_error_files:
        assert errors["error_reason"].tolist() == [
            "DUPLICATE_IMAGE_ID"
        ] * len(expected_error_files)

    assert len(clean) + len(errors) == len(frame)


# DQ-08: phát hiện nhiều lỗi trên cùng một dòng.
def test_multiple_errors_are_collected():
    frame = pd.DataFrame({
        "image_id": [None],
        "filename": ["anh_loi.jpg"],
        "file_size": [-100],
        "width": [0],
        "height": [480],
        "category": ["unknown"],
        "format": ["exe"],
        "created_at": ["not-a-date"],
    })

    clean, errors = validate_and_clean(frame)

    assert clean.empty
    assert len(errors) == 1

    actual_reasons = errors.iloc[0]["error_reason"].split(";")

    expected_reasons = {
        "NULL_IMAGE_ID",
        "INVALID_FILE_SIZE",
        "INVALID_WIDTH",
        "INVALID_CATEGORY",
        "INVALID_FORMAT",
        "INVALID_CREATED_AT",
    }

    assert set(actual_reasons) == expected_reasons
    assert len(actual_reasons) == len(expected_reasons)
    assert len(clean) + len(errors) == len(frame)


# DQ-09: không có record nào bị rớt, tất cả đều được xử lý.
def test_empty_input_returns_empty_results():
    frame = pd.DataFrame(columns=[
        "image_id", "filename", "file_size",
        "width", "height", "category", "format", "created_at",
    ])

    clean, errors = validate_and_clean(frame)

    assert clean.empty
    assert errors.empty
    assert list(clean.columns) == list(frame.columns)
    assert "error_reason" in errors.columns


def test_all_invalid_records_are_rejected():
    frame = pd.DataFrame({
        "image_id": [1, 2],
        "filename": ["anh_a.jpg", "anh_b.jpg"],
        "file_size": [0, -100],
        "width": [640, 640],
        "height": [480, 480],
        "category": ["do_an", "do_an"],
        "format": ["jpg", "jpg"],
        "created_at": ["2025-01-01", "2025-01-01"],
    })

    clean, errors = validate_and_clean(frame)

    assert clean.empty
    assert errors["image_id"].tolist() == [1, 2]
    assert errors["error_reason"].tolist() == [
        "INVALID_FILE_SIZE",
        "INVALID_FILE_SIZE",
    ]

def test_validation_does_not_modify_input():
    frame = pd.DataFrame({
        "image_id": [1, 2],
        "filename": ["anh_a.jpg", "anh_b.jpg"],
        "file_size": [50000, -100],
        "width": [640, 640],
        "height": [480, 480],
        "category": ["do_an", "do_an"],
        "format": ["jpg", "jpg"],
        "created_at": ["2025-01-01", "2025-01-01"],
    })

    original = frame.copy(deep=True)

    validate_and_clean(frame)

    pd.testing.assert_frame_equal(frame, original)