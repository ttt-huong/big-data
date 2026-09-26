import pandas as pd

from generator.data_generator import generate_metadata
from pipeline.validation import validate_and_clean


def test_generator_is_reproducible():
    first = generate_metadata(20, seed=10)
    second = generate_metadata(20, seed=10)
    pd.testing.assert_frame_equal(first, second)


def test_validation_separates_bad_records():
    frame = generate_metadata(100, error_ratio=0.2, seed=10)
    clean, errors = validate_and_clean(frame)
    assert len(clean) + len(errors) == 100
    assert len(errors) > 0
    assert errors["error_reason"].notna().all()


def test_duplicate_is_rejected():
    frame = generate_metadata(3, seed=1)
    frame.loc[2, "image_id"] = frame.loc[0, "image_id"]
    clean, errors = validate_and_clean(frame)
    assert len(clean) == 2
    assert "DUPLICATE_IMAGE_ID" in errors.iloc[0]["error_reason"]


def test_generator_rejects_invalid_error_ratio():
    try:
        generate_metadata(10, error_ratio=1.1)
    except ValueError:
        pass
    else:
        raise AssertionError("error_ratio outside [0, 1] should fail")
