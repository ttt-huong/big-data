from generator.data_generator import generate_metadata
from pipeline.validation import validate_and_clean


def test_clean_data_has_expected_columns():
    clean, errors = validate_and_clean(generate_metadata(5, seed=5))
    assert len(clean) == 5
    assert errors.empty
    assert {"year", "month"}.issubset(clean.columns)
