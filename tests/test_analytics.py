import pandas as pd
from deltalake import write_deltalake

from query_analytics import run_queries


def test_duckdb_analytics_counts_clean_and_errors(tmp_path):
    target = str(tmp_path / "table")
    errors = tmp_path / "errors"
    errors.mkdir()
    clean = pd.DataFrame({"image_id": [1, 2], "category": ["san_pham", "do_an"], "format": ["jpg", "png"], "year": [2025, 2025], "month": [1, 2]})
    write_deltalake(target, clean, mode="overwrite", partition_by=["category"])
    pd.DataFrame({"image_id": [3], "error_reason": ["INVALID_FORMAT"]}).to_parquet(errors / "errors_1.parquet", index=False)
    result = run_queries(target, str(errors))
    assert result["total_clean_records"] == 2
    assert result["error_records"] == 1
