import pandas as pd
from deltalake import write_deltalake

from query_analytics import run_queries, query_formats_by_period


def test_duckdb_analytics_counts_clean_and_errors(tmp_path):
    target = str(tmp_path / "table")
    errors = tmp_path / "errors"
    errors.mkdir()
    clean = pd.DataFrame({"image_id": [1, 2], "file_size": [1000, 3000], "category": ["san_pham", "do_an"], "format": ["jpg", "png"], "year": [2025, 2025], "month": [1, 2]})
    write_deltalake(target, clean, mode="overwrite", partition_by=["category"])
    pd.DataFrame({"image_id": [3], "error_reason": ["INVALID_FORMAT"]}).to_parquet(errors / "errors_1.parquet", index=False)
    result = run_queries(target, str(errors))
    assert result["total_clean_records"] == 2
    assert result["error_records"] == 1


# OLAP-01/02/03: kiểm tra thống kê theo danh mục, tháng và định dạng.
def test_storage_aggregates(tmp_path):
    target = str(tmp_path / "table")
    errors = tmp_path / "errors"
    errors.mkdir()

    clean = pd.DataFrame({
        "image_id": [1, 2, 3],
        "category": ["do_an", "do_an", "san_pham"],
        "format": ["jpg", "png", "jpg"],
        "file_size": [1000, 3000, 6000],
        "year": [2025, 2025, 2025],
        "month": [1, 1, 2],
    })
    write_deltalake(target, clean, mode="overwrite", partition_by=["category"])

    result = run_queries(target, str(errors))

    assert result["storage_by_category"] == [
        ("do_an", 2, 4000, 2000.0),
        ("san_pham", 1, 6000, 6000.0),
    ]
    assert result["storage_by_month"] == [
        (2025, 1, 2, 4000),
        (2025, 2, 1, 6000),
    ]

    assert result["storage_by_format"] == [
        ("jpg", 2, 7000, 3500.0),
        ("png", 1, 3000, 3000.0),
    ]

    if __name__ == "__main__":
        for key, value in run_queries().items():
            print(f"{key}: {value}")

    assert result["storage_overview"] == (3, 10000, 10000 / 3)


def test_errors_by_reason_counts_each_violation(tmp_path):
    target = str(tmp_path / "table")
    errors = tmp_path / "errors"
    errors.mkdir()

    clean = pd.DataFrame({"image_id": [1], "file_size": [1000], "category": ["do_an"], "format": ["jpg"], "year": [2025], "month": [1]})
    write_deltalake(target, clean, mode="overwrite", partition_by=["category"])

    pd.DataFrame({
        "image_id": [2, 3],
        "error_reason": [
            "INVALID_FORMAT;INVALID_FILE_SIZE",
            "INVALID_FORMAT",
        ],
    }).to_parquet(errors / "errors_1.parquet", index=False)

    result = run_queries(target, str(errors))

    assert result["error_records"] == 2
    assert result["errors_by_reason"] == [
        ("INVALID_FILE_SIZE", 1),
        ("INVALID_FORMAT", 2),
    ]

def test_formats_filtered_by_category_and_period(tmp_path):
    target = str(tmp_path / "table")
    clean = pd.DataFrame({
        "image_id": [1, 2, 3, 4, 5],
        "category": ["do_an", "do_an", "san_pham", "do_an", "do_an"],
        "format": ["jpg", "png", "jpg", "jpg", "jpg"],
        "file_size": [1000, 3000, 6000, 8000, 9000],
        "created_at": pd.to_datetime([
            "2025-01-01", "2025-01-20", "2025-01-10",
            "2025-02-01", "2024-12-31",
        ]),
    })
    write_deltalake(target, clean, mode="overwrite", partition_by=["category"])

    result = query_formats_by_period(target, "do_an", "2025-01-01", "2025-02-01")

    assert result == [
        ("jpg", 1, 1000),
        ("png", 1, 3000),
    ]