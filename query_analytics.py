"""Run reproducible DuckDB queries over the local Delta and error data."""

import glob
import os

import duckdb
from deltalake import DeltaTable

import config


def run_queries(target_path: str = config.TARGET_DELTA_PATH, error_dir: str = config.ERROR_DATA_DIR) -> dict:
    if not DeltaTable.is_deltatable(target_path):
        raise FileNotFoundError(f"Delta table not found: {target_path}")

    connection = duckdb.connect()
    connection.register("clean_data", DeltaTable(target_path).to_pyarrow_dataset())
    result = {
        "total_clean_records": connection.sql("SELECT COUNT(*) FROM clean_data").fetchone()[0],
        "by_category": connection.sql("SELECT category, COUNT(*) AS records FROM clean_data GROUP BY category ORDER BY category").fetchall(),
        "by_format": connection.sql("SELECT format, COUNT(*) AS records FROM clean_data GROUP BY format ORDER BY format").fetchall(),
        "by_month": connection.sql("SELECT year, month, COUNT(*) AS records FROM clean_data GROUP BY year, month ORDER BY year, month LIMIT 12").fetchall(),
        "error_records": 0,
    }
    error_files = glob.glob(os.path.join(error_dir, "*.parquet"))
    if error_files:
        pattern = os.path.join(error_dir, "*.parquet").replace("\\", "/")
        result["error_records"] = connection.sql(f"SELECT COUNT(*) FROM read_parquet('{pattern}')").fetchone()[0]
    return result


if __name__ == "__main__":
    for key, value in run_queries().items():
        print(f"{key}: {value}")
