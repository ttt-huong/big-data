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
        "errors_by_reason": [],
        "storage_overview": connection.sql("SELECT COUNT(*) AS image_count, COALESCE(SUM(file_size), 0) AS total_bytes, AVG(file_size) AS avg_bytes FROM clean_data").fetchone(),
        "by_category": connection.sql("SELECT category, COUNT(*) AS records FROM clean_data GROUP BY category ORDER BY category").fetchall(),
        "by_format": connection.sql("SELECT format, COUNT(*) AS records FROM clean_data GROUP BY format ORDER BY format").fetchall(),
        "storage_by_format": connection.sql("SELECT format, COUNT(*) AS image_count, SUM(file_size) AS total_bytes, AVG(file_size) AS avg_bytes FROM clean_data GROUP BY format ORDER BY format").fetchall(),
        "by_month": connection.sql("SELECT year, month, COUNT(*) AS records FROM clean_data GROUP BY year, month ORDER BY year, month LIMIT 12").fetchall(),
        "storage_by_month": connection.sql("SELECT year, month, COUNT(*) AS image_count, SUM(file_size) AS total_bytes FROM clean_data GROUP BY year, month ORDER BY year, month").fetchall(),
        "storage_by_category": connection.sql("SELECT category, COUNT(*) AS image_count, SUM(file_size) AS total_bytes, AVG(file_size) AS avg_bytes FROM clean_data GROUP BY category ORDER BY category").fetchall(),
        "error_records": 0,
    }
    error_files = glob.glob(os.path.join(error_dir, "*.parquet"))
    if error_files:
        pattern = os.path.join(error_dir, "*.parquet").replace("\\", "/")
        result["error_records"] = connection.execute("SELECT COUNT(*) FROM read_parquet(?)", [pattern]).fetchone()[0]
        result["errors_by_reason"] = connection.execute("SELECT reason, COUNT(*) AS occurrences FROM (SELECT UNNEST(string_split(error_reason, ';')) AS reason FROM read_parquet(?)) WHERE reason <> '' GROUP BY reason ORDER BY reason", [pattern]).fetchall()

    connection.close()
    return result


def query_formats_by_period(target_path: str, category: str, start_date: str, end_date: str) -> list:
    with duckdb.connect() as connection:
        connection.register("clean_data", DeltaTable(target_path).to_pyarrow_dataset())
        return connection.execute("SELECT format, COUNT(*) AS image_count, SUM(file_size) AS total_bytes FROM clean_data WHERE category = ? AND created_at >= CAST(? AS TIMESTAMP) AND created_at < CAST(? AS TIMESTAMP) GROUP BY format ORDER BY format", [category, start_date, end_date]).fetchall()

if __name__ == "__main__":
    for key, value in run_queries().items():
        print(f"{key}: {value}")