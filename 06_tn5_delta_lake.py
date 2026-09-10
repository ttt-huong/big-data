"""
TN5: Lakehouse / Delta Lake
- Ghi metadata thành Delta table trên MinIO (s3-compatible)
- Thao tác UPDATE (chứng minh ACID transaction)
- Xem lại phiên bản cũ (time travel)
- Thêm cột mới vào batch dữ liệu mới (schema evolution)
- Query lại bằng DuckDB

Chạy: python 06_tn5_delta_lake.py --n 100000
"""

import argparse
import os
import sys
import pandas as pd
from deltalake import DeltaTable, write_deltalake
import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from importlib import import_module
gen = import_module("01_generate_metadata")
import config

TABLE_URI = f"s3://{config.BUCKET_METADATA}/orders_delta"


def run(n: int):
    df = gen.generate_metadata(n)

    print(f"\n=== TN5 Bước 1: Ghi {n:,} dòng thành Delta table trên MinIO ===")
    write_deltalake(
        TABLE_URI,
        df,
        mode="overwrite",
        storage_options=config.DELTA_STORAGE_OPTIONS,
        partition_by=["category"],
    )
    print(f"Đã ghi Delta table tại {TABLE_URI}")

    dt = DeltaTable(TABLE_URI, storage_options=config.DELTA_STORAGE_OPTIONS)
    print(f"Version hiện tại: {dt.version()}")

    print("\n=== TN5 Bước 2: ACID transaction - UPDATE dữ liệu ===")
    # Tăng file_size của category 'san_pham' thêm 10% (mô phỏng chỉnh sửa dữ liệu)
    # Cần ép kiểu CAST(... AS BIGINT) để đảm bảo không sai schema từ int64 thành float64
    dt.update(
        predicate="category = 'san_pham'",
        updates={"file_size": "CAST(file_size * 1.1 AS BIGINT)"},
    )
    print(f"Đã UPDATE. Version mới: {dt.version()}")

    print("\n=== TN5 Bước 3: Time travel - so sánh version cũ và mới ===")
    con = duckdb.connect()

    current = dt.to_pyarrow_dataset()
    con.register("orders_current", current)
    print("--- Dữ liệu HIỆN TẠI (sau update), category = san_pham ---")
    con.sql("""
        SELECT category, AVG(file_size) AS avg_size
        FROM orders_current WHERE category = 'san_pham'
        GROUP BY category
    """).show()

    dt_old = DeltaTable(TABLE_URI, storage_options=config.DELTA_STORAGE_OPTIONS, version=0)
    old = dt_old.to_pyarrow_dataset()
    con.register("orders_v0", old)
    print("--- Dữ liệu Ở VERSION 0 (trước update), category = san_pham ---")
    con.sql("""
        SELECT category, AVG(file_size) AS avg_size
        FROM orders_v0 WHERE category = 'san_pham'
        GROUP BY category
    """).show()

    print("--- Lịch sử các thao tác trên bảng ---")
    history = dt.history()
    for h in history:
        print(f"  version={h.get('version')}, operation={h.get('operation')}, "
              f"timestamp={h.get('timestamp')}")

    print("\n=== TN5 Bước 4: Schema evolution - thêm cột mới ===")
    new_batch = gen.generate_metadata(1000, seed=999).head(100).copy()
    new_batch["is_reviewed"] = True  # cột mới, dữ liệu cũ chưa từng có

    write_deltalake(
        TABLE_URI,
        new_batch,
        mode="append",
        schema_mode="merge",   # cho phép thêm cột mới mà không lỗi
        storage_options=config.DELTA_STORAGE_OPTIONS,
        partition_by=["category"],
    )
    dt = DeltaTable(TABLE_URI, storage_options=config.DELTA_STORAGE_OPTIONS)
    print(f"Đã append kèm cột mới 'is_reviewed'. Version mới: {dt.version()}")

    con.register("orders_after_schema_change", dt.to_pyarrow_dataset())
    print("--- Schema bảng sau khi thêm cột (các dòng cũ có is_reviewed = NULL) ---")
    con.sql("""
        SELECT is_reviewed, COUNT(*) AS num_rows
        FROM orders_after_schema_change
        GROUP BY is_reviewed
    """).show()

    print("\n=== TN5 HOÀN TẤT ===")
    print("Đã chứng minh: ACID transaction, time travel, schema evolution trên Delta Lake.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=100_000,
                         help="Số dòng ban đầu ghi vào Delta table (khuyên dùng mức nhỏ, vd 100K, để demo nhanh)")
    args = parser.parse_args()
    run(args.n)
