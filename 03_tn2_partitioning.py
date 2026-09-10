"""
TN2: Không partition vs có partition
- Ghi cùng 1 bộ dữ liệu theo 2 cách: 1 file Parquet duy nhất, và có partition theo category
- Dùng DuckDB đo thời gian truy vấn khi lọc theo trường partition (category)

Chạy: python 03_tn2_partitioning.py --n 1000000
"""

import argparse
import os
import sys
import shutil
import time
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from importlib import import_module
gen = import_module("01_generate_metadata")
import config


def run(n: int):
    df = gen.generate_metadata(n)
    table = pa.Table.from_pandas(df)

    no_partition_path = f"{config.LOCAL_DATA_DIR}/no_partition_{n}.parquet"
    partition_dir = f"{config.LOCAL_DATA_DIR}/partitioned_{n}"

    # --- Ghi KHÔNG partition (1 file duy nhất) ---
    pq.write_table(table, no_partition_path)

    # --- Ghi CÓ partition theo category ---
    shutil.rmtree(partition_dir, ignore_errors=True)
    pq.write_to_dataset(table, root_path=partition_dir, partition_cols=["category"])

    con = duckdb.connect()
    target_category = "san_pham"

    # Chuẩn hóa đường dẫn cho DuckDB SQL string trên Windows (tránh lỗi escape '\')
    no_partition_path_sql = no_partition_path.replace("\\", "/")
    partition_dir_sql = partition_dir.replace("\\", "/")

    # Query 1: không partition -> phải quét toàn bộ file rồi filter
    t0 = time.time()
    con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{no_partition_path_sql}')
        WHERE category = '{target_category}'
    """).fetchall()
    no_partition_time = time.time() - t0

    # Query 2: có partition -> DuckDB chỉ đọc đúng thư mục con của category đó
    t0 = time.time()
    con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{partition_dir_sql}/**/*.parquet', hive_partitioning=true)
        WHERE category = '{target_category}'
    """).fetchall()
    partition_time = time.time() - t0

    print("\n=== KẾT QUẢ TN2 (n =", f"{n:,})", "===")
    print(f"Query không partition : {no_partition_time:.4f} giây")
    print(f"Query có partition    : {partition_time:.4f} giây")
    speedup = no_partition_time / partition_time if partition_time > 0 else float("inf")
    print(f"Tăng tốc              : {speedup:.2f} lần")

    import pandas as pd
    result_df = pd.DataFrame([{
        "n_rows": n,
        "no_partition_query_s": round(no_partition_time, 4),
        "partition_query_s": round(partition_time, 4),
        "speedup_x": round(speedup, 2),
    }])
    result_df.to_csv(f"{config.LOCAL_RESULTS_DIR}/tn2_partitioning.csv", index=False)
    print(f"\nĐã lưu kết quả vào {config.LOCAL_RESULTS_DIR}/tn2_partitioning.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=1_000_000)
    args = parser.parse_args()
    run(args.n)
