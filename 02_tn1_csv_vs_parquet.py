"""
TN1: So sánh CSV vs Parquet
- Dung lượng file
- Thời gian ghi
- Thời gian đọc lại toàn bộ
- Thời gian đọc + lọc (filter theo category)

Chạy: python 02_tn1_csv_vs_parquet.py --n 1000000
"""

import argparse
import os
import sys
import time
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from importlib import import_module
gen = import_module("01_generate_metadata")
import config


def benchmark(n: int) -> dict:
    df = gen.generate_metadata(n)

    csv_path = f"{config.LOCAL_DATA_DIR}/bench_{n}.csv"
    parquet_path = f"{config.LOCAL_DATA_DIR}/bench_{n}.parquet"

    # --- Ghi ---
    t0 = time.time()
    df.to_csv(csv_path, index=False)
    write_csv_time = time.time() - t0

    t0 = time.time()
    df.to_parquet(parquet_path, index=False)
    write_parquet_time = time.time() - t0

    # --- Dung lượng ---
    csv_size = os.path.getsize(csv_path) / (1024 ** 2)       # MB
    parquet_size = os.path.getsize(parquet_path) / (1024 ** 2)

    # --- Đọc toàn bộ ---
    t0 = time.time()
    pd.read_csv(csv_path)
    read_csv_time = time.time() - t0

    t0 = time.time()
    pd.read_parquet(parquet_path)
    read_parquet_time = time.time() - t0

    # --- Đọc có lọc (mô phỏng truy vấn thật) ---
    t0 = time.time()
    tmp = pd.read_csv(csv_path)
    _ = tmp[tmp["category"] == "san_pham"]
    filter_csv_time = time.time() - t0

    t0 = time.time()
    tmp = pd.read_parquet(parquet_path, filters=[("category", "==", "san_pham")])
    filter_parquet_time = time.time() - t0

    return {
        "n_rows": n,
        "csv_size_mb": round(csv_size, 2),
        "parquet_size_mb": round(parquet_size, 2),
        "write_csv_s": round(write_csv_time, 3),
        "write_parquet_s": round(write_parquet_time, 3),
        "read_csv_s": round(read_csv_time, 3),
        "read_parquet_s": round(read_parquet_time, 3),
        "filter_csv_s": round(filter_csv_time, 3),
        "filter_parquet_s": round(filter_parquet_time, 3),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=1_000_000)
    args = parser.parse_args()

    print(f"Benchmark CSV vs Parquet với {args.n:,} dòng...")
    result = benchmark(args.n)

    result_df = pd.DataFrame([result])
    out_path = f"{config.LOCAL_RESULTS_DIR}/tn1_csv_vs_parquet.csv"
    result_df.to_csv(out_path, index=False)

    print("\n=== KẾT QUẢ TN1 ===")
    print(result_df.T)
    print(f"\nĐã lưu kết quả vào {out_path}")
