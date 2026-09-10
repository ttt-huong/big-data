"""
TN4: Query/Analytics - dùng DuckDB đọc trực tiếp file Parquet và chạy các câu SQL phân tích.

Chạy: python 05_tn4_query_duckdb.py --n 1000000
"""

import argparse
import os
import sys
import duckdb
import config


def run(n: int):
    parquet_path = f"{config.LOCAL_DATA_DIR}/metadata_{n}.parquet"
    if not os.path.exists(parquet_path):
        print(f"[LỖI] Không tìm thấy file {parquet_path}.")
        print(f"Vui lòng chạy bước 1 trước: python 01_generate_metadata.py --n {n}")
        sys.exit(1)

    parquet_path_sql = parquet_path.replace("\\", "/")
    con = duckdb.connect()

    print(f"\n=== TN4: Truy vấn metadata ({n:,} dòng) từ {parquet_path} ===\n")

    print("--- Query 1: Tổng số ảnh ---")
    con.sql(f"SELECT COUNT(*) AS total_images FROM read_parquet('{parquet_path_sql}')").show()

    print("--- Query 2: Số lượng ảnh theo từng category (GROUP BY) ---")
    con.sql(f"""
        SELECT category, COUNT(*) AS num_images, ROUND(AVG(file_size)/1024, 1) AS avg_size_kb
        FROM read_parquet('{parquet_path_sql}')
        GROUP BY category
        ORDER BY num_images DESC
    """).show()

    print("--- Query 3: Lọc ảnh theo năm/tháng (FILTER trên partition key) ---")
    con.sql(f"""
        SELECT year, month, COUNT(*) AS num_images
        FROM read_parquet('{parquet_path_sql}')
        WHERE year = 2025
        GROUP BY year, month
        ORDER BY month
    """).show()

    print("--- Query 4: Top 5 định dạng ảnh chiếm nhiều dung lượng nhất ---")
    con.sql(f"""
        SELECT format, SUM(file_size)/1024/1024 AS total_mb
        FROM read_parquet('{parquet_path_sql}')
        GROUP BY format
        ORDER BY total_mb DESC
        LIMIT 5
    """).show()

    print("--- Query 5: Ảnh có kích thước lớn hơn 1000px mỗi chiều ---")
    con.sql(f"""
        SELECT COUNT(*) AS large_images
        FROM read_parquet('{parquet_path_sql}')
        WHERE width >= 1024 AND height >= 1024
    """).show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=1_000_000,
                         help="Phải khớp với file đã sinh ở bước 01 (metadata_<n>.parquet)")
    args = parser.parse_args()
    run(args.n)
