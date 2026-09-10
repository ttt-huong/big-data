"""
BƯỚC 1: Sinh dữ liệu metadata giả lập, quy mô lớn.

QUAN TRỌNG: dùng numpy/pandas vectorized thay vì vòng lặp for từng dòng,
vì với hàng triệu dòng, vòng lặp Python thuần sẽ rất chậm (có thể mất hàng giờ).

Cột: image_id, filename, file_size, width, height, format, category, created_at, year, month

Chạy: python 01_generate_metadata.py --n 1000000
"""

import argparse
import time
import numpy as np
import pandas as pd

import config


def generate_metadata(n: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    image_id = np.arange(1, n + 1)
    filename = np.array([f"img_{i:07d}.jpg" for i in image_id])

    file_size = rng.integers(10_000, 5_000_000, size=n)          # bytes
    width = rng.choice([64, 128, 256, 512, 1024], size=n)
    height = rng.choice([64, 128, 256, 512, 1024], size=n)
    fmt = rng.choice(config.FORMATS, size=n)
    category = rng.choice(config.CATEGORIES, size=n)

    start = pd.Timestamp(config.DATE_RANGE_START).value // 10**9
    end = pd.Timestamp(config.DATE_RANGE_END).value // 10**9
    created_at_unix = rng.integers(start, end, size=n)
    created_at = pd.to_datetime(created_at_unix, unit="s")

    df = pd.DataFrame({
        "image_id": image_id,
        "filename": filename,
        "file_size": file_size,
        "width": width,
        "height": height,
        "format": fmt,
        "category": category,
        "created_at": created_at,
    })
    df["year"] = df["created_at"].dt.year
    df["month"] = df["created_at"].dt.month
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=100_000, help="Số dòng metadata cần sinh")
    parser.add_argument("--out", type=str, default=None, help="Đường dẫn file output (parquet)")
    args = parser.parse_args()

    print(f"Đang sinh {args.n:,} dòng metadata...")
    t0 = time.time()
    df = generate_metadata(args.n)
    elapsed = time.time() - t0
    print(f"Sinh xong trong {elapsed:.2f} giây.")

    out_path = args.out or f"{config.LOCAL_DATA_DIR}/metadata_{args.n}.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Đã lưu vào {out_path}")
    print(df.head())
