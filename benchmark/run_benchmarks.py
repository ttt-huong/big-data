"""
BENCHMARK SUITE CHO HỆ THỐNG ETL/ELT PIPELINE
Thực thi 4 thí nghiệm đánh giá hiệu năng chuyên sâu:
- TN1: Thời gian xử lý Pipeline theo Quy mô Dữ liệu (100K -> 5M)
- TN2: So sánh Full Load vs Incremental Load
- TN3: So sánh xử lý Dữ liệu Sạch vs Dữ liệu Lỗi
- TN4: Ảnh hưởng của Batch Size (1K, 10K, 50K)

Chạy: python -m benchmark.run_benchmarks
hoặc: python benchmark/run_benchmarks.py
"""

import os
import sys
import time
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from generator.data_generator import generate_clean_metadata, generate_dirty_metadata
from main_pipeline import run_pipeline


def run_tn1_scale():
    """TN1: Benchmark Thời gian xử lý Pipeline theo quy mô dữ liệu."""
    print("\n" + "="*60)
    print("THÍ NGHIỆM 1: THỜI GIAN XỬ LÝ THEO QUY MÔ DỮ LIỆU (100K -> 5M)")
    print("="*60)
    
    scales = [100_000, 500_000, 1_000_000]  # Thêm 5_000_000 nếu máy đủ mạnh
    results = []

    for n in scales:
        print(f"\n--- Benchmark Scale n = {n:,} bản ghi ---")
        raw_data = generate_dirty_metadata(n, error_ratio=0.05, seed=42)
        metrics = run_pipeline(raw_data, mode="full")
        
        results.append({
            "n_rows": n,
            "extract_time_s": metrics.extract_time_s,
            "transform_time_s": metrics.transform_time_s,
            "load_time_s": metrics.load_time_s,
            "total_time_s": metrics.total_time_s,
            "valid_records": metrics.valid_records,
            "error_records": metrics.error_records,
        })

    df_res = pd.DataFrame(results)
    out_file = os.path.join(config.LOCAL_RESULTS_DIR, "etl_tn1_scale.csv")
    df_res.to_csv(out_file, index=False)
    print(f"\n[TN1 DONE] Kết quả đã lưu vào {out_file}:")
    print(df_res)


def run_tn2_full_vs_incremental():
    """TN2: Benchmark So sánh Full Load vs Incremental Load."""
    print("\n" + "="*60)
    print("THÍ NGHIỆM 2: FULL LOAD VS INCREMENTAL LOAD")
    print("="*60)

    # 1. Full Load với 1,000,000 bản ghi
    print("\n--- 1. Thực hiện Full Load (1,000,000 bản ghi) ---")
    raw_full = generate_dirty_metadata(1_000_000, error_ratio=0.05, seed=42, start_id=1)
    metrics_full = run_pipeline(raw_full, mode="full")

    # 2. Incremental Load chỉ thêm 20,000 bản ghi mới
    print("\n--- 2. Thực hiện Incremental Load (20,000 bản ghi mới) ---")
    raw_inc = generate_dirty_metadata(20_000, error_ratio=0.05, seed=999, start_id=1_000_001)
    metrics_inc = run_pipeline(raw_inc, mode="incremental")

    results = [
        {
            "load_mode": "Full Load",
            "extracted_records": metrics_full.extracted_records,
            "loaded_records": metrics_full.loaded_records,
            "total_time_s": metrics_full.total_time_s,
        },
        {
            "load_mode": "Incremental Load",
            "extracted_records": metrics_inc.extracted_records,
            "loaded_records": metrics_inc.loaded_records,
            "total_time_s": metrics_inc.total_time_s,
        }
    ]

    df_res = pd.DataFrame(results)
    out_file = os.path.join(config.LOCAL_RESULTS_DIR, "etl_tn2_full_vs_inc.csv")
    df_res.to_csv(out_file, index=False)
    print(f"\n[TN2 DONE] Kết quả đã lưu vào {out_file}:")
    print(df_res)


def run_tn3_clean_vs_dirty():
    """TN3: Benchmark Dữ liệu Sạch vs Dữ liệu Lỗi."""
    print("\n" + "="*60)
    print("THÍ NGHIỆM 3: DỮ LIỆU SẠCH VS DỮ LIỆU CÓ LỖI")
    print("="*60)

    n = 500_000
    # 1. Dataset 100% Sạch
    print(f"\n--- 1. Chạy với Dataset 100% SẠCH ({n:,} dòng) ---")
    clean_raw = generate_clean_metadata(n, seed=42)
    metrics_clean = run_pipeline(clean_raw, mode="full")

    # 2. Dataset 10% Lỗi
    print(f"\n--- 2. Chạy với Dataset 10% LỖI ({n:,} dòng) ---")
    dirty_raw = generate_dirty_metadata(n, error_ratio=0.10, seed=42)
    metrics_dirty = run_pipeline(dirty_raw, mode="full")

    results = [
        {
            "dataset_type": "Dữ liệu 100% Sạch",
            "extracted_records": metrics_clean.extracted_records,
            "valid_records": metrics_clean.valid_records,
            "error_records": metrics_clean.error_records,
            "transform_time_s": metrics_clean.transform_time_s,
            "total_time_s": metrics_clean.total_time_s,
        },
        {
            "dataset_type": "Dữ liệu 10% Lỗi",
            "extracted_records": metrics_dirty.extracted_records,
            "valid_records": metrics_dirty.valid_records,
            "error_records": metrics_dirty.error_records,
            "transform_time_s": metrics_dirty.transform_time_s,
            "total_time_s": metrics_dirty.total_time_s,
        }
    ]

    df_res = pd.DataFrame(results)
    out_file = os.path.join(config.LOCAL_RESULTS_DIR, "etl_tn3_clean_vs_dirty.csv")
    df_res.to_csv(out_file, index=False)
    print(f"\n[TN3 DONE] Kết quả đã lưu vào {out_file}:")
    print(df_res)


def run_tn4_batch_size():
    """TN4: Benchmark Ảnh hưởng của Batch Size."""
    print("\n" + "="*60)
    print("THÍ NGHIỆM 4: ẢNH HƯỞNG CỦA BATCH SIZE (1K, 10K, 50K)")
    print("="*60)

    n = 200_000
    raw_data = generate_dirty_metadata(n, error_ratio=0.05, seed=42)
    batch_sizes = [1_000, 10_000, 50_000]
    results = []

    for b in batch_sizes:
        print(f"\n--- Chạy thử với Batch Size = {b:,} ---")
        metrics = run_pipeline(raw_data, mode="full", batch_size=b)
        results.append({
            "batch_size": b,
            "extracted_records": metrics.extracted_records,
            "total_time_s": metrics.total_time_s,
            "records_per_second": round(metrics.extracted_records / metrics.total_time_s, 1) if metrics.total_time_s > 0 else 0
        })

    df_res = pd.DataFrame(results)
    out_file = os.path.join(config.LOCAL_RESULTS_DIR, "etl_tn4_batch_size.csv")
    df_res.to_csv(out_file, index=False)
    print(f"\n[TN4 DONE] Kết quả đã lưu vào {out_file}:")
    print(df_res)


if __name__ == "__main__":
    print("\n=======================================================")
    print("   BẮT ĐẦU CHẠY BENCHMARK SUITE TỔNG HỢP CHO ETL PIPELINE")
    print("=======================================================")
    run_tn1_scale()
    run_tn2_full_vs_incremental()
    run_tn3_clean_vs_dirty()
    run_tn4_batch_size()
    print("\n[CHÚC MỪNG] Tất cả 4 bài thí nghiệm ETL đã hoàn thành!")
    print("Dùng lệnh `python 07_plot_results.py` để xuất biểu đồ đồ họa.")
