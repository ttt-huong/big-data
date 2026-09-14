"""
Vẽ biểu đồ đồ họa từ kết quả Benchmark ETL/ELT (TN1 -> TN4) để chèn vào báo cáo/slide.

Chạy: python 07_plot_results.py
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt

import config


def format_n(val):
    if val >= 1_000_000:
        return f"{val / 1_000_000:.0f}M" if val % 1_000_000 == 0 else f"{val / 1_000_000:.1f}M"
    if val >= 1_000:
        return f"{val / 1_000:.0f}K"
    return str(val)


def plot_etl_tn1_scale():
    csv_file = os.path.join(config.LOCAL_RESULTS_DIR, "etl_tn1_scale.csv")
    if not os.path.exists(csv_file):
        print(f"[CẢNH BÁO] Không tìm thấy {csv_file}. Bỏ qua TN1.")
        return

    df = pd.read_csv(csv_file)
    x_labels = [format_n(v) for v in df["n_rows"]]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(x_labels, df["extract_time_s"], label="Extract Time (s)", color="#3498db")
    ax.bar(x_labels, df["transform_time_s"], bottom=df["extract_time_s"], label="Transform Time (s)", color="#e74c3c")
    ax.bar(x_labels, df["load_time_s"], bottom=df["extract_time_s"] + df["transform_time_s"], label="Load Time (s)", color="#2ecc71")

    ax.set_xlabel("Số lượng bản ghi (Rows)")
    ax.set_ylabel("Thời gian xử lý (Giây)")
    ax.set_title("TN1: Thời gian xử lý Pipeline ETL theo Quy mô dữ liệu")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    out_path = os.path.join(config.LOCAL_RESULTS_DIR, "chart_etl_tn1_scale.png")
    plt.savefig(out_path, dpi=150)
    print(f"Đã lưu biểu đồ TN1 vào {out_path}")


def plot_etl_tn2_full_vs_inc():
    csv_file = os.path.join(config.LOCAL_RESULTS_DIR, "etl_tn2_full_vs_inc.csv")
    if not os.path.exists(csv_file):
        print(f"[CẢNH BÁO] Không tìm thấy {csv_file}. Bỏ qua TN2.")
        return

    df = pd.read_csv(csv_file)
    fig, ax1 = plt.subplots(figsize=(7, 5))

    color = '#1f77b4'
    ax1.set_xlabel('Chế độ Load')
    ax1.set_ylabel('Thời gian thực thi (Giây)', color=color)
    bars = ax1.bar(df['load_mode'], df['total_time_s'], color=['#e67e22', '#27ae60'], width=0.4)
    ax1.tick_params(axis='y', labelcolor=color)

    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height, f'{height:.3f}s', ha='center', va='bottom')

    ax1.set_title("TN2: So sánh Hiệu năng Full Load vs Incremental Load")
    plt.tight_layout()
    out_path = os.path.join(config.LOCAL_RESULTS_DIR, "chart_etl_tn2_full_vs_inc.png")
    plt.savefig(out_path, dpi=150)
    print(f"Đã lưu biểu đồ TN2 vào {out_path}")


def plot_etl_tn3_clean_vs_dirty():
    csv_file = os.path.join(config.LOCAL_RESULTS_DIR, "etl_tn3_clean_vs_dirty.csv")
    if not os.path.exists(csv_file):
        print(f"[CẢNH BÁO] Không tìm thấy {csv_file}. Bỏ qua TN3.")
        return

    df = pd.read_csv(csv_file)
    fig, ax = plt.subplots(figsize=(7, 5))

    ax.bar(df["dataset_type"], df["valid_records"], label="Bản ghi Sạch (Valid)", color="#2ecc71", width=0.4)
    ax.bar(df["dataset_type"], df["error_records"], bottom=df["valid_records"], label="Bản ghi Lỗi (Error)", color="#e74c3c", width=0.4)

    ax.set_ylabel("Số lượng bản ghi")
    ax.set_title("TN3: Khả năng Phát hiện & Phân loại Dữ liệu Lỗi")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    out_path = os.path.join(config.LOCAL_RESULTS_DIR, "chart_etl_tn3_clean_vs_dirty.png")
    plt.savefig(out_path, dpi=150)
    print(f"Đã lưu biểu đồ TN3 vào {out_path}")


def plot_etl_tn4_batch_size():
    csv_file = os.path.join(config.LOCAL_RESULTS_DIR, "etl_tn4_batch_size.csv")
    if not os.path.exists(csv_file):
        print(f"[CẢNH BÁO] Không tìm thấy {csv_file}. Bỏ qua TN4.")
        return

    df = pd.read_csv(csv_file)
    fig, ax = plt.subplots(figsize=(7, 5))

    labels = [format_n(b) for b in df["batch_size"]]
    ax.plot(labels, df["records_per_second"], marker="o", linewidth=2, color="#8e44ad")

    ax.set_xlabel("Batch Size")
    ax.set_ylabel("Tốc độ xử lý (Bản ghi / Giây)")
    ax.set_title("TN4: Tốc độ xử lý theo Kích thước Batch Size")
    ax.grid(alpha=0.3)

    for i, txt in enumerate(df["records_per_second"]):
        ax.annotate(f"{txt:,.0f} r/s", (labels[i], df["records_per_second"][i] + 500), ha="center")

    plt.tight_layout()
    out_path = os.path.join(config.LOCAL_RESULTS_DIR, "chart_etl_tn4_batch_size.png")
    plt.savefig(out_path, dpi=150)
    print(f"Đã lưu biểu đồ TN4 vào {out_path}")


if __name__ == "__main__":
    plot_etl_tn1_scale()
    plot_etl_tn2_full_vs_inc()
    plot_etl_tn3_clean_vs_dirty()
    plot_etl_tn4_batch_size()
    print("\n[THÀNH CÔNG] Tất cả các biểu đồ PNG mới đã xuất vào thư mục results/!")
