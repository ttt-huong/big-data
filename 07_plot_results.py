"""
Vẽ biểu đồ từ kết quả benchmark TN1/TN2/TN3 để đưa vào báo cáo/slide.

Chạy: python 07_plot_results.py
(cần chạy 03_tn2_partitioning.py và 04_tn3_scale_benchmark.py trước)
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


def plot_tn3_scale():
    csv_file = f"{config.LOCAL_RESULTS_DIR}/tn3_scale_benchmark.csv"
    if not os.path.exists(csv_file):
        print(f"[CẢNH BÁO] Không tìm thấy {csv_file}. Bỏ qua vẽ biểu đồ TN3.")
        print("Vui lòng chạy script trước: python 04_tn3_scale_benchmark.py")
        return

    df = pd.read_csv(csv_file)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    x_labels = [format_n(v) for v in df["n_rows"]]

    axes[0].plot(x_labels, df["csv_size_mb"], marker="o", label="CSV")
    axes[0].plot(x_labels, df["parquet_size_mb"], marker="o", label="Parquet")
    axes[0].set_xlabel("Số dòng dữ liệu")
    axes[0].set_ylabel("Dung lượng (MB)")
    axes[0].set_title("Dung lượng: CSV vs Parquet theo quy mô")
    axes[0].legend()
    axes[0].grid(alpha=0.3)

    axes[1].plot(x_labels, df["read_csv_s"], marker="o", label="Đọc CSV")
    axes[1].plot(x_labels, df["read_parquet_s"], marker="o", label="Đọc Parquet")
    axes[1].set_xlabel("Số dòng dữ liệu")
    axes[1].set_ylabel("Thời gian (giây)")
    axes[1].set_title("Thời gian đọc: CSV vs Parquet theo quy mô")
    axes[1].legend()
    axes[1].grid(alpha=0.3)

    plt.tight_layout()
    out_path = f"{config.LOCAL_RESULTS_DIR}/chart_tn3_scale.png"
    plt.savefig(out_path, dpi=150)
    print(f"Đã lưu biểu đồ TN3 vào {out_path}")


def plot_tn2_partition():
    csv_file = f"{config.LOCAL_RESULTS_DIR}/tn2_partitioning.csv"
    if not os.path.exists(csv_file):
        print(f"[CẢNH BÁO] Không tìm thấy {csv_file}. Bỏ qua vẽ biểu đồ TN2.")
        print("Vui lòng chạy script trước: python 03_tn2_partitioning.py")
        return

    df = pd.read_csv(csv_file)

    fig, ax = plt.subplots(figsize=(6, 5))
    row = df.iloc[0]
    ax.bar(["Không partition", "Có partition"],
           [row["no_partition_query_s"], row["partition_query_s"]],
           color=["#D85A30", "#1D9E75"])
    ax.set_ylabel("Thời gian truy vấn (giây)")
    ax.set_title(f"Hiệu quả Partitioning (n={format_n(int(row['n_rows']))})")
    for i, v in enumerate([row["no_partition_query_s"], row["partition_query_s"]]):
        ax.text(i, v, f"{v:.4f}s", ha="center", va="bottom")

    plt.tight_layout()
    out_path = f"{config.LOCAL_RESULTS_DIR}/chart_tn2_partition.png"
    plt.savefig(out_path, dpi=150)
    print(f"Đã lưu biểu đồ TN2 vào {out_path}")


if __name__ == "__main__":
    plot_tn3_scale()
    plot_tn2_partition()
    print("\nCác file .png trong thư mục results/ dùng để chèn trực tiếp vào báo cáo/slide.")
