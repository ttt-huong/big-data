"""
TN3: Tăng quy mô dữ liệu và đo lại toàn bộ benchmark ở từng mức.
Chạy lần lượt qua các mốc trong config.SCALE_LEVELS, gộp kết quả TN1 lại thành 1 bảng.

Lưu ý: mức 5M-10M có thể tốn vài phút và vài GB RAM/đĩa tùy máy.
Nếu máy yếu, chỉnh config.SCALE_LEVELS còn [100_000, 1_000_000] là đủ thuyết phục.

Chạy: python 04_tn3_scale_benchmark.py
"""

import os
import sys
import gc
import pandas as pd
from importlib import import_module

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

tn1 = import_module("02_tn1_csv_vs_parquet")
import config



def run_scale_benchmark():
    all_results = []
    for n in config.SCALE_LEVELS:
        print(f"\n{'=' * 50}")
        print(f"Đang benchmark với quy mô {n:,} dòng...")
        print("=" * 50)
        
        gc.collect()
        try:
            result = tn1.benchmark(n)
            all_results.append(result)
            print(pd.DataFrame([result]).T)
        except Exception as e:
            print(f"Lỗi khi chạy mốc quy mô {n:,}: {e}")
            all_results.append({"n_rows": n, "error": str(e)})

        # Dọn dẹp file tạm để tiết kiệm ổ đĩa
        for fmt in ["csv", "parquet"]:
            fpath = os.path.join(config.LOCAL_DATA_DIR, f"bench_{n}.{fmt}")
            if os.path.exists(fpath):
                try:
                    os.remove(fpath)
                except Exception:
                    pass

    result_df = pd.DataFrame(all_results)
    out_path = os.path.join(config.LOCAL_RESULTS_DIR, "tn3_scale_benchmark.csv")
    result_df.to_csv(out_path, index=False)

    print("\n=== TỔNG HỢP TN3 (tất cả quy mô) ===")
    print(result_df)
    print(f"\nĐã lưu vào {out_path}")
    print("Dùng script 07_plot_results.py để vẽ biểu đồ từ file này.")
    return result_df


if __name__ == "__main__":
    run_scale_benchmark()
