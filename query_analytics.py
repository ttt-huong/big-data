"""
DUCKDB SQL ANALYTICS FOR ETL TARGET DATA STORE & ERROR RECORDS
Truy vấn phân tích dữ liệu sau khi được xử lý bởi Pipeline ETL.

Chạy: python query_analytics.py
"""

import os
import duckdb
from deltalake import DeltaTable

import config


def query_target_store():
    con = duckdb.connect()
    
    print("\n=======================================================")
    print("   DUCKDB SQL ANALYTICS ON ETL TARGET DATA STORE")
    print("=======================================================")

    # 1. Truy vấn Delta Table trên MinIO nếu có, hoặc Parquet local fallback
    try:
        dt = DeltaTable(config.TARGET_DELTA_URI, storage_options=config.DELTA_STORAGE_OPTIONS)
        dataset = dt.to_pyarrow_dataset()
        con.register("target_data", dataset)
        source_name = f"Delta Table ({config.TARGET_DELTA_URI})"
    except Exception as e:
        fallback_path = os.path.join(config.LOCAL_DATA_DIR, "clean_dataset_fallback")
        if os.path.exists(fallback_path):
            fallback_sql = fallback_path.replace("\\", "/")
            con.execute(f"CREATE VIEW target_data AS SELECT * FROM read_parquet('{fallback_sql}/**/*.parquet')")
            source_name = f"Parquet Fallback ({fallback_path})"
        else:
            print(f"[CẢNH BÁO] Chưa có dữ liệu trong Target Store ({e}). Vui lòng chạy main_pipeline.py trước.")
            return

    print(f"\n[Nguồn dữ liệu]: {source_name}")

    print("\n--- 1. Thống kê tổng số bản ghi SẠCH đã nạp vào Target Store ---")
    con.sql("SELECT COUNT(*) AS total_clean_records FROM target_data").show()

    print("--- 2. Phân bố bản ghi và dung lượng trung bình theo Category ---")
    con.sql("""
        SELECT category, COUNT(*) AS num_images, ROUND(AVG(size_mb), 2) AS avg_size_mb
        FROM target_data
        GROUP BY category
        ORDER BY num_images DESC
    """).show()

    print("--- 3. Thống kê theo năm/tháng của thuộc tính created_at ---")
    con.sql("""
        SELECT year, month, COUNT(*) AS num_images
        FROM target_data
        WHERE year = 2025
        GROUP BY year, month
        ORDER BY month
        LIMIT 6
    """).show()

    # 2. Tạo Semantic Views cho truy vấn nhanh
    con.execute("CREATE OR REPLACE VIEW v_clean_summary AS SELECT category, format, COUNT(*) AS num_images, ROUND(AVG(size_mb), 2) AS avg_size_mb, ROUND(AVG(aspect_ratio), 2) AS avg_aspect_ratio FROM target_data GROUP BY category, format")

    print("\n--- 4. Phân tích tỉ lệ khung hình (Aspect Ratio) & Format qua Semantic View ---")
    con.sql("SELECT category, format, num_images, avg_size_mb, avg_aspect_ratio FROM v_clean_summary ORDER BY num_images DESC LIMIT 10").show()

    print("\n--- 5. Thống kê Dữ liệu Muộn (Late-Arriving Data Tagging) ---")
    try:
        con.sql("""
            SELECT is_late_arriving, COUNT(*) AS num_records
            FROM target_data
            GROUP BY is_late_arriving
        """).show()
    except Exception as e:
        print(f"Chưa có cột is_late_arriving trong dataset cũ: {e}")

    # 3. Truy vấn Bảng Dữ Liệu Lỗi (Error Records Store)
    error_dir = os.path.splitext(config.ERROR_RECORDS_PATH)[0]
    if os.path.exists(error_dir) or os.path.exists(config.ERROR_RECORDS_PATH):
        print("\n-------------------------------------------------------")
        print("   THỐNG KÊ DỮ LIỆU LỖI (ERROR RECORDS STORE)")
        print("-------------------------------------------------------")
        
        normalized_error_dir = error_dir.replace("\\", "/")
        normalized_default_error = config.ERROR_RECORDS_PATH.replace("\\", "/")
        path_to_read = f"{normalized_error_dir}/*.parquet" if os.path.exists(error_dir) and os.listdir(error_dir) else normalized_default_error
        print(f"\n--- Top các lý do dữ liệu bị từ chối/bị lỗi (Nguồn: {path_to_read}) ---")
        con.sql(f"""
            SELECT error_reason, COUNT(*) AS count
            FROM read_parquet('{path_to_read}')
            GROUP BY error_reason
            ORDER BY count DESC
        """).show()
    else:
        print("\nChưa có bản ghi lỗi nào trong error_records.")



if __name__ == "__main__":
    query_target_store()
