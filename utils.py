import glob
import os
import duckdb
import streamlit as st
import config
from storage import download_objects, enabled as object_storage_enabled

# Dùng đường dẫn tuyệt đối để dashboard hoạt động dù Streamlit được khởi
# động từ thư mục nào.
DATA_ROOT = config.LOCAL_DATA_DIR
RESULTS_ROOT = config.LOCAL_RESULTS_DIR

BRONZE_PATHS = [
    os.path.join(DATA_ROOT, "raw", "*.parquet"),
    os.path.join(DATA_ROOT, "raw", "**", "*.parquet"),
]

# Silver = Delta Lake table mà Core ETL ghi ra (data/lakehouse/clean_metadata/)
SILVER_PATHS = [
    os.path.join(DATA_ROOT, "lakehouse", "clean_metadata", "**", "*.parquet"),
    os.path.join(DATA_ROOT, "lakehouse", "clean_metadata", "*.parquet"),
]

# DLQ = các file Parquet lỗi trong data/errors/ (mỗi batch tạo 1 file)
DLQ_PATHS = [
    os.path.join(DATA_ROOT, "errors", "*.parquet"),
]

GOLD_PATHS = [
    os.path.join(DATA_ROOT, "gold", "*.parquet"),
]

BENCHMARK_TN1 = os.path.join(RESULTS_ROOT, "etl_tn1_scale.csv")
BENCHMARK_TN4 = os.path.join(RESULTS_ROOT, "etl_tn4_batch_size.csv")



def get_clean_parquet_files(path_list):
    """
    Lấy danh sách các file Parquet thực sự chứa dữ liệu,
    TỰ ĐỘNG BỎ QUA các file log/checkpoint của Delta Lake (_delta_log).
    """
    matched_files = []
    for p in path_list:
        files = glob.glob(p, recursive=True)
        # Bỏ qua hoàn toàn các file nằm trong thư mục _delta_log
        clean_files = [
            f for f in files
            if "_delta_log" not in f and os.path.isfile(f)
        ]
        if clean_files:
            matched_files.extend(clean_files)

    # Loại bỏ file trùng lặp
    seen = set()
    unique_files = [f for f in matched_files if not (f in seen or seen.add(f))]
    return unique_files


def find_valid_pattern(path_list):
    """
    Trả về danh sách các file parquet sạch (kiểu list) để DuckDB đọc trực tiếp
    """
    files = get_clean_parquet_files(path_list)
    return files if files else None


def _ensure_gold_summary():
    """Tự động tổng hợp Gold summary từ Delta Lake nếu tầng Gold chưa có dữ liệu."""
    silver_files = find_valid_pattern(SILVER_PATHS)
    if not silver_files:
        return
    gold_dir = os.path.join(DATA_ROOT, "gold")
    os.makedirs(gold_dir, exist_ok=True)
    gold_file = os.path.join(gold_dir, "summary_by_category.parquet")
    con = duckdb.connect(":memory:")
    sql_src = parquet_source_sql(silver_files)
    con.sql(f"""
        COPY (
            SELECT 
                category,
                format,
                year,
                month,
                COUNT(*) AS total_records,
                AVG(file_size) AS avg_file_size,
                SUM(file_size) AS total_storage_bytes
            FROM read_parquet({sql_src}, union_by_name=True)
            GROUP BY category, format, year, month
        ) TO '{gold_file.replace(chr(92), "/")}' (FORMAT PARQUET)
    """)


def find_layer_files(layer: str, fallback_paths):
    """Lấy file của một tầng từ object storage, fallback về local."""
    if object_storage_enabled():
        prefix = getattr(config, "S3_PREFIXES", {}).get(layer, layer)
        try:
            remote_files = download_objects(prefix)
            if remote_files:
                return remote_files
        except Exception as error:
            st.warning(f"Không đọc được object storage ({layer}), dùng dữ liệu local: {error}")
    files = find_valid_pattern(fallback_paths)
    if not files and layer == "gold":
        try:
            _ensure_gold_summary()
            files = find_valid_pattern(fallback_paths)
        except Exception:
            pass
    return files



def parquet_source_sql(paths):
    """Tạo biểu thức DuckDB an toàn cho một hoặc nhiều file Parquet."""
    values = paths if isinstance(paths, list) else [paths]
    escaped = [
        "'" + str(path).replace("\\", "/").replace("'", "''") + "'"
        for path in values
    ]
    return "[" + ", ".join(escaped) + "]"


def init_page_config(page_title, page_icon=None):
    """Cấu hình giao diện dùng chung cho toàn bộ dashboard."""
    st.set_page_config(
        page_title=page_title, page_icon=page_icon, layout="wide"
    )

    st.markdown(
        """
        <style>
            html, body, [class*="css"] {
                font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
                background-color: #F5F7FB;
            }

            .block-container {
                max-width: 1440px;
                padding-top: 2.5rem;
                padding-bottom: 3rem;
            }

            [data-testid="stSidebar"] {
                background: #FFFFFF;
                border-right: 1px solid #E5E7EB;
            }

            [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
                color: #475569;
            }

            .title-gradient {
                font-weight: 800;
                font-size: 2.35rem;
                color: #172554;
                letter-spacing: -0.035em;
                margin-bottom: 0.35rem;
            }

            .subtitle {
                color: #64748B;
                font-size: 1rem;
                font-weight: 400;
                margin-bottom: 1.75rem;
            }

            .glass-card {
                background: #FFFFFF;
                border: 1px solid #E2E8F0;
                border-radius: 12px;
                padding: 22px 24px;
                box-shadow: 0 4px 18px rgba(15, 23, 42, 0.05);
            }

            .kpi-label {
                font-size: 0.78rem;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                color: #64748B;
            }

            .kpi-value {
                font-size: 2.4rem;
                font-weight: 800;
                color: #0F172A;
                margin: 8px 0 12px;
            }

            .kpi-trend {
                font-size: 0.82rem;
                font-weight: 600;
                padding: 5px 9px;
                border-radius: 6px;
                display: inline-block;
            }
            .trend-up { background: #ECFDF5; color: #047857; }
            .trend-error { background: #FEF2F2; color: #B91C1C; }

            div[data-testid="stDataFrame"] {
                border: 1px solid #E2E8F0;
                border-radius: 10px;
            }

            button[kind="secondary"] {
                border-color: #CBD5E1;
                color: #334155;
            }
        </style>
    """,
        unsafe_allow_html=True,
    )


def get_duckdb_connection():
    """Tạo connection mới cho mỗi lần render.

    Không cache connection: nếu một truy vấn trước đó lỗi, DuckDB có thể giữ
    transaction ở trạng thái aborted và làm các truy vấn sau lỗi theo.
    """
    return duckdb.connect(":memory:")
