import os
import streamlit as st

from utils import (
    DLQ_PATHS,
    SILVER_PATHS,
    find_layer_files,
    get_duckdb_connection,
    init_page_config,
    parquet_source_sql,
)


# 1. Cấu hình trang
init_page_config("Silver Layer")
con = get_duckdb_connection()

# 2. Tiêu đề
st.markdown(
    '<div class="title-gradient">Silver Layer | Clean Data & DLQ</div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="subtitle">Dữ liệu đã kiểm tra chất lượng và phân loại bản ghi lỗi</div>',
    unsafe_allow_html=True,
)

# 3. Chuẩn hóa đường dẫn dữ liệu Silver & DLQ
silver_pattern = find_layer_files("silver", SILVER_PATHS)
dlq_pattern = find_layer_files("errors", DLQ_PATHS)

silver_sql = parquet_source_sql(silver_pattern) if silver_pattern else None

dlq_sql = parquet_source_sql(dlq_pattern) if dlq_pattern else None

if silver_sql:
    try:
        # Tổng số bản ghi sạch Silver
        silver_count = con.sql(
            f"SELECT COUNT(*) FROM read_parquet({silver_sql}, union_by_name=True)"
        ).fetchone()[0]

        # Kiểm tra file DLQ
        dlq_count = 0
        if dlq_sql:
            try:
                dlq_count = con.sql(
                    f"SELECT COUNT(*) FROM (SELECT DISTINCT * EXCLUDE(logged_at, error_batch_id) FROM read_parquet({dlq_sql}, union_by_name=True))"
                ).fetchone()[0]
            except Exception:
                dlq_count = 0

        # Hiển thị KPI Cards
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(
                f"""
                <div class="glass-card">
                    <div class="kpi-label">Bản Ghi Sạch (Silver)</div>
                    <div class="kpi-value">{silver_count:,} <span style="font-size: 1rem; color: #64748B;">dòng</span></div>
                    <div class="kpi-trend trend-up">✓ Valid Records</div>
                </div>
            """,
                unsafe_allow_html=True,
            )
        with col2:
            st.markdown(
                f"""
                <div class="glass-card">
                    <div class="kpi-label">Bản Ghi Lỗi (DLQ)</div>
                    <div class="kpi-value">{dlq_count:,} <span style="font-size: 1rem; color: #64748B;">dòng</span></div>
                    <div class="kpi-trend trend-error">⚠️ Quarantined</div>
                </div>
            """,
                unsafe_allow_html=True,
            )

        st.write("---")

        # Hiển thị các Tab xem dữ liệu
        tab1, tab2 = st.tabs(["Clean records", "DLQ audit"])

        with tab1:
            df_silver = con.sql(
                f"SELECT * FROM read_parquet({silver_sql}, union_by_name=True) LIMIT 100"
            ).df()
            st.dataframe(df_silver, use_container_width=True)

        with tab2:
            if dlq_sql and dlq_count > 0:
                df_dlq = con.sql(
                    f"SELECT DISTINCT * FROM read_parquet({dlq_sql}, union_by_name=True) LIMIT 100"
                ).df()
                st.dataframe(df_dlq, use_container_width=True)
            else:
                st.info("Hiện không có bản ghi lỗi nào trong DLQ (Dead Letter Queue).")

    except Exception as e:
        st.error(f"Lỗi truy vấn Silver: {e}")
else:
    st.warning(
        "Chưa tìm thấy dữ liệu Silver. Hãy chạy script main_pipeline.py để tiến hành lọc dữ liệu!"
    )
