import os
import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

import config
from utils import (
    DLQ_PATHS,
    SILVER_PATHS,
    find_layer_files,
    init_page_config,
    parquet_source_sql,
)

# 1. Cấu hình trang chính
init_page_config("Lakehouse Control Center")

# Header chính
st.markdown('<div class="title-gradient">Data Lakehouse Control Center</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Giám sát pipeline, chất lượng dữ liệu và các lớp Medallion</div>', unsafe_allow_html=True)

# Sidebar chung
with st.sidebar:
    st.markdown("### System overview")
    st.success("DuckDB engine active")
    st.info(f"Storage: {config.STORAGE_BACKEND} + Delta Lake / Parquet")
    if st.button("Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# Kết nối DuckDB
con = duckdb.connect()

# Tạo 4 Tab quản lý tập trung
tab_overview, tab_gold, tab_dlq, tab_perf = st.tabs([
    "Overview",
    "Gold analytics",
    "DLQ audit",
    "Performance",
])

# ==========================================
# TAB 0: TỔNG QUAN KIẾN TRÚC & CÔNG NGHỆ
# ==========================================
with tab_overview:
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
            <div class="glass-card">
                <h3>Mô hình Medallion Architecture</h3>
                <p>Sử dụng thanh bên trái (Sidebar) để điều hướng chi tiết qua các tầng:</p>
                <ul>
                    <li><b>Bronze:</b> Dữ liệu thô (Raw Ingestion).</li>
                    <li><b>Silver:</b> Dữ liệu làm sạch & Kiểm soát lỗi (DLQ).</li>
                    <li><b>Gold:</b> Dữ liệu tổng hợp phục vụ Business Intelligence.</li>
                </ul>
            </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
            <div class="glass-card">
                <h3>Công nghệ Cốt lõi</h3>
                <ul>
                    <li><b>Processing:</b> Python, Pandas</li>
                    <li><b>Storage:</b> Delta Lake (delta-rs / MinIO)</li>
                    <li><b>Query Engine:</b> DuckDB (In-process OLAP)</li>
                    <li><b>UI/UX:</b> Streamlit (Modern Multi-page)</li>
                </ul>
            </div>
        """, unsafe_allow_html=True)

# ==========================================
# TAB 1: GOLD LAYER ANALYTICS
# ==========================================
with tab_gold:
    st.header("Clean data overview")
    
    silver_files = find_layer_files("silver", SILVER_PATHS)
    if silver_files:
        try:
            silver_sql = parquet_source_sql(silver_files)
            df_clean = con.sql(
                f"SELECT * FROM read_parquet({silver_sql}, union_by_name=True)"
            ).df()
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Tổng bản ghi sạch", f"{len(df_clean):,}")
            if 'file_size' in df_clean.columns:
                col2.metric("Dung lượng TB", f"{df_clean['file_size'].mean() / (1024*1024):.2f} MB")
            col3.metric("Số lượng Category", df_clean['category'].nunique() if 'category' in df_clean.columns else 0)

            st.subheader("Phân bố Ảnh theo Category")
            if 'category' in df_clean.columns:
                fig = px.bar(df_clean['category'].value_counts().reset_index(), x='category', y='count', color='category')
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.warning(f"Chưa có dữ liệu trong Target Lakehouse: {e}")
    else:
        st.info("Chưa tìm thấy dữ liệu target. Hãy chạy `python main_pipeline.py` trước.")

# ==========================================
# TAB 2: ERROR RECORDS AUDIT (DLQ)
# ==========================================
with tab_dlq:
    st.header("Rejected records | Dead Letter Queue")
    error_files = find_layer_files("errors", DLQ_PATHS)
    if error_files:
        try:
            error_sql = parquet_source_sql(error_files)
            df_err = con.sql(
                f"SELECT * FROM read_parquet({error_sql}, union_by_name=True)"
            ).df()
            
            st.warning(f"Tổng số bản ghi lỗi phát hiện: {len(df_err):,}")
            
            st.subheader("Top lý do dữ liệu bị loại bỏ")
            if 'error_reason' in df_err.columns:
                reason_counts = df_err['error_reason'].value_counts().reset_index()
                fig_err = px.pie(reason_counts, values='count', names='error_reason', hole=0.4)
                st.plotly_chart(fig_err, use_container_width=True)
            
            st.subheader("Mẫu dữ liệu lỗi")
            st.dataframe(df_err.head(100), use_container_width=True)
        except Exception as e:
            st.info("Chưa có dữ liệu lỗi.")
    else:
        st.success("No rejected records found.")

# ==========================================
# TAB 3: PIPELINE PERFORMANCE
# ==========================================
with tab_perf:
    st.header("Pipeline benchmark results")
    res_file = os.path.join(config.LOCAL_RESULTS_DIR, "etl_tn1_scale.csv")
    if os.path.exists(res_file):
        df_bench = pd.read_csv(res_file)
        st.dataframe(df_bench, use_container_width=True)
        if 'n_rows' in df_bench.columns and 'total_time_s' in df_bench.columns:
            fig_bench = px.line(df_bench, x='n_rows', y='total_time_s', markers=True, title="Thời gian chạy theo Scale (Dữ liệu)")
            st.plotly_chart(fig_bench, use_container_width=True)
    else:
        st.info("Hãy chạy `python benchmark/run_benchmarks.py` để sinh dữ liệu benchmark.")
