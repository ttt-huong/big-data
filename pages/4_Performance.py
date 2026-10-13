import os
import pandas as pd
import streamlit as st
from utils import init_page_config, BENCHMARK_TN1, BENCHMARK_TN4

init_page_config("Pipeline Benchmarks")

st.markdown('<div class="title-gradient">Pipeline Performance Metrics</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Đánh giá khả năng mở rộng và tối ưu hóa Batch Size</div>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    with st.container(border=True):
        st.markdown("#### Scale test | TN1")
        if os.path.exists(BENCHMARK_TN1):
            df_tn1 = pd.read_csv(BENCHMARK_TN1)
            st.dataframe(df_tn1, use_container_width=True)
            if "n_rows" in df_tn1.columns and "total_time_s" in df_tn1.columns:
                st.line_chart(df_tn1.set_index("n_rows")["total_time_s"], color="#10B981")
            stage_columns = [
                column for column in (
                    "extract_time_s", "transform_time_s", "load_time_s", "overhead_time_s"
                ) if column in df_tn1.columns
            ]
            if stage_columns:
                st.caption("Median thời gian theo stage (giây)")
                st.line_chart(df_tn1.set_index("n_rows")[stage_columns])
        else:
            st.info("Chưa có dữ liệu `etl_tn1_scale.csv`")

with col2:
    with st.container(border=True):
        st.markdown("#### Batch size tuning | TN4")
        if os.path.exists(BENCHMARK_TN4):
            df_tn4 = pd.read_csv(BENCHMARK_TN4)
            st.dataframe(df_tn4, use_container_width=True)
            if "batch_size" in df_tn4.columns and "total_time_s" in df_tn4.columns:
                st.bar_chart(df_tn4.set_index("batch_size")["total_time_s"], color="#6366F1")
        else:
            st.info("Chưa có dữ liệu `etl_tn4_batch_size.csv`")
