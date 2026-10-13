import plotly.express as px
import streamlit as st
from utils import (
    GOLD_PATHS,
    find_layer_files,
    get_duckdb_connection,
    init_page_config,
    parquet_source_sql,
)

# 1. Cấu hình trang
init_page_config("Gold Layer")
con = get_duckdb_connection()

# 2. Tiêu đề
st.markdown(
    '<div class="title-gradient">Gold Layer | Analytics Ready</div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="subtitle">Các chỉ số tổng hợp sẵn sàng cho báo cáo và phân tích</div>',
    unsafe_allow_html=True,
)

# 3. Kiểm tra đường dẫn dữ liệu Gold
gold_pattern = find_layer_files("gold", GOLD_PATHS)

if gold_pattern:
    try:
        gold_sql = parquet_source_sql(gold_pattern)

        # Lọc riêng dữ liệu Gold hợp lệ (bắt buộc có cột total_records)
        base_query = f"""
            FROM read_parquet({gold_sql}, union_by_name=True)
            WHERE total_records IS NOT NULL
        """

        # 1. Lấy thông số KPI chính
        kpi_df = con.sql(
            f"""
            SELECT 
                CAST(COALESCE(SUM(total_records), 0) AS BIGINT) AS total_items,
                CAST(COUNT(*) AS BIGINT) AS total_groups
            {base_query}
        """
        ).df()

        total_items = (
            int(kpi_df["total_items"].iloc[0]) if not kpi_df.empty else 0
        )
        total_groups = (
            int(kpi_df["total_groups"].iloc[0]) if not kpi_df.empty else 0
        )

        col_kpi1, col_kpi2 = st.columns(2)
        with col_kpi1:
            st.markdown(
                f"""
                <div class="glass-card">
                    <div class="kpi-label">Tổng Số Bản Ghi Thực Tế</div>
                    <div class="kpi-value">{total_items:,} <span style="font-size: 1rem; color: #64748B;">bản ghi</span></div>
                    <div class="kpi-trend trend-up">Aggregated from Silver</div>
                </div>
            """,
                unsafe_allow_html=True,
            )
        with col_kpi2:
            st.markdown(
                f"""
                <div class="glass-card">
                    <div class="kpi-label">Số Nhóm Mẫu Tổng Hợp (Data Marts)</div>
                    <div class="kpi-value">{total_groups:,} <span style="font-size: 1rem; color: #64748B;">nhóm</span></div>
                    <div class="kpi-trend trend-up">Dimension groups</div>
                </div>
            """,
                unsafe_allow_html=True,
            )

        st.write("---")
        col1, col2 = st.columns(2)

        # 2. Biểu đồ 1: Cột tổng bản ghi theo Category
        with col1:
            st.markdown("#### Records by category")
            df_cat = con.sql(
                f"""
                SELECT 
                    COALESCE(category, 'Chưa xác định') AS category, 
                    CAST(SUM(total_records) AS BIGINT) AS total_count 
                {base_query}
                GROUP BY 1 
                ORDER BY total_count DESC
            """
            ).df()

            if not df_cat.empty and df_cat["total_count"].sum() > 0:
                fig_cat = px.bar(
                    df_cat,
                    x="category",
                    y="total_count",
                    color="category",
                    text_auto=True,
                    labels={
                        "total_count": "Số lượng bản ghi",
                        "category": "Danh mục",
                    },
                )
                st.plotly_chart(fig_cat, use_container_width=True)
            else:
                st.info("Chưa có dữ liệu danh mục hợp lệ trong tầng Gold.")

        # 3. Biểu đồ 2: Xu hướng theo Tháng
        with col2:
            st.markdown("#### Monthly trend")
            df_month = con.sql(
                f"""
                SELECT 
                    CAST(COALESCE(month, 0) AS INT) AS month_num, 
                    CAST(SUM(total_records) AS BIGINT) AS total_count 
                {base_query}
                GROUP BY 1 
                ORDER BY month_num ASC
            """
            ).df()

            if not df_month.empty and df_month["total_count"].sum() > 0:
                df_month["month_str"] = df_month["month_num"].apply(
                    lambda x: f"Tháng {x}" if x > 0 else "N/A"
                )
                fig_month = px.line(
                    df_month,
                    x="month_str",
                    y="total_count",
                    markers=True,
                    labels={
                        "total_count": "Số lượng bản ghi",
                        "month_str": "Thời gian",
                    },
                )
                st.plotly_chart(fig_month, use_container_width=True)
            else:
                st.info("Chưa có dữ liệu thời gian hợp lệ trong tầng Gold.")

        # 4. Bảng chi tiết Gold Layer
        with st.container(border=True):
            st.markdown("#### Gold summary | First 100 rows")
            df_sample = con.sql(f"SELECT * {base_query} LIMIT 100").df()
            st.dataframe(df_sample, use_container_width=True)

    except Exception as e:
        st.error(f"Lỗi truy vấn Gold: {e}")
else:
    st.warning(
        "Chưa tìm thấy dữ liệu Gold. Hãy chạy main_pipeline.py trước!"
    )
