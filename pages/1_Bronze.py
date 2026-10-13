import streamlit as st
from utils import (
    BRONZE_PATHS,
    find_layer_files,
    get_duckdb_connection,
    init_page_config,
    parquet_source_sql,
)

# 1. Cấu hình trang
init_page_config("Bronze Layer")
con = get_duckdb_connection()

# 2. Tiêu đề
st.markdown(
    '<div class="title-gradient">Bronze Layer | Raw Data</div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="subtitle">Dữ liệu thô tiếp nhận từ nguồn, chưa qua chuẩn hóa</div>',
    unsafe_allow_html=True,
)

# 3. Tìm đường dẫn file hợp lệ
bronze_pattern = find_layer_files("bronze", BRONZE_PATHS)

if bronze_pattern:
    try:
        # CHUẨN HÓA CÚ PHÁP SQL:
        # - Nếu bronze_pattern là List -> giữ nguyên định dạng dạng array: ['path1', 'path2']
        # - Nếu bronze_pattern là String -> bọc dấu nháy đơn: 'path/*.parquet'
        path_sql = parquet_source_sql(bronze_pattern)

        # Tạo câu lệnh SQL đúng định dạng
        count_query = f"SELECT COUNT(*) FROM read_parquet({path_sql}, union_by_name=True)"
        sample_query = f"SELECT * FROM read_parquet({path_sql}, union_by_name=True) LIMIT 100"

        # Thực thi truy vấn
        count = con.sql(count_query).fetchone()[0]

        # Hiển thị KPI Card
        st.markdown(
            f"""
                <div class="glass-card" style="margin-bottom: 2rem;">
                    <div class="kpi-label">Tổng Dữ Liệu Thô Đã Tiếp Nhận</div>
                    <div class="kpi-value">{count:,} <span style="font-size: 1rem; color: #64748B;">bản ghi</span></div>
                    <div class="kpi-trend trend-up">↑ Ingested Successfully</div>
                    <p style="margin-top: 10px; color: #64748B; font-size: 0.9rem;">Source: {bronze_pattern}</p>
                </div>
            """,
            unsafe_allow_html=True,
        )

        # Hiển thị bảng dữ liệu mẫu
        with st.container(border=True):
            st.markdown("#### Sample records | First 100")
            df = con.sql(sample_query).df()
            st.dataframe(df, use_container_width=True)

    except Exception as e:
        st.error(f"Lỗi truy vấn: {e}")
else:
    st.warning(
        "Chưa tìm thấy dữ liệu Bronze. Hãy đảm bảo file .parquet được lưu đúng trong thư mục ./data/raw/"
    )
