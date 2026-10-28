import streamlit as st

st.set_page_config(
    page_title="Điểm thi THPT TP.HCM",
    page_icon="📊",
    layout="wide",
)

st.title("PHÂN TÍCH ĐIỂM THI THPT TP.HCM 2022–2024")
st.info("Chọn một trong ba màn hình ở thanh điều hướng bên trái.")

st.markdown(
    """
Dashboard chỉ đọc các bảng tổng hợp **Gold** trên HDFS:

- **Tổng quan:** số thí sinh, điểm trung bình theo môn và kết quả KMeans A00.
- **Phổ điểm:** thống kê và phân bố điểm theo năm, môn.
- **Tổ hợp xét tuyển:** thống kê A00, A01, B00, C00, D01 và mô phỏng ngưỡng.

Bronze và Silver không được tải vào Pandas hay chỉnh sửa bởi ứng dụng này.
"""
)
