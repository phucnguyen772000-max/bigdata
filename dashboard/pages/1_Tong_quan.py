import streamlit as st

from data_loader import (
    load_cluster_statistics,
    load_subject_statistics,
    load_yearly_summary,
)

st.set_page_config(page_title="Tổng quan", page_icon="📊", layout="wide")
st.title("TỔNG QUAN ĐIỂM THI TP.HCM 2022–2024")

try:
    yearly = load_yearly_summary()
    subjects = load_subject_statistics()
    clusters = load_cluster_statistics()
except Exception as exc:
    st.error(f"Không đọc được dữ liệu Gold: {exc}")
    st.stop()

if yearly.empty:
    st.warning("Bảng yearly_summary không có dữ liệu.")
    st.stop()

year_counts = {int(row.Nam): int(row.SoThiSinh) for row in yearly.itertuples()}
total_candidates = int(yearly["SoThiSinh"].sum())

cols = st.columns(4)
cols[0].metric("Tổng số thí sinh TP.HCM", f"{total_candidates:,}")
for index, year in enumerate((2022, 2023, 2024), start=1):
    cols[index].metric(f"Thí sinh năm {year}", f"{year_counts.get(year, 0):,}")

st.header("Số thí sinh theo năm")
st.bar_chart(yearly, x="Nam", y="SoThiSinh", use_container_width=True)

st.header("Điểm trung bình từng môn theo năm")
if subjects.empty:
    st.info("Không có dữ liệu subject_statistics.")
else:
    chart_data = subjects[["Nam", "Mon", "DiemTB"]].copy()
    st.line_chart(
        chart_data,
        x="Nam",
        y="DiemTB",
        color="Mon",
        use_container_width=True,
    )

st.header("PHÂN NHÓM THÍ SINH A00 BẰNG KMEANS")
kpi1, kpi2 = st.columns(2)
kpi1.metric("BEST_K", "2")
kpi2.metric("Silhouette", "0.549144")
st.caption(
    "Cluster ID là nhãn kỹ thuật. Theo dữ liệu Gold, Cluster 0 có A00 trung bình "
    "thấp hơn và Cluster 1 có A00 trung bình cao hơn."
)

if clusters.empty:
    st.info("Không có dữ liệu a00_cluster_statistics.")
else:
    display_clusters = clusters.copy()
    display_clusters["Cluster"] = display_clusters["cluster"].map(
        lambda value: f"Cluster {int(value)}"
    )
    columns = [
        "Nam", "Cluster", "SoThiSinh", "ToanTB", "VatLyTB", "HoaHocTB", "A00TB"
    ]
    st.dataframe(display_clusters[columns], use_container_width=True, hide_index=True)

    chart1, chart2 = st.columns(2)
    with chart1:
        st.subheader("Số thí sinh theo cluster")
        st.bar_chart(
            display_clusters,
            x="Nam",
            y="SoThiSinh",
            color="Cluster",
            use_container_width=True,
        )
    with chart2:
        st.subheader("A00 trung bình theo cluster")
        st.bar_chart(
            display_clusters,
            x="Nam",
            y="A00TB",
            color="Cluster",
            use_container_width=True,
        )
