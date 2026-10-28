import streamlit as st

from data_loader import load_subject_distribution, load_subject_statistics

st.set_page_config(page_title="Phổ điểm", page_icon="📈", layout="wide")
st.title("PHÂN TÍCH PHỔ ĐIỂM")

SUBJECTS = [
    "Toan", "NguVan", "VatLy", "HoaHoc", "SinhHoc",
    "LichSu", "DiaLy", "GDCD", "NgoaiNgu",
]
SUBJECT_LABELS = {
    "Toan": "Toán",
    "NguVan": "Ngữ văn",
    "VatLy": "Vật lý",
    "HoaHoc": "Hóa học",
    "SinhHoc": "Sinh học",
    "LichSu": "Lịch sử",
    "DiaLy": "Địa lý",
    "GDCD": "GDCD",
    "NgoaiNgu": "Ngoại ngữ",
}

filter1, filter2 = st.columns(2)
year = filter1.selectbox("Năm", [2022, 2023, 2024], index=2)
subject = filter2.selectbox(
    "Môn", SUBJECTS, format_func=lambda value: SUBJECT_LABELS[value]
)

try:
    statistics = load_subject_statistics(year, subject)
    distribution = load_subject_distribution(year, subject)
except Exception as exc:
    st.error(f"Không đọc được dữ liệu Gold: {exc}")
    st.stop()

if statistics.empty:
    st.warning(f"Không có thống kê {SUBJECT_LABELS[subject]} năm {year}.")
else:
    row = statistics.iloc[0]
    first = st.columns(4)
    first[0].metric("Số thí sinh", f"{int(row['SoThiSinh']):,}")
    first[1].metric("Điểm trung bình", f"{float(row['DiemTB']):.2f}")
    first[2].metric("Điểm nhỏ nhất", f"{float(row['DiemMin']):.2f}")
    first[3].metric("Điểm lớn nhất", f"{float(row['DiemMax']):.2f}")
    second = st.columns(2)
    second[0].metric("Số thí sinh ≥ 5", f"{int(row['Tu5Diem']):,}")
    second[1].metric("Số thí sinh ≥ 8", f"{int(row['Tu8Diem']):,}")

st.header(f"Phổ điểm {SUBJECT_LABELS[subject]} năm {year}")
if distribution.empty:
    st.info("Không có dữ liệu phân bố cho lựa chọn này.")
else:
    st.bar_chart(
        distribution,
        x="NhomDiem",
        y="SoThiSinh",
        use_container_width=True,
    )
    st.dataframe(
        distribution[["Nam", "Mon", "NhomDiem", "SoThiSinh"]],
        use_container_width=True,
        hide_index=True,
    )
