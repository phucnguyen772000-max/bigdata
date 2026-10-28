import pandas as pd
import streamlit as st

from data_loader import load_combination_statistics
from delta_loader import load_admission_scenarios, save_admission_scenario

st.set_page_config(page_title="Tổ hợp xét tuyển", page_icon="🎓", layout="wide")
st.title("PHÂN TÍCH TỔ HỢP XÉT TUYỂN")

filter1, filter2 = st.columns(2)
year = filter1.selectbox("Năm", [2022, 2023, 2024], index=2)
combination = filter2.selectbox("Tổ hợp", ["A00", "A01", "B00", "C00", "D01"])

try:
    statistics = load_combination_statistics(year, combination)
except Exception as exc:
    st.error(f"Không đọc được dữ liệu Gold: {exc}")
    st.stop()

if statistics.empty:
    st.warning(f"Không có thống kê {combination} năm {year}.")
    st.stop()

row = statistics.iloc[0]
metrics = st.columns(4)
metrics[0].metric("Số thí sinh", f"{int(row['SoThiSinh']):,}")
metrics[1].metric("Điểm trung bình", f"{float(row['DiemTB']):.2f}")
metrics[2].metric("Điểm nhỏ nhất", f"{float(row['DiemMin']):.2f}")
metrics[3].metric("Điểm lớn nhất", f"{float(row['DiemMax']):.2f}")

thresholds = [18, 20, 22, 24, 26, 28]
counts = [int(row[f"Tu{threshold}"]) for threshold in thresholds]
threshold_data = pd.DataFrame(
    {"Ngưỡng điểm": [f"≥{value}" for value in thresholds], "Số thí sinh": counts}
)

if any(left < right for left, right in zip(counts, counts[1:])):
    st.error("Dữ liệu Gold vi phạm điều kiện Tu18 ≥ Tu20 ≥ ... ≥ Tu28.")

st.header(f"Số thí sinh {combination} đạt các ngưỡng năm {year}")
st.bar_chart(
    threshold_data,
    x="Ngưỡng điểm",
    y="Số thí sinh",
    use_container_width=True,
)
st.dataframe(threshold_data, use_container_width=True, hide_index=True)

st.header("MÔ PHỎNG NGƯỠNG ĐIỂM XÉT TUYỂN")
selected_threshold = st.selectbox("Ngưỡng điểm", thresholds, index=3)
selected_count = int(row[f"Tu{selected_threshold}"])
st.metric(
    f"Số thí sinh {combination} năm {year} đạt từ {selected_threshold} điểm",
    f"{selected_count:,}",
)
st.caption("Kết quả được lấy trực tiếp từ cột ngưỡng tương ứng trong Gold.")

st.header("LƯU KỊCH BẢN XÉT TUYỂN")

try:
    saved_scenarios = load_admission_scenarios()
except Exception as exc:
    st.error(f"Không đọc được bảng Delta admission_scenarios: {exc}")
    st.stop()

scenario_options = ["Tạo kịch bản mới"] + saved_scenarios["scenario_id"].tolist()
selected_scenario = st.selectbox("Chế độ lưu", scenario_options)
selected_id = None if selected_scenario == "Tạo kịch bản mới" else selected_scenario

defaults = None
if selected_id is not None:
    defaults = saved_scenarios[saved_scenarios["scenario_id"] == selected_id].iloc[0]

form_year_default = int(defaults["Nam"]) if defaults is not None else year
form_combination_default = defaults["ToHop"] if defaults is not None else combination
form_threshold_default = int(defaults["NguongDiem"]) if defaults is not None else selected_threshold
form_name_default = defaults["TenKichBan"] if defaults is not None else f"{combination} {year}"

years = [2022, 2023, 2024]
combinations = ["A00", "A01", "B00", "C00", "D01"]

with st.form("admission_scenario_form"):
    form_year = st.selectbox("Năm kịch bản", years, index=years.index(form_year_default))
    form_combination = st.selectbox(
        "Tổ hợp kịch bản",
        combinations,
        index=combinations.index(form_combination_default),
    )
    form_threshold = st.selectbox(
        "Ngưỡng điểm kịch bản",
        thresholds,
        index=thresholds.index(form_threshold_default),
    )
    form_name = st.text_input("Tên kịch bản", value=form_name_default)
    submitted = st.form_submit_button("Lưu kịch bản")

if submitted:
    try:
        saved_id = save_admission_scenario(
            form_year,
            form_combination,
            form_threshold,
            form_name,
            selected_id,
        )
        st.success(f"Đã lưu kịch bản {saved_id} vào Delta Lake.")
        saved_scenarios = load_admission_scenarios()
    except Exception as exc:
        st.error(f"Không lưu được kịch bản: {exc}")

st.subheader("CÁC KỊCH BẢN ĐÃ LƯU")

gold_combinations = load_combination_statistics()
threshold_lookup = {}
for gold_row in gold_combinations.itertuples():
    for threshold in thresholds:
        threshold_lookup[(int(gold_row.Nam), gold_row.ToHop, threshold)] = int(
            getattr(gold_row, f"Tu{threshold}")
        )

scenario_display = saved_scenarios.copy()
scenario_display["SoThiSinhDatNguong"] = scenario_display.apply(
    lambda scenario: threshold_lookup.get(
        (int(scenario["Nam"]), scenario["ToHop"], int(scenario["NguongDiem"]))
    ),
    axis=1,
)

st.dataframe(
    scenario_display[
        [
            "scenario_id",
            "Nam",
            "ToHop",
            "NguongDiem",
            "TenKichBan",
            "SoThiSinhDatNguong",
            "CreatedAt",
            "UpdatedAt",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)
