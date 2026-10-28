from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window, functions as F


BRONZE_BASE = "hdfs://namenode:9000/lakehouse/diemthi_hcm/bronze"
SILVER_PATH = "hdfs://namenode:9000/lakehouse/diemthi_hcm/silver"
QUARANTINE_PATH = (
    "hdfs://namenode:9000/lakehouse/diemthi_hcm/_quarantine/bronze_to_silver"
)
YEARS = (2022, 2023, 2024)
HCM_TINH_CODE = 2

EXPECTED_COLUMNS = [
    "SBD", "Nam", "Tinh", "SBD_New", "Toan", "NguVan", "VatLy",
    "HoaHoc", "SinhHoc", "LichSu", "DiaLy", "GDCD", "NgoaiNgu",
    "MaMonNgoaiNgu", "TongDiem", "KhoiA", "KhoiA1", "KhoiB", "KhoiC",
    "KhoiD", "KHTN", "KHXH", "KhoiA02", "KhoiC01", "KhoiD07",
    "TongDiemKHTN", "TongDiemKHXH",
]

SUBJECT_SCORE_COLUMNS = [
    "Toan", "NguVan", "VatLy", "HoaHoc", "SinhHoc",
    "LichSu", "DiaLy", "GDCD", "NgoaiNgu",
]

DOUBLE_COLUMNS = [
    "Toan", "NguVan", "VatLy", "HoaHoc", "SinhHoc", "LichSu", "DiaLy",
    "GDCD", "NgoaiNgu", "TongDiem", "KhoiA", "KhoiA1", "KhoiB", "KhoiC",
    "KhoiD", "KHTN", "KHXH", "KhoiA02", "KhoiC01", "KhoiD07",
    "TongDiemKHTN", "TongDiemKHXH",
]

# Các mã hội đồng thi xuất hiện nhất quán trong cả ba bộ dữ liệu: 01-19, 21-64.
VALID_TINH_CODES = list(range(1, 20)) + list(range(21, 65))


def clean_header(name: str) -> str:
    return name.lstrip("\ufeff").strip()


def nonempty(column_name: str):
    return F.col(column_name).isNotNull() & (F.trim(F.col(column_name)) != "")


def read_year(spark: SparkSession, year: int) -> tuple[DataFrame, int]:
    path = f"{BRONZE_BASE}/year={year}/"
    raw = spark.read.option("header", "true").option("inferSchema", "false").csv(path)
    normalized_names = [clean_header(column) for column in raw.columns]

    if len(normalized_names) != 27:
        raise ValueError(f"Year {year}: expected 27 columns, found {len(normalized_names)}")
    if normalized_names != EXPECTED_COLUMNS:
        raise ValueError(
            f"Year {year}: unexpected columns. expected={EXPECTED_COLUMNS}, actual={normalized_names}"
        )

    frame = raw.toDF(*normalized_names)
    row_count = frame.count()
    print(f"BRONZE_YEAR_{year}_ROWS={row_count}")
    print(f"BRONZE_YEAR_{year}_COLUMN_COUNT={len(normalized_names)}")

    source_year = F.trim(F.col("Nam"))
    frame = frame.withColumn(
        "_invalid_source_year",
        source_year.isNull() | ~source_year.isin(str(year), str(year % 100)),
    )

    for column in frame.columns:
        if column.startswith("_"):
            continue
        frame = frame.withColumn(
            column,
            F.when(F.trim(F.col(column)) == "", F.lit(None)).otherwise(F.trim(F.col(column))),
        )

    for column in DOUBLE_COLUMNS:
        frame = frame.withColumn(f"_raw_{column}", F.col(column))
        frame = frame.withColumn(column, F.col(column).cast("double"))

    frame = frame.withColumn("_raw_Tinh", F.col("Tinh"))
    frame = frame.withColumn("_raw_SBD_New", F.col("SBD_New"))
    frame = frame.withColumn("Tinh", F.col("Tinh").cast("int"))
    frame = frame.withColumn("SBD_New", F.col("SBD_New").cast("long"))
    frame = frame.withColumn("Nam", F.lit(year).cast("int"))
    return frame, row_count


def valid_combo(columns: list[str]):
    present = reduce(lambda left, right: left & right, [F.col(c).isNotNull() for c in columns])
    total = reduce(lambda left, right: left + right, [F.col(c) for c in columns])
    return F.when(present, total.cast("double"))


spark = (
    SparkSession.builder
    .appName("BigDataDiemThi-BronzeToSilver")
    .config("spark.sql.shuffle.partitions", "12")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

frames = []
bronze_counts = {}
for input_year in YEARS:
    yearly_frame, yearly_count = read_year(spark, input_year)
    frames.append(yearly_frame)
    bronze_counts[input_year] = yearly_count

bronze = reduce(lambda left, right: left.unionByName(right), frames)

numeric_parse_conditions = []
for numeric_column in DOUBLE_COLUMNS:
    numeric_parse_conditions.append(
        F.col(f"_raw_{numeric_column}").isNotNull() & F.col(numeric_column).isNull()
    )
numeric_parse_conditions.extend(
    [
        F.col("_raw_Tinh").isNotNull() & F.col("Tinh").isNull(),
        F.col("_raw_SBD_New").isNotNull() & F.col("SBD_New").isNull(),
    ]
)
invalid_numeric = reduce(lambda left, right: left | right, numeric_parse_conditions)

invalid_score_conditions = [
    F.col(column).isNotNull() & ((F.col(column) < 0) | (F.col(column) > 10))
    for column in SUBJECT_SCORE_COLUMNS
]
invalid_score = reduce(lambda left, right: left | right, invalid_score_conditions)

missing_sbd = F.col("SBD").isNull() | (F.trim(F.col("SBD")) == "")
invalid_tinh = F.col("Tinh").isNull() | ~F.col("Tinh").isin(VALID_TINH_CODES)
sbd_prefix = F.substring(F.lpad(F.col("SBD"), 8, "0"), 1, 2).cast("int")
invalid_tinh_sbd_match = (
    F.col("SBD").isNotNull()
    & F.col("Tinh").isNotNull()
    & (sbd_prefix != F.col("Tinh"))
)

duplicate_window = Window.partitionBy("Nam", "SBD")
quality = bronze.withColumn("_duplicate_count", F.count(F.lit(1)).over(duplicate_window))
quality = quality.withColumn("_is_missing_sbd", missing_sbd)
quality = quality.withColumn("_is_duplicate", F.col("_duplicate_count") > 1)
quality = quality.withColumn("_is_invalid_score", invalid_score)
quality = quality.withColumn("_is_invalid_numeric", invalid_numeric)
quality = quality.withColumn("_is_invalid_tinh", invalid_tinh)
quality = quality.withColumn("_is_tinh_sbd_mismatch", invalid_tinh_sbd_match)
quality = quality.withColumn(
    "_dq_reasons",
    F.concat_ws(
        "|",
        F.when(F.col("_is_missing_sbd"), F.lit("missing_sbd")),
        F.when(F.col("_is_duplicate"), F.lit("duplicate_nam_sbd")),
        F.when(F.col("_is_invalid_score"), F.lit("score_out_of_range")),
        F.when(F.col("_invalid_source_year"), F.lit("source_year_mismatch")),
        F.when(F.col("_is_invalid_numeric"), F.lit("numeric_parse_error")),
        F.when(F.col("_is_invalid_tinh"), F.lit("invalid_tinh")),
        F.when(F.col("_is_tinh_sbd_mismatch"), F.lit("tinh_sbd_mismatch")),
    ),
)
quality = quality.persist(StorageLevel.DISK_ONLY)

total_bronze = sum(bronze_counts.values())
missing_sbd_count = quality.filter(F.col("_is_missing_sbd")).count()
duplicate_record_count = quality.filter(F.col("_is_duplicate")).count()
duplicate_key_count = (
    quality.filter(F.col("_is_duplicate"))
    .select("Nam", "SBD")
    .distinct()
    .count()
)
invalid_score_count = quality.filter(F.col("_is_invalid_score")).count()
invalid_year_count = quality.filter(F.col("_invalid_source_year")).count()
invalid_numeric_count = quality.filter(F.col("_is_invalid_numeric")).count()
invalid_tinh_count = quality.filter(F.col("_is_invalid_tinh")).count()
tinh_sbd_mismatch_count = quality.filter(F.col("_is_tinh_sbd_mismatch")).count()

quarantine = quality.filter(F.length(F.col("_dq_reasons")) > 0)
quarantine_count = quarantine.count()

internal_columns = [column for column in quality.columns if column.startswith("_raw_")]
quarantine_output = quarantine.drop(*internal_columns)
(
    quarantine_output.repartition("Nam")
    .write.mode("overwrite")
    .partitionBy("Nam")
    .parquet(QUARANTINE_PATH)
)

valid = quality.filter(F.length(F.col("_dq_reasons")) == 0)
hcm = valid.filter(F.col("Tinh") == HCM_TINH_CODE)
hcm = hcm.withColumn("A00", valid_combo(["Toan", "VatLy", "HoaHoc"]))
hcm = hcm.withColumn("A01", valid_combo(["Toan", "VatLy", "NgoaiNgu"]))
hcm = hcm.withColumn("B00", valid_combo(["Toan", "HoaHoc", "SinhHoc"]))
hcm = hcm.withColumn("C00", valid_combo(["NguVan", "LichSu", "DiaLy"]))
hcm = hcm.withColumn("D01", valid_combo(["Toan", "NguVan", "NgoaiNgu"]))

silver_drop_columns = [column for column in hcm.columns if column.startswith("_")]
silver = hcm.drop(*silver_drop_columns)

hcm_counts = {row["Nam"]: row["count"] for row in silver.groupBy("Nam").count().collect()}
silver_count = sum(hcm_counts.values())
combo_counts_row = silver.agg(
    *[F.count(F.col(combo)).alias(combo) for combo in ("A00", "A01", "B00", "C00", "D01")]
).first()

(
    silver.repartition(6, "Nam")
    .write.mode("overwrite")
    .partitionBy("Nam")
    .parquet(SILVER_PATH)
)

print("DQ_SUMMARY_BEGIN")
print(f"TOTAL_BRONZE_ROWS={total_bronze}")
for summary_year in YEARS:
    print(f"BRONZE_YEAR_{summary_year}_ROWS={bronze_counts[summary_year]}")
print(f"MISSING_SBD_ROWS={missing_sbd_count}")
print(f"DUPLICATE_RECORD_ROWS={duplicate_record_count}")
print(f"DUPLICATE_KEYS={duplicate_key_count}")
print(f"INVALID_SCORE_ROWS={invalid_score_count}")
print(f"INVALID_SOURCE_YEAR_ROWS={invalid_year_count}")
print(f"INVALID_NUMERIC_ROWS={invalid_numeric_count}")
print(f"INVALID_TINH_ROWS={invalid_tinh_count}")
print(f"TINH_SBD_MISMATCH_ROWS={tinh_sbd_mismatch_count}")
print(f"QUARANTINE_ROWS={quarantine_count}")
for summary_year in YEARS:
    print(f"HCM_YEAR_{summary_year}_ROWS={hcm_counts.get(summary_year, 0)}")
print(f"SILVER_TOTAL_ROWS={silver_count}")
for combo in ("A00", "A01", "B00", "C00", "D01"):
    print(f"VALID_{combo}_ROWS={combo_counts_row[combo]}")
print("DQ_SUMMARY_END")

print("SILVER_SCHEMA_BEGIN")
silver.printSchema()
print("SILVER_SCHEMA_END")

quality.unpersist()
spark.stop()
