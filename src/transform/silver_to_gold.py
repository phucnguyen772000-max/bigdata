from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# ĐƯỜNG DẪN DỮ LIỆU
# ============================================================

SILVER_PATH = "hdfs://namenode:9000/lakehouse/diemthi_hcm/silver"
GOLD_BASE = "hdfs://namenode:9000/lakehouse/diemthi_hcm/gold"


# Các môn cần phân tích
SUBJECTS = [
    "Toan",
    "NguVan",
    "VatLy",
    "HoaHoc",
    "SinhHoc",
    "LichSu",
    "DiaLy",
    "GDCD",
    "NgoaiNgu"
]


# Các tổ hợp đã tạo ở Silver
COMBINATIONS = [
    "A00",
    "A01",
    "B00",
    "C00",
    "D01"
]


# ============================================================
# KHỞI TẠO SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("SilverToGold")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 1. ĐỌC SILVER
# ============================================================

silver = (
    spark.read
    .parquet(SILVER_PATH)
    .cache()
)

total_rows = silver.count()

print("=" * 60)
print(f"SILVER_TOTAL_ROWS={total_rows}")
print("=" * 60)


# ============================================================
# 2. YEARLY SUMMARY
# Tổng số thí sinh từng năm
# ============================================================

yearly_summary = (
    silver
    .groupBy("Nam")
    .agg(
        F.count("*").alias("SoThiSinh")
    )
    .orderBy("Nam")
)

print("YEARLY SUMMARY")
yearly_summary.show(truncate=False)

(
    yearly_summary
    .write
    .mode("overwrite")
    .parquet(f"{GOLD_BASE}/yearly_summary")
)


# ============================================================
# 3. CHUYỂN CÁC MÔN SANG DẠNG DỌC
# ============================================================

subject_frames = []

for subject in SUBJECTS:
    df_subject = (
        silver
        .select(
            "Nam",
            F.lit(subject).alias("Mon"),
            F.col(subject).cast("double").alias("Diem")
        )
        .filter(F.col("Diem").isNotNull())
    )

    subject_frames.append(df_subject)


subjects_long = reduce(
    lambda left, right: left.unionByName(right),
    subject_frames
)


# ============================================================
# 4. SUBJECT STATISTICS
# Thống kê từng môn
# ============================================================

subject_statistics = (
    subjects_long
    .groupBy("Nam", "Mon")
    .agg(
        F.count("*").alias("SoThiSinh"),
        F.round(F.avg("Diem"), 2).alias("DiemTB"),
        F.round(F.min("Diem"), 2).alias("DiemMin"),
        F.round(F.max("Diem"), 2).alias("DiemMax"),

        F.sum(
            F.when(F.col("Diem") >= 5, 1).otherwise(0)
        ).alias("Tu5Diem"),

        F.sum(
            F.when(F.col("Diem") >= 8, 1).otherwise(0)
        ).alias("Tu8Diem")
    )
    .orderBy("Nam", "Mon")
)

print("SUBJECT STATISTICS")
subject_statistics.show(100, truncate=False)

(
    subject_statistics
    .write
    .mode("overwrite")
    .partitionBy("Nam")
    .parquet(f"{GOLD_BASE}/subject_statistics")
)


# ============================================================
# 5. SUBJECT DISTRIBUTION
# Phổ điểm từng môn
# ============================================================

subjects_bucket = (
    subjects_long
    .withColumn(
        "NhomDiem",
        F.when(
            F.col("Diem") == 10,
            F.lit(10)
        ).otherwise(
            F.floor(F.col("Diem")).cast("int")
        )
    )
)

subject_distribution = (
    subjects_bucket
    .groupBy(
        "Nam",
        "Mon",
        "NhomDiem"
    )
    .agg(
        F.count("*").alias("SoThiSinh")
    )
    .orderBy(
        "Nam",
        "Mon",
        "NhomDiem"
    )
)

(
    subject_distribution
    .write
    .mode("overwrite")
    .partitionBy("Nam")
    .parquet(f"{GOLD_BASE}/subject_distribution")
)


# ============================================================
# 6. CHUYỂN CÁC TỔ HỢP SANG DẠNG DỌC
# ============================================================

combo_frames = []

for combo in COMBINATIONS:
    df_combo = (
        silver
        .select(
            "Nam",
            F.lit(combo).alias("ToHop"),
            F.col(combo).cast("double").alias("Diem")
        )
        .filter(F.col("Diem").isNotNull())
    )

    combo_frames.append(df_combo)


combinations_long = reduce(
    lambda left, right: left.unionByName(right),
    combo_frames
)


# ============================================================
# 7. COMBINATION STATISTICS
# Thống kê tổ hợp xét tuyển
# ============================================================

combination_statistics = (
    combinations_long
    .groupBy(
        "Nam",
        "ToHop"
    )
    .agg(
        F.count("*").alias("SoThiSinh"),

        F.round(
            F.avg("Diem"),
            2
        ).alias("DiemTB"),

        F.round(
            F.min("Diem"),
            2
        ).alias("DiemMin"),

        F.round(
            F.max("Diem"),
            2
        ).alias("DiemMax"),

        F.sum(
            F.when(F.col("Diem") >= 18, 1).otherwise(0)
        ).alias("Tu18"),

        F.sum(
            F.when(F.col("Diem") >= 20, 1).otherwise(0)
        ).alias("Tu20"),

        F.sum(
            F.when(F.col("Diem") >= 22, 1).otherwise(0)
        ).alias("Tu22"),

        F.sum(
            F.when(F.col("Diem") >= 24, 1).otherwise(0)
        ).alias("Tu24"),

        F.sum(
            F.when(F.col("Diem") >= 26, 1).otherwise(0)
        ).alias("Tu26"),

        F.sum(
            F.when(F.col("Diem") >= 28, 1).otherwise(0)
        ).alias("Tu28")
    )
    .orderBy(
        "Nam",
        "ToHop"
    )
)

print("COMBINATION STATISTICS")
combination_statistics.show(100, truncate=False)

(
    combination_statistics
    .write
    .mode("overwrite")
    .partitionBy("Nam")
    .parquet(f"{GOLD_BASE}/combination_statistics")
)


print("=" * 60)
print("GOLD_BUILD_SUCCESS")
print(f"GOLD_PATH={GOLD_BASE}")
print("=" * 60)

spark.stop()
