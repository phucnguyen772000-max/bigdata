from pyspark.sql import SparkSession, functions as F


SILVER_PATH = "hdfs://namenode:9000/lakehouse/diemthi_hcm/silver/"
COMBOS = ("A00", "A01", "B00", "C00", "D01")

spark = SparkSession.builder.appName("BigDataDiemThi-VerifySilver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver = spark.read.parquet(SILVER_PATH)
print("SILVER_READBACK_SCHEMA_BEGIN")
silver.printSchema()
print("SILVER_READBACK_SCHEMA_END")
print(f"SILVER_READBACK_TOTAL_ROWS={silver.count()}")
print("SILVER_READBACK_BY_YEAR")
silver.groupBy("Nam").count().orderBy("Nam").show(truncate=False)
print("SILVER_READBACK_TINH")
silver.groupBy("Tinh").count().orderBy("Tinh").show(truncate=False)
counts = silver.agg(*[F.count(F.col(combo)).alias(combo) for combo in COMBOS]).first()
for combo in COMBOS:
    print(f"SILVER_READBACK_VALID_{combo}_ROWS={counts[combo]}")

combo_subjects = {
    "A00": ("Toan", "VatLy", "HoaHoc"),
    "A01": ("Toan", "VatLy", "NgoaiNgu"),
    "B00": ("Toan", "HoaHoc", "SinhHoc"),
    "C00": ("NguVan", "LichSu", "DiaLy"),
    "D01": ("Toan", "NguVan", "NgoaiNgu"),
}
for combo, subjects in combo_subjects.items():
    missing_subject = F.col(subjects[0]).isNull() | F.col(subjects[1]).isNull() | F.col(subjects[2]).isNull()
    violation_count = silver.filter(missing_subject & F.col(combo).isNotNull()).count()
    print(f"{combo}_NULL_RULE_VIOLATIONS={violation_count}")

spark.stop()
