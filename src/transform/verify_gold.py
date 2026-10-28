from pyspark.sql import SparkSession


GOLD_BASE = "hdfs://namenode:9000/lakehouse/diemthi_hcm/gold"
EXPECTED = {
    "yearly_summary": {"Nam", "SoThiSinh"},
    "subject_statistics": {"Nam", "Mon", "SoThiSinh", "DiemTB"},
    "subject_distribution": {"Nam", "Mon", "NhomDiem", "SoThiSinh"},
    "combination_statistics": {"Nam", "ToHop", "SoThiSinh", "DiemTB"},
}

spark = SparkSession.builder.appName("Verify-Gold").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

for dataset, required_columns in EXPECTED.items():
    path = f"{GOLD_BASE}/{dataset}"
    frame = spark.read.parquet(path)
    missing = required_columns - set(frame.columns)
    if missing:
        raise RuntimeError(f"Gold dataset {dataset} missing columns: {sorted(missing)}")
    count = frame.count()
    if count == 0:
        raise RuntimeError(f"Gold dataset {dataset} is empty")
    print(f"VERIFY_GOLD_DATASET={dataset} ROWS={count} PATH={path}")

print("VERIFY_GOLD_SUCCESS")
spark.stop()

