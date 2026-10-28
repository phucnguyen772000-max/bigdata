from pyspark.sql import SparkSession, functions as F


spark = SparkSession.builder.appName("BigDataDiemThi-BronzeProfile").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

for year in (2022, 2023, 2024):
    path = f"hdfs://namenode:9000/lakehouse/diemthi_hcm/bronze/year={year}/"
    frame = spark.read.option("header", "true").option("inferSchema", "false").csv(path)
    normalized = frame.toDF(*[column.lstrip("\ufeff").strip() for column in frame.columns])
    print(f"YEAR={year}")
    print(f"RAW_COLUMN_COUNT={len(frame.columns)}")
    print("RAW_COLUMNS=" + "|".join(repr(column) for column in frame.columns))
    print("NORMALIZED_COLUMNS=" + "|".join(normalized.columns))
    print(f"ROW_COUNT={normalized.count()}")
    print("TINH_DISTRIBUTION")
    normalized.groupBy("Tinh").count().orderBy(F.col("Tinh").cast("int")).show(100, truncate=False)
    print("TINH_PREFIX_CONSISTENCY")
    (
        normalized
        .select(
            F.col("Tinh"),
            F.substring(F.lpad(F.trim(F.col("SBD")), 8, "0"), 1, 2).alias("SBD_PREFIX"),
        )
        .groupBy("Tinh", "SBD_PREFIX")
        .count()
        .orderBy(F.col("Tinh").cast("int"), "SBD_PREFIX")
        .show(100, truncate=False)
    )
    print("TINH_2_SAMPLE")
    normalized.filter(F.col("Tinh") == "2").select("SBD", "Nam", "Tinh").show(5, truncate=False)

spark.stop()
