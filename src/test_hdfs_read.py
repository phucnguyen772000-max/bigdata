from pyspark.sql import SparkSession


HDFS_PATH = "hdfs://namenode:9000/lakehouse/diemthi_hcm/bronze/year=2022/"

spark = (
    SparkSession.builder
    .appName("BigDataDiemThi-HDFSReadTest")
    .getOrCreate()
)

dataframe = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(HDFS_PATH)
)

print(f"SPARK_MASTER={spark.sparkContext.master}")
print(f"HDFS_PATH={HDFS_PATH}")
dataframe.printSchema()
row_count = dataframe.count()
print(f"ROW_COUNT={row_count}")
dataframe.show(5, truncate=False)

spark.stop()
