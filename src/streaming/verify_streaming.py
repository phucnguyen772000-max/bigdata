from pyspark.sql import SparkSession


VALID_PATH = "hdfs://namenode:9000/lakehouse/diemthi_hcm/streaming/bronze"
QUARANTINE_PATH = "hdfs://namenode:9000/lakehouse/diemthi_hcm/streaming/quarantine"

spark = SparkSession.builder.appName("Verify-DiemThi-Streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

valid = spark.read.parquet(VALID_PATH)
quarantine = spark.read.parquet(QUARANTINE_PATH)
valid_count = valid.count()
quarantine_count = quarantine.count()

print(f"STREAMING_VALID_ROWS={valid_count}")
print(f"STREAMING_QUARANTINE_ROWS={quarantine_count}")
print("STREAMING_VALID_SCHEMA_BEGIN")
valid.printSchema()
print("STREAMING_VALID_SCHEMA_END")
valid.orderBy("kafka_offset").show(20, truncate=False)
if quarantine_count:
    quarantine.orderBy("kafka_offset").show(20, truncate=False)
print("VERIFY_STREAMING_SUCCESS")

spark.stop()

