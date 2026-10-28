import argparse
from functools import reduce

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


KAFKA_SERVERS = "kafka:9092"
TOPIC = "diemthi_realtime"
STREAMING_BRONZE = "hdfs://namenode:9000/lakehouse/diemthi_hcm/streaming/bronze"
STREAMING_QUARANTINE = "hdfs://namenode:9000/lakehouse/diemthi_hcm/streaming/quarantine"
CHECKPOINT_BASE = "hdfs://namenode:9000/lakehouse/diemthi_hcm/checkpoints/diemthi_realtime"
SCORE_COLUMNS = [
    "Toan", "NguVan", "VatLy", "HoaHoc", "SinhHoc", "LichSu", "DiaLy", "NgoaiNgu"
]
VALID_TINH_CODES = list(range(1, 20)) + list(range(21, 65))

EVENT_SCHEMA = StructType(
    [
        StructField("SBD", StringType()),
        StructField("Nam", IntegerType()),
        StructField("Tinh", IntegerType()),
        *[StructField(column, DoubleType()) for column in SCORE_COLUMNS],
        StructField("event_time", StringType()),
        StructField("source", StringType()),
    ]
)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Kafka to HDFS Structured Streaming job")
    parser.add_argument("--mode", choices=("console", "hdfs"), default="hdfs")
    parser.add_argument("--available-now", action="store_true")
    return parser.parse_args()


def add_quality_columns(frame):
    missing_sbd = F.col("SBD").isNull() | (F.trim(F.col("SBD")) == "")
    invalid_tinh = F.col("Tinh").isNull() | ~F.col("Tinh").isin(VALID_TINH_CODES)
    invalid_score_conditions = [
        F.col(column).isNotNull() & ((F.col(column) < 0) | (F.col(column) > 10))
        for column in SCORE_COLUMNS
    ]
    invalid_score = reduce(lambda left, right: left | right, invalid_score_conditions)
    malformed_json = F.col("_parsed").isNull()
    return (
        frame.withColumn(
            "_dq_reasons",
            F.concat_ws(
                "|",
                F.when(malformed_json, F.lit("malformed_json")),
                F.when(missing_sbd, F.lit("missing_sbd")),
                F.when(invalid_tinh, F.lit("invalid_tinh")),
                F.when(invalid_score, F.lit("score_out_of_range")),
            ),
        )
        .withColumn("ingest_time", F.current_timestamp())
        .withColumn("is_valid", F.length(F.col("_dq_reasons")) == 0)
    )


args = parse_arguments()
spark = (
    SparkSession.builder.appName("DiemThi-Kafka-To-HDFS")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

decoded = raw.select(
    F.col("key").cast("string").alias("kafka_key"),
    F.col("value").cast("string").alias("raw_json"),
    F.col("topic").alias("kafka_topic"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
    F.col("timestamp").alias("kafka_timestamp"),
).withColumn("_parsed", F.from_json(F.col("raw_json"), EVENT_SCHEMA))

quality = add_quality_columns(decoded.select("*", "_parsed.*"))
output_columns = [
    "SBD", "Nam", "Tinh", *SCORE_COLUMNS, "event_time", "source", "ingest_time",
    "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
    "raw_json", "_dq_reasons", "is_valid",
]

if args.mode == "console":
    writer = (
        quality.select(*output_columns)
        .writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/console")
    )
    if args.available_now:
        writer = writer.trigger(availableNow=True)
    query = writer.start()
    query.awaitTermination()
else:
    valid = quality.filter(F.col("is_valid")).select(*output_columns)
    invalid = quality.filter(~F.col("is_valid")).select(*output_columns)

    valid_writer = (
        valid.writeStream.format("parquet")
        .outputMode("append")
        .option("path", STREAMING_BRONZE)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/valid")
    )
    invalid_writer = (
        invalid.writeStream.format("parquet")
        .outputMode("append")
        .option("path", STREAMING_QUARANTINE)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/quarantine")
    )
    if args.available_now:
        valid_writer = valid_writer.trigger(availableNow=True)
        invalid_writer = invalid_writer.trigger(availableNow=True)

    valid_query = valid_writer.start(queryName="diemthi_valid_to_hdfs")
    quarantine_query = invalid_writer.start(queryName="diemthi_invalid_to_hdfs")
    print(f"VALID_OUTPUT={STREAMING_BRONZE}")
    print(f"QUARANTINE_OUTPUT={STREAMING_QUARANTINE}")
    print(f"CHECKPOINT_BASE={CHECKPOINT_BASE}")
    if args.available_now:
        valid_query.awaitTermination()
        quarantine_query.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()

spark.stop()

