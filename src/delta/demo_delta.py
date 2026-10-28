from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TABLE_PATH = "hdfs://namenode:9000/lakehouse/diemthi_hcm/delta/admission_scenarios"
LOG_DIR = Path("/app/docs")


def make_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DeltaLakeAdmissionScenarioDemo")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def table_exists(spark: SparkSession) -> bool:
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(TABLE_PATH)
    return path.getFileSystem(spark._jsc.hadoopConfiguration()).exists(path)


def detail_metrics(spark: SparkSession) -> dict:
    row = spark.sql(f"DESCRIBE DETAIL delta.`{TABLE_PATH}`").first()
    files = int(row["numFiles"])
    size = int(row["sizeInBytes"])
    return {
        "numFiles": files,
        "sizeInBytes": size,
        "averageFileSize": size / files if files else 0,
    }


def write_log(name: str, lines: list[str]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    (LOG_DIR / name).write_text("\n".join(lines) + "\n", encoding="utf-8")


spark = make_spark()
spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("scenario_id", StringType(), False),
        StructField("Nam", IntegerType(), False),
        StructField("ToHop", StringType(), False),
        StructField("NguongDiem", DoubleType(), False),
        StructField("TenKichBan", StringType(), False),
        StructField("CreatedAt", TimestampType(), False),
        StructField("UpdatedAt", TimestampType(), False),
    ]
)

if not table_exists(spark):
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    initial = spark.createDataFrame(
        [("SC001", 2024, "A00", 24.0, "A00 2024", created_at, created_at)],
        schema,
    )
    initial.write.format("delta").mode("errorifexists").save(TABLE_PATH)

    history_after_create = DeltaTable.forPath(spark, TABLE_PATH).history()
    initial_version = int(history_after_create.agg({"version": "max"}).first()[0])

    updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    source = spark.createDataFrame(
        [
            ("SC001", 2024, "A00", 26.0, "A00 2024 cap nhat", created_at, updated_at),
            ("SC002", 2024, "A01", 24.0, "A01 2024", updated_at, updated_at),
        ],
        schema,
    )

    delta_table = DeltaTable.forPath(spark, TABLE_PATH)
    (
        delta_table.alias("target")
        .merge(source.alias("source"), "target.scenario_id = source.scenario_id")
        .whenMatchedUpdate(
            set={
                "Nam": "source.Nam",
                "ToHop": "source.ToHop",
                "NguongDiem": "source.NguongDiem",
                "TenKichBan": "source.TenKichBan",
                "UpdatedAt": "source.UpdatedAt",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    resumed = False
else:
    delta_table = DeltaTable.forPath(spark, TABLE_PATH)
    current_for_resume = delta_table.toDF()
    resume_sc001 = current_for_resume.filter("scenario_id = 'SC001'").first()
    resume_sc002_count = current_for_resume.filter("scenario_id = 'SC002'").count()
    if (
        current_for_resume.count() != 2
        or resume_sc001 is None
        or float(resume_sc001["NguongDiem"]) != 26.0
        or resume_sc002_count != 1
    ):
        raise RuntimeError(
            "Existing Delta table is not the expected post-MERGE demo state; "
            "refusing to overwrite or modify it."
        )
    initial_version = int(delta_table.history().agg({"version": "min"}).first()[0])
    resumed = True

old_rows = spark.read.format("delta").option("versionAsOf", initial_version).load(TABLE_PATH)
old_sc001 = old_rows.filter("scenario_id = 'SC001'").first().asDict()
before_count = old_rows.count()

current = spark.read.format("delta").load(TABLE_PATH)
after_count = current.count()
latest_version = int(DeltaTable.forPath(spark, TABLE_PATH).history().agg({"version": "max"}).first()[0])
new_sc001 = current.filter("scenario_id = 'SC001'").first().asDict()

assert before_count == 1
assert after_count == 2
assert old_sc001["NguongDiem"] == 24.0
assert new_sc001["NguongDiem"] == 26.0
assert current.filter("scenario_id = 'SC001'").count() == 1
assert current.filter("scenario_id = 'SC002'").count() == 1

history_rows = (
    spark.sql(f"DESCRIBE HISTORY delta.`{TABLE_PATH}`")
    .select("version", "timestamp", "operation", "operationParameters")
    .orderBy("version")
    .collect()
)
history_lines = ["version\ttimestamp\toperation\toperationParameters"]
for row in history_rows:
    history_lines.append(
        f"{row['version']}\t{row['timestamp']}\t{row['operation']}\t"
        f"{json.dumps(row['operationParameters'], ensure_ascii=False, default=str)}"
    )
write_log("delta-history.txt", history_lines)

time_travel_lines = [
    f"TABLE_PATH={TABLE_PATH}",
    f"OLD_VERSION={initial_version}",
    f"OLD_SC001_NGUONGDIEM={old_sc001['NguongDiem']}",
    f"OLD_SC001_TENKICHBAN={old_sc001['TenKichBan']}",
    f"LATEST_VERSION_BEFORE_OPTIMIZE={latest_version}",
    f"LATEST_SC001_NGUONGDIEM={new_sc001['NguongDiem']}",
    f"LATEST_SC001_TENKICHBAN={new_sc001['TenKichBan']}",
    "TIME_TRAVEL_DIFFERENT=true",
]
write_log("delta-time-travel.txt", time_travel_lines)

before_optimize = detail_metrics(spark)
optimize_result = spark.sql(f"OPTIMIZE delta.`{TABLE_PATH}`").collect()
after_optimize = detail_metrics(spark)
optimize_lines = [
    f"TABLE_PATH={TABLE_PATH}",
    "OPTIMIZE_SUPPORTED=true",
    f"BEFORE={json.dumps(before_optimize)}",
    f"AFTER={json.dumps(after_optimize)}",
    "OPTIMIZE_RESULT:",
] + [str(row.asDict(recursive=True)) for row in optimize_result]
write_log("delta-optimize.txt", optimize_lines)

final_rows = [str(row.asDict()) for row in current.orderBy("scenario_id").collect()]
demo_lines = [
    "DELTA_VERSION=3.3.2",
    f"TABLE_PATH={TABLE_PATH}",
    f"INITIAL_VERSION={initial_version}",
    f"LATEST_VERSION_BEFORE_OPTIMIZE={latest_version}",
    f"COUNT_BEFORE_MERGE={before_count}",
    f"COUNT_AFTER_MERGE={after_count}",
    "SC001_UPDATED=true",
    "SC002_INSERTED=true",
    f"RESUMED_AFTER_LOG_PERMISSION_FAILURE={str(resumed).lower()}",
    "ROWS:",
] + final_rows + ["DELTA_DEMO_SUCCESS=true"]
write_log("delta-demo.txt", demo_lines)

print("\n".join(demo_lines))
print("\n".join(time_travel_lines))
print("OPTIMIZE_BEFORE", before_optimize)
print("OPTIMIZE_AFTER", after_optimize)
print("DELTA_DEMO_COMPLETE")

spark.stop()
