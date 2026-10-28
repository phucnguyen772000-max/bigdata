import argparse

from pyspark.sql import SparkSession


ROOT = "hdfs://namenode:9000/lakehouse/diemthi_hcm"
YEARS = (2022, 2023, 2024)


def hadoop_path_exists(spark, path: str) -> bool:
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    return jvm.org.apache.hadoop.fs.FileSystem.get(conf).exists(jvm.org.apache.hadoop.fs.Path(path))


parser = argparse.ArgumentParser()
parser.add_argument("--check", required=True, choices=("hdfs", "bronze"))
args = parser.parse_args()

spark = SparkSession.builder.appName(f"PipelineCheck-{args.check}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

if args.check == "hdfs":
    if not hadoop_path_exists(spark, ROOT):
        raise RuntimeError(f"HDFS lakehouse root does not exist: {ROOT}")
    print(f"CHECK_HDFS_SUCCESS path={ROOT}")
else:
    for year in YEARS:
        path = f"{ROOT}/bronze/year={year}"
        if not hadoop_path_exists(spark, path):
            raise RuntimeError(f"Bronze partition does not exist: {path}")
        if not spark.read.option("header", "true").csv(path).take(1):
            raise RuntimeError(f"Bronze partition is empty: {path}")
        print(f"CHECK_BRONZE_YEAR_SUCCESS year={year} path={path}")
    print("CHECK_BRONZE_SUCCESS")

spark.stop()

