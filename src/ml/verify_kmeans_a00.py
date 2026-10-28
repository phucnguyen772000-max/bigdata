from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, functions as F


FEATURES_PATH = "hdfs://namenode:9000/lakehouse/diemthi_hcm/features/a00_clusters"
MODEL_PATH = "hdfs://namenode:9000/lakehouse/diemthi_hcm/models/kmeans_a00"
GOLD_STATS_PATH = "hdfs://namenode:9000/lakehouse/diemthi_hcm/gold/a00_cluster_statistics"

spark = SparkSession.builder.appName("Verify-KMeans-A00").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

features = spark.read.parquet(FEATURES_PATH)
stats = spark.read.parquet(GOLD_STATS_PATH)
model = PipelineModel.load(MODEL_PATH)

print("FEATURES_SCHEMA_BEGIN")
features.printSchema()
print("FEATURES_SCHEMA_END")
print(f"FEATURES_TOTAL_ROWS={features.count()}")
print("FEATURES_BY_YEAR_CLUSTER")
features.groupBy("Nam", "cluster").count().orderBy("Nam", "cluster").show(100, False)

print("GOLD_CLUSTER_STATISTICS")
stats.orderBy("Nam", "cluster").show(100, False)
print(f"GOLD_STATS_ROWS={stats.count()}")

print(f"MODEL_STAGE_COUNT={len(model.stages)}")
for index, stage in enumerate(model.stages):
    print(f"MODEL_STAGE_{index}={stage.__class__.__name__}")

print("VERIFY_KMEANS_A00_SUCCESS")
spark.stop()
