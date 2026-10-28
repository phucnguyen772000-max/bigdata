from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, functions as F


SILVER_PATH = "hdfs://namenode:9000/lakehouse/diemthi_hcm/silver"
FEATURES_PATH = "hdfs://namenode:9000/lakehouse/diemthi_hcm/features/a00_clusters"
MODEL_PATH = "hdfs://namenode:9000/lakehouse/diemthi_hcm/models/kmeans_a00"
GOLD_STATS_PATH = (
    "hdfs://namenode:9000/lakehouse/diemthi_hcm/gold/a00_cluster_statistics"
)

FEATURE_COLUMNS = ["Toan", "VatLy", "HoaHoc"]
K_VALUES = [2, 3, 4, 5, 6]
SEED = 42


spark = (
    SparkSession.builder
    .appName("BigDataDiemThi-KMeans-A00")
    .config("spark.sql.shuffle.partitions", "12")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("KMEANS_A00_START")
print(f"SILVER_PATH={SILVER_PATH}")

silver = spark.read.parquet(SILVER_PATH)
silver_total = silver.count()

a00_input = (
    silver
    .select("SBD", "Nam", "Toan", "VatLy", "HoaHoc", "A00")
    .filter(
        F.col("Toan").isNotNull()
        & F.col("VatLy").isNotNull()
        & F.col("HoaHoc").isNotNull()
        & F.col("A00").isNotNull()
    )
    .persist(StorageLevel.MEMORY_AND_DISK)
)

a00_count = a00_input.count()
null_filtered_count = silver_total - a00_count

print(f"SILVER_TOTAL_ROWS={silver_total}")
print(f"A00_TRAINING_ROWS={a00_count}")
print(f"ROWS_EXCLUDED_MISSING_A00_SUBJECTS={null_filtered_count}")
print("A00_TRAINING_ROWS_BY_YEAR")
a00_input.groupBy("Nam").count().orderBy("Nam").show(truncate=False)

if a00_count == 0:
    raise RuntimeError("No complete A00 rows are available for KMeans training")

assembler = VectorAssembler(
    inputCols=FEATURE_COLUMNS,
    outputCol="rawFeatures",
    handleInvalid="error",
)

scaler = StandardScaler(
    inputCol="rawFeatures",
    outputCol="scaledFeatures",
    withMean=True,
    withStd=True,
)

evaluator = ClusteringEvaluator(
    featuresCol="scaledFeatures",
    predictionCol="cluster",
    metricName="silhouette",
    distanceMeasure="squaredEuclidean",
)

scores = []
best_k = None
best_score = float("-inf")
best_pipeline_model = None

for k in K_VALUES:
    kmeans = KMeans(
        k=k,
        seed=SEED,
        maxIter=50,
        tol=1e-4,
        featuresCol="scaledFeatures",
        predictionCol="cluster",
        distanceMeasure="euclidean",
    )

    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    pipeline_model = pipeline.fit(a00_input)
    predictions = pipeline_model.transform(a00_input)
    silhouette = evaluator.evaluate(predictions)
    scores.append((k, silhouette))

    print(f"K={k} SILHOUETTE={silhouette:.6f}")

    if silhouette > best_score:
        best_k = k
        best_score = silhouette
        best_pipeline_model = pipeline_model

if best_pipeline_model is None:
    raise RuntimeError("KMeans model selection did not produce a model")

print(f"BEST_K={best_k}")
print(f"BEST_SILHOUETTE={best_score:.6f}")

best_predictions = (
    best_pipeline_model
    .transform(a00_input)
    .persist(StorageLevel.MEMORY_AND_DISK)
)

prediction_count = best_predictions.count()
print(f"PREDICTION_ROWS={prediction_count}")

scaler_model = best_pipeline_model.stages[1]
kmeans_model = best_pipeline_model.stages[2]
scaled_centers = kmeans_model.clusterCenters()
means = list(scaler_model.mean)
stds = list(scaler_model.std)

print("CLUSTER_CENTERS_SCALED_BEGIN")
for cluster_id, center in enumerate(scaled_centers):
    values = ", ".join(f"{value:.6f}" for value in center)
    print(f"CLUSTER={cluster_id} CENTER_SCALED=[{values}]")
print("CLUSTER_CENTERS_SCALED_END")

print("CLUSTER_CENTERS_ORIGINAL_SCALE_BEGIN")
for cluster_id, center in enumerate(scaled_centers):
    original = [
        (float(center[index]) * stds[index]) + means[index]
        for index in range(len(FEATURE_COLUMNS))
    ]
    print(
        f"CLUSTER={cluster_id} "
        f"Toan={original[0]:.4f} "
        f"VatLy={original[1]:.4f} "
        f"HoaHoc={original[2]:.4f} "
        f"A00_APPROX={sum(original):.4f}"
    )
print("CLUSTER_CENTERS_ORIGINAL_SCALE_END")

prediction_output = best_predictions.select(
    "SBD",
    "Nam",
    "Toan",
    "VatLy",
    "HoaHoc",
    "A00",
    F.col("cluster").cast("int").alias("cluster"),
)

(
    prediction_output
    .repartition(6, "Nam")
    .write
    .mode("overwrite")
    .partitionBy("Nam")
    .parquet(FEATURES_PATH)
)

best_pipeline_model.write().overwrite().save(MODEL_PATH)

cluster_statistics = (
    prediction_output
    .groupBy("Nam", "cluster")
    .agg(
        F.count("*").alias("SoThiSinh"),
        F.round(F.avg("Toan"), 4).alias("ToanTB"),
        F.round(F.avg("VatLy"), 4).alias("VatLyTB"),
        F.round(F.avg("HoaHoc"), 4).alias("HoaHocTB"),
        F.round(F.avg("A00"), 4).alias("A00TB"),
    )
    .orderBy("Nam", "cluster")
)

print("A00_CLUSTER_STATISTICS_BEGIN")
cluster_statistics.show(100, truncate=False)
print("A00_CLUSTER_STATISTICS_END")

(
    cluster_statistics
    .repartition("Nam")
    .write
    .mode("overwrite")
    .partitionBy("Nam")
    .parquet(GOLD_STATS_PATH)
)

print(f"FEATURES_PATH={FEATURES_PATH}")
print(f"MODEL_PATH={MODEL_PATH}")
print(f"GOLD_STATS_PATH={GOLD_STATS_PATH}")
print("KMEANS_A00_SUCCESS")

best_predictions.unpersist()
a00_input.unpersist()
spark.stop()
