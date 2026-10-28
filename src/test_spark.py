from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("BigDataDiemThi-ClusterTest")
    .getOrCreate()
)

sc = spark.sparkContext
numbers = sc.parallelize(range(1, 100_001), 8)
partition_count = numbers.getNumPartitions()
distributed_sum = numbers.map(lambda value: value * value).sum()

print(f"SPARK_MASTER={sc.master}")
print(f"DEFAULT_PARALLELISM={sc.defaultParallelism}")
print(f"PARTITION_COUNT={partition_count}")
print(f"DISTRIBUTED_SUM_OF_SQUARES={int(distributed_sum)}")

spark.stop()
