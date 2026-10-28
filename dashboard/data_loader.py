from __future__ import annotations

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession, functions as F

HDFS_GOLD = "hdfs://namenode:9000/lakehouse/diemthi_hcm/gold"


@st.cache_resource(show_spinner="Đang khởi tạo Spark...")
def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DiemThiHCMStreamlit")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "768m")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def _read_gold(table: str):
    return get_spark().read.parquet(f"{HDFS_GOLD}/{table}")


@st.cache_data(ttl=600, show_spinner="Đang đọc tổng quan từ Gold...")
def load_yearly_summary() -> pd.DataFrame:
    return _read_gold("yearly_summary").orderBy("Nam").toPandas()


@st.cache_data(ttl=600, show_spinner="Đang đọc thống kê môn học từ Gold...")
def load_subject_statistics(
    year: int | None = None, subject: str | None = None
) -> pd.DataFrame:
    frame = _read_gold("subject_statistics")
    if year is not None:
        frame = frame.filter(F.col("Nam") == year)
    if subject is not None:
        frame = frame.filter(F.col("Mon") == subject)
    return frame.orderBy("Nam", "Mon").toPandas()


@st.cache_data(ttl=600, show_spinner="Đang đọc phổ điểm từ Gold...")
def load_subject_distribution(year: int, subject: str) -> pd.DataFrame:
    frame = (
        _read_gold("subject_distribution")
        .filter((F.col("Nam") == year) & (F.col("Mon") == subject))
        .withColumn(
            "_NhomDiemSort",
            F.regexp_extract(F.col("NhomDiem").cast("string"), r"-?\d+(?:\.\d+)?", 0).cast("double"),
        )
        .orderBy("_NhomDiemSort", "NhomDiem")
        .drop("_NhomDiemSort")
    )
    return frame.toPandas()


@st.cache_data(ttl=600, show_spinner="Đang đọc thống kê tổ hợp từ Gold...")
def load_combination_statistics(
    year: int | None = None, combination: str | None = None
) -> pd.DataFrame:
    frame = _read_gold("combination_statistics")
    if year is not None:
        frame = frame.filter(F.col("Nam") == year)
    if combination is not None:
        frame = frame.filter(F.col("ToHop") == combination)
    return frame.orderBy("Nam", "ToHop").toPandas()


@st.cache_data(ttl=600, show_spinner="Đang đọc kết quả KMeans từ Gold...")
def load_cluster_statistics() -> pd.DataFrame:
    return _read_gold("a00_cluster_statistics").orderBy("Nam", "cluster").toPandas()
