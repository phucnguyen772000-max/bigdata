from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pandas as pd
import streamlit as st
from delta.tables import DeltaTable
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_loader import get_spark

ADMISSION_SCENARIOS_PATH = (
    "hdfs://namenode:9000/lakehouse/diemthi_hcm/delta/admission_scenarios"
)

SCENARIO_SCHEMA = StructType(
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


@st.cache_data(ttl=60, show_spinner="Đang đọc các kịch bản từ Delta...")
def load_admission_scenarios() -> pd.DataFrame:
    spark = get_spark()
    return (
        spark.read.format("delta")
        .load(ADMISSION_SCENARIOS_PATH)
        .orderBy("scenario_id")
        .toPandas()
    )


def save_admission_scenario(
    year: int,
    combination: str,
    threshold: float,
    name: str,
    scenario_id: str | None = None,
) -> str:
    clean_name = name.strip()
    if not clean_name:
        raise ValueError("Tên kịch bản không được để trống.")

    spark = get_spark()
    selected_id = scenario_id or f"WEB-{uuid4()}"
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    table = DeltaTable.forPath(spark, ADMISSION_SCENARIOS_PATH)
    existing = (
        table.toDF()
        .filter(table.toDF().scenario_id == selected_id)
        .select("CreatedAt")
        .first()
    )
    created_at = existing["CreatedAt"] if existing else now

    source = spark.createDataFrame(
        [
            (
                selected_id,
                int(year),
                combination,
                float(threshold),
                clean_name,
                created_at,
                now,
            )
        ],
        SCENARIO_SCHEMA,
    )

    (
        table.alias("target")
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

    load_admission_scenarios.clear()
    return selected_id
