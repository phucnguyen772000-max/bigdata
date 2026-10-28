from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"
PROJECT_ROOT = "/opt/project"


def spark_job(script: str, arguments: str = "") -> str:
    return " ".join(
        part
        for part in (
            SPARK_SUBMIT,
            f"--master {SPARK_MASTER}",
            "--conf spark.driver.host=airflow-scheduler",
            "--conf spark.driver.bindAddress=0.0.0.0",
            "--conf spark.cores.max=4",
            "--conf spark.executor.memory=1g",
            f"{PROJECT_ROOT}/{script}",
            arguments,
        )
        if part
    )


default_args = {
    "owner": "bigdata-diemthi",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="diemthi_bigdata_pipeline",
    description="HDFS Bronze to Silver, Gold and KMeans pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["bigdata", "hdfs", "spark", "kmeans"],
) as dag:
    check_hdfs = BashOperator(
        task_id="check_hdfs",
        bash_command=spark_job("src/orchestration/pipeline_checks.py", "--check hdfs"),
    )
    check_bronze = BashOperator(
        task_id="check_bronze",
        bash_command=spark_job("src/orchestration/pipeline_checks.py", "--check bronze"),
    )
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=spark_job("src/transform/bronze_to_silver.py"),
    )
    verify_silver = BashOperator(
        task_id="verify_silver",
        bash_command=spark_job("src/transform/verify_silver.py"),
    )
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=spark_job("src/transform/silver_to_gold.py"),
    )
    verify_gold = BashOperator(
        task_id="verify_gold",
        bash_command=spark_job("src/transform/verify_gold.py"),
    )
    train_kmeans = BashOperator(
        task_id="train_kmeans",
        bash_command=spark_job("src/ml/train_kmeans_a00.py"),
    )
    verify_model = BashOperator(
        task_id="verify_model",
        bash_command=spark_job("src/ml/verify_kmeans_a00.py"),
    )

    (
        check_hdfs
        >> check_bronze
        >> bronze_to_silver
        >> verify_silver
        >> silver_to_gold
        >> verify_gold
        >> train_kmeans
        >> verify_model
    )

