ARG AIRFLOW_VERSION=2.10.5
ARG SPARK_RUNTIME_IMAGE=bigdata-diemthi-spark-client:3.5.9
FROM ${SPARK_RUNTIME_IMAGE} AS spark_runtime

FROM apache/airflow:${AIRFLOW_VERSION}-python3.10

USER root
COPY --from=spark_runtime /opt/spark /opt/spark
COPY --from=spark_runtime /opt/java/openjdk /opt/java/openjdk

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java/openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:/opt/java/openjdk/bin:${PATH}
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

USER airflow
RUN pip install --no-cache-dir numpy==1.26.4

