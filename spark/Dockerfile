ARG SPARK_IMAGE=apache/spark:3.5.9-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl python3-numpy \
    && curl -fL -o /opt/spark/jars/delta-spark_2.12-3.3.2.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar \
    && curl -fL -o /opt/spark/jars/delta-storage-3.3.2.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar \
    && curl -fL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.9.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9/spark-sql-kafka-0-10_2.12-3.5.9.jar \
    && curl -fL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.9.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.9/spark-token-provider-kafka-0-10_2.12-3.5.9.jar \
    && curl -fL -o /opt/spark/jars/kafka-clients-3.4.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    && curl -fL -o /opt/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    && python3 -m pip install --no-cache-dir --no-deps delta-spark==3.3.2 \
    && rm -rf /var/lib/apt/lists/*

USER spark
WORKDIR /opt/spark/work-dir
