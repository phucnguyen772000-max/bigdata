ARG SPARK_IMAGE=apache/spark:3.5.9
FROM ${SPARK_IMAGE}

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && curl -fL -o /opt/spark/jars/delta-spark_2.12-3.3.2.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar \
    && curl -fL -o /opt/spark/jars/delta-storage-3.3.2.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m pip install --no-cache-dir --no-deps delta-spark==3.3.2 \
    && python3 -m pip install --no-cache-dir \
    streamlit==1.39.0 \
    pandas==2.2.3

WORKDIR /app/dashboard
EXPOSE 8501

USER spark

CMD ["python3", "-m", "streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
