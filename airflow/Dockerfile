FROM apache/airflow:2.10.2
COPY requirements.txt .
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir pyspark==4.0.2 \
    && pip install --no-cache-dir --no-deps apache-airflow-providers-apache-spark==4.11.3
