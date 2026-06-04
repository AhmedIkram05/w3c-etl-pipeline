FROM apache/airflow:2.10.2
COPY requirements.txt .
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Microsoft ODBC Driver 18 for SQL Server (required by dbt-sqlserver + pyodbc)
# Base image is Debian Bookworm
RUN apt-get update && apt-get install -y curl gnupg2 && rm -rf /var/lib/apt/lists/* \
    && curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir pyspark==4.0.2 \
    && pip install --no-cache-dir --no-deps apache-airflow-providers-apache-spark==4.11.3 \
    && pip install --no-cache-dir apache-airflow-providers-databricks==4.6.0
