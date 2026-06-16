# Clean ETL Reset Guide

**Purpose:** Drop every table in both Azure SQL and Databricks Unity Catalog so the pipeline rebuilds from scratch. Useful after schema changes, code fixes, or corrupted state.

**Prerequisites:**
- `databricks` CLI (authenticated: `databricks auth login`)
- `pymssql` (`pip install pymssql`)
- Connectivity to Azure SQL (`sql-w3c-etl.database.windows.net`, port 1433)
- Access to Databricks workspace `adb-7405616994554630.10.azuredatabricks.net`

---

## Step 1: Drop Azure SQL Tables

Run this Python script to drop all tables across the three schemas (`dbo`, `staging`, `marts`):

```python
import pymssql
import os

# Credentials from Databricks secrets (or env vars)
server = "sql-w3c-etl.database.windows.net"
database = "w3c-etl-db"
username = "sqladmin"
password = os.environ["AZURE_SQL_PASS"]  # set this beforehand

conn = pymssql.connect(
    server=server,
    database=database,
    user=username,
    password=password,
    port=1433,
)
conn.autocommit(True)
cursor = conn.cursor()

# Drop all 20 tables — order matters due to FK constraints
# First drop marts and staging (which reference dbo dims)
# Then drop dbo tables

# Marts (6)
marts = [
    "mart_country_browser_share",
    "mart_daily_aggregates",
    "mart_crawler_analysis",
    "mart_browser_analysis",
    "mart_timeofday_analysis",
    "mart_page_performance",
]

# Staging (10)
staging = [
    "fact_webrequest",
    "dim_visit_buckets",
    "dim_visitortype",
    "dim_referrer",
    "dim_method",
    "dim_status",
    "dim_page",
    "dim_time",
    "dim_date",
    "crawler_ips",
]

# dbo (4)
dbo = [
    "dim_useragent",
    "dim_geolocation",
    "raw_enriched_loaded",
    "raw_enriched",
]

for table in marts:
    cursor.execute(f"DROP TABLE IF EXISTS marts.{table}")

for table in staging:
    cursor.execute(f"DROP TABLE IF EXISTS staging.{table}")

for table in dbo:
    cursor.execute(f"DROP TABLE IF EXISTS dbo.{table}")

print("All Azure SQL tables dropped.")

# Verify
for schema in ["dbo", "staging", "marts"]:
    cursor.execute(
        f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}'"
    )
    remaining = [row[0] for row in cursor.fetchall()]
    status = "empty" if not remaining else f"still has: {remaining}"
    print(f"  {schema}: {status}")

conn.close()
```

---

## Step 2: Drop Databricks Tables

Use the Databricks CLI to drop bronze and silver tables:

```bash
databricks auth login  # if not already authenticated

# Drop Silver materialized view
databricks warehouses execute \
    --warehouse-id "$(databricks warehouses list --output json | jq -r '.warehouses[0].id')" \
    --sql "DROP MATERIALIZED VIEW IF EXISTS w3c_etl_databricks.silver.silver_enriched_logs"

# Drop Bronze streaming table
databricks warehouses execute \
    --warehouse-id "$(databricks warehouses list --output json | jq -r '.warehouses[0].id')" \
    --sql "DROP STREAMING TABLE IF EXISTS w3c_etl_databricks.bronze.bronze_raw_logs"
```

Or via the SQL Warehouse endpoint directly:

```bash
databricks warehouses execute \
    --warehouse-id "<your-warehouse-id>" \
    --sql "DROP MATERIALIZED VIEW IF EXISTS w3c_etl_databricks.silver.silver_enriched_logs"

databricks warehouses execute \
    --warehouse-id "<your-warehouse-id>" \
    --sql "DROP STREAMING TABLE IF EXISTS w3c_etl_databricks.bronze.bronze_raw_logs"
```

---

## Step 3: Verify Clean State

### Azure SQL
```sql
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('dbo', 'staging', 'marts')
ORDER BY TABLE_SCHEMA, TABLE_NAME;
```
Expected result: **0 rows**.

### Databricks Unity Catalog
```bash
databricks warehouses execute \
    --warehouse-id "<your-warehouse-id>" \
    --sql "SHOW TABLES IN w3c_etl_databricks.bronze"

databricks warehouses execute \
    --warehouse-id "<your-warehouse-id>" \
    --sql "SHOW TABLES IN w3c_etl_databricks.silver"
```
Expected: **"No rows"** (or empty) for both.

---

## Step 4: Trigger Pipeline

Once everything is clean, trigger the pipeline via Airflow:

1. Unpause both DAGs (if paused):
   - `spark_ingestion_azure` → `dbt_marts_azure`

2. Trigger `spark_ingestion_azure` manually:
   ```bash
   # Via Airflow UI → DAGs → spark_ingestion_azure → Trigger DAG
   # Or via Airflow CLI / REST API
   curl -X POST "http://localhost:8080/api/v1/dags/spark_ingestion_azure/dagRuns" \
     -H "Content-Type: application/json" \
     -u "admin:admin" \
     -d '{}'
   ```

3. `dbt_marts_azure` will automatically trigger via Dataset when `spark_ingestion_azure` completes (no manual trigger needed).

---

## Pipeline Output (Expected)

After a clean run with all fixes applied:

| Component | Tables | Rows (approximate) |
|-----------|--------|-------------------|
| **DLT Bronze** | `bronze.bronze_raw_logs` | ~153K |
| **DLT Silver** | `silver.silver_enriched_logs` | ~153K (31 columns with geo + computed fields) |
| **SQL raw_enriched** | `dbo.raw_enriched` | ~153K |
| **SQL tracking** | `dbo.raw_enriched_loaded` | ~100–300 (distinct source files) |
| **SQL dim_geolocation** | `dbo.dim_geolocation` | ~1,586 |
| **SQL dim_useragent** | `dbo.dim_useragent` | ~2,041 |
| **dbt staging** | `staging.fact_webrequest` | ~153K (27 columns, with valid geo + UA SKs) |
| **dbt staging** | `staging.dim_*` | 5 dimension tables |
| **dbt marts** | `marts.mart_*` | 6 aggregate tables (browser_analysis should be non-empty) |
| **CSV output** | `Star-Schema/*.csv` | 18 files (fact now has is_weekend, day_of_week, request_hour) |

---

## Troubleshooting

| Problem | Likely Cause | Fix |
|---------|-------------|-----|
| `DROP` on Azure SQL fails with FK constraint | Wrong drop order | Drop marts → staging → dbo (FKs point from staging → dbo) |
| `databricks warehouses execute` fails | No active SQL warehouse | Start one: `databricks warehouses start <warehouse-id>` |
| DAG trigger returns 404 | Airflow not running or DAG paused | Open Airflow UI at `localhost:8080`, unpause the DAG |
| Pipeline runs but fact still has `geolocation_sk = -1` | Env var mismatch | Verify `AZURE_SQL_DATABASE` and `AZURE_SQL_DB` resolve to same value |

---

## References

- DAG definitions: `airflow/dags/w3c/spark_ingestion_azure.py`, `airflow/dags/w3c/dbt_marts_azure.py`
- dbt models: `airflow/dbt/w3c/models/staging/`, `airflow/dbt/w3c/models/marts/`
- DLT scripts: `airflow/spark/databricks/dlt_bronze.py`, `airflow/spark/databricks/dlt_silver.py`
- JDBC export: `airflow/spark/databricks/jdbc_export_azure.py`
- CSV export: `airflow/plugins/operators/export_csv_azure.py`
- Audit report: `plans/csv_data_quality_audit.md`
