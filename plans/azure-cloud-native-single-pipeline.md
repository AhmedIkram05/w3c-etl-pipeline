# Azure Cloud-Native Single-Pipeline ETL Platform Implementation Plan

**Version:** v2.9
**Status:** Phase 8a ✅ and 8b ✅ complete
**Budget:** $149 Azure credit cap
**CV Impact:** High - demonstrates cloud-native DE, Databricks DLT, Unity Catalog, dbt, Azure SQL, and end-to-end data platform ownership
**Target Roles:** Data Engineer, Cloud Data Engineer, Data Platform Engineer

---

## Goal

Build a single, cloud-native ETL pipeline that processes W3C web logs from ADLS Gen2 through Databricks Delta Live Tables (Bronze → Silver), exports to Azure SQL, builds dimensional models via dbt, and produces Power BI-ready CSV exports. The pipeline eliminates architectural complexity by removing the parallel Docker Spark production path, establishing Databricks DLT as the sole production ETL engine while preserving Docker for local development and orchestration hosting.

**Target Resume Line:**
"Architected and implemented a cloud-native W3C log ETL platform on Azure using Databricks Delta Live Tables, Unity Catalog, and Azure SQL. Built Bronze/Silver DLT pipelines with custom W3C parsing, MaxMind GeoIP enrichment, and data quality checks. Orchestrated via Airflow with Databricks Workflows integration, deployed dimensional models with dbt (T-SQL migration), and automated CI/CD with split-tier GitHub Actions. Delivered 18 Power BI-ready CSV exports with end-to-end monitoring and $100 cost controls."

---

## Architecture Overview

### Single Pipeline Flow

```
W3C Log Files (IIS format)
      │
      ▼
ADLS Gen2 (raw-logs/ container)
      │
      ▼
Databricks Auto Loader (Binary File format)
      │
      ▼
DLT Bronze Pipeline (dlt_bronze.py)
  - Custom W3C parser UDF (rsplit field-counting for unquoted UA strings)
  - 14-field and 18-field IIS format detection
  - @dlt.expect_or_drop quality rules
  - Partitioned by log_date
  - ROW_NUMBER dedup CTE for full_refresh idempotency
      │
      ▼
DLT Silver Pipeline (dlt_silver.py)
  - 7 MaxMind GeoLite2 UDFs (country, region, city, latitude, longitude, postcode, isp)
  - 5 computed field UDFs (page_category, referrer_domain, traffic_type, is_crawler, size_band)
  - UA columns (agent_type, browser_name, browser_version, os, device_type) excluded from Silver DDL
  - 6 geo columns preserved in Silver (country, region, city, lat, lon, isp) for dim_geolocation
  - @dlt.expect_or_drop quality rules
      │
      ▼
JDBC Export (jdbc_export_azure.py — Databricks notebook_task on serverless)
  - Reads directly from Silver (no Gold table)
  - Tracking table for idempotency (IF OBJECT_ID DDL guards)
  - Writes to Azure SQL dbo.raw_enriched via pymssql (not JDBC driver — serverless limitation)
  - Retry logic with exponential backoff for Azure SQL auto-resume
      │
      ▼
Azure SQL Database (dbo.raw_enriched)
      │
      ▼
spark_ingestion_azure.py DAG — export_dimensions task (PythonOperator)
  - Reads from Azure SQL dbo.raw_enriched
  - Builds dim_geolocation (MERGE upsert on geo_hash)
  - Builds dim_useragent (MERGE upsert on ua_hash)
  - Fires Dataset outlet (mssql://azure-sql/dbo/raw_enriched_loaded) for dbt DAG
      │
      ▼
dbt (w3c_azure profile → Azure SQL target)
  - 16 models with inline {% if target.type == 'sqlserver' %} T-SQL macros
  - ~62 tests
  - dbt docs / data catalog generated and hosted
  - 18 Power BI CSV exports
      │
      ▼
Power BI (18 CSV files under airflow/data/Star-Schema/)
```

### Orchestration Flow

```
Airflow (Docker container)

  DAG 1: spark_ingestion_azure.py (Daily 2 AM UTC — schedule: "0 2 * * *")
    │
    ├─ Task 1: bronze_silver_jdbc_pipeline
    │    └─ DatabricksRunNowOperator → Databricks Workflows (job_id: 847995192336508)
    │         ├─ Task 1: DLT Bronze pipeline (pipeline_task → w3c_etl_databricks.bronze.bronze_raw_logs)
    │         ├─ Task 2: DLT Silver pipeline (pipeline_task → w3c_etl_databricks.silver.silver_enriched_logs)
    │         └─ Task 3: jdbc_export_azure.py (notebook_task → dbo.raw_enriched via pymssql)
    │
    └─ Task 2: export_dimensions (depends on Task 1)
         └─ PythonOperator (inline _export_dimensions callable)
              ├─ Builds dbo.dim_geolocation (MERGE upsert on geo_hash)
              ├─ Builds dbo.dim_useragent (MERGE upsert on ua_hash)
              └─ Fires Dataset: mssql://azure-sql/dbo/raw_enriched_loaded

  DAG 2: dbt_marts_azure.py (Dataset-triggered by spark_ingestion_azure's outlet)
    └─ DatabricksSubmitRunOperator running dbt against Azure SQL
         ├─ dbt run --profile w3c_azure
         ├─ dbt test --profile w3c_azure
         └─ dbt docs generate --profile w3c_azure
```

### Docker Role (Development Only)

Docker is used exclusively for:

- Airflow scheduler and webserver (orchestration layer, not Spark execution)
- PostgreSQL (Airflow metastore only)
- Local PySpark scripts (unit test substrate — business logic shared with DLT code)
- Grafana + Prometheus (monitoring)
- CI substrate for unit tests and DAG integrity tests

Docker is NOT a production data platform.

---

## Technology Stack

| Technology | Role in Pipeline |
|------------|------------------|
| **Azure Data Lake Storage Gen2** | Raw log storage (raw-logs/), Bronze/Silver/Gold Delta tables |
| **Databricks** | ETL execution engine, DLT pipelines, Unity Catalog, Workflows orchestration |
| **Delta Live Tables (DLT)** | Bronze and Silver pipeline orchestration with Auto Loader |
| **Unity Catalog** | Governance layer (w3c_catalog, bronze/silver/gold schemas) |
| **Azure SQL Database (Serverless)** | Analytics warehouse, dimensional model storage |
| **Apache Airflow** | Pipeline orchestration, DAG scheduling, Dataset triggers |
| **dbt** | Transformation layer, dimensional models, data quality tests |
| **MaxMind GeoLite2** | IP geolocation enrichment (country, region, city, lat, lon, postcode, ISP) |
| **MSSQL JDBC Driver** | Direct Silver → Azure SQL export from Databricks |
| **Power BI** | Analytics consumption layer (CSV-based semantic model) |
| **Grafana + Prometheus** | Monitoring and alerting |
| **Terraform** | Infrastructure as Code (Azure + Databricks resources) |
| **GitHub Actions** | CI/CD automation (split-tier: every push + nightly integration) |
| **Python 3.11** | Primary language for PySpark, UDFs, Airflow operators |
| **PySpark** | Data processing framework (Databricks Runtime 15.4.x) |

---

## Current State Audit (pre-plan beginning)

### What Exists (Carried Forward from v1.8)

**Infrastructure:**

- Terraform Part A/Part B split structure
- Azure resource group and networking templates
- Databricks workspace configuration
- ADLS Gen2 container definitions
- Azure SQL serverless configuration

**Code Assets:**

- `airflow/spark/jobs/utils/geoip.py` — 7 MaxMind UDFs (reference implementation)
- `airflow/spark/jobs/utils/ua_parser.py` — 5 UA UDFs (NOT materialized in Silver DDL)
- `airflow/spark/jobs/utils/transformations.py` — 5 computed UDFs
- `airflow/spark/jobs/utils/w3c_parser.py` — authoritative W3C line parser
- `airflow/spark/databricks/01_bronze_ingestion.py` — reference for parse_log_line + detect_format
- `airflow/spark/databricks/03_export_warehouse.py` — reference for JDBC export pattern
- `airflow/dbt/w3c/` — 16 dbt models, ~62 tests (PostgreSQL dialect)
- Existing test suite: 6 test files, 136+ tests
- `.github/workflows/ci.yml` — base CI structure

**Monitoring:**

- Grafana + Prometheus stack configured
- Airflow StatsD exporter

**Dependencies:**

- `pyodbc>=5.1,<6.0`, `sqlalchemy>=2.0,<3.0`, `user-agents>=2.2,<3.0` in requirements
- ODBC Driver 18 for SQL Server in Dockerfile

### What Is Missing (New for v2.0)

**Infrastructure:**

- Terraform remote state backend configuration (Azure Blob Storage)
- Databricks secret scope creation via CLI v1+
- Unity Catalog schema definitions (bronze, silver, gold)
- Databricks Workflow job definitions (Bronze, Silver, JDBC export, GE, dbt)

**DLT Pipelines:**

- `dlt_bronze.py` — Bronze DLT pipeline with Auto Loader and W3C parser
- `dlt_silver.py` — Silver DLT pipeline with GeoIP and computed fields
- MaxMind GeoLite2 database files uploaded to DBFS

**Azure Integration:**

- `jdbc_export_azure.py` — Silver → Azure SQL JDBC export with tracking table
- `export_dimensions_azure` — Airflow operator for dim table building
- Azure SQL DDL for dbo.raw_enriched and dbo.raw_enriched_loaded
- T-SQL compatibility macros for dbt

**dbt Azure Migration:**

- `macros/t_sql_compat.sql` — T-SQL macro file
- Inline T-SQL conditionals in all 16 dbt models
- `dbt docs generate` integration
- `dbt source freshness` checks

**CI/CD:**

- Tier 1 CI (every push, no Azure creds)
- Tier 2 CI (nightly integration, protected by GitHub Environment)
- Service principal credential setup for Terraform backend + Tier 2 CI

**Documentation:**

- Architecture diagram updated for single pipeline
- Deployment documentation for Azure resources
- Cost management and teardown procedures

---

## Constraints and Decisions

### Locked Architectural Decisions

1. **dbt runs as a Databricks job** (not in the Airflow Docker container). The `dbt_marts_azure.py` DAG uses `DatabricksSubmitRunOperator` to submit a Databricks Python task that installs `dbt-core` + `dbt-sqlserver` on the cluster, runs `dbt run --profile w3c_azure`, then `dbt docs generate`. This means `dbt-core` and `dbt-sqlserver` are cluster-level PyPI libraries on the Databricks job cluster — NOT in the Airflow Docker requirements. The Terraform Part B job definition must include these as library installs. The Azure SQL JDBC connection from within Databricks uses the same secret scope credentials (`w3c-etl-pipeline`) as the JDBC export task.

2. **Terraform remote state: Azure Blob Storage backend.** Bootstrap is a manual pre-Phase-1 step, documented in Phase 0. Required to prevent unrecoverable resource leaks if local `.tfstate` is lost while Azure credits are active.

3. **Databricks CLI: new v1+ CLI** (`brew install databricks` / `winget install Databricks.DatabricksCLI`). All CLI commands in the plan must use v1+ syntax:
   - Auth: `databricks auth login --host <workspace-url>` (not `databricks configure --token`)
   - Workspace import: `databricks workspace import <local-path> <remote-path>` (not `--file/--format` flags)
   - Secrets scope: `databricks secrets create-scope <name>` (not `put-scope` or `create-scope` with AKV flags)
   - Secrets put: `databricks secrets put --scope <scope> --key <key>` (same in v1+)
   - DBFS: `databricks fs cp <src> <dst>` (same in v1+)
   - Do NOT mix legacy and v1+ syntax anywhere in the plan.

### Infrastructure Constraints

- **Terraform Part A/Part B split:** Part A (core infra) must complete before Part B (DLT pipeline + Workflows) because Part B resources depend on DLT source code existing first.
- **Provider versions pinned:** `azurerm ~> 4.75.0`, `databricks ~> 1.115`, Terraform `>= 1.10.5, < 2.0`
- **ADLS Gen2 containers:** `raw-logs`, `bronze`, `silver`, `gold` (fixed naming)
- **Azure SQL Serverless:** `GP_S_Gen5`, 1 vCore, auto-pause 60 min (cost optimization)
- **Databricks Premium tier:** Required for Unity Catalog
- **Unity Catalog:** `w3c_catalog` catalog, `bronze` / `silver` / `gold` schemas (fixed naming)
- **Terraform directory structure:** `terraform/part_a/` and `terraform/part_b/` with `environments/dev/` and `modules/` subdirectories
- **Budget alerts:** $50 alert, $100 hard cap (Azure Cost Management)
- **Terraform lock file:** `terraform providers lock` and committed `terraform.lock.hcl`
- **Serverless DLT:** No `cluster {}` block needed in pipeline config; `lifecycle { ignore_changes = [] }` (serverless pipelines have no cluster to ignore changes on)
- **ADLS Gen2 RBAC:** `Storage Blob Data Contributor` assigned to Databricks workspace managed identity
- **Databricks secret scope:** `w3c-etl-pipeline` for credentials (storage access key, Azure SQL creds)
- **VNet configuration:** Databricks-delegated subnet + Azure SQL subnet; private endpoints disabled by default (`var.enable_private_endpoints = false`)
- **Azure SQL collation:** `SQL_Latin1_General_CP1_CI_AS`

### DLT Bronze Constraints

- **Serverless DLT enabled** (`serverless: true`) — eliminates VM provisioning in capacity-constrained westus3 region
- **No cluster configuration required** — serverless DLT uses Databricks-managed compute, no `node_type_id`, `autoscale`, or `spark_version` needed
- **Auto-scales to zero** when idle — cost-effective for low-volume pipelines
- **Auto Loader:** `binaryFile` format with `cloudFiles.includeExistingFiles = true`, `maxFilesPerTrigger = 10`, `maxFileSize = 209715200`
- **Auto Loader:** `binaryFile` format with `cloudFiles.includeExistingFiles = true`, `maxFilesPerTrigger = 10`, `maxFileSize = 209715200`
- **W3C parser:** Uses `rsplit()` field-counting to handle unquoted user-agent strings (matching authoritative `w3c_parser.py`)
- **UDF pattern:** UDF+explode pattern (NOT foreachBatch — foreachBatch returns StreamingQuery, not DataFrame)
- **Format detection:** 14-field vs 18-field IIS format detection
- **Deduplication:** ROW_NUMBER dedup CTE for full_refresh idempotency (option b — preferred)
- **Self-contained parser:** `parse_log_line` function self-contained in DLT script (ported from `01_bronze_ingestion.py` reference)
- **Sample log files (REPLACED):** 93 real W3C IIS log files in `airflow/data/LogFiles/` (old `data/samples/` deleted)
- **Bronze table:** `w3c_catalog.bronze.bronze_raw_logs`, partitioned by `log_date`
- **Delta properties:** `delta.enableChangeDataFeed = true`, `delta.autoOptimize.optimizeWrite = true`
- **Pipeline creation:** Via Terraform Part B (NOT UI) to avoid resource conflict; pipelines may be created via CLI temporarily for testing before Terraform Part B is deployed

### DLT Silver Constraints

- **GeoIP: MaxMind GeoLite2 ONLY** (not ip-api.com). 7 UDFs: country, region, city, latitude, longitude, postcode, isp
- **GeoLite2 databases:** GeoLite2-City.mmdb and GeoLite2-ASN.mmdb uploaded to Unity Catalog volume at `/dbfs/Volumes/w3c_etl_databricks/bronze/w3c_data/` for Silver pipeline
- **Lazy reader factory pattern:** `_make_geo_reader()` and `_make_asn_reader()` — `spark.conf.get()` called at driver level, NOT inside UDF body (UDFs run on workers where spark context is unavailable)
- **Computed UDFs:** 5 computed UDFs: `page_category`, `referrer_domain`, `traffic_type`, `is_crawler`, `size_band`
- **Plain Python function:** `_extract_domain()` is a plain Python function (NOT a UDF) used inside `traffic_type` UDF — calling a UDF inside another UDF body causes a runtime error
- **UA columns excluded:** UA columns (agent_type, browser_name, browser_version, operating_system, device_type) are NOT written to Silver DDL
- **Geo columns preserved:** 6 geo columns MUST stay in Silver: country, region, city, latitude, longitude, isp — `export_dimensions_azure` reads them to build `dim_geolocation`
- **Postcode handling:** `postcode` is a computed field, stays in Silver core columns
- **PyPI library:** `geoip2==5.0.1` installed via pipeline `libraries { pypi { package = "geoip2==5.0.1" } }` block (serverless DLT supports PyPI libraries in pipeline config)
- **Silver table:** `w3c_catalog.silver.silver_enriched_logs`

### JDBC Export Constraints (pymssql on serverless)

- **Source:** Reads from Silver directly via `spark.table("w3c_etl_databricks.silver.silver_enriched_logs")` (no Gold table — Gold table was eliminated as adding no value)
- **Serverless limitation:** Databricks serverless (Spark Connect) only supports `sqlserver` data source for **reads**, not writes — forces `collect()` + pymssql path
- **Idempotency:** Tracking table pattern: `dbo.raw_enriched_loaded` with `IF OBJECT_ID(...) IS NULL` guard in DDL (no error 208 traversal needed)
- **DDL execution:** `pymssql cursor.execute()` with `IF OBJECT_ID(...) IS NULL` guards — NOT py4j (no py4j gateway on serverless)
- **Batch INSERT:** `cursor.executemany()` with `BATCH_SIZE=5000` — rows collected as Spark `Row` objects, serialized via `tuple(row)` (no `asDict()` overhead)
- **Performance — Spark-side filter:** Already-loaded files filtered via `~col("source_file").isin(loaded_files)` **before** `collect()` — only new rows reach driver. Critical for incremental run memory.
- **Retry logic:** 4 attempts, `15 * (2 ** attempt)` backoff (15s/30s/60s), covers Azure SQL serverless cold-start (longer waits than originally planned)
- **Library:** `pymssql>=2.2.11` as job environment dependency (pure-Python, no JVM library needed)
- **Connection:** `pymssql.connect(server, database, user, password, port=1433, login_timeout=30)` with retry for DB auto-resume

### export_dimensions_azure Constraints

- **Source:** Reads from `dbo.raw_enriched` via Azure SQL (NOT local Delta path)
- **Column rename:** `_build_dim_geolocation()`: renames `client_ip` → `ip` (critical — PK column is `ip` not `client_ip`)
- **UA parsing:** `_build_dim_useragent()`: uses `user_agents` library for UA parsing
- **Upsert pattern:** `_write_dim_to_azure()`: MERGE upsert on natural key (`ip` / `user_agent`)
- **Unknown rows:** Preserves `geolocation_sk = -1` and `user_agent_sk = -1` unknown rows (FK integrity)
- **DDL pattern:** `IF NOT EXISTS` DDL pattern for Azure SQL dim table creation
- **Identity insert:** `SET IDENTITY_INSERT ... ON/OFF` for seeding the -1 unknown rows
- **Dataset trigger:** Fires Airflow Dataset outlet to trigger downstream dbt DAG

### Airflow DAG Constraints

- **DAG 1:** `spark_ingestion_azure.py` (Daily 2 AM UTC): orchestrates entire ETL with 2 sequential tasks:
  - Task 1: `bronze_silver_jdbc_pipeline` — `DatabricksRunNowOperator` triggers Databricks Workflow (job ID from `DATABRICKS_JOB_ID` env var)
  - Task 2: `export_dimensions` — `PythonOperator` with inline `_export_dimensions` callable; builds `dim_geolocation` + `dim_useragent` from Azure SQL via pyodbc MERGE upsert; fires `Dataset("mssql://azure-sql/dbo/raw_enriched_loaded")` outlet for downstream dbt DAG
- **DAG 2:** `dbt_marts_azure.py`: Dataset-triggered; runs dbt against Azure SQL target (not yet implemented)
- **No standalone operator file:** Dimension export logic is inlined in `spark_ingestion_azure.py` as `_export_dimensions()` — no separate `airflow/dags/operators/` directory needed. The existing `airflow/dags/w3c/` directory hosts all DAG files.
- **Provider version:** `apache-airflow-providers-databricks==4.6.0` installed in Dockerfile (NOT in requirements.txt — avoid pip conflict)
- **Databricks connection:** `databricks_default` in Airflow with workspace URL + PAT token
- **Environment variables:** `DATABRICKS_JOB_ID` (default: `847995192336508`), `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USER`, `AZURE_SQL_PASS`

### dbt T-SQL Migration Constraints

- **Inline conditionals:** `{% if target.type == 'sqlserver' %}...{% else %}...{% endif %}` (do NOT create `_azure.sql` duplicates — dbt would parse both as separate models)
- **Macro file:** Global macro file: `macros/t_sql_compat.sql`
- **Required macros:** `tsql_cast`, `tsql_datepart`, `tsql_month_name`, `tsql_day_name`, `tsql_dow`, `tsql_format_date`
- **Coverage:** All 80+ PostgreSQL-specific expressions must be covered: `::` casts → `CAST()`, `~*` → `LIKE+COLLATE`, `EXTRACT` → `DATEPART`, `TO_CHAR` → `FORMAT/DATENAME`, `SPLIT_PART` → `CHARINDEX/SUBSTRING`, `generate_series` → `GENERATE_SERIES` (Azure SQL compat level 160), `REGEXP_REPLACE` → manual string ops, `CREATE INDEX IF NOT EXISTS` → `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE ...) EXEC(...)`, `PERCENTILE_CONT ... WITHIN GROUP ... OVER ()` (SQL Server requires explicit `OVER ()`)
- **PostgreSQL path:** PostgreSQL path (`--profile w3c`) must remain fully intact in the `{% else %}` branch
- **CI compatibility:** `dbt compile --profile w3c` must pass in CI without Azure credentials

### CI/CD Constraints

- **Tier 1 (every push):** Ruff, mypy, pytest (unit + DAG integrity only — NOT integration), `dbt compile --profile w3c` (PostgreSQL dialect, no cloud needed), `terraform validate`, `terraform fmt --check`
- **Tier 2 (nightly or manual):** Full integration test — upload sample logs to ADLS Gen2, trigger Databricks Workflow via REST API, poll job status, query Azure SQL for expected row counts, assert 18 CSV exports produced. Also triggers `dbt docs generate` on Databricks and validates `catalog.json` artifact is written.
- **Tier 2 protection:** Tier 2 GitHub Actions job uses `azure/login` with service principal credentials. Required secrets: `ARM_CLIENT_ID`, `ARM_CLIENT_SECRET`, `ARM_SUBSCRIPTION_ID`, `ARM_TENANT_ID`, `DATABRICKS_TOKEN`, `AZURE_SQL_SERVER`, `AZURE_SQL_DB`, `AZURE_SQL_USER`, `AZURE_SQL_PASSWORD`
- **GitHub Environment:** Tier 2 is protected by a GitHub Environment (`azure-integration`) with manual approval gate to prevent accidental credit spend
- **Shared credentials:** Terraform remote state backend credentials (`ARM_*`) are the same service principal used by Tier 2 CI — document this shared credential reuse in Phase 0

### Docker Role Constraints

- Docker is NOT a production data platform
- Docker is used for: Airflow orchestration, PostgreSQL metastore, local PySpark unit tests, Grafana/Prometheus monitoring, CI substrate
- Docker Spark scripts (`bronze_ingestion.py`, `silver_enrichment.py`) are local dev/test tools only, not pipeline stages

---

## Power BI Semantic Contract

### Exactly 18 CSV Exports

The pipeline must produce exactly 18 CSV files in `airflow/data/Star-Schema/`. The v1.8 bug of 17 exports is fixed — `mart_country_browser_share` is included.

**Export Lists:**

```python
STAGING_TABLES = [
    "dbt_staging.fact_webrequest", "dbt_staging.dim_date", "dbt_staging.dim_time",
    "dbt_staging.dim_page", "dbt_staging.dim_status", "dbt_staging.dim_referrer",
    "dbt_staging.dim_method", "dbt_staging.dim_visitortype", "dbt_staging.dim_visit_buckets",
    "dbt_staging.crawler_ips",
]

MART_TABLES = [
    "dbt_marts.mart_page_performance", "dbt_marts.mart_daily_aggregates",
    "dbt_marts.mart_crawler_analysis", "dbt_marts.mart_browser_analysis",
    "dbt_marts.mart_timeofday_analysis", "dbt_marts.mart_country_browser_share",
]

PUBLIC_TABLES = [
    "dbo.dim_geolocation",
    "dbo.dim_useragent",
]
```

### Header Validation

Compare Azure SQL export headers to Docker baseline before declaring Power BI compatibility. All 18 CSV files must have identical column names and ordering to the PostgreSQL-based exports.

### DAX Measure Field Dependencies

The following fields must be present and correctly typed for Power BI DAX measures:

- `is_404` (BIT/boolean)
- `bytes_sent` (BIGINT)
- `bytes_received` (BIGINT)
- `response_time_ms` (BIGINT)
- `is_crawler` (BIT/boolean)
- `is_direct_traffic` (BIT/boolean)
- `active_countries` (INT)
- `pct_crawler` (FLOAT/decimal)
- `top_browser_share` (FLOAT/decimal)
- `peak_hour_requests` (BIGINT)
- `peak_traffic_hour` (INT/VARCHAR)
- `device_type` (VARCHAR)
- `day_name` (VARCHAR)
- `is_weekend` (BIT/boolean)
- `holiday_flag` (BIT/boolean)

### dbo.raw_enriched Schema Contract

This is the bridge table between Silver and dbt. The exact column list and DDL must be used in Phase 5 JDBC export scaffold — any deviation breaks dbt source references and Power BI.

**EXPORT_COLUMNS (31 columns — 25 warehouse core + 6 GeoIP columns for dim_geolocation build):**

```python
EXPORT_COLUMNS = [
    "log_date", "log_time", "server_ip", "method", "uri_stem",
    "uri_query", "client_ip", "user_agent", "cookie", "referrer",
    "status", "sub_status", "win32_status", "bytes_sent", "bytes_recv",
    "server_port", "username", "time_taken", "source_file",
    "postcode", "page_category", "referrer_domain", "traffic_type",
    "is_crawler", "size_band",
    "country", "region", "city", "latitude", "longitude", "isp",
]
```

**RAW_ENRICHED_DDL (exact T-SQL types — do not change column names, types, or nullability):**

```sql
CREATE TABLE dbo.raw_enriched (
    log_date         DATE,
    log_time         VARCHAR(20),
    server_ip        VARCHAR(45),
    method           VARCHAR(10),
    uri_stem         NVARCHAR(MAX),
    uri_query        NVARCHAR(MAX),
    client_ip        VARCHAR(45),
    user_agent       NVARCHAR(MAX),
    cookie           NVARCHAR(MAX),
    referrer         NVARCHAR(MAX),
    status           INT,
    sub_status       INT,
    win32_status     INT,
    bytes_sent       BIGINT,
    bytes_recv       BIGINT,
    server_port      INT,
    username         VARCHAR(100),
    time_taken       BIGINT,
    source_file      VARCHAR(255),
    postcode         VARCHAR(20),
    page_category    VARCHAR(50),
    referrer_domain  VARCHAR(255),
    traffic_type     VARCHAR(50),
    is_crawler       BIT,
    size_band        VARCHAR(20),
    country          VARCHAR(100),
    region           VARCHAR(100),
    city             VARCHAR(100),
    latitude         FLOAT,
    longitude        FLOAT,
    isp              VARCHAR(200)
);
```

**TRACKING_DDL:**

```sql
CREATE TABLE dbo.raw_enriched_loaded (
    source_file VARCHAR(255) PRIMARY KEY
);
```

**Important Notes:**

- `is_crawler` is `BIT` in Azure SQL — must be cast from Silver's string `"true"/"false"` via `CASE WHEN TRIM(LOWER(is_crawler)) = 'true' THEN 1 ELSE 0 END` before the JDBC write. This is the same cast applied in `export_warehouse.py`.
- `dbo.raw_enriched` is intentionally wider than the PostgreSQL `public.raw_enriched` (which has 25 columns). The 6 extra GeoIP columns (country, region, city, latitude, longitude, isp) exist so `export_dimensions_azure` can build `dbo.dim_geolocation` by reading from Azure SQL alone, without needing to touch Unity Catalog Silver directly.

---

## Phased Implementation Plan

### Phase 0 — Prerequisites (✅ Done)

**Phase Goal:** Set up all required accounts, CLI tools, credentials, and bootstrap infrastructure before beginning Terraform deployment.

**Summary:** All prerequisite setup completed. Azure service principal (`w3c-etl-pipeline-sp`) created with Contributor role. Terraform remote state backend bootstrapped (`rg-tfstate` resource group, storage account with `tfstate` container). MaxMind GeoLite2 databases downloaded to `data/geoip/`. Backend configuration files created for both Part A and Part B. Shared credential documentation created in `docs/credentials.md`. Sample log files (14-field and 18-field variants) later superseded by 93 real log files from `airflow/data/LogFiles/` in Phase 3.

**Verified State:** Azure CLI authenticated, Terraform >= 1.10.5 installed, Databricks CLI v1+ installed, GeoLite2 databases present, remote state backend created, `.env.azure` with ARM_* credentials, `.gitignore` updated with env/tfstate/geoip patterns.

---

### Phase 1 — Terraform Part A (Core Infrastructure) (✅ Complete)

**Phase Goal:** Deploy core Azure infrastructure (resource groups, networking, storage, Databricks workspace, Azure SQL) using Terraform Part A.

**Summary:** All core Azure infrastructure deployed in `westus3`. Resources include: resource group `rg-w3c-etl`, VNet with 2 subnets (Databricks-delegated `snet-databricks` 10.0.1.0/24 + SQL `snet-sql` 10.0.2.0/24), ADLS Gen2 storage account `stw3cetlwestus3` with 4 containers (`raw-logs`, `bronze`, `silver`, `gold`), Databricks Premium workspace `w3c-etl-databricks`, and Azure SQL serverless database `w3c-etl-db` (GP_S_Gen5_1, auto-pause 60 min). NSG-based subnet isolation implemented. Network rules on storage account with `Deny` default action.

**Deployed Outputs (key for downstream phases):**
```bash
storage_account_name      = "stw3cetlwestus3"
databricks_workspace_url  = "https://adb-7405616994554630.10.azuredatabricks.net/"
server_fqdn               = "sql-w3c-etl.database.windows.net"
database_name             = "w3c-etl-db"
location                  = "westus3"
resource_group_name       = "rg-w3c-etl"
managed_identity_id       = "0c3a72bc-5782-4879-8f21-b27dedde6906"
databricks_subnet_id      = "/subscriptions/2cfbc457-25bd-4007-8585-6bfa6765ec30/resourceGroups/rg-w3c-etl/providers/Microsoft.Network/virtualNetworks/vnet-w3c-etl/subnets/snet-databricks"
sql_subnet_id             = "/subscriptions/2cfbc457-25bd-4007-8585-6bfa6765ec30/resourceGroups/rg-w3c-etl/providers/Microsoft.Network/virtualNetworks/vnet-w3c-etl/subnets/snet-sql"
vnet_id                   = "/subscriptions/2cfbc457-25bd-4007-8585-6bfa6765ec30/resourceGroups/rg-w3c-etl/providers/Microsoft.Network/virtualNetworks/vnet-w3c-etl"
```

**Verified State:** Terraform init/validate/plan/apply all passed. All 8+ resources confirmed in Azure portal. `.terraform.lock.hcl` committed.

---

### Phase 2 — Deploy and Verify Azure Infrastructure (✅ Complete)

**Phase Goal:** Verify all Azure resources from Phase 1 are operational, configure budget alerts, and set up local environment variables.

**Summary:** All Azure infrastructure verified and operational. Databricks workspace authenticated with PAT token. Secret scope `w3c-etl-pipeline` created with 5 secrets: `storage-access-key`, `azure.sql.server`, `azure.sql.database`, `azure.sql.username`, `azure.sql.password`. Unity Catalog `w3c_etl_databricks` (existing managed catalog) with schemas `bronze`, `silver`, `gold` created. ADLS Gen2 containers accessible. Azure SQL connectivity verified via pyodbc. Budget alerts configured via Azure Portal ($50 warning, $100 hard cap).

---

### Phase 3 — DLT Bronze Pipeline (✅ Complete — Serverless DLT)

**Phase Goal:** Create and deploy the DLT Bronze pipeline with Auto Loader, W3C parser UDF, and quality expectations.

**Summary:** Bronze pipeline `a6ea62d3-5f3a-4f53-ae8b-4bfb156703ad` operational on **Serverless DLT**. Ingests W3C IIS log files from ADLS Gen2 via per-file UDF that detects the `#Fields:` header, applies 7 quality expectations, and writes to `w3c_etl_databricks.bronze.bronze_raw_logs` (Materialized View). **153,380 rows** from **93 real IIS log files** (sourced from `airflow/data/LogFiles/`). All 7 expectations pass; **0 rows dropped**. Old synthetic `data/samples/` directory deleted — all data now uses real logs with public IPs enabling full GeoIP enrichment.

**Key Implementation Details:**
- **Serverless DLT**: `serverless: true`, no `cluster {}` block — bypasses westus3 VM capacity shortage.
- **`@dlt.table` not `@dlt.streaming_table`**: Serverless DLT does NOT support `streaming_table` decorator.
- **Schema evolution mode**: `"none"` (not `"addNewColumns"`) — `addNewColumns` incompatible with `binaryFile` format in DLT.
- **Storage auth**: ADLS storage account key stored in pipeline `configuration` as `fs.azure.account.key.stw3cetlwestus3.dfs.core.windows.net`.
- **ROW_NUMBER dedup REMOVED**: Not supported on streaming DataFrames in DLT. Dedup handled upstream (files processed once) and in Silver (`left_anti` join).
- **Bronze table type**: Materialized View (expected for serverless DLT).
- **MaxFilesPerTrigger**: `"10"`. **Pipeline ID**: `a6ea62d3-5f3a-4f53-ae8b-4bfb156703ad` — IDLE state.

**⚠️ Differences from Plan Scaffold:**

| Plan Scaffold | Actual Implementation | Reason |
|---------------|----------------------|--------|
| Per-line UDF with hardcoded `detect_file_format()` returning 18 | Per-file `_parse_file_content()` UDF reads `#Fields:` header per file | Fixes CRIT-01 (hardcoded format) and CRIT-02 (UDF closure over stale variable) |
| `maxFilesPerTrigger: "1000"` | `maxFilesPerTrigger: "10"` | Matches constraint in plan (DLT Bronze Constraints) |
| `schemaLocation: "/dbfs/mnt/w3c-data/_schemas/bronze"` | `schemaLocation: "dbfs:/Volumes/w3c_etl_databricks/bronze/w3c_data/_schemas/bronze"` | Uses Unity Catalog volume path instead of DBFS mount |
| `explode(split(decode(col("content"), "utf-8"), "\n"))` per-line pattern | Per-file UDF returns `ArrayType(Struct)` then `explode()` | Detects format once per file, not per line |
| No storage.account_name validation | Validates `spark.conf.get("storage.account_name")` at runtime | Fail-fast if config missing |
| `@dlt.streaming_table` used | `@dlt.table` used | Serverless DLT does not support `streaming_table` |
| `schemaEvolutionMode: "addNewColumns"` | `schemaEvolutionMode: "none"` | `addNewColumns` incompatible with `binaryFile` format in DLT |
| `ROW_NUMBER() OVER (PARTITION BY ...)` dedup | Dedup removed from Bronze (left_anti for Silver only) | ROW_NUMBER not supported on streaming DataFrames in DLT |

**Verified State:**
- Pipeline: `a6ea62d3-5f3a-4f53-ae8b-4bfb156703ad` — serverless, IDLE
- Table: **153,380 rows**, 7 expectations pass, **0 rows dropped**
- Schema: 18 data columns + `source_file` + `_rescued_data` + partition cols
- SQL Warehouse: `e150f7269187352b` (Serverless Starter Warehouse) for verification
- Source: 93 real IIS log files in `raw-logs@stw3cetlwestus3.dfs.core.windows.net/`

**Phase 3 → Phase 4 Handoff:** ✅ Ready for Silver layer. Bronze pipeline operational with 153,380 rows, GeoIP databases in UC volume, public IPs available in real log files.

---

### Phase 4 — DLT Silver Pipeline (✅ Complete — GeoIP Enrichment Working)

**Phase Goal:** Create and deploy the DLT Silver pipeline with MaxMind GeoIP enrichment and computed fields.

**Summary:** Silver pipeline `98c7675f-5425-4a14-95b6-247af6da9626` operational on **Serverless DLT**, completes successfully with full GeoIP enrichment. Reads from Bronze via `spark.table("w3c_etl_databricks.bronze.bronze_raw_logs")`, applies GeoIP enrichment via `maxminddb==2.8.*` (pure Python), computes 5 derived fields, filters via `valid_country` expectation, writes to `w3c_etl_databricks.silver.silver_enriched_logs`. **153,377 rows** with full geographic coverage across **30+ countries**. All quality expectations pass; only **3 rows** dropped. The `valid_country` expect_or_drop kept as **original** (not downgraded to warning) — GeoIP enrichment works correctly with real public IP data.

**Key Implementation Details:**
- **GeoIP Library**: `maxminddb==2.8.*` (pure Python, no compiled deps). `geoip2` rejected — requires `libmaxminddb` C library unavailable on serverless DLT.
- **Environment Dependencies**: `environment.dependencies = ["maxminddb==2.8.*"]` in pipeline spec.
- **Lazy Singleton Pattern**: `_ensure_geo_reader()` and `_ensure_asn_reader()` initialize singleton readers on first UDF invocation per executor — avoids PicklingError from serializing non-serializable `maxminddb.Reader` instances.
- **Module-level flag**: `_HAS_MAXMINDDB = True/False` — gracefully degrades to NULLs if import fails.
- **Cross-catalog read**: Uses `spark.table(...)` instead of `dlt.read()` — required when Bronze and Silver are in separate pipelines.
- **Notebook import**: Use `--format SOURCE --language PYTHON --file <local_path> <workspace_path>` to preserve NOTEBOOK type. Pipe/redirect truncates content.
- **GeoIP DB path**: Serverless DLT uses `/Volumes/...` (NOT `/dbfs/Volumes/...`) — FUSE mount not accessible on serverless executors.
- **Dedup**: `left_anti` join on `source_file`, wrapped in try/except for first run.
- **Pipeline completes in ~3 minutes** with no errors.

**⚠️ Differences from Plan Scaffold:**

| Plan Scaffold | Actual Implementation | Reason |
|---------------|----------------------|--------|
| 7 separate scalar GeoIP UDFs (`get_country`, `get_region`, ...) | Single `get_geo_fields` struct UDF (6 fields from 1 City DB call) + `get_isp` scalar UDF (1 field from ASN DB) | 3.5x performance gain — 7x redundant MaxMind lookups → 2 total |
| Module-level reader init at import time | Lazy singleton on first UDF invocation per executor | Avoids PicklingError from non-serializable MaxMind reader |
| No Silver deduplication logic | `left_anti` join on `source_file` in try/except | Ensures idempotent re-runs |
| `geoip2==5.0.1` as cluster PyPI library | `maxminddb==2.8.*` as pipeline environment dependency | `geoip2` has compiled C deps, can't install on serverless DLT |
| Silver reads with `dlt.read("bronze_raw_logs")` | `spark.table("w3c_etl_databricks.bronze.bronze_raw_logs")` | Cross-pipeline reads require direct table path |
| GeoIP paths via DBFS mount (`/dbfs/Volumes/...`) | GeoIP paths via `/Volumes/...` | FUSE mount not accessible on serverless executors for Python file I/O |
| Serverless DLT supports `geoip2` as system library | `maxminddb` not pre-installed — added via env deps | Discovered through testing |
| 8 GeoIP UDF columns (incl. asn_number, as_organization) | 7 GeoIP columns: 6 from struct UDF + scalar `isp` | Single ASN org field sufficient for analytics |

**Verified State:**
- Pipeline: `98c7675f-5425-4a14-95b6-247af6da9626` — serverless, IDLE
- Table: **153,377 rows** (31 columns: 25 core + 6 geo)
- GeoIP coverage: US (56,548), UK (31,818), Russia (11,387), China (6,737), Argentina (6,631), Canada (5,063), Germany (4,232), Brazil (2,948), France (2,756), India (2,507), Australia (2,057), Italy (1,846), Netherlands (1,600), Japan (1,342), Mexico (1,243), Poland (1,044), Spain (949), Sweden (810), Ukraine (784), Czech Republic (667), Switzerland (616), others (12,778)
- Quality: `valid_country` dropped only 3 rows — kept as `expect_or_drop` (original, not downgraded)

**Critical Lessons Learned:**
1. **Notebook import MUST use `--format SOURCE --language PYTHON --file <path>`**. Pipe/redirect truncates content. `--format AUTO` produces `FILE` type instead of `NOTEBOOK`, causing UNSUPPORTED_LANGUAGE in DLT. Verified via `databricks workspace get-status` confirms `object_type: NOTEBOOK, language: PYTHON`.
2. **`--format AUTO` for `.py` files without `# Databricks notebook source` header** → creates `FILE` type (not NOTEBOOK).
3. **`--format SOURCE` with pipe/redirect** → reads only first line. Always use `--file` flag for local file imports.
4. **Sample data IPs must be public** for GeoIP enrichment to work. Private IPs (10.x.x.x, 192.168.x.x, 172.16.x.x) never match GeoLite2. Resolved — all data uses real W3C logs with public IPs.
5. **Serverless DLT does NOT support**: `@dlt.streaming_table`, `ROW_NUMBER()` on streaming DataFrames, `schemaEvolutionMode: addNewColumns` with `binaryFile`, `geoip2` (has compiled deps).
6. **`/Volumes/...` (not `/dbfs/Volumes/...`)** is the correct local file path for Unity Catalog volume access from serverless DLT Python processes. FUSE mount not available on serverless executors.
7. **`maxminddb` (pure Python) works as pipeline environment dependency** on serverless DLT. Do not attempt `geoip2` — requires `libmaxminddb` C shared library.

**Phase 4 → Phase 5 Handoff:** ✅ Ready for JDBC export. Silver pipeline operational with 153,377 rows, full GeoIP enrichment (30+ countries), 31-column schema matching EXPORT_COLUMNS spec, dedup via left_anti join confirmed.

---

### Phase 5 — JDBC Export from Silver to Azure SQL (✅ Complete — 45s optimized export verified on Databricks Serverless)

**Phase Goal:** Create and deploy the Azure SQL export task that reads from Silver and writes to Azure SQL with idempotency tracking.

**Summary:** Export implemented as Databricks `notebook_task` on serverless compute using pymssql (pure Python) — NOT `df.write.jdbc()` because serverless (Spark Connect) only supports JDBC reads, not writes. `airflow/spark/databricks/jdbc_export_azure.py` (296 lines) deployed to Repos. Uses tracking table pattern (`dbo.raw_enriched_loaded`), retry logic (4 attempts, 15×2^attempt backoff for Azure SQL cold-start), exact 31-column `RAW_ENRICHED_DDL` schema, and `is_crawler` BIT cast. Export tested with **153,377 rows** — job SUCCESS. Final optimized run: **45s** (run 658447448322322) — **8-9× faster** than initial 413s via: removed `.cache()` (unsupported on serverless), Spark-side filter before `collect()`, `tuple(row)` instead of `row.asDict()` (eliminates 4.7M dict allocations), removed redundant `export_df.count()`.

**Key Implementation Details:**
- **Source**: `spark.table("w3c_etl_databricks.silver.silver_enriched_logs")` — reads Silver via Unity Catalog.
- **Library**: `pymssql>=2.2.11` as job environment dependency (pure-Python, no JVM library needed).
- **No Maven libraries needed**: MSSQL JDBC driver not required.
- **DDL**: `pymssql cursor.execute()` with `IF OBJECT_ID(...) IS NULL` guards — no py4j gateway on serverless.
- **Batch INSERT**: `cursor.executemany()` with `BATCH_SIZE=5000` via `tuple(row)`.
- **Spark-side filter**: Already-loaded files filtered via `~col("source_file").isin(loaded_files)` **before** `collect()` — only new rows reach driver. Critical for incremental run memory.
- **Credentials**: `dbutils.secrets.get()` via `DBUtils(spark)` from `w3c-etl-pipeline` scope.
- **`.cache()` NOT supported on serverless**: Restructured to `collect()` first, `len()` from collected rows.

**Phase 5 Performance Optimizations (initial 413s → final 45s):**

| # | Issue | Before | After | Impact |
|---|-------|--------|-------|--------|
| 0 | `.cache()` unsupported on serverless | `new_data_df.cache()` failed with `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE` | `collect()` first, then `len(rows)` — removes both `.cache()` call AND redundant `.count()` scan | Fixed initial crash + saved one full scan |
| 1 | `collect()` before Spark-side filter | All 153K rows collected to driver, then filtered in Python | Filter via `~col("source_file").isin(loaded_files)` **before** `collect()` | On incremental: ~0 rows vs 153K. Prevents OOM. |
| 2 | Wasteful `export_df.count()` for logging | `total_rows = export_df.count()` scanned entire Silver table for a log message | Removed | ~10s saved per run |
| 3 | `asDict()` serialization overhead | `row.asDict()` — 31 keys × 153K rows = 4.7M dict allocations | `tuple(row)` — no dict overhead | ~50s saved on initial run |

**⚠️ Differences from Plan Scaffold:**

| Plan Scaffold | Actual Implementation | Reason | Phase 6+ Impact |
|---------------|----------------------|--------|-----------------|
| `df.write.jdbc()` for data export | `collect()` → pymssql `cursor.executemany()` batch INSERT | Serverless DLT only supports JDBC reads, not writes | Task type: `notebook_task` on serverless (not `spark_python_task`) |
| py4j DDL via `spark._jvm.java.sql.DriverManager` | pymssql `cursor.execute()` with `IF OBJECT_ID(...) IS NULL` guards | No py4j gateway on serverless compute | No Maven libraries needed |
| MSSQL JDBC Maven library (`com.microsoft.sqlserver:mssql-jdbc`) | pymssql as job environment dependency (`pymssql>=2.2.11`) | pymssql is pure-Python, no JVM dependency | Phase 6: declare in job `environment.dependencies[]`, NOT cluster `custom_library.maven[]` |
| Error code 208 traversal via `getErrorCode()` | `IF OBJECT_ID(...) IS NULL` guard in DDL | Simpler, no exception traversal | No impact |
| Retry backoff: `5 × 2^attempt` (5s/10s/20s) | Retry backoff: `15 × 2^attempt` (15s/30s/60s) | Azure SQL serverless cold-start needs longer waits | No impact |
| `spark_python_task` with `job_cluster` | `notebook_task` on serverless (no cluster) | See above — pymssql works on serverless | **Phase 6: remove `job_cluster` block, use serverless `notebook_task`** |

**Architectural constants preserved (no change needed downstream):**
- `EXPORT_COLUMNS` — exact 31-column list unchanged
- `RAW_ENRICHED_DDL` — exact T-SQL schema unchanged (consumed by dbt sources.yml)
- `TRACKING_DDL` — `dbo.raw_enriched_loaded` with `source_file VARCHAR(255) PRIMARY KEY`
- Credentials: `dbutils.secrets.get(scope='w3c-etl-pipeline', key='azure.sql.*')`
- Silver source: `spark.table("w3c_etl_databricks.silver.silver_enriched_logs")`

**Verified State:**
- `jdbc_export_azure.py` (296 lines) deployed to Databricks Repos
- Azure SQL tables `dbo.raw_enriched`, `dbo.raw_enriched_loaded` created
- **153,377 rows** exported to Azure SQL (initial run 413s)
- **Final optimized run: 45s** (run 658447448322322) — 8-9× faster
- Tracking table populated with source files; idempotency confirmed on re-run
- Deployed via `databricks workspace import --format SOURCE --language PYTHON --file`

---

### Phase 6 — Databricks Workflows + Terraform Part B + Airflow DAG (✅ Complete — Full Orchestration)

**Phase Goal:** Deploy Databricks Workflows orchestration with Bronze, Silver, and JDBC export tasks using Terraform Part B, and create the Airflow DAG that triggers the entire pipeline.

**Summary:** Phase 6 delivered three major components:

1. **Terraform Part B** (`terraform/part_b/` with main.tf, variables.tf, outputs.tf, dev tfvars): Manages 3 resources — existing Bronze pipeline `a6ea62d3`, existing Silver pipeline `98c7675f`, and new `w3c-etl-workflow` job (ID: `847995192336508`). All 3 tasks use **serverless compute** with daily schedule at 2 AM UTC. End-to-end test (Run `574928159107936`) confirmed all tasks complete: 93 files → GeoIP enrichment (30+ countries) → 153K+ rows to Azure SQL.

2. **Airflow DAG** (`airflow/dags/w3c/spark_ingestion_azure.py`, 263 lines): 2-task DAG — `bronze_silver_jdbc_pipeline` (DatabricksRunNowOperator → workflow) and `export_dimensions` (PythonOperator, inline `_export_dimensions()` callable, fires `Dataset("mssql://azure-sql/dbo/raw_enriched_loaded")` outlet).

3. **Terraform fixes**: Provider version `~> 1.70` → `~> 1.115`; `storage_access_key` validation block added (fail-fast on missing env var); unused `databricks_token` variable removed.

The storage account key remains in pipeline configurations (via `TF_VAR_storage_access_key` env var) because serverless DLT Auto Loader with ABFSS source cannot resolve flows without it — managed identity RBAC alone is insufficient. Dev artifact `w3c-jdbc-export-test` (job 514306075636810) deleted. `lifecycle { ignore_changes = [task] }` suppresses noisy plan diffs from provider task-set reordering. A 39-test suite (`tests/test_terraform_part_b.py`, `@pytest.mark.terraform`) validates configuration. **76 unit tests pass, 0 failures.**

**⚠️ Differences from Plan Scaffold:**

| Plan Scaffold | Actual Implementation | Reason |
|---------------|----------------------|--------|
| `edition = "core"` on pipeline resources | `edition` field omitted entirely | Serverless DLT requires Advanced; provider default (`ADVANCED`) is correct |
| `spark_python_task` + `job_cluster` for JDBC export | `notebook_task` on serverless (no cluster block) | Phase 5 discovered pymssql works on serverless |
| `workflow_tasks { ... }` block syntax | `task { ... }` block syntax (singular) | Terraform provider v1.70 uses `task {}` |
| `job_cluster { ... }` with `custom_library.maven.mssql-jdbc` | No `job_cluster` block at all | All 3 tasks use serverless; pymssql as job env dependency |
| `storage_access_key` stored in tfvars | `storage_access_key` variable with `default = ""` — key via `TF_VAR_` env var | Avoids committing storage key to version control |
| Pipelines created from scratch in Terraform | Existing pipelines imported via `terraform import` | Preserves IDs, avoids recreation |
| `development = true` in pipeline config | `development` not set in Terraform (removed during apply) | DLT UI-managed setting; removing prevents unnecessary diffs |
| Storage key removed from pipeline config (managed identity only) | Storage key restored conditionally via `storage_access_key` ternary | Serverless DLT Auto Loader with ABFSS source fails without explicit storage key |
| No `lifecycle` block on job resource | `lifecycle { ignore_changes = [task] }` added | Provider stores tasks as unordered TypeSet — every plan shuffles task blocks without this |
| No Terraform validation tests planned | `tests/test_terraform_part_b.py` created (39 tests, `@pytest.mark.terraform`) | Catches config drift early in CI |
| Dev/test job `w3c-jdbc-export-test` preserved | Deleted via API (job 514306075636810) | Redundant — workflow Task 3 runs the same notebook |

**Post-Completion Fixes Applied — `spark_ingestion_azure.py` Dimension Export:**

| # | Issue | Fix |
|---|-------|-----|
| 1 | SQL Server `CONVERT` used as standalone expression in SELECT | Wrapped in `SELECT CONVERT(...)` for valid T-SQL |
| 2 | PostgreSQL-style param markers (`%s`) in pyodbc | Replaced with SQL Server style (`?`) |
| 3 | Hash computed via Azure SQL `HASHBYTES` for UA | Geo: SQL `HASHBYTES` (batch in MERGE subquery); UA: Python `hashlib.sha256()` (alongside local parsing) |
| 4 | User-agent strings unparsed — dims built from existing columns only | Full UA parsing via `user-agents` library → agent_type, browser_name, browser_version, os, device_type |
| 5 | `SELECT DISTINCT` in geo_hash subquery included `isp` → duplicate `geo_hash` IntegrityError | Changed to `GROUP BY country, region, city, latitude, longitude` + `MAX(isp)` |
| 6 | Hash column DDL used shorthand SQL Server types | Replaced with explicit `VARBINARY(32)` |
| 7 | `-1 sentinel row` INSERT failed due to identity column | Fixed `SET IDENTITY_INSERT ON/OFF` pattern |

**Final E2E Verification (Run `manual__2026-06-09_verify_final`):**
- **Task 1** (`bronze_silver_jdbc_pipeline`): ✅ SUCCESS in 188s (Workflow: Bronze → Silver → JDBC to Azure SQL, 153K+ rows)
- **Task 2** (`export_dimensions`): ✅ SUCCESS — Parsed 2,040 user-agent strings, Inserted 2,040 rows into `dim_useragent`
- **Azure SQL dim tables**: `dim_geolocation` **1,585 rows**, `dim_useragent` **219 rows** — 0 duplicates, 0 integrity violations
- **Dataset outlet** `mssql://azure-sql/dbo/raw_enriched_loaded` fired — ready for downstream dbt DAG

**Changes from Plan Scaffold (Post-Completion):**

| Plan Scaffold | Actual Implementation | Reason |
|---------------|----------------------|--------|
| No UA parsing library | `user-agents` library used for full UA parsing | Raw UA strings need parsing for clean dimension tables |
| `HASHBYTES('SHA2_256', ...)` in SQL for both hashes | `HASHBYTES` in SQL for geo; `hashlib.sha256()` in Python for UA | UA hash computed alongside parsing avoids extra SQL round-trip |
| `SELECT DISTINCT` for geo dedup | `GROUP BY` + `MAX(isp)` per unique geo key | `DISTINCT` included `isp` (not in hash) causing duplicate `geo_hash` |
| No -1 sentinel rows | `SET IDENTITY_INSERT ON/OFF` with -1 unknown rows for both dims | Maintains FK integrity for orphaned fact rows |
| `user_agent` as natural key in MERGE | `ua_hash` as MERGE match key | Consistent with geo_hash pattern; hash-based matching faster |

**Verified State:**
- Workflow job: `847995192336508` — serverless, multi-task, schedule 0 0 2 * * ? UTC
- Bronze pipeline: `a6ea62d3-5f3a-4f53-ae8b-4bfb156703ad` — serverless, ADVANCED, no development flag
- Silver pipeline: `98c7675f-5425-4a14-95b6-247af6da9626` — serverless, ADVANCED, maxminddb env dep
- Dev job `w3c-jdbc-export-test`: **deleted** (replaced by workflow Task 3)
- Terraform state: 3 managed resources (bronze pipeline, silver pipeline, workflow job)
- Terraform tests: `tests/test_terraform_part_b.py` — 39 tests, `@pytest.mark.terraform`
- All 3 workflow tasks confirmed SUCCESS in end-to-end test
- **76 unit tests pass, 0 failures**; terraform validate succeeds; DAG syntax verified
- Storage key: injected via `TF_VAR_storage_access_key` (conditional — not in tfvars)
- Managed identity RBAC insufficient for serverless DLT ABFSS flow resolution — storage key workaround remains

**Phase 6 → Phase 7 Handoff:** ✅ Complete — Phase 7 already integrated into Phase 6 (dimension export inlined in DAG). All orchestration operational, Airflow DAG triggers Workflow, inline dimension export verified with 1,585 geo rows + 219 UA rows.

---

### Phase 7 — Dimension Export (✅ Complete — E2E Verified 2026-06-09)

**Phase Goal:** Build dimensional tables (`dim_geolocation`, `dim_useragent`) from Azure SQL `dbo.raw_enriched` using MERGE upsert, with full user-agent string parsing, and fire the Dataset outlet for the downstream dbt DAG.

**Summary:** Dimension export inline in `spark_ingestion_azure.py` as `_export_dimensions()` PythonOperator (wired as Task 2 in the DAG, runs after Workflow completes). Reads `dbo.raw_enriched` via `pyodbc`, computes geo composite hashes via Azure SQL `HASHBYTES('SHA2_256', ...)` inside the MERGE subquery (efficient batch computation where source data lives), parses raw UA strings via `user-agents` library with `hashlib.sha256()` hashing (alongside parsing to avoid extra SQL round-trips). Upserts into `dbo.dim_geolocation` (MERGE on `geo_hash` — SHA-256 of country/region/city/lat/lon) and `dbo.dim_useragent` (MERGE on `ua_hash` — SHA-256 of parsed UA fields). Both dims seed `-1` sentinel unknown rows for FK integrity via `SET IDENTITY_INSERT ON/OFF`. Dataset outlet `mssql://azure-sql/dbo/raw_enriched_loaded` fired. **All runs succeed** — 1,585 unique geo rows and 219 unique UA rows with zero duplicates and zero integrity violations.

**⚠️ Differences from Plan Scaffold:**

| Plan Scaffold | Actual Implementation (Phase 6/7 Fixes) | Reason |
|---------------|--------------------------------|--------|
| Separate `operators/export_dimensions_azure.py` file | Inline `_export_dimensions()` callable in `spark_ingestion_azure.py` | Simpler project structure; dimension build tightly coupled to DAG lifecycle |
| SQLAlchemy for Azure SQL connection | Raw `pyodbc` with DRIVER+conn_str | Fewer dependencies; pyodbc already available in Airflow container |
| No UA parsing library needed | `user-agents` library used for full UA parsing | Raw UA strings need parsing; produces clean component columns |
| `HASHBYTES('SHA2_256', ...)` in SQL MERGE for both hashes | `HASHBYTES` in SQL for geo (batch-computed inside MERGE subquery); `hashlib.sha256()` in Python for UA (computed alongside parsing) | Geo stays in SQL where source data is already batched; UA hash computed locally avoids extra SQL round-trip |
| `SELECT DISTINCT` for geo dedup subquery | `GROUP BY country, region, city, latitude, longitude` + `MAX(isp)` | `DISTINCT` included `isp` (not part of hash) → duplicate `geo_hash` rows → IntegrityError on unique constraint |
| No -1 sentinel rows | `SET IDENTITY_INSERT ON/OFF` with `WHERE NOT EXISTS` for -1 rows | Maintains FK integrity for orphaned fact rows; analytics tools expect -1 sentinel |
| Dataset: `azure://w3c-etl/dimensions_ready` | Dataset: `mssql://azure-sql/dbo/raw_enriched_loaded` | More descriptive — signals which database was loaded |
| `dim_geolocation` with `ip` as MERGE key | `dim_geolocation` with `geo_hash` as MERGE match key (SHA-256 of geo fields) | More robust — handles IP changes across regions; hash match faster than string compare |
| `dim_useragent` with `user_agent` as MERGE key | `dim_useragent` with `ua_hash` as MERGE match key (SHA-256 of UA fields) | Consistent with geo_hash pattern |

**Verified State:**
- `dim_geolocation`: **1,585 rows**, 0 duplicate `geo_hash`, clean geolocation from 30+ countries
- `dim_useragent`: **219 rows** (unique parsed UAs), 0 duplicate `ua_hash`, correctly parsed into `agent_type`/`browser_name`/`browser_version`/`os`/`device_type`
- Both tables have correct schema (`VARBINARY(32)` hash columns), -1 sentinel rows seeded, zero integrity violations
- MERGE upsert idempotent (no duplicates on re-run)
- Dataset outlet `mssql://azure-sql/dbo/raw_enriched_loaded` fired — ready for downstream dbt DAG
- Graceful degradation: logs warning if pyodbc/Azure SQL creds missing; core pipeline unaffected

---

### Phase 8a — dbt T-SQL Macros + Simple Model Migration (✅ Complete)

**Phase Goal:** Create T-SQL compatibility macros and migrate simple dbt models with casts, EXTRACT, TO_CHAR, and boolean-to-int conversions.

**Summary:** Created `macros/t_sql_compat.sql` with 17 T-SQL macros (tsql_cast, tsql_datepart, tsql_format_date, tsql_hash_md5, tsql_generate_series, tsql_percentile_cont, etc.), each with inline `{% if target.type == 'sqlserver' %}...{% else %}...{% endif %}` branches preserving PostgreSQL dialect. Migrated all 10 staging models, replacing `::` casts → `CAST()`, `EXTRACT` → `DATEPART`, `TO_CHAR` → `FORMAT/DATENAME`, `~*` regex → `LOWER/LIKE`/`CHARINDEX`/`SUBSTRING`, `SPLIT_PART` → `CASE/CHARINDEX/SUBSTRING`. Key discovery: dbt Jinja does not support variadic `*args` in macros — removed `tsql_concat(*args)` (crashed at parse time) and replaced with `tsql_hash_md5(concat_expr)` taking a single expression string. Setup work: Python 3.11 venv at `/tmp/dbt-venv` (system 3.14 incompatible with dbt's mashumaro), `dbt-core==1.10.8` + `dbt-sqlserver==1.10.0rc1` (1.8.4 had `get_pyodbc_attrs_before` import crash), Azure SQL firewall rule for dev IP 37.120.235.38. `dbt compile --profile w3c` (16 models/67 tests/565 macros) and `--profile w3c_azure` (614 macros) both pass.

**Key Implementation Details:**
- **dbt version**: `dbt-core==1.8.9` + `dbt-postgres==1.8.2` for PostgreSQL; `dbt-core==1.10.8` + `dbt-sqlserver==1.10.0rc1` for Azure SQL. The 1.8.4 dbt-sqlserver builds on dbt-fabric which has incompatible import paths with dbt-core 1.8.x's internal adapter module structure. **Phase 8c pin recommendation:** `dbt-core==1.10.8` + `dbt-sqlserver==1.10.0rc1` — update the Docker `requirements.txt` and Databricks task library pins.
- **Python 3.11**: System Python 3.14 breaks dbt 1.8.9's `mashumaro` dependency (`NoneType.split`). Use Python 3.11 for all dbt operations.
- **No `_azure.sql` duplicates**: All T-SQL compatibility uses inline `{% if target.type == 'sqlserver' %}` inside existing model files. Creating separate `_azure.sql` files would cause dbt to parse both as independent models.

**⚠️ Differences from Plan Scaffold:**

| Plan Scaffold | Actual Implementation | Reason |
|---------------|----------------------|--------|
| `tsql_concat(*args)` variadic macro | `tsql_hash_md5(concat_expr)` single-expression macro | dbt Jinja rejects `*args` in macro signatures — crashes at parse time |
| No setup complexity noted | Python 3.11 venv required, dbt version compatibility fix needed, Azure SQL firewall rule required | System Python 3.14 incompatible; dbt-sqlserver 1.8.x needs dbt-core 1.10.x |
| `REPLACE(REPLACE(...))` for `tsql_regexp_replace` T-SQL branch | Not used in any model (all regex handled by LOWER/LIKE/CHARINDEX) | Macro defined but effectively dead code for T-SQL — all callers use `tsql_case_insensitive_like` instead |

**Verified State:**
- `macros/t_sql_compat.sql`: 17 macros (tsql_cast, tsql_datepart, tsql_month_name, tsql_day_name, tsql_dow, tsql_format_date, tsql_split_part, tsql_regexp_replace, tsql_generate_series, tsql_percentile_cont, tsql_create_index_if_not_exists, tsql_hash_md5, tsql_boolean_to_int, tsql_bool_literal, tsql_true_val, tsql_false_val, tsql_extract_domain, tsql_case_insensitive_like)
- 10 staging models migrated: fact_webrequest, dim_date, dim_time, dim_page, dim_status, dim_referrer, dim_method, dim_visitortype, dim_visit_buckets, crawler_ips
- Ruff lint ✅, mypy 13 files ✅, pytest 132 passed/31 skipped ✅
- `dbt compile --profile w3c` (PostgreSQL): 16 models, 67 tests, 565 macros ✅
- `dbt compile --profile w3c_azure` (Azure SQL): 16 models, 67 tests, 614 macros ✅

**Phase 8a → Phase 8b Handoff:** ✅ Complete. Macro layer established, staging models working on both dialects. Ready for mart model migration with complex patterns.

---

### Phase 8b — dbt Complex Model Migration (✅ Complete)

**Phase Goal:** Migrate complex dbt models with regex, generate_series, PERCENTILE_CONT, and advanced T-SQL patterns.

**Summary:** Migrated all 6 mart models (mart_page_performance, mart_daily_aggregates, mart_crawler_analysis, mart_browser_analysis, mart_timeofday_analysis, mart_country_browser_share) with inline T-SQL conditionals. Implemented additional macros: `tsql_generate_series` (Azure SQL compat level 160+), `tsql_percentile_cont`, `tsql_create_index_if_not_exists`, `tsql_hash_md5`. Migrated singular test `tests/singular/fact_webrequest_dedup_safety.sql` to use `tsql_hash_md5`. Compiled T-SQL output verified for all key patterns: `HASHBYTES('MD5', CONCAT(...))` ✅, `GENERATE_SERIES` in CROSS JOIN for time dimension ✅, `PERCENTILE_CONT` with explicit `OVER ()` ✅, `LOWER/LIKE` for regex replacements ✅, `CAST(... AS NUMERIC(10,2))` for division ✅. Post-completion review (June 10, 2026) caught 1 semantic bug and 3 code-quality issues, all fixed and verified.

**Post-Completion Fixes Applied (June 10, 2026):**

| # | Issue | Fix |
|---|-------|-----|
| 6 | `LIKE '%w3c.org'` in T-SQL branch matched end-of-string instead of contains (missing trailing `%`) — would misclassify internal W3C referral traffic on Azure SQL | Changed to `LIKE '%w3c.org%'` in `dim_referrer.sql:42` |
| 4 | Inline domain extraction in T-SQL branch duplicated `tsql_extract_domain` macro logic (15 lines of CASE/SUBSTRING) | Replaced with `{{ tsql_extract_domain('rr.referrer_url') }}` call in `dim_referrer.sql` |
| 12 | Post-hooks used `'{{ this }}'` (nested Jinja) instead of `this.identifier` — fragile double-rendering pattern | Changed all 6 post_hook arguments across 3 model files + macro to `this.identifier` |
| 5 | `tsql_split_part` silently returns 0-length for part>=3 (PostgreSQL-native SPLIT_PART supports any part) | Added inline NOTE comment in `t_sql_compat.sql:65` documenting the limitation |

**⚠️ Differences from Plan Scaffold:**

| Plan Scaffold | Actual Implementation | Reason |
|---------------|----------------------|--------|
| All macros used as intended | `tsql_generate_series` and `tsql_percentile_cont` remain as global macros; `dim_time.sql` uses inline `GENERATE_SERIES` subquery instead of macro call | Inline pattern simpler for CROSS JOIN use case; macros available for any future model |
| Post-hooks use `'{{ this }}'` | Post-hooks use `this.identifier` | Prevents fragile nested Jinja double-rendering |

**Verified State:**
- 6 mart models migrated with inline T-SQL conditionals
- All mart_CASE/WHEN/PERCENTILE_CONT/GENERATE_SERIES patterns verified in Azure SQL compiled output
- Ruff lint ✅, pytest 132 passed/31 skipped ✅
- `dbt compile --profile w3c` ✅, `dbt compile --profile w3c_azure` ✅
- Singular test `fact_webrequest_dedup_safety.sql` uses `HASHBYTES('MD5', CONCAT(...))` on Azure SQL ✅
- Phase 8a/8b review documented in `plans/phase8ab-review-issues.md` (15 findings, 4 fixed/verified)

**Phase 8b → Phase 8c Handoff:** ✅ Complete. All 16 dbt models, 67 tests, and macros operational on both PostgreSQL and Azure SQL. Ready for dbt docs, source freshness, CSV export DAG, and Power BI integration.

---

### Phase 8c — dbt Docs, Source Freshness, and CSV Export

**Phase Goal:** Configure dbt docs generation, source freshness checks, hosting for the data catalog, and automated CSV export from Azure SQL to Airflow for Power BI consumption.

**Checklist:**

- [ ] Create `profiles.yml` with w3c_azure profile
- [ ] Configure Azure SQL connection in profiles.yml
- [ ] Update `dbt_project.yml` to preserve dynamic profile selection (`{{ env_var('DBT_PROFILE', 'w3c') }}`)
- [ ] Update `sources.yml` to define `w3c` source with `raw_enriched`, `dim_geolocation`, and `dim_useragent` tables for Azure SQL
- [ ] Set `DBT_PROFILE=w3c_azure` in `.env.azure` to activate the Azure target
- [ ] Create Python operator `airflow/dags/operators/export_csv_azure.py` for exporting tables to CSVs
- [ ] Create Python operator `airflow/dags/operators/export_dbt_docs_azure.py` to sync docs files from DBFS/Blob storage to local Airflow
- [ ] Create `airflow/dags/dbt_marts_azure.py` to trigger Databricks dbt tasks and run downstream export tasks
- [ ] Pin `dbt-core==1.8.9` and `dbt-sqlserver==1.8.4` in all Databricks task libraries arrays
- [ ] Test source freshness, model runs, docs generation, and CSV export end-to-end
- [ ] Verify exactly 18 CSV files are written to `/opt/airflow/data/Star-Schema/` with matching schemas and column ordering

**Code Scaffolds:**

**profiles.yml (add w3c_azure profile):**

```yaml
w3c:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: airflow
      password: airflow
      port: 5432
      dbname: airflow
      schema: public
      threads: 4

w3c_azure:
  target: dev
  outputs:
    dev:
      type: sqlserver
      host: "{{ env_var('AZURE_SQL_SERVER') }}"
      user: "{{ env_var('AZURE_SQL_USER') }}"
      password: "{{ env_var('AZURE_SQL_PASSWORD') }}"
      port: 1433
      database: "{{ env_var('AZURE_SQL_DB') }}"
      schema: dbo
      threads: 4
```

**dbt_project.yml (preserve dynamic profile loading):**

```yaml
name: 'w3c'
version: '1.0.0'
config-version: 2

# Dynamically loaded based on environment to allow local dev (w3c) vs Azure (w3c_azure)
profile: "{{ env_var('DBT_PROFILE', 'w3c') }}"

model-paths: ["models"]
test-paths: ["tests"]
analysis-paths: ["analyses"]
macro-paths: ["macros"]

packages-install-path: "dbt_packages"

clean-targets:
  - "target"
  - "dbt_packages"

models:
  w3c:
    staging:
      +materialized: table
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
```

**sources.yml (define Azure SQL and PostgreSQL compatible source schema):**

```yaml
version: 2

sources:
  - name: w3c
    database: "{% if target.type == 'sqlserver' %}{{ env_var('AZURE_SQL_DB', 'w3c-etl-db') }}{% else %}w3c_warehouse{% endif %}"
    schema: "{% if target.type == 'sqlserver' %}dbo{% else %}public{% endif %}"
    tables:
      - name: raw_enriched
        description: "Enriched W3C web logs from Azure SQL / PostgreSQL"
        freshness:
          warn_after: {count: 24, period: hour}
          error_after: {count: 48, period: hour}
        loaded_at_field: log_date
        columns:
          - name: log_date
            tests:
              - not_null
          - name: client_ip
            tests:
              - not_null

      - name: dim_geolocation
        description: "Geolocation dimension table built from Azure SQL"
        columns:
          - name: geolocation_sk
            tests:
              - unique
              - not_null
          - name: ip
            tests:
              - not_null

      - name: dim_useragent
        description: "User agent dimension table built from Azure SQL"
        columns:
          - name: user_agent_sk
            tests:
              - unique
              - not_null
          - name: user_agent
            tests:
              - not_null
```

**dbt packages.yml (ensure dbt-utils is defined):**

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

**requirements.txt additions (pin versions to align local and Databricks execution):**

```yaml
dbt-core==1.8.9
dbt-sqlserver==1.8.4

```

**airflow/dags/operators/export_csv_azure.py (new CSV export operator logic):**

```python
import os
import pyodbc
import pandas as pd

def export_csv_azure(**context):
    """Export dbt mart and staging tables from Azure SQL to CSV files for Power BI."""
    server = os.getenv("AZURE_SQL_SERVER")
    database = os.getenv("AZURE_SQL_DB")
    username = os.getenv("AZURE_SQL_USER")
    password = os.getenv("AZURE_SQL_PASSWORD")

    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};DATABASE={database};"
        f"UID={username};PWD={password};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
    )

    STAR_SCHEMA_DIR = "/opt/airflow/data/Star-Schema"
    os.makedirs(STAR_SCHEMA_DIR, exist_ok=True)

    STAGING_TABLES = [
        "dbt_staging.fact_webrequest", "dbt_staging.dim_date", "dbt_staging.dim_time",
        "dbt_staging.dim_page", "dbt_staging.dim_status", "dbt_staging.dim_referrer",
        "dbt_staging.dim_method", "dbt_staging.dim_visitortype", "dbt_staging.dim_visit_buckets",
        "dbt_staging.crawler_ips",
    ]

    MART_TABLES = [
        "dbt_marts.mart_page_performance", "dbt_marts.mart_daily_aggregates",
        "dbt_marts.mart_crawler_analysis", "dbt_marts.mart_browser_analysis",
        "dbt_marts.mart_timeofday_analysis", "dbt_marts.mart_country_browser_share",
    ]

    PUBLIC_TABLES = [
        "dbo.dim_geolocation",
        "dbo.dim_useragent",
    ]

    conn = pyodbc.connect(conn_str)
    try:
        # Export staging and mart tables
        for table in STAGING_TABLES + MART_TABLES:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            df.to_csv(f"{STAR_SCHEMA_DIR}/{table}.csv", index=False)
            print(f"Exported {table} to CSV")

        # Export public/dbo tables, but save them with 'public.' prefix to match Power BI contract
        for table in PUBLIC_TABLES:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            file_name = table.replace("dbo.", "public.")
            df.to_csv(f"{STAR_SCHEMA_DIR}/{file_name}.csv", index=False)
            print(f"Exported {table} as {file_name} to CSV")
    finally:
        conn.close()
```

**airflow/dags/operators/export_dbt_docs_azure.py (new docs sync operator):**

```python
import os
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

def export_dbt_docs_to_airflow(**context):
    """Download dbt docs from Azure Blob Storage (gold container) to local Airflow directory."""
    local_docs_dir = "/opt/airflow/data/dbt-docs"
    os.makedirs(local_docs_dir, exist_ok=True)

    hook = WasbHook(wasb_conn_id="wasb_default")
    container = "gold"

    for filename in ["index.html", "manifest.json", "catalog.json"]:
        blob_path = f"dbt-docs/{filename}"
        local_path = os.path.join(local_docs_dir, filename)
        if hook.check_for_blob(container_name=container, blob_name=blob_path):
            hook.get_file(local_path, container_name=container, blob_name=blob_path)
            print(f"Downloaded {blob_path} to {local_path}")
```

**airflow/dags/dbt_marts_azure.py (updated dbt orchestration DAG):**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import os

from operators.export_dimensions_azure import export_dimensions_azure
from operators.export_csv_azure import export_csv_azure
from operators.export_dbt_docs_azure import export_dbt_docs_to_airflow

# Dataset from export_dimensions_azure
DIMENSIONS_DATASET = Dataset("azure://w3c-etl/dimensions_ready")
DBT_DOCS_DATASET = Dataset("azure://w3c-etl/dbt_docs_ready")
CSV_EXPORTS_DATASET = Dataset("azure://w3c-etl/csv_exports_ready")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_marts_azure",
    default_args=default_args,
    schedule_interval=[DIMENSIONS_DATASET],
    catchup=False,
    description="Run dbt models against Azure SQL and generate docs/CSVs"
) as dag:

    # dbt source freshness check
    dbt_source_freshness = DatabricksSubmitRunOperator(
        task_id="dbt_source_freshness",
        databricks_conn_id="databricks_default",
        existing_cluster_id="{{ var.value.DATABRICKS_CLUSTER_ID }}",
        libraries=[
            {"pypi": {"package": "dbt-core==1.8.9"}},
            {"pypi": {"package": "dbt-sqlserver==1.8.4"}}
        ],
        notebook_task={
            "notebook_path": "/Repos/w3c-etl-pipeline/airflow/spark/databricks/dbt_freshness.py",
            "base_parameters": {
                "azure_sql_server": os.getenv("AZURE_SQL_SERVER"),
                "azure_sql_database": os.getenv("AZURE_SQL_DB"),
                "azure_sql_user": os.getenv("AZURE_SQL_USER"),
                "azure_sql_password": os.getenv("AZURE_SQL_PASSWORD"),
            }
        }
    )

    # dbt run
    dbt_run = DatabricksSubmitRunOperator(
        task_id="dbt_run",
        databricks_conn_id="databricks_default",
        existing_cluster_id="{{ var.value.DATABRICKS_CLUSTER_ID }}",
        libraries=[
            {"pypi": {"package": "dbt-core==1.8.9"}},
            {"pypi": {"package": "dbt-sqlserver==1.8.4"}}
        ],
        notebook_task={
            "notebook_path": "/Repos/w3c-etl-pipeline/airflow/spark/databricks/dbt_run.py",
            "base_parameters": {
                "azure_sql_server": os.getenv("AZURE_SQL_SERVER"),
                "azure_sql_database": os.getenv("AZURE_SQL_DB"),
                "azure_sql_user": os.getenv("AZURE_SQL_USER"),
                "azure_sql_password": os.getenv("AZURE_SQL_PASSWORD"),
            }
        }
    )

    # dbt test
    dbt_test = DatabricksSubmitRunOperator(
        task_id="dbt_test",
        databricks_conn_id="databricks_default",
        existing_cluster_id="{{ var.value.DATABRICKS_CLUSTER_ID }}",
        libraries=[
            {"pypi": {"package": "dbt-core==1.8.9"}},
            {"pypi": {"package": "dbt-sqlserver==1.8.4"}}
        ],
        notebook_task={
            "notebook_path": "/Repos/w3c-etl-pipeline/airflow/spark/databricks/dbt_test.py",
            "base_parameters": {
                "azure_sql_server": os.getenv("AZURE_SQL_SERVER"),
                "azure_sql_database": os.getenv("AZURE_SQL_DB"),
                "azure_sql_user": os.getenv("AZURE_SQL_USER"),
                "azure_sql_password": os.getenv("AZURE_SQL_PASSWORD"),
            }
        }
    )

    # dbt docs generate
    dbt_docs = DatabricksSubmitRunOperator(
        task_id="dbt_docs",
        databricks_conn_id="databricks_default",
        existing_cluster_id="{{ var.value.DATABRICKS_CLUSTER_ID }}",
        libraries=[
            {"pypi": {"package": "dbt-core==1.8.9"}},
            {"pypi": {"package": "dbt-sqlserver==1.8.4"}}
        ],
        notebook_task={
            "notebook_path": "/Repos/w3c-etl-pipeline/airflow/spark/databricks/dbt_docs.py",
            "base_parameters": {
                "azure_sql_server": os.getenv("AZURE_SQL_SERVER"),
                "azure_sql_database": os.getenv("AZURE_SQL_DB"),
                "azure_sql_user": os.getenv("AZURE_SQL_USER"),
                "azure_sql_password": os.getenv("AZURE_SQL_PASSWORD"),
                "docs_output_path": "/dbfs/mnt/w3c-data/dbt-docs"
            }
        }
    )

    # Export dbt docs to Airflow
    export_dbt_docs = PythonOperator(
        task_id="export_dbt_docs",
        python_callable=export_dbt_docs_to_airflow,
        outlets=[DBT_DOCS_DATASET]
    )

    # Export Power BI CSV files from Azure SQL
    export_csv = PythonOperator(
        task_id="export_csv",
        python_callable=export_csv_azure,
        outlets=[CSV_EXPORTS_DATASET]
    )

    # Task dependencies
    dbt_source_freshness >> dbt_run >> dbt_test
    dbt_test >> dbt_docs >> export_dbt_docs
    dbt_test >> export_csv
```

**dbt_docs.py (Databricks notebook):**

```python
from pyspark.sql import SparkSession
import subprocess
import os
import shutil

if __name__ == "__main__":
    spark = SparkSession.builder.appName("dbt Docs Generate").getOrCreate()

    # Set environment variables
    os.environ["AZURE_SQL_SERVER"] = spark.conf.get("azure.sql.server")
    os.environ["AZURE_SQL_DATABASE"] = spark.conf.get("azure.sql.database")
    os.environ["AZURE_SQL_USER"] = spark.conf.get("azure.sql.username")
    os.environ["AZURE_SQL_PASSWORD"] = spark.conf.get("azure.sql.password")

    # Change to dbt project directory
    os.chdir("/dbfs/Repos/w3c-etl-pipeline/airflow/dbt/w3c")

    # Run dbt docs generate
    result = subprocess.run(
        ["dbt", "docs", "generate", "--profile", "w3c_azure"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"dbt docs generate failed: {result.stderr}")
        raise Exception("dbt docs generate failed")

    print(f"dbt docs generate succeeded")

    # Copy docs to output path
    docs_output_path = spark.conf.get("docs.output.path", "/dbfs/mnt/w3c-data/dbt-docs")
    os.makedirs(docs_output_path, exist_ok=True)

    shutil.copytree("target", docs_output_path, dirs_exist_ok=True)

    print(f"dbt docs copied to {docs_output_path}")

    spark.stop()
```

**Hosting options:**

**Option 1: GitHub Pages (simpler)**

```bash
# Add to CI/CD Tier 2
- name: Deploy dbt docs to GitHub Pages
  uses: peaceiris/actions-gh-pages@v3
  with:
    github_token: ${{ secrets.GITHUB_TOKEN }}
    publish_dir: ./airflow/data/dbt-docs
    publish_branch: gh-pages
```

**Option 2: Azure Static Web Apps (more complex but uses Azure Credits)**

```bash
# Create Azure Static Web App
az staticwebapp create \
  --name w3c-etl-docs \
  --resource-group rg-w3c-etl-dev \
  --source dbt-docs \
  --branch main \
  --location eastus
```

**Acceptance Criteria:**

- profiles.yml configured with w3c_azure profile
- Azure SQL connection configured
- dbt docs generate added to dbt DAG
- Source freshness checks configured
- dbt source freshness check added to dbt DAG
- dbt docs artifacts exported to airflow/data/dbt-docs/
- CSV export operator written and integrated in dbt DAG
- Exactly 18 CSV files exported to airflow/data/Star-Schema/ matching PostgreSQL baseline schemas
- dbt docs hosting configured (GitHub Pages or Azure Static Web Apps)
- dbt docs generation tested successfully
- Source freshness checks tested successfully
- catalog.json artifact verified
- dbt docs accessible via hosting URL

**Phase Handoff Validation:**

```bash
# Verify environment variable for dbt target profile
source .env.azure
echo $DBT_PROFILE  # Should show w3c_azure

# Test dbt docs generation locally
cd airflow/dbt/w3c
dbt docs generate --profile w3c_azure

# Verify artifacts
ls -la target/

# Test source freshness
dbt source freshness --profile w3c_azure

# Verify exactly 18 CSV files have been exported
count=$(ls ../../data/Star-Schema/*.csv | wc -l)
echo "CSV File Count: $count" # Should print 18

# Verify docs hosting
curl https://<username>.github.io/w3c-etl-pipeline/
```

---

### Phase 9 — Split CI/CD

**Phase Goal:** Configure split-tier CI/CD with Tier 1 (every push, no Azure creds) and Tier 2 (nightly integration, protected by GitHub Environment).

**Checklist:**

- [ ] Create `.github/workflows/ci.yml` with Tier 1 jobs
- [ ] Configure Tier 1: Ruff linting
- [ ] Configure Tier 1: mypy type checking
- [ ] Configure Tier 1: pytest unit + DAG integrity tests
- [ ] Configure Tier 1: dbt compile --profile w3c (PostgreSQL)
- [ ] Configure Tier 1: terraform validate
- [ ] Configure Tier 1: terraform fmt --check
- [ ] Create `.github/workflows/ci-integration.yml` with Tier 2 jobs
- [ ] Configure Tier 2: Upload sample logs to ADLS
- [ ] Configure Tier 2: Trigger Databricks Workflow via REST API
- [ ] Configure Tier 2: Poll job status
- [ ] Configure Tier 2: Query Azure SQL for expected row counts
- [ ] Configure Tier 2: Assert 18 CSV exports produced
- [ ] Configure Tier 2: Validate catalog.json dbt docs artifact
- [ ] Configure GitHub Environment `azure-integration`
- [ ] Add manual approval gate to Tier 2
- [ ] Configure Tier 2 secrets (ARM_*, DATABRICKS_TOKEN, AZURE_SQL_*)
- [ ] Document shared credential usage (Terraform backend + Tier 2 CI)
- [ ] Test Tier 1 CI on push
- [ ] Test Tier 2 CI via workflow_dispatch

**Code Scaffolds:**

**.github/workflows/ci.yml (Tier 1):**

```yaml
name: CI Tier 1 - Every Push

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main, develop]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Install dependencies
        run: |
          pip install ruff mypy pytest
          pip install -r requirements.txt
      - name: Run Ruff
        run: ruff check .
      - name: Run mypy
        run: mypy .

  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Install dependencies
        run: |
          pip install pytest
          pip install -r requirements.txt
      - name: Run unit tests
        run: pytest tests/unit/ -v
      - name: Run DAG integrity tests
        run: pytest tests/dag_integrity/ -v

  dbt-compile:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Install dbt
        run: |
          pip install dbt-core dbt-postgres
      - name: Compile dbt models
        run: |
          cd airflow/dbt/w3c
          dbt compile --profile w3c

  terraform-validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Terraform
        uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.10.5"
      - name: Terraform fmt check Part A
        run: |
          cd terraform/part_a
          terraform fmt --check
      - name: Terraform fmt check Part B
        run: |
          cd terraform/part_b
          terraform fmt --check
      - name: Terraform validate Part A
        run: |
          cd terraform/part_a
          terraform init -backend=false
          terraform validate
      - name: Terraform validate Part B
        run: |
          cd terraform/part_b
          terraform init -backend=false
          terraform validate
```

**.github/workflows/ci-integration.yml (Tier 2):**

```yaml
name: CI Tier 2 - Azure Integration

on:
  workflow_dispatch:
  schedule:
    - cron: "0 2 * * *"  # Daily at 2 AM UTC

env:
  ARM_CLIENT_ID: ${{ secrets.ARM_CLIENT_ID }}
  ARM_CLIENT_SECRET: ${{ secrets.ARM_CLIENT_SECRET }}
  ARM_SUBSCRIPTION_ID: ${{ secrets.ARM_SUBSCRIPTION_ID }}
  ARM_TENANT_ID: ${{ secrets.ARM_TENANT_ID }}
  DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
  AZURE_SQL_SERVER: ${{ secrets.AZURE_SQL_SERVER }}
  AZURE_SQL_DB: ${{ secrets.AZURE_SQL_DB }}
  AZURE_SQL_USER: ${{ secrets.AZURE_SQL_USER }}
  AZURE_SQL_PASSWORD: ${{ secrets.AZURE_SQL_PASSWORD }}

jobs:
  integration-test:
    runs-on: ubuntu-latest
    environment: azure-integration
    steps:
      - uses: actions/checkout@v4

      - name: Azure Login
        uses: azure/login@v2
        with:
          client-id: ${{ secrets.ARM_CLIENT_ID }}
          tenant-id: ${{ secrets.ARM_TENANT_ID }}
          subscription-id: ${{ secrets.ARM_SUBSCRIPTION_ID }}

      - name: Upload sample logs to ADLS
        run: |
          # Upload real log files (93 files in airflow/data/LogFiles/) for integration test
          for f in airflow/data/LogFiles/*.log; do
            az storage blob upload \
              --container-name raw-logs \
              --file "$f" \
              --name "ci-test/$(basename "$f")" \
              --account-name ${{ secrets.STORAGE_ACCOUNT_NAME }}
          done

      - name: Trigger Databricks Workflow
        run: |
          curl -X POST \
            "https://${{ secrets.DATABRICKS_HOST }}/api/2.1/jobs/run-now" \
            -H "Authorization: Bearer ${{ secrets.DATABRICKS_TOKEN }}" \
            -H "Content-Type: application/json" \
            -d '{"job_id": ${{ secrets.DATABRICKS_WORKFLOW_ID }}}'

      - name: Poll job status
        run: |
          # Poll job status until completion
          # Implementation depends on Databricks API
          echo "Polling job status..."

      - name: Query Azure SQL row counts
        run: |
          # Query dbo.raw_enriched for expected row counts
          sqlcmd -S ${{ secrets.AZURE_SQL_SERVER }} \
            -d ${{ secrets.AZURE_SQL_DB }} \
            -U ${{ secrets.AZURE_SQL_USER }} \
            -P ${{ secrets.AZURE_SQL_PASSWORD }} \
            -Q "SELECT COUNT(*) FROM dbo.raw_enriched"

      - name: Assert 18 CSV exports produced
        run: |
          # Verify 18 CSV files in airflow/data/Star-Schema/
          count=$(ls airflow/data/Star-Schema/*.csv | wc -l)
          if [ $count -ne 18 ]; then
            echo "Expected 18 CSV files, found $count"
            exit 1
          fi

      - name: Validate catalog.json
        run: |
          # Verify catalog.json exists in airflow/data/dbt-docs/
          if [ ! -f airflow/data/dbt-docs/catalog.json ]; then
            echo "catalog.json not found"
            exit 1
          fi
          # Validate JSON structure
          python -m json.tool airflow/data/dbt-docs/catalog.json > /dev/null

      - name: Azure Logout
        if: always()
        run: az logout
```

**GitHub Environment configuration:**

```bash
# Create GitHub Environment via GitHub UI or CLI
# Navigate to: Settings > Environments > New environment
# Name: azure-integration
# Add protection rules:
#   - Required reviewers: add your username
#   - Wait timer: 0 minutes
#   - Restrict branches to: main, develop
```

**Secrets configuration (GitHub repository settings):**

```bash
# Tier 2 secrets (add to GitHub repository)
ARM_CLIENT_ID=<service-principal-appId>
ARM_CLIENT_SECRET=<service-principal-password>
ARM_SUBSCRIPTION_ID=<subscription-id>
ARM_TENANT_ID=<tenant-id>
DATABRICKS_TOKEN=<personal-access-token>
DATABRICKS_HOST=<workspace-url>
DATABRICKS_WORKFLOW_ID=<workflow-job-id>
AZURE_SQL_SERVER=<server-fqdn>
AZURE_SQL_DB=w3c-etl-db
AZURE_SQL_USER=sqladmin
AZURE_SQL_PASSWORD=<strong-password>
STORAGE_ACCOUNT_NAME=<storage-account-name>
```

**Acceptance Criteria:**

- Tier 1 CI configured with all checks
- Tier 1 runs on every push to main/develop
- Tier 1 passes without Azure credentials
- Tier 2 CI configured with integration tests
- Tier 2 triggers on workflow_dispatch and nightly schedule
- Tier 2 protected by GitHub Environment azure-integration
- Manual approval gate configured for Tier 2
- Tier 2 secrets configured in GitHub repository
- Shared credential usage documented
- Tier 1 CI tested on push
- Tier 2 CI tested via workflow_dispatch
- Integration tests pass end-to-end

**Phase Handoff Validation:**

```bash
# Trigger Tier 1 CI
git push origin develop

# Verify Tier 1 passes in GitHub Actions

# Trigger Tier 2 CI manually
# Navigate to GitHub Actions > CI Tier 2 > Run workflow
# Request approval
# Approve in GitHub Environments

# Verify Tier 2 passes
```

---

### Phase 10 — Monitoring

**Phase Goal:** Configure Grafana + Prometheus monitoring for Airflow and Azure Monitor alerts for budget and Databricks pipeline failures.

**Checklist:**

- [ ] Verify Grafana + Prometheus stack is running
- [ ] Configure Airflow StatsD exporter
- [ ] Create Grafana dashboard for Airflow metrics
- [ ] Configure Prometheus to scrape Airflow metrics
- [ ] Configure Azure Monitor budget alerts ($50 warning, $100 hard cap)
- [ ] Configure Azure Monitor alert for Databricks pipeline failures
- [ ] Configure alert notification channels (email, Slack)
- [ ] Test alert notifications
- [ ] Verify Databricks pipeline event log review process

**Code Scaffolds:**

**Airflow StatsD exporter configuration (docker-compose.yml):**

```yaml
services:
  airflow-webserver:
    environment:
      - AIRFLOW__METRICS__STATSD_ON=true
      - AIRFLOW__METRICS__STATSD_HOST=prometheus
      - AIRFLOW__METRICS__STATSD_PORT=9125
      - AIRFLOW__METRICS__STATSD_PREFIX=airflow

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - ./monitoring/grafana-dashboards:/etc/grafana/provisioning/dashboards
      - ./monitoring/grafana-datasources:/etc/grafana/provisioning/datasources
```

**Prometheus configuration (monitoring/prometheus.yml):**

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'airflow'
    static_configs:
      - targets: ['airflow-webserver:9125']
```

**Grafana dashboard JSON (monitoring/grafana-dashboards/airflow-dashboard.json):**

```json
{
  "dashboard": {
    "title": "Airflow Metrics",
    "panels": [
      {
        "title": "DAG Run Duration",
        "targets": [
          {
            "expr": "airflow_dag_run_duration_seconds"
          }
        ]
      },
      {
        "title": "Task Success Rate",
        "targets": [
          {
            "expr": "rate(airflow_task_success_count[5m])"
          }
        ]
      },
      {
        "title": "Task Failure Rate",
        "targets": [
          {
            "expr": "rate(airflow_task_failure_count[5m])"
          }
        ]
      }
    ]
  }
}
```

**Azure Monitor budget alerts (already configured in Phase 2, verify):**

```bash
# Verify budget alerts
az consumption budget list --resource-group rg-w3c-etl-dev

# Update notification email if needed
az consumption budget update \
  --name w3c-etl-budget-warning \
  --resource-group rg-w3c-etl-dev \
  --notification '{"threshold":50,"contactEmails":["your-email@example.com"],"operator":"GreaterThan"}'
```

**Azure Monitor alert for Databricks pipeline failures:**

```bash
# Create alert rule for Databricks pipeline failures
az monitor metrics alert create \
  --name databricks-pipeline-failure \
  --resource-group rg-w3c-etl-dev \
  --scopes /subscriptions/<subscription-id>/resourceGroups/rg-w3c-etl-dev/providers/Microsoft.Databricks/workspaces/w3c-etl-databricks-dev \
  --condition "avg pipeline_failure_count > 0" \
  --window-size 5m \
  --evaluation-frequency 1m \
  --action-groups /subscriptions/<subscription-id>/resourceGroups/rg-w3c-etl-dev/providers/Microsoft.Insights/actionGroups/email-alert
```

**Acceptance Criteria:**

- Grafana + Prometheus stack running
- Airflow StatsD exporter configured
- Prometheus scraping Airflow metrics
- Grafana dashboard created for Airflow metrics
- Azure Monitor budget alerts configured ($50 warning, $100 hard cap)
- Azure Monitor alert configured for Databricks pipeline failures
- Alert notification channels configured (email, Slack)
- Alert notifications tested successfully
- Databricks pipeline event log review process documented

**Phase Handoff Validation:**

```bash
# Verify Prometheus is scraping
curl http://localhost:9090/api/v1/targets

# Verify Grafana dashboard
open http://localhost:3000

# Verify Azure budget alerts
az consumption budget list --resource-group rg-w3c-etl-dev

# Verify Databricks pipeline alert
az monitor metrics alert list --resource-group rg-w3c-etl-dev
```

---

### Phase 11 — Final Verification

**Phase Goal:** Perform end-to-end verification of the complete pipeline, validate Power BI compatibility, and confirm all 18 CSV exports are produced correctly.

**Checklist:**

- [ ] Run complete end-to-end pipeline test
- [ ] Upload sample logs to ADLS Gen2
- [ ] Trigger Databricks Workflow
- [ ] Verify Bronze pipeline execution
- [ ] Verify Silver pipeline execution
- [ ] Verify JDBC export to Azure SQL

- [ ] Verify export_dimensions_azure execution
- [ ] Verify dbt run execution
- [ ] Verify dbt test execution
- [ ] Verify dbt docs generation
- [ ] Verify 18 CSV exports produced
- [ ] Validate CSV headers against baseline
- [ ] Validate DAX measure field dependencies
- [ ] Verify Power BI semantic contract
- [ ] Run Tier 2 CI integration test
- [ ] Verify all monitoring alerts
- [ ] Document any issues and resolutions

**Code Scaffolds:**

**End-to-end test script (scripts/e2e_test.sh):**

```bash
#!/bin/bash
set -e

echo "Starting end-to-end verification..."

# Upload sample logs
echo "Uploading sample logs to ADLS..."
for f in airflow/data/LogFiles/*.log; do
  az storage blob upload \
    --container-name raw-logs \
    --file "$f" \
    --name "e2e-test/$(basename "$f")" \
    --account-name $STORAGE_ACCOUNT_NAME
done

# Trigger Databricks Workflow
echo "Triggering Databricks Workflow..."
databricks jobs run-now --job-id $DATABRICKS_WORKFLOW_ID

# Poll for completion
echo "Polling for workflow completion..."
# Add polling logic here

# Verify Bronze table
echo "Verifying Bronze table..."
bronze_count=$(databricks sql execute --warehouse-id $WAREHOUSE_ID --sql "SELECT COUNT(*) FROM w3c_catalog.bronze.bronze_raw_logs" --output json | jq -r '.[0][0]')
echo "Bronze row count: $bronze_count"

# Verify Silver table
echo "Verifying Silver table..."
silver_count=$(databricks sql execute --warehouse-id $WAREHOUSE_ID --sql "SELECT COUNT(*) FROM w3c_catalog.silver.silver_enriched_logs" --output json | jq -r '.[0][0]')
echo "Silver row count: $silver_count"

# Verify Azure SQL
echo "Verifying Azure SQL..."
sql_count=$(sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "SELECT COUNT(*) FROM dbo.raw_enriched" -h -1)
echo "Azure SQL row count: $sql_count"

# Verify dim tables
echo "Verifying dim tables..."
geo_count=$(sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "SELECT COUNT(*) FROM dbo.dim_geolocation" -h -1)
ua_count=$(sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "SELECT COUNT(*) FROM dbo.dim_useragent" -h -1)
echo "dim_geolocation count: $geo_count"
echo "dim_useragent count: $ua_count"

# Verify CSV exports
echo "Verifying CSV exports..."
csv_count=$(ls airflow/data/Star-Schema/*.csv | wc -l)
if [ $csv_count -ne 18 ]; then
    echo "ERROR: Expected 18 CSV files, found $csv_count"
    exit 1
fi
echo "CSV export count: $csv_count"

# Validate CSV headers
echo "Validating CSV headers..."
# Add header validation logic here

# Verify dbt docs
echo "Verifying dbt docs..."
if [ ! -f airflow/data/dbt-docs/catalog.json ]; then
    echo "ERROR: catalog.json not found"
    exit 1
fi
echo "dbt docs verified"

echo "End-to-end verification completed successfully!"
```

**Acceptance Criteria:**

- End-to-end pipeline test completed successfully
- Bronze pipeline executed without errors
- Silver pipeline executed without errors
- JDBC export completed successfully

- export_dimensions_azure completed successfully
- dbt run completed successfully
- dbt tests passed
- dbt docs generated successfully
- Exactly 18 CSV exports produced
- CSV headers match baseline
- DAX measure field dependencies validated
- Power BI semantic contract verified
- Tier 2 CI integration test passed
- All monitoring alerts functional
- Issues and resolutions documented

**Phase Handoff Validation:**

```bash
# Run end-to-end test
./scripts/e2e_test.sh

# Verify CSV exports
ls -la airflow/data/Star-Schema/

# Verify dbt docs
cat airflow/data/dbt-docs/catalog.json | python -m json.tool

# Verify monitoring
curl http://localhost:9090/api/v1/targets
```

---

### Phase 12 — Cost Management and Teardown Documentation - Will be reworked before beginning

**Phase Goal:** Document cost management procedures and provide clear teardown instructions to prevent unexpected charges.

**Checklist:**

- [ ] Create `docs/cost-management.md`
- [ ] Document cost optimization strategies
- [ ] Document budget alert configuration
- [ ] Document resource cost breakdown
- [ ] Create `docs/teardown.md`
- [ ] Document teardown procedure for all Azure resources
- [ ] Document Databricks workspace deletion
- [ ] Document Terraform state cleanup
- [ ] Document service principal deletion
- [ ] Add teardown script automation

**Code Scaffolds:**

**docs/cost-management.md:**

```markdown
# Cost Management

## Cost Optimization Strategies

### Azure SQL Serverless
- Auto-pause after 60 minutes of inactivity
- 1 vCore configuration (GP_S_Gen5)
- Monitor auto-pause behavior via Azure Monitor

### Databricks
- Serverless DLT pipelines (no cluster management, auto-scales to zero)
- Auto-scales to zero when idle — cost-effective for intermittent workloads
- No VM provisioning overhead in capacity-constrained regions

### ADLS Gen2
- Hot tier for frequent access (raw-logs, bronze, silver)
- Cool tier for archival (gold, dbt-docs)
- Lifecycle policies for data retention (90 days for raw logs)

### CI/CD
- Tier 2 CI limited to nightly runs (not on every push)
- Manual approval gate prevents accidental integration test runs
- Integration tests use minimal sample data

## Budget Alerts

### Configuration
- Warning alert: $50 (email notification)
- Hard cap: $100 (automatic resource shutdown)

### Monitoring
- Azure Cost Management dashboard
- Daily cost review during development
- Weekly cost report via email

## Resource Cost Breakdown

Estimated monthly costs (based on usage patterns):

| Resource | Estimated Cost | Notes |
|----------|---------------|-------|
| Azure SQL Serverless | $15-30 | Depends on query volume |
| Databricks Premium | $40-60 | Depends on cluster runtime |
| ADLS Gen2 | $5-10 | Depends on data volume |
| Databricks Workflow | Included in Premium | No additional cost |
| Total | $60-100 | Within $100 budget cap |

## Cost Monitoring Commands

\`\`\`bash
# View current costs
az consumption usage list --resource-group rg-w3c-etl-dev

# View budget status
az consumption budget list --resource-group rg-w3c-etl-dev

# View cost by resource
az consumption usage list --resource-group rg-w3c-etl-dev --query "[].name" -o tsv
\`\`\`
```

**docs/teardown.md:**

```markdown
# Teardown Procedure

## Prerequisites

- Azure CLI installed and authenticated
- Terraform installed
- Access to GitHub repository (for CI/CD cleanup)

## Step 1: Delete Databricks Resources

\`\`\`bash
# Delete Databricks Workflows
databricks jobs delete --job-id <workflow-job-id>

# Delete DLT pipelines
databricks pipelines delete --pipeline-id <bronze-pipeline-id>
databricks pipelines delete --pipeline-id <silver-pipeline-id>

# Delete Unity Catalog (if needed)
# Via Databricks SQL UI or CLI
\`\`\`

## Step 2: Delete Azure Resources via Terraform

\`\`\`bash
# Destroy Part B resources
cd terraform/part_b
terraform destroy -auto-approve

# Destroy Part A resources
cd ../part_a
terraform destroy -auto-approve
\`\`\`

## Step 3: Delete Terraform State Backend

\`\`\`bash
# Delete tfstate storage account
az storage account delete \
  --name tfstate<unique-suffix> \
  --resource-group rg-tfstate \
  --yes

# Delete tfstate resource group
az group delete --name rg-tfstate --yes
\`\`\`

## Step 4: Delete Service Principal

\`\`\`bash
# Get service principal app ID
SP_ID=$(az ad sp list --display-name "w3c-etl-pipeline-sp" --query "[].appId" -o tsv)

# Delete service principal
az ad sp delete --id $SP_ID
\`\`\`

## Step 5: Clean Up GitHub Secrets

\`\`\`bash
# Via GitHub UI: Settings > Secrets and variables > Actions
# Delete all Tier 2 secrets:
# - ARM_CLIENT_ID
# - ARM_CLIENT_SECRET
# - ARM_SUBSCRIPTION_ID
# - ARM_TENANT_ID
# - DATABRICKS_TOKEN
# - DATABRICKS_HOST
# - DATABRICKS_WORKFLOW_ID
# - AZURE_SQL_SERVER
# - AZURE_SQL_DB
# - AZURE_SQL_USER
# - AZURE_SQL_PASSWORD
# - STORAGE_ACCOUNT_NAME
\`\`\`

## Step 6: Delete GitHub Environment

\`\`\`bash
# Via GitHub UI: Settings > Environments
# Delete azure-integration environment
\`\`\`

## Step 7: Verify Cleanup

\`\`\`bash
# Verify no Azure resources remain
az resource list --location eastus --query "[?contains(name, 'w3c')].name" -o tsv

# Verify no service principal remains
az ad sp list --display-name "w3c-etl-pipeline-sp"

# Verify Terraform state deleted
az storage account list --query "[?contains(name, 'tfstate')].name" -o tsv
\`\`\`

## Automated Teardown Script

\`\`\`bash
#!/bin/bash
# teardown.sh

set -e

echo "Starting teardown..."

# Delete Databricks resources
echo "Deleting Databricks Workflows..."
databricks jobs list --output json | jq -r '.[].job_id' | xargs -I {} databricks jobs delete --job-id {}

echo "Deleting DLT pipelines..."
databricks pipelines list --output json | jq -r '.[].pipeline_id' | xargs -I {} databricks pipelines delete --pipeline-id {}

# Destroy Terraform resources
echo "Destroying Terraform Part B..."
cd terraform/part_b
terraform destroy -auto-approve

echo "Destroying Terraform Part A..."
cd ../part_a
terraform destroy -auto-approve

# Delete Terraform state backend
echo "Deleting Terraform state backend..."
az storage account delete --name tfstate<unique-suffix> --resource-group rg-tfstate --yes
az group delete --name rg-tfstate --yes

# Delete service principal
echo "Deleting service principal..."
SP_ID=$(az ad sp list --display-name "w3c-etl-pipeline-sp" --query "[].appId" -o tsv)
az ad sp delete --id $SP_ID

echo "Teardown complete. Please manually delete GitHub secrets and environment."
\`\`\`
```

**Acceptance Criteria:**

- Cost management documentation created
- Cost optimization strategies documented
- Budget alert configuration documented
- Resource cost breakdown documented
- Teardown documentation created
- Teardown procedure documented for all Azure resources
- Databricks workspace deletion documented
- Terraform state cleanup documented
- Service principal deletion documented
- Automated teardown script created

**Phase Handoff Validation:**

```bash
# Verify documentation exists
cat docs/cost-management.md
cat docs/teardown.md

# Verify teardown script is executable
chmod +x scripts/teardown.sh
./scripts/teardown.sh --dry-run
```

---

## Risk Register

| Risk | Impact | Probability | Mitigation Strategy |
|------|--------|-------------|---------------------|
| Azure credit exhaustion before completion | High | Medium | Budget alerts at $50/$100, daily cost monitoring, auto-pause on Azure SQL |
| **Azure westus3 region lacks VM capacity** | **High** | **Certain** | **Use Serverless DLT (`serverless: true`) — eliminates VM provisioning entirely; bypasses westus3 capacity constraints** |
| Databricks Premium tier cost overruns | High | Medium | Cluster auto-termination, limit worker count, monitor cluster runtime |
| DLT pipeline idempotency issues | Medium | Low | left_anti join on source_file (Silver), tracking table pattern (JDBC export), full_refresh mode testing — verified working with real data |
| GeoIP database license expiration | Medium | Low | Monitor license validity, set renewal reminders, use free tier |
| T-SQL migration syntax errors | High | Medium | Comprehensive testing of all macros, Azure SQL compat level verification |
| dbt docs generation failure | Low | Low | Separate task in workflow, error handling, manual fallback |

| CI/CD Tier 2 approval delays | Low | Medium | Multiple approvers, clear documentation, automated retry logic |
| Service principal credential compromise | High | Low | Regular rotation (quarterly), monitoring for unusual activity, immediate revocation if compromised |
| Terraform state corruption | High | Low | Azure Blob Storage backend, regular state backups, state locking |
| Power BI semantic contract violations | High | Medium | Header validation, DAX field dependency checks, baseline comparison |
| MaxMind GeoIP API rate limits | Medium | Low | Local database files (not API), no rate limit concerns |
| Azure SQL serverless cold-start delays | Low | High | Retry logic with exponential backoff, warm-up queries, monitoring |
| Databricks CLI v1+ syntax errors | Medium | Low | Documentation review, CLI command validation, testing in dev environment |

---

## Acceptance Criteria (Definition of Done)

### Infrastructure

- [x] Terraform Part A deployed successfully (resource groups, networking, storage, Databricks workspace, Azure SQL)
- [x] Terraform Part B deployed successfully (DLT pipelines, Databricks Workflows — 3 serverless tasks)
- [x] Terraform remote state backend configured (Azure Blob Storage)
- [x] Unity Catalog created with bronze/silver/gold schemas
- [x] Databricks secret scope `w3c-etl-pipeline` configured
- [x] Budget alerts configured ($50 warning, $100 hard cap)
- [x] `spark_ingestion_azure.py` DAG created — orchestrates entire pipeline (2-task DAG)
- [x] All resources visible and accessible in Azure portal

### DLT Pipelines

- [x] Bronze DLT pipeline deployed and operational — **153,380 rows** from 93 files
- [x] Silver DLT pipeline deployed and operational — **153,377 rows** with GeoIP enrichment
- [x] W3C parser handles both 14-field and 18-field IIS formats (per-file `#Fields:` header detection)
- [x] Auto Loader configured with binaryFile format (serverless DLT compatible)
- [x] Quality expectations (@dlt.expect_or_drop) implemented — 7 Bronze + 3 Silver, all passing
- [x] Idempotency via left_anti join on source_file (Silver layer) — ROW_NUMBER removed from Bronze due to serverless DLT limitations
- [x] Bronze table partitioned by log_date
- [x] Silver table with 31 columns (25 core + 6 geo) — schema verified
- [x] MaxMind GeoLite2 enrichment working — 30+ countries resolved using `maxminddb` (pure Python)
- [x] Computed fields (5 UDFs) working — page_category, referrer_domain, traffic_type, is_crawler, size_band


### Azure Integration

- [x] JDBC export from Silver to Azure SQL operational — **45s for 153,377 rows** (run 658447448322322)
- [x] Tracking table pattern implemented (dbo.raw_enriched_loaded)
- [x] Serverless compute used — `notebook_task` with pymssql (no cluster, no Maven)
- [x] Retry logic (4 attempts, exponential backoff: 15×2^attempt for DB auto-resume)
- [x] is_crawler BIT cast from string (`when(col(...) == "true", lit(1)).otherwise(lit(0))`)
- [x] Exact RAW_ENRICHED_DDL schema (31 columns)
- [x] **`spark_ingestion_azure.py` DAG created** — triggers Workflow + inline `_export_dimensions` task
- [x] **Inline dimension export** — `_export_dimensions()` PythonOperator with MERGE upsert on geo_hash/ua_hash
- [x] **dim_geolocation DDL** — `IF OBJECT_ID` guard, MERGE on geo_hash (SHA2_256 composite)
- [x] **dim_useragent DDL** — `IF OBJECT_ID` guard, MERGE on ua_hash (SHA2_256 composite)
- [x] **Dataset outlet** — `Dataset("mssql://azure-sql/dbo/raw_enriched_loaded")` fires for dbt DAG
- [x] **Graceful degradation** — logs warning if pyodbc/Azure SQL creds missing; core pipeline unaffected

### dbt Migration

- [ ] T-SQL compatibility macros created (tsql_cast, tsql_datepart, tsql_format_date, etc.)
- [ ] All 16 dbt models migrated with inline T-SQL conditionals
- [ ] PostgreSQL dialect preserved in {% else %} branches
- [ ] Boolean to int conversions implemented
- [ ] Complex patterns handled (generate_series, PERCENTILE_CONT, regex)
- [ ] `dbt compile --profile w3c` passes (PostgreSQL)
- [ ] `dbt compile --profile w3c_azure` passes (Azure SQL)
- [ ] dbt docs generate operational
- [ ] Source freshness checks configured
- [ ] dbt docs hosted (GitHub Pages or Azure Static Web Apps)

### CI/CD

- [ ] Tier 1 CI configured (every push, no Azure creds)
- [ ] Tier 1 includes: Ruff, mypy, pytest, dbt compile, terraform validate
- [ ] Tier 2 CI configured (nightly integration)
- [ ] Tier 2 includes: end-to-end integration test, 18 CSV assertion, catalog.json validation
- [ ] GitHub Environment `azure-integration` configured
- [ ] Manual approval gate for Tier 2
- [ ] Tier 2 secrets configured (ARM_*, DATABRICKS_TOKEN, AZURE_SQL_*)
- [ ] Shared credential usage documented

### Monitoring

- [ ] Grafana + Prometheus stack running
- [ ] Airflow StatsD exporter configured
- [ ] Grafana dashboard for Airflow metrics
- [ ] Azure Monitor budget alerts functional
- [ ] Azure Monitor Databricks pipeline failure alert functional
- [ ] Alert notification channels configured (email, Slack)

### Documentation

- [ ] Cost management documentation created
- [ ] Teardown documentation created
- [ ] Automated teardown script created
- [ ] README.md updated with single-pipeline architecture
- [ ] Deployment section added
- [ ] Badges added (CI status, docs, license)
- [ ] All links verified

### Power BI Compatibility

- [ ] Exactly 18 CSV exports produced
- [ ] CSV headers match baseline
- [ ] DAX measure field dependencies validated
- [ ] Power BI semantic contract verified

### End-to-End Verification

- [ ] Complete pipeline test passed
- [ ] All phases executed successfully
- [ ] Data flows end-to-end: ADLS → Bronze → Silver → Azure SQL → dbt → CSV
- [ ] Tier 2 CI integration test passed
- [ ] Issues and resolutions documented

---

## Implementation Order Diagram

```
Phase 0  → Phase 1  → Phase 2  → Phase 3  → Phase 4  → Phase 5 ✅
(Prereqs) (Infra A) (Verify)  (Bronze)  (Silver)  (JDBC)
    ↓         ↓         ↓         ↓         ↓         ↓
    └─────────┴─────────┴─────────┴─────────┴─────────┘
                          ↓
Phase 6 ✅ → Phase 7 ✅ → Phase 8a ✅ → Phase 8b ✅ → Phase 8c
(Wf+TF+DAG) (Dims inline) (Macros)  (Complex) (Docs)
                          (pending)
    ↓              ↓         ↓         ↓         ↓
    └──────────────┴─────────┴─────────┴─────────┘
                          ↓
Phase 9  → Phase 10 → Phase 11
(CI/CD)  (Monitor)    (E2E)
```
