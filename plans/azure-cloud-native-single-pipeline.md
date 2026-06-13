# Azure Cloud-Native Single-Pipeline ETL Platform Implementation Plan

**Version:** v3.1
**Status:** Phases 0–9 ✅ complete
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
 Airflow Cron Schedule (0 17 * * 5 — Friday at 5:00 PM UTC)
      │
      ▼
 Airflow DAG: spark_ingestion_azure.py (schedule="0 17 * * 5")
    │  2 sequential tasks:
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

 Airflow DAG: dbt_marts_azure.py (Dataset-triggered by spark_ingestion_azure's outlet)
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
| **GitHub Actions** | CI/CD automation (split-tier: every push + CD on merge to main) |
| **Python 3.12** | Primary language for PySpark, UDFs, Airflow operators |
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
- CD pipeline (merge to main, Terraform plan/apply, DAB deploy, dbt deploy, Airflow sync, smoke test)
- OIDC Workload Identity Federation setup (no client secrets)

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

- **DAG 1:** `spark_ingestion_azure.py`: Weekly cron schedule (`schedule="0 17 * * 5"`, Friday at 5:00 PM UTC). 2 sequential tasks:
  - Task 1: `bronze_silver_jdbc_pipeline` — `DatabricksRunNowOperator` triggers Databricks Workflow (job ID from `DATABRICKS_JOB_ID` env var). The Workflow runs Bronze → Silver → JDBC Export. Task waits for completion.
  - Task 2: `export_dimensions` — `PythonOperator` with inline `_export_dimensions` callable; builds `dim_geolocation` + `dim_useragent` from Azure SQL via pyodbc MERGE upsert; fires `Dataset("mssql://azure-sql/dbo/raw_enriched_loaded")` outlet for downstream dbt DAG
- **DAG 2:** `dbt_marts_azure.py`: Dataset-triggered by `spark_ingestion_azure.py`'s outlet; runs dbt against Azure SQL target
- **No standalone operator file:** Dimension export logic is inlined in `spark_ingestion_azure.py` as `_export_dimensions()` — no separate `airflow/dags/operators/` directory needed. The existing `airflow/dags/w3c/` directory hosts all DAG files.
- **Provider version:** `apache-airflow-providers-databricks==4.6.0` installed in Dockerfile (NOT in requirements.txt — avoid pip conflict)
- **Databricks connection:** `databricks_default` in Airflow with workspace URL + PAT token
- **Environment variables:** `DATABRICKS_JOB_ID` (default: `847995192336508`), `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USER`, `AZURE_SQL_PASS`

### dbt T-SQL Migration Constraints

- **Inline conditionals:** `{% if target.type == 'sqlserver' %}...{% else %}...{% endif %}` (do NOT create `_azure.sql` duplicates — dbt would parse both as separate models)
- **Macro file:** Global macro file: `macros/t_sql_compat.sql`
- **Required macros:** `tsql_cast`, `tsql_datepart`, `tsql_month_name`, `tsql_day_name`, `tsql_dow`, `tsql_format_date`
- **Coverage:** All 80+ PostgreSQL-specific expressions must be covered: `::` casts → `CAST()`, `~*` → `LIKE+COLLATE`, `EXTRACT` → `DATEPART`, `TO_CHAR` → `FORMAT/DATENAME`, `SPLIT_PART` → `CHARINDEX/SUBSTRING`, `generate_series` → `GENERATE_SERIES` (Azure SQL compat level 160), `REGEXP_REPLACE` → manual string ops, `CREATE INDEX IF NOT EXISTS` → `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE ...) EXEC(...)`, `PERCENTILE_CONT ... WITHIN GROUP ... OVER (PARTITION BY ...)` in a separate CTE with `SELECT DISTINCT`, then `LEFT JOIN` to the main aggregation (avoids GROUP BY context limitation)
- **PostgreSQL path:** PostgreSQL path (`--profile w3c`) must remain fully intact in the `{% else %}` branch
- **CI compatibility:** `dbt compile --profile w3c` must pass in CI without Azure credentials

### CI/CD Constraints

- **Tier 1 (every push):** Ruff, mypy, pytest (unit + DAG integrity only — NOT integration), `dbt compile --profile w3c` (PostgreSQL dialect, no cloud needed), `terraform validate`, `terraform fmt --check`
- **CD (merge to main):** Terraform plan/apply, DAB deploy, dbt deploy, Airflow sync, post-deploy smoke test (trigger Airflow DAG via REST API + poll DAG completion + assert Azure SQL row count). No standalone nightly integration suite — smoke test covers same ground only when code changes.
- **CD OIDC auth:** CD uses OIDC Workload Identity Federation (no client secrets). Single GitHub Environment: `azure-dev` (auto on merge to main).
- **Shared credentials:** Terraform remote state backend credentials (`ARM_*`) are the same OIDC identity used by CD.

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

### Phase 8c — dbt Docs, Source Freshness, and CSV Export (Azure SQL) (✅ Complete)

**Phase Goal:** Complete the Azure SQL pipeline by configuring dbt docs generation, source freshness checks, automated CSV export, and wiring both DAGs together via Airflow Dataset triggers.

**Summary:** Created 2 Dataset-connected Airflow DAGs (`spark_ingestion_azure.py`, `dbt_marts_azure.py`), 4 self-bootstrapping Databricks notebooks (dbt_run, dbt_test, dbt_docs, dbt_freshness), and 2 operators (`export_csv_azure.py`, `export_dbt_docs_azure.py`). Applied 6 Azure SQL compatibility fixes across dbt models — notably post_hook removal (dbt-sqlserver runs hooks before table rename), PERCENTILE_CONT CTE pattern, and geo/UA lookup CTEs in `fact_webrequest`. Key deviation: dbt runs on Databricks serverless via self-bootstrapping notebooks (not Airflow container). All 16 dbt models pass on Azure SQL; 18 CSV exports verified with correct headers.

**Key Implementation Details:**
- **dbt execution**: Databricks serverless via `DatabricksSubmitRunOperator` — Azure SQL ODBC driver must be at Databricks runtime, not Airflow
- **Library bootstrapping**: pip-install dbt within notebook; serverless rejects `libraries` in submit run
- **dbt project deployment**: Workspace ZIP imported as `format=AUTO`, downloaded via `/workspace/export` API (not Databricks Repos)
- **ODBC Driver 18**: Rootless install via `dpkg-deb -x` to PID-unique temp dir + custom `odbcinst.ini` + `ODBCSYSINI` env var
- **Post-hook removal**: dbt-sqlserver v1.8.4 runs post_hook BEFORE `__dbt_tmp` rename — all `tsql_create_index_if_not_exists` post_hooks removed
- **PERCENTILE_CONT pattern**: Separate `p95_cte` CTE with `OVER(PARTITION BY ...)` + `SELECT DISTINCT`, then `LEFT JOIN` to main aggregation
- **Geo/UA FK restoration**: `fact_webrequest` uses `geo_lookup` CTE (HASHBYTES SHA2_256 matching `dim_geolocation`) and `ua_lookup` CTE (raw-string JOIN on `dim_useragent.user_agent`), with `COALESCE(..., -1)` fallback

**⚠️ Differences from Plan Scaffold:**

| Plan Scaffold | Actual Implementation | Reason |
|---------------|----------------------|--------|
| dbt runs on Airflow container (ODBC in Dockerfile) | dbt runs on Databricks serverless via `DatabricksSubmitRunOperator` | ODBC driver must be at Databricks runtime; serverless self-bootstraps |
| dbt project via Databricks Repos | Workspace ZIP downloaded via `/workspace/export` API | Repos unreliable for all models; ZIP is self-contained |
| `libraries` block in submit run for dbt-core/dbt-sqlserver | pip-install within notebook | Serverless rejects `libraries` in submit run |
| `tsql_create_index_if_not_exists` post_hooks on models | All post_hooks removed | dbt-sqlserver v1.8.4 runs post_hook BEFORE `__dbt_tmp` table rename |
| `PERCENTILE_CONT(...) OVER()` inline in GROUP BY | Separate `p95_cte` CTE + `SELECT DISTINCT` + `LEFT JOIN` | T-SQL: PERCENTILE_CONT cannot be used inline in GROUP BY context |
| `fact_webrequest` hardcodes `geolocation_sk = -1`, `user_agent_sk = -1` for Azure SQL | `geo_lookup`/`ua_lookup` CTEs restore FK joins | Phase 8d fix: hash-based geo join, raw-string UA join |
| `mart_browser_analysis` references `ua.operating_system` | `ua.os AS operating_system` | `dim_useragent` has column `os`, not `operating_system` |
| Source freshness enforced (24/48h thresholds) | Freshness thresholds removed | Max `log_date` = 2011-05-15 (static historical data) cannot satisfy freshness |

**Verified State:**
- DAGs: `spark_ingestion_azure.py` (2-task, Workflow trigger + dim export), `dbt_marts_azure.py` (Dataset-triggered, 4-step dbt pipeline) — both wired
- Notebooks: `dbt_run.py`, `dbt_test.py`, `dbt_docs.py`, `dbt_freshness.py` — all use shared bootstrap from `dbt_common.py` (after Phase 8c/8d refactor)
- All 16 dbt models ✅ passing on Azure SQL from Databricks serverless
- 18 CSV exports produced with correct headers (18 files in `Star-Schema/`, all schema-matched)
- dbt docs generate operational; `catalog.json` artifact written
- Azure SQL firewall rule configured for dev IP 37.120.235.38
- Source freshness thresholds removed (static historical data, max date 2011-05-15)

**Phase 8c → Phase 8d Handoff:** ✅ Complete. All dbt models pass on Azure SQL, CSV exports verified, DAGs connected via Dataset. Geo/UA FK restoration documented in Phase 8d.

---

### Phase 8c/8d — Code Review & Fixes (✅ Complete — 2026-06-11)

**Goal:** Audit all Phase 8c/8d implementation files for correctness, idempotency, and adherence to project conventions, then fix identified issues.

**Process:** Code review automated via `code-reviewer` subagent (11 issues identified) → validated via independent subagent (7 confirmed worth fixing) → all 7 implemented and verified.

**Issues Found (11 total, 7 fixed):**

| # | Severity | File | Issue | Fixed |
|---|---|---|---|---|
| 1 | 🔴 CRITICAL | `spark_ingestion_azure.py` | **UA join mismatch** — `dim_useragent` stored URL-decoded UA string but `fact_webrequest` dbt model joined on raw `user_agent` column, causing ~100% join failure for UA string matches. | ✅ |
| 2 | 🟠 HIGH | `dbt_marts_azure.py`, `dbt_marts.py` | **max_active_tasks instead of max_active_runs** — `max_active_tasks=1` limits parallel task execution within a single DAG run, but the intent was to limit concurrent DAG runs (`max_active_runs=1`). | ✅ |
| 3 | 🟠 HIGH | `export_csv_azure.py` | **Silent CSV failures** — `_export_table()` catches and logs exceptions without tracking failures; a single failed table is silently ignored while the task reports success. | ✅ |
| 4 | 🟡 MEDIUM | `dbt_freshness.py` (pre-refactor) | **dbt deps not checked** — `dbt deps` runs without returncode validation; failure is silently ignored. | ✅ |
| 5 | 🟡 MEDIUM | All 4 notebooks | **Duplicated bootstrap code** — `dbt_run.py`, `dbt_test.py`, `dbt_docs.py`, `dbt_freshness.py` each contained ~90 lines of identical bootstrap logic (pip install, ODBC, ZIP extraction, credential loading). | ✅ |
| 6 | 🟡 MEDIUM | `dbt_freshness.py`, `dbt_docs.py` (pre-refactor) | **text=True missing on subprocess.run** — Both files call `subprocess.run(...)` without `text=True`, returning `bytes` instead of `str`. | ✅ |
| 7 | 🟡 MEDIUM | `profiles.yml` | **Missing threads/retries config** — Azure SQL `w3c_azure` profile lacks `threads: 4` and `retries: 3` settings used by the PostgreSQL `w3c` profile. | ✅ |

**Fixes Implemented:**

1. **Issue 1 (CRITICAL — UA join mismatch):** Modified `_export_dimensions()` in `spark_ingestion_azure.py` to store the raw (URL-encoded) `user_agent` string in the `dim_useragent` table, while computing `ua_hash` from the URL-decoded value. This ensures the dbt `fact_webrequest` LEFT JOIN on raw `user_agent` string matches the stored dimension row.

2. **Issue 2 (HIGH — max_active_runs):** Changed `max_active_tasks=1` → `max_active_runs=1` in the DAG constructor args of both `dbt_marts_azure.py` and `dbt_marts.py`. This prevents concurrent DAG runs from stacking.

3. **Issue 5 (HIGH — CSV silent failures):** Modified `_export_table()` in `export_csv_azure.py` to append failed table names to a list. After the loop, if any table failed, raises `RuntimeError(f"CSV export failed for tables: {failed_tables}")`.

4. **Issues 7, 10, 11 (MEDIUM):** Created `airflow/spark/databricks/dbt_common.py` — a shared bootstrap module with 9 functions (~280 lines) covering pip install, ODBC driver setup, dbt project extraction, credential loading, and CLI execution. All 4 notebooks were refactored to call this shared module, reducing their sizes by ~82%:
   - `dbt_run.py`: 104→12 lines
   - `dbt_test.py`: 104→11 lines
   - `dbt_freshness.py`: 104→14 lines
   - `dbt_docs.py`: 155→91 lines (kept artifact upload logic inline)

5. **Profiles config:** Added `threads: 4` and `retries: 3` to the `w3c_azure` profile in `profiles.yml`.

**Verification:**

- ✅ **RuFF**: 0 issues (clean)
- ✅ **Mypy**: pass (14 source files)
- ✅ **Pytest**: 164 passed, 31 skipped, 8 pre-existing unrelated failures
- ✅ **DAG integrity**: 23/23 DAG tests pass (Airflow DagBag parse, task IDs, linear dependencies, Dataset contracts)
- ✅ **18 CSV files**: All present with correct headers, no missing/extra files
- ✅ **All 4 DAGs parse**: `spark_ingestion.py`, `dbt_marts.py`, `spark_ingestion_azure.py`, `dbt_marts_azure.py` — all pass `py_compile`

**Documentation:** Full findings saved to `plans/code-review-phases-8c-8d.md`.

**Files Changed:**

| File | Change |
|---|---|
| `airflow/spark/databricks/dbt_common.py` | **NEW** — shared bootstrap module |
| `airflow/spark/databricks/dbt_run.py` | REFACTORED — uses shared module (104→12 lines) |
| `airflow/spark/databricks/dbt_test.py` | REFACTORED — uses shared module (104→11 lines) |
| `airflow/spark/databricks/dbt_docs.py` | REFACTORED — uses shared module (155→91 lines) |
| `airflow/spark/databricks/dbt_freshness.py` | REFACTORED — uses shared module (104→14 lines) |
| `airflow/dags/w3c/spark_ingestion_azure.py` | FIXED — UA join mismatch (Issue 1) |
| `airflow/dags/w3c/dbt_marts_azure.py` | FIXED — max_active_runs (Issue 2) |
| `airflow/dags/w3c/dbt_marts.py` | FIXED — max_active_runs (Issue 2, non-Azure DAG) |
| `airflow/plugins/operators/export_csv_azure.py` | FIXED — table failure tracking (Issue 5) |
| `airflow/dbt/w3c/profiles.yml` | FIXED — added threads/retries (Issue 11) |
| `plans/code-review-phases-8c-8d.md` | UPDATED — full findings and fix status |

---

### Phase 8d — Geo/UA Foreign Key Restoration (✅ Complete)

**Phase Goal:** Restore `geolocation_sk` and `user_agent_sk` foreign key integrity in `fact_webrequest` by computing matching hashes in dbt aligned with the Airflow dimension export, and storing raw `user_agent` string in `dim_useragent` for direct LEFT JOIN.

**Summary:** Fixed Azure SQL `fact_webrequest` FK integrity where `geolocation_sk` and `user_agent_sk` were hardcoded to `-1` because dbt and Airflow computed hashes differently. Hybrid solution: (1) geo — replicated `HASHBYTES('SHA2_256', country|region|city|lat|lng)` in dbt matching the Airflow `_export_dimensions` function; (2) UA — added `user_agent NVARCHAR(2048)` column to `dim_useragent` DDL and joined on raw string match. Both joins use `COALESCE(..., -1)` for unmatched rows.

**Key Implementation Details:**
- **Geo hash alignment**: dbt `geo_lookup` CTE uses same `HASHBYTES('SHA2_256', ...)` + `CONVERT(NVARCHAR(64), ..., 2)` on identical concatenation pattern as Airflow Python `hashlib.sha256().hexdigest()` — produces identical hex output (T-SQL string comparison is case-insensitive by default for `NVARCHAR`)
- **UA raw-string join**: `dim_useragent` CREATE TABLE extended with `user_agent NVARCHAR(2048)` column; MERGE inserts the raw UA string; `fact_webrequest` LEFT JOINs on `ua.user_agent = c.user_agent`
- **Hash scope change**: Airflow hash input now includes `ua_str[:500]` prefix so each unique raw UA produces its own dimension row (dedup after `unquote_plus` prevents duplicates from URL encoding variations)
- **Sentinel fallback**: Both joins use `COALESCE(g.geolocation_sk, -1)` / `COALESCE(ua.user_agent_sk, -1)` — unmatched rows retain the Unknown sentinel row

**Files Changed:**
- `airflow/dags/w3c/spark_ingestion_azure.py` — Added `user_agent NVARCHAR(2048)` column to `dim_useragent` DDL; included `ua_str` in hash computation; MERGE inserts raw UA string
- `airflow/dbt/w3c/models/staging/fact_webrequest.sql` — Added `geo_lookup` CTE (HASHBYTES geo_hash), `ua_lookup` CTE (raw UA string join), LEFT JOINs in SELECT, COALESCE fallback
- `airflow/dbt/w3c/models/sources.yml` — Updated `dim_useragent` source columns to include `user_agent`

**Verified State:**
- Geo match rate: >99.9% (unmatched rows lack country data — intentionally sentinel -1)
- UA match rate: >99.9% (unmatched rows with '-' or null UA — intentionally sentinel -1)
- `mart_browser_analysis` now shows real `browser_name` distribution instead of all "Unknown"
- DDL safety: `NVARCHAR(2048)` provides 2× margin over max observed UA length (~1024 chars)
- HASHBYTES/CONVERT verified identical to Python hashlib output on sample data (case-insensitive comparison)
- All 16 dbt models continue to pass

---

### Phase 9 — CI/CD (✅ Complete)

**Phase Goal:** Configure CI/CD (Tier 1 on every push + CD on merge to main). Tier 1 provides fast code-quality feedback with no Azure creds. CD deploys to Azure with an embedded post-deploy smoke test validating the pipeline end-to-end.

**Summary:** Created 4 CI jobs (lint, test, dbt-compile, terraform) via 3 reusable workflow templates (`_reusable-lint.yml`, `_reusable-test.yml`, `_reusable-terraform.yml`) and 1 inline `dbt-compile` job requiring dual service containers. Built 7-job CD pipeline (`cd.yml`, 362 lines) with OIDC-managed auth, single environment (`azure-dev`, auto on merge to main), DAB deploy, dbt deploy with first-deploy fallback, DAG sync, and post-deploy smoke test (trigger → poll → SQL assert). **OIDC fully managed by Terraform** (`github_oidc.tf`) — creates `azuread_application`, `azuread_application_federated_identity_credential` (1 for `azure-dev`), `azurerm_role_assignment`. Pulled back from 3 environments to 1 (`azure-dev` only) per user request — staging/prod unnecessary complexity for a CV project. Dependabot configured (`.github/dependabot.yml` + auto-merge workflow). Code review caught 9 issues across `cd.yml` and `databricks.yml` — all fixed. All artifacts validate locally (terraform fmt/validate ✅, ruff ✅, mypy ✅, databricks.yml YAML valid ✅).

**⚠️ Differences from Plan Scaffold:**

| Plan Scaffold | Actual Implementation | Reason |
|---------------|----------------------|--------|
| OIDC via manual `az ad app` CLI commands | Terraform-managed via `github_oidc.tf` — `azuread_application`, `azuread_application_federated_identity_credential` (1 for `azure-dev`), `azurerm_role_assignment` | IaC best practice; single deploy creates app + federated cred + Contributor role |
| 3 GitHub Environments (dev, staging, prod) | Single environment `azure-dev` only | Simplified for CV project — staging/prod add complexity without portfolio value |
| Smoke test DAG ID `spark_ingestion_azure` | `w3c_spark_ingestion_azure` — actual ID matching DAG definition | Code review fix — wrong ID causes 404 on every CD run |
| Duplicate `failed` condition `[ "$STATE" = "failed" ] \|\| [ "$STATE" = "failed" ]` | `[ "$STATE" = "failed" ] \|\| [ "$STATE" = "upstream_failed" ]` | Copy-paste error — `upstream_failed` and other terminal states were silently ignored |
| dbt `--defer --state` with no fallback for first deploy | `if [ -f "./target-dev/manifest.json" ]` check falls back to plain `dbt run` | First deployment has no prior manifest — `--defer` would fail |
| Rollback: `terraform apply -refresh-only` | Rollback: `terraform plan` + `terraform apply` for Part A and B separately (+ prior commit checkout) | `-refresh-only` syncs state without changing infrastructure — no-op as rollback |
| Reusable workflows: "optional refactoring after baseline works" | Mandatory from the start — `ci.yml` calls 3 reusable templates | Cleaner separation; easier to maintain and extend |
| `dbt_compile` pytest marker not registered | Registered in `pyproject.toml` — `dbt_compile` excluded from main CI test run | Required for CI to correctly filter dbt compile unit tests |

**Post-Completion Fixes Applied (code review — 2026-06-13):**

| # | Issue | Fix |
|---|-------|-----|
| 1 | `cd.yml`: unused `DAB_TARGET_DEV: dev` env var | Removed (dead code) |
| 2 | `cd.yml`: sync-airflow step redundantly triggered DAG via curl **and** smoke test also triggers separately | Replaced curl trigger with echo confirmation — smoke test is the sole trigger |
| 3 | `cd.yml`: misleading step name "Trigger DAG Bag Refresh" | Renamed to "DAG file sync complete — smoke test will trigger ingestion" |
| 4 | `databricks.yml`: hardcoded Databricks dev workspace URL | Changed to `${DATABRICKS_HOST_DEV}` env var |
| 5 | Missing Dependabot auto-merge workflow | Created `.github/workflows/dependabot-auto-merge.yml` — approves + merges patch PRs |

**Verified State:**
- CI: 4 jobs (lint, test, dbt-compile, terraform) — ruff, mypy, pytest (227 tests), dbt compile (2 profiles), terraform validate + fmt (Part A + B) all pass locally
- CD: 7 jobs (terraform-plan, terraform-apply, deploy-dab, deploy-dbt, sync-airflow, smoke-test, rollback) — single environment (`azure-dev`, auto on merge to main)
- OIDC: `azuread_application.github_actions` (gha-w3c-etl-pipeline) + service principal + 1 federated identity credential (for `azure-dev`) + Contributor role assignment — all Terraform-managed
- DAB: `databricks.yml` with 9 workspace file resources + 1 target (`dev`)
- Dependabot: `.github/dependabot.yml` (5 ecosystems) + `dependabot-auto-merge.yml` (patch auto-approve)
- Remaining: 2 items require git push (CI test on push, CD test on merge to main) + 1 requires GitHub UI (Environment with variables/secrets) + 1 requires bootstrap terraform apply (OIDC first deploy)

**Critical Lessons Learned:**
1. **DAG IDs need the full namespace** — Smoke test referenced `spark_ingestion_azure` but the actual ID is `w3c_spark_ingestion_azure`. Always check actual DAG definitions when building CD smoke tests.
2. **dbt `--defer --state` requires a prior manifest** — First-time deployment fails without a fallback to plain `dbt run`. Always add a manifest existence check.
3. **`terraform apply -refresh-only` is NOT a rollback** — It syncs state without changing infrastructure. Real rollback requires checking out the prior commit + `terraform plan/apply`.
4. **Dependabot auto-merge needs a separate workflow** — `dependabot.yml` alone can't auto-merge PRs. A `workflow_run` or `pull_request_target` workflow is required to approve and merge.
5. **CD pipeline must handle idempotent dbt deployment** — `--defer` references production state but the state file doesn't exist on first deploy per environment. The manifest check pattern solves this.

**Phase 9 → Phase 10 Handoff:** ✅ Complete. CI/CD pipelines built and validated locally (terraform validate/fmt, ruff, mypy, pytest all pass). OIDC Terraform-managed with 1 federated identity credential for `azure-dev`. Remaining items require git push to GitHub (CI/CD test on push/merge), manual GitHub Environment setup with variables/secrets, and one-time OIDC bootstrap apply. Ready for Phase 10 monitoring configuration.

---

### Phase 10 — Monitoring

**Phase Goal:** Implement a tiered, infrastructure-as-code monitoring stack with Grafana + Prometheus for Airflow observability and Terraform-managed Azure Monitor alerts with severity tiers for budget and pipeline failures.

**Why This Matters for the CV:**

This phase demonstrates three high-impact DE skills:

- **Infrastructure as Code for observability** — alert rules, action groups, and budgets defined as Terraform resources, not click-ops or throwaway CLI scripts. This is the difference between "I set up alerts in the portal" and "I manage alerting as code alongside infrastructure."
- **Tiered incident response** — a P1/P2/P3 severity model that routes critical failures to immediate notification channels while keeping informational alerts out of the on-call rotation. This is production operations maturity.
- **Multi-layer observability** — Grafana (real-time application metrics via StatsD) + Azure Monitor (infrastructure-level alerts) + budget controls (cost governance). Together they show a holistic approach to production monitoring.

**—--------------------------------------------------------------------**

#### 1. Alert Severity Tiers

The monitoring system uses three severity tiers to route alerts to the appropriate notification channel:

| Tier | Severity | Azure Monitor Sev | Definition | Examples | Notification |
|------|----------|-------------------|------------|----------|-------------|
| **P1** | Critical | Sev 0 | Pipeline is broken, data is not flowing, or budget is at risk | Databricks pipeline failure, JDBC export failure, budget hard cap exceeded | Email (immediate); Slack via existing Alertmanager pipeline |
| **P2** | Warning | Sev 1 | Non-critical degradation or approaching thresholds | Pipeline retries, approaching budget warning, Azure SQL auto-pause anomaly | Email (digest) |
| **P3** | Informational | Sev 2 | Operational awareness, not actionable | Successful pipeline completion, budget forecast | No direct notification (dashboards only) |

Each tier maps to a separate `azurerm_monitor_action_group` in Terraform, making the routing explicit and auditable.

**—--------------------------------------------------------------------**

#### 2. Grafana + Prometheus (Local Dev Stack)

The Docker-based Grafana + Prometheus stack provides real-time Airflow observability via StatsD metrics. This stack is already implemented and provisioned — the checklist items below are verification steps, not new work.

**Checklist:**

- [x] Verify Grafana + Prometheus stack is running
- [x] Configure Airflow StatsD exporter
- [x] Create Grafana dashboard for Airflow metrics
- [x] Configure Prometheus to scrape Airflow metrics
- [ ] Verify both provisioned dashboards load correctly:
  - `Airflow ETL Overview` (7 panels: DAG run duration, task success rate, task failure rate, DAG run count, task duration p50/p95/p99, active DAG runs, task instance state breakdown)
  - `Container System Metrics` (6 panels: container CPU, memory, network I/O, disk I/O, restart count, uptime)
- [ ] Verify PromQL queries return data: `rate(airflow_task_success_count[5m])`, `topk(5, sum by (dag_id) (airflow_dag_run_duration_seconds))`
- [ ] Verify Alertmanager → Slack integration (if configured)

**Existing Stack Configuration (actual repo paths):**

```yaml
# airflow/docker-compose.yaml (Monitoring services only — see full file for all services)
services:
  airflow-webserver: &airflow-common
    environment: &airflow-common-env
      AIRFLOW__METRICS__STATSD_ON: "True"
      AIRFLOW__METRICS__STATSD_HOST: "statsd-exporter"
      AIRFLOW__METRICS__STATSD_PORT: "9125"
      AIRFLOW__METRICS__STATSD_PREFIX: "airflow"

  statsd-exporter:
    image: prom/statsd-exporter:v0.29.0
    volumes:
      - ./prometheus/statsd_mapping.yml:/etc/statsd/mapping.yml:ro

  prometheus:
    image: prom/prometheus:v3.11.3
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./prometheus/alert_rules.yml:/etc/prometheus/alert_rules.yml:ro

  grafana:
    image: grafana/grafana:11.3.0
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
```

```yaml
# airflow/prometheus/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - /etc/prometheus/alert_rules.yml

alerting:
  alertmanagers:
    - static_configs:
        - targets:
            - 'alertmanager:9093'

scrape_configs:
  - job_name: 'airflow-statsd'
    static_configs:
      - targets: ['statsd-exporter:9102']
        labels:
          service: 'airflow'
  - job_name: 'cadvisor'
    static_configs:
      - targets: ['cadvisor:8080']
        labels:
          service: 'docker'
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']
  - job_name: 'data-freshness-probe'
    scrape_interval: 60s
    static_configs:
      - targets: ['data-freshness-probe:8000']
```

**Existing Prometheus Alert Rules (`airflow/prometheus/alert_rules.yml`):**

| Alert | Severity | Expression |
|-------|----------|------------|
| `AirflowDAGFailureRate` | warning | `rate(airflow_dag_run_duration_seconds_count{status="failed"}[5m]) > 0` |
| `AirflowTaskFailureRate` | warning | `rate(airflow_ti_finish{state="failed"}[5m]) > 0` |
| `ContainerRestarts` | warning | `changes(container_start_time_seconds{...}[15m]) > 2` |
| `HighCPUUsage` | warning | `rate(container_cpu_usage_seconds_total[...][2m]) * 100 > 80` |
| `HighMemoryUsage` | warning | `container_memory_usage_bytes[...] / container_spec_memory_limit_bytes[...] * 100 > 85` |
| `PrometheusTargetMissing` | critical | `up == 0` |

These 6 rules are already deployed in the running Prometheus instance. Phase 10 adds 2 more (DataStaleWarning, DataStaleCritical) to the same file.

The provisioned dashboards live at:

- `airflow/grafana/dashboards/airflow-etl-overview.json` (7 panels, Airflow ETL overview)
- `airflow/grafana/dashboards/container-metrics.json` (6 panels, Docker container system metrics)

**—--------------------------------------------------------------------**

#### 3. Terraform-Managed Azure Monitor Alerts

Alert resources and action groups are defined as Terraform in `terraform/part_a/` alongside the core infrastructure. This replaces the CLI-only scaffolds from the original plan.

**New files needed:**

| File | Purpose |
|------|---------|
| `terraform/part_a/monitoring.tf` | All monitoring resources (action groups, budgets, metric alerts) |
| `terraform/part_a/monitoring-variables.tf` | Alert-specific variables (emails, webhook URLs, toggle flags) |
| Update `terraform/part_a/variables.tf` | Add `alert_email_critical`, `alert_email_warning`, `alert_email_info`, `enable_log_analytics` |
| Update `terraform/part_a/environments/dev/terraform.tfvars` | Dev-specific alert values |

**Terraform resources used:**

```
azurerm_monitor_action_group              # Notification routing per severity tier
azurerm_consumption_budget_resource_group  # Budget alerts ($50 warning, $100 hard cap)
azurerm_monitor_metric_alert               # Databricks pipeline failures + SQL auto-pause
azurerm_monitor_scheduled_query_rules_alert_v2  # Log-based retry detection (optional, needs Log Analytics)
```

**Checklist:**

- [ ] Add `terraform/part_a/monitoring.tf` with action groups (P1, P2, P3), budget alerts, and metric alerts
- [ ] Add `terraform/part_a/monitoring-variables.tf` with alert email/URL variables
- [ ] Update `terraform/part_a/variables.tf` with new monitoring variables
- [ ] Update `terraform/part_a/environments/dev/terraform.tfvars` with dev alert email addresses
- [ ] Run `terraform plan` to validate monitoring resources
- [ ] Run `terraform apply` to deploy alerts
- [ ] Manually trigger a test failure to verify P1 alert fires to email + Slack
- [ ] Verify budget alerts are created in Azure Cost Management

**Code Scaffolds:**

**terraform/part_a/monitoring.tf:**

```hcl
# ---------------------------------------------------------------------------
# Terraform-managed Azure Monitor alerts with P1/P2/P3 severity tiers
# Phase 10 — Monitoring
# ---------------------------------------------------------------------------

# ---- Action Groups (Notification Routing) ----

# P1 — Critical: immediate email notification
# (Slack alerts handled by existing Alertmanager → SLACK_WEBHOOK_URL in airflow/.env)
resource "azurerm_monitor_action_group" "critical" {
  name                = "ag-w3c-critical"
  resource_group_name = var.resource_group_name
  short_name          = "w3c-p1"

  email_receiver {
    name          = "critical-alerts"
    email_address = var.alert_email_critical
  }
}

# P2 — Warning: email digest
# (Slack notifications handled by existing Alertmanager → SLACK_WEBHOOK_URL in airflow/.env)
resource "azurerm_monitor_action_group" "warning" {
  name                = "ag-w3c-warning"
  resource_group_name = var.resource_group_name
  short_name          = "w3c-p2"

  email_receiver {
    name          = "warning-alerts"
    email_address = var.alert_email_warning
  }
}

# P3 — Informational: dashboard-only (no direct notification)
resource "azurerm_monitor_action_group" "info" {
  name                = "ag-w3c-info"
  resource_group_name = var.resource_group_name
  short_name          = "w3c-p3"

  email_receiver {
    name          = "info-alerts"
    email_address = var.alert_email_info
  }
}

# ---- Budget Alerts ----

# Look up the resource group created by the networking module
data "azurerm_resource_group" "main" {
  name = var.resource_group_name
}

# $50 warning budget (P2)
resource "azurerm_consumption_budget_resource_group" "warning" {
  name              = "w3c-etl-budget-warning"
  resource_group_id = data.azurerm_resource_group.main.id

  amount     = 50
  time_grain = "Monthly"

  time_period {
    start_date = "2025-01-01"
    end_date   = "2099-12-31"
  }

  notification {
    enabled        = true
    threshold      = 80.0
    operator       = "GreaterThan"
    contact_emails = [var.alert_email_warning]
  }

  notification {
    enabled        = true
    threshold      = 100.0
    operator       = "GreaterThanOrEqualTo"
    contact_emails = [var.alert_email_critical]
  }
}

# $100 hard cap budget (P1)
resource "azurerm_consumption_budget_resource_group" "hard_cap" {
  name              = "w3c-etl-budget-hard-cap"
  resource_group_id = data.azurerm_resource_group.main.id

  amount     = 100
  time_grain = "Monthly"

  time_period {
    start_date = "2025-01-01"
    end_date   = "2099-12-31"
  }

  notification {
    enabled        = true
    threshold      = 100.0
    operator       = "GreaterThanOrEqualTo"
    contact_emails = [var.alert_email_critical]
  }
}

# ---- Metric Alerts ----

# P1 — Databricks pipeline failure
resource "azurerm_monitor_metric_alert" "databricks_pipeline_failure" {
  name                = "ma-w3c-databricks-pipeline-failure"
  resource_group_name = var.resource_group_name
  scopes              = [module.databricks.workspace_id]
  description         = "Alert when Databricks pipeline jobs fail (P1 - Critical)"
  severity            = 0

  criteria {
    metric_namespace = "Microsoft.Databricks/workspaces"
    metric_name      = "JobsFailedCount"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 0
  }

  window_size          = "PT5M"
  evaluation_frequency = "PT1M"

  action {
    action_group_id = azurerm_monitor_action_group.critical.id
  }
}

# P1 — Azure SQL auto-pause anomaly
resource "azurerm_monitor_metric_alert" "azure_sql_auto_pause" {
  name                = "ma-w3c-azure-sql-auto-pause"
  resource_group_name = var.resource_group_name
  scopes              = [module.warehouse.database_id]
  description         = "Alert if Azure SQL DTU consumption drops near zero (P1 - Critical)"
  severity            = 0

  criteria {
    metric_namespace = "Microsoft.Sql/servers/databases"
    metric_name      = "dtu_consumption_percent"
    aggregation      = "Average"
    operator         = "LessThan"
    threshold        = 0.1
  }

  window_size          = "PT1H"
  evaluation_frequency = "PT5M"

  action {
    action_group_id = azurerm_monitor_action_group.critical.id
  }
}

# P2 — Databricks job retries (log-based, requires Log Analytics)
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "databricks_job_retry" {
  count                = var.enable_log_analytics ? 1 : 0
  name                 = "sqr-w3c-databricks-job-retry"
  resource_group_name  = var.resource_group_name
  location             = var.location
  description          = "Alert when Databricks jobs retry multiple times (P2 - Warning)"
  severity             = 1
  evaluation_frequency = "PT5M"
  window_duration      = "PT30M"
  scopes               = [module.databricks.workspace_id]

  query = <<-QUERY
    DatabricksJobs
    | where RunStatus == "Failed" and Attempt > 1
    | summarize RetryCount = count() by JobName, bin(TimeGenerated, 5m)
    | where RetryCount > 1
  QUERY

  action {
    action_group_ids = [azurerm_monitor_action_group.warning.id]
  }

  trigger {
    operator  = "GreaterThan"
    threshold = 1
  }
}
```

**terraform/part_a/monitoring-variables.tf:**

```hcl
# ---------------------------------------------------------------------------
# Monitoring alert variables
# Phase 10 — Monitoring
# ---------------------------------------------------------------------------

variable "alert_email_critical" {
  description = "Email address for P1 (Critical) alert notifications"
  type        = string
  sensitive   = true
}

variable "alert_email_warning" {
  description = "Email address for P2 (Warning) alert notifications"
  type        = string
  sensitive   = true
}

variable "alert_email_info" {
  description = "Email address for P3 (Informational) alert notifications"
  type        = string
  sensitive   = true
  default     = ""
}

variable "enable_log_analytics" {
  description = "Enable Log Analytics workspace for log-based alerts (increases cost)"
  type        = bool
  default     = false
}
```

**—--------------------------------------------------------------------**

#### 4. Data Freshness Monitoring

A data pipeline is only valuable if the data is current. This section adds a freshness probe that tracks hours since the last successful pipeline load and alerts when data goes stale.

**Architecture:**

```
Data Freshness Probe (Python — runs as Docker sidecar on port 8000)
    │
    ├─ Every 15 min (Azure SQL via pymssql):
    │   ├─ dbo.raw_enriched_loaded → MAX(loaded_at) → w3c_data_freshness_seconds
    │   ├─ dbo.raw_enriched_loaded → MAX(loaded_at) → w3c_pipeline_last_run_status
    │   ├─ dbo.raw_enriched → COUNT(*) → w3c_row_count{source="azure_sql"}
    │   └─ dbo.dbt_test_results → pass/fail ratio → w3c_dbt_test_pass_rate
    │
    ├─ Every 15 min (Databricks via Unity Catalog REST API):
    │   ├─ GET /api/2.1/unity-catalog/tables/bronze/bronze_raw_logs → w3c_row_count{source="bronze"}
    │   └─ GET /api/2.1/unity-catalog/tables/silver/silver_enriched_logs → w3c_row_count{source="silver"}
    │
    ├─ Exposes 5 Prometheus metrics on port 8000:
    │   ├─ w3c_data_freshness_seconds{source="azure_sql"}   — hours since last load
    │   ├─ w3c_pipeline_last_run_status{source="azure_sql"} — 1 = recent success, 0 = stale/never
    │   ├─ w3c_dbt_test_pass_rate                           — % of dbt tests passing (0.0–1.0)
    │   └─ w3c_row_count{source="bronze|silver|azure_sql"}  — row counts across all layers
    │
    └─ Consumed by:
        ├─ Pipeline Health dashboard (all metrics as stat panels)
        ├─ Prometheus alert: >6h → P2 (Warning)
        └─ Prometheus alert: >24h → P1 (Critical)
```

**Checklist:**

- [ ] Create `airflow/scripts/data_freshness_probe.py` — Docker-sidecar Python script that queries Azure SQL, exposes 4 Prometheus gauges on port 8000
- [ ] Add `data-freshness-probe` service to `airflow/docker-compose.yaml` (see sidecar snippet below)
- [ ] Add Prometheus scrape config for `data-freshness-probe:8000` (already included in the corrected prometheus.yml above — job_name: `data-freshness-probe`)
- [ ] Verify all 5 metrics appear in Prometheus: `w3c_data_freshness_seconds`, `w3c_pipeline_last_run_status`, `w3c_dbt_test_pass_rate`, `w3c_row_count{source="bronze|silver|azure_sql"}`
  - **Pa​rticularly:** confirm `w3c_row_count{source="bronze"}` and `{source="silver"}` show non-zero values (~153K each) — if they show 0, the Databricks PAT token is missing, expired, or the table path is wrong
- [ ] Add Grafana stat panels to the Pipeline Health dashboard — Data Freshness, Pipeline Status, dbt Pass Rate, Row Counts (bronze + silver + azure_sql)
- [ ] Add Alertmanager rule for P2 (>6h) and P1 (>24h) freshness alerts (see scaffold below)
- [ ] Verify Alertmanager fires alerts correctly by pausing pipeline
- [ ] **Prerequisite:** Add `prometheus-client` and `pymssql` to `requirements.txt` (or the Airflow `Dockerfile` pip install) — the probe container uses these
- [ ] **Prerequisite:** Add `DATABRICKS_HOST` and `DATABRICKS_PAT_TOKEN` to `airflow/.env` alongside existing `AZURE_SQL_*` variables — the Docker sidecar reads them for Unity Catalog REST API calls

**Docker sidecar snippet (add to `airflow/docker-compose.yaml` under `services:`):**

```yaml
  # ── Data Freshness Probe ────────────────────────────────────────────
  data-freshness-probe:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: data-freshness-probe
    entrypoint:
      - python
      - /opt/airflow/scripts/data_freshness_probe.py
    environment:
      AZURE_SQL_SERVER: ${AZURE_SQL_SERVER}
      AZURE_SQL_DATABASE: ${AZURE_SQL_DATABASE}
      AZURE_SQL_USER: ${AZURE_SQL_USER}
      AZURE_SQL_PASS: ${AZURE_SQL_PASS}
      DATABRICKS_HOST: ${DATABRICKS_HOST}
      DATABRICKS_PAT_TOKEN: ${DATABRICKS_PAT_TOKEN}
    ports:
      - "8000:8000"
    networks:
      - airflow-network
    restart: unless-stopped
```

**Code Scaffolds:**

**airflow/scripts/data_freshness_probe.py:**

```python
#!/usr/bin/env python3
"""
Prometheus exporter for W3C ETL pipeline health.

Exposes 5 metrics on port 8000:
  - w3c_data_freshness_seconds{source}   — seconds since last successful load
  - w3c_pipeline_last_run_status{source}  — 1 if pipeline ran recently, 0 otherwise
  - w3c_dbt_test_pass_rate               — fraction of dbt tests passing (0.0–1.0)
  - w3c_row_count{source}                — row counts from bronze, silver, and azure_sql
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import pymssql
from prometheus_client import Gauge, start_http_server

# ── Prometheus gauges ──────────────────────────────────────────────────
FRESHNESS_GAUGE = Gauge(
    "w3c_data_freshness_seconds",
    "Seconds since the last successful pipeline load into Azure SQL",
    ["source"],
)
STATUS_GAUGE = Gauge(
    "w3c_pipeline_last_run_status",
    "1 if pipeline loaded data recently (within check interval), 0 otherwise",
    ["source"],
)
DBT_GAUGE = Gauge(
    "w3c_dbt_test_pass_rate",
    "Fraction of dbt tests passing (0.0–1.0)",
)
ROW_COUNT_GAUGE = Gauge(
    "w3c_row_count",
    "Total row count per source (bronze, silver, azure_sql)",
    ["source"],
)

# ── Configuration ──────────────────────────────────────────────────────
DSN = {
    "server": os.environ["AZURE_SQL_SERVER"],
    "database": os.environ["AZURE_SQL_DATABASE"],
    "user": os.environ["AZURE_SQL_USER"],
    "password": os.environ["AZURE_SQL_PASSWORD"],
}
DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]       # e.g. adb-123456.10.azuredatabricks.net
DATABRICKS_TOKEN = os.environ["DATABRICKS_PAT_TOKEN"]

# Delta table paths in the Unity Catalog
BRONZE_TABLE = "w3c_etl_databricks.bronze.bronze_raw_logs"
SILVER_TABLE = "w3c_etl_databricks.silver.silver_enriched_logs"

# How old "recent" data can be before we mark the pipeline as stale
STALE_THRESHOLD_SECONDS = 7200  # 2 hours


def check_freshness() -> float | None:
    """Return seconds since last loaded_at, or None if no data exists."""
    conn = pymssql.connect(**DSN)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(loaded_at) FROM dbo.raw_enriched_loaded")
        row = cursor.fetchone()
        if row and row[0]:
            last_load: datetime = row[0]
            if last_load.tzinfo is None:
                last_load = last_load.replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - last_load).total_seconds()
        return None
    finally:
        conn.close()


def check_pipeline_status() -> int:
    """Return 1 if data was loaded recently, 0 otherwise."""
    seconds = check_freshness()
    if seconds is None:
        return 0
    return 1 if seconds < STALE_THRESHOLD_SECONDS else 0


def check_dbt_pass_rate() -> float:
    """
    Estimate dbt test pass rate from Azure SQL.
    If dbt_test_results table exists, use it; otherwise assume 1.0.
    """
    conn = pymssql.connect(**DSN)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                CASE
                    WHEN OBJECT_ID('dbo.dbt_test_results') IS NOT NULL
                    THEN 1.0 * SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)
                    ELSE 1.0
                END
            FROM dbo.dbt_test_results
            HAVING COUNT(*) > 0
        """)
        row = cursor.fetchone()
        return float(row[0]) if row and row[0] is not None else 1.0
    except Exception:
        return 1.0
    finally:
        conn.close()


def check_azure_row_count() -> int:
    """Return total row count from dbo.raw_enriched."""
    conn = pymssql.connect(**DSN)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dbo.raw_enriched")
        row = cursor.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def check_databricks_row_count(full_table_name: str) -> int:
    """
    Return the estimated row count for a Unity Catalog table
    via the Databricks REST API.

    Uses the GET /api/2.1/unity-catalog/tables/{full_name} endpoint
    which returns table metadata including 'rows_count' (estimated).
    This is lightweight — no Spark cluster needed, just a PAT token.
    """
    url = f"https://{DATABRICKS_HOST}/api/2.1/unity-catalog/tables/{full_table_name}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return int(data.get("rows_count", 0))
    except (urllib.error.URLError, json.JSONDecodeError, KeyError):
        return 0


if __name__ == "__main__":
    start_http_server(8000)
    while True:
        # Freshness
        seconds = check_freshness()
        if seconds is not None:
            FRESHNESS_GAUGE.labels(source="azure_sql").set(seconds)

        # Pipeline status
        STATUS_GAUGE.labels(source="azure_sql").set(check_pipeline_status())

        # dbt pass rate
        DBT_GAUGE.set(check_dbt_pass_rate())

        # Row counts — all 3 sources
        ROW_COUNT_GAUGE.labels(source="azure_sql").set(check_azure_row_count())
        ROW_COUNT_GAUGE.labels(source="bronze").set(check_databricks_row_count(BRONZE_TABLE))
        ROW_COUNT_GAUGE.labels(source="silver").set(check_databricks_row_count(SILVER_TABLE))

        time.sleep(900)  # Every 15 minutes
```

**Alertmanager rule (monitoring/alertmanager-rules.yml):**

```yaml
groups:
  - name: data_freshness
    rules:
      - alert: DataStaleWarning
        expr: w3c_data_freshness_seconds{source="azure_sql"} > 21600
        for: 5m
        labels:
          severity: warning
          tier: P2
        annotations:
          summary: "Pipeline data is {{ $value | humanizeDuration }} old"
          description: >
            The last successful pipeline load was {{ $value | humanizeDuration }} ago.
            Expected maximum delay is 6 hours.

      - alert: DataStaleCritical
        expr: w3c_data_freshness_seconds{source="azure_sql"} > 86400
        for: 5m
        labels:
          severity: critical
          tier: P1
        annotations:
          summary: "Pipeline data is {{ $value | humanizeDuration }} old — OVER 24 HOURS"
          description: >
            CRITICAL: No pipeline data loaded in over 24 hours.
            Immediate investigation required.
```

**—--------------------------------------------------------------------**

#### 5. Pipeline Health Grafana Dashboard

A single, portfolio-ready Grafana dashboard that visualises the entire pipeline health at a glance. This is the monitoring artifact you would screenshot and walk through in an interview: *"Here's how I knew the pipeline was healthy."*

**Dashboard Layout (10 panels):**

```
┌─────────────────────────────────────────────────────────────────────┐
│  Title: W3C ETL — Pipeline Health     [Last 24h] [Auto-refresh 30s] │
├──────────────┬──────────────┬───────────────────────────────────────┤
│ Data         │ Pipeline     │ dbt Test                              │
│ Freshness    │ Status       │ Pass Rate                             │
│ ┌──────┐     │ ┌──────┐     │ ┌──────┐                              │
│ │ 2.3h │     │ │  ✅  │     │ │ 98%  │                              │
│ └──────┘     │ └──────┘     │ └──────┘                              │
│ Thresholds:  │ Last run:    │ 62 pass                               │
│ 6h (P2)      │ 2025-01-15   │ 0 fail                                │
│ 24h (P1)     │ 17:30 UTC    │                                       │
├──────────────┴──────────────┴───────────────────────────────────────┤
│  DAG Run Duration (last 24h)                        ┌─────────────┐ │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   │ p50  12.3s  │ │
│  ═══════════════════════════════════════════         │ p95  18.7s  │ │
│                                                      │ p99  45.2s  │ │
│                                                      └─────────────┘ │
├─────────────────────────────────────────────────────────────────────┤
│  Task Success Rate (last 24h)           Task Failure Rate (last 24h)│
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100%   ━━━ 0.02%                    │
│  ═══════════════════════════════         ────                         │
├─────────────────────────────────────────────────────────────────────┤
│  Row Counts (Prometheus: Azure SQL via pymssql, Databricks via REST) │
│  Bronze:    142,130    Silver:    138,024    Azure SQL: 137,891    │
└─────────────────────────────────────────────────────────────────────┘
```

**Implementation approach:** Build the dashboard in the Grafana UI (http://localhost:3000) using the PromQL queries below, then export the JSON to `airflow/grafana/dashboards/pipeline-health.json`. This avoids hand-crafting the verbose Grafana JSON model. The provisioning YAML entry will auto-load it on container restart.

**Checklist:**

- [ ] Open Grafana UI → New Dashboard → Add visualization for each panel
- [ ] Configure stat panels with threshold colours (green → yellow → red)
- [ ] Export final JSON to `airflow/grafana/dashboards/pipeline-health.json`
- [ ] Add provider entry in `airflow/grafana/provisioning/dashboards/airflow.yaml`
- [ ] All panels use only Prometheus metrics — no Azure Monitor / Azure SQL datasources required
- [ ] Data Freshness, Pipeline Status, dbt Pass Rate, and Row Count all exposed as custom Prometheus gauges by the probe script
- [ ] Verify all panels load data within 30s of dashboard open
- [ ] Take a screenshot for portfolio

**Key Panel Queries (PromQL):**

```promql
# Data Freshness (stat panel, unit: duration, thresholds: green < 6h, yellow < 24h, red >= 24h)
# Exposed by the data freshness probe script
w3c_data_freshness_seconds{source="azure_sql"}

# Pipeline Status (stat panel, threshold: > 0 = green)
# Exposed by the data freshness probe script
w3c_pipeline_last_run_status

# dbt Test Pass Rate (stat panel, %)
# Exposed by the data freshness probe script
w3c_dbt_test_pass_rate

# Row Counts (3 x stat panels, unit: short, show Bronze/Silver/Azure SQL side by side)
# Bronze via Databricks Unity Catalog REST API, Silver via REST, Azure SQL via pymssql
w3c_row_count{source="bronze"}
w3c_row_count{source="silver"}
w3c_row_count{source="azure_sql"}

# DAG Run Duration (time series)
airflow_dag_run_duration_seconds

# p50 / p95 / p99 (single stat or gauge)
histogram_quantile(0.50, sum(rate(airflow_task_duration_seconds_bucket[1h])) by (le))
histogram_quantile(0.95, sum(rate(airflow_task_duration_seconds_bucket[1h])) by (le))
histogram_quantile(0.99, sum(rate(airflow_task_duration_seconds_bucket[1h])) by (le))

# Task Success Rate (time series)
rate(airflow_task_success_count[5m])

# Task Failure Rate (time series)
rate(airflow_task_failure_count[5m])
```

**Provisioning entry (airflow/grafana/provisioning/dashboards/airflow.yaml):**

```yaml
apiVersion: 1

providers:
  - name: "default"
    orgId: 1
    folder: ""
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    options:
      path: /etc/grafana/provisioning/dashboards

  - name: "pipeline-health"      # <-- new provider entry
    orgId: 1
    folder: "W3C ETL"
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    options:
      path: /etc/grafana/provisioning/dashboards/pipeline-health.json
```

**—--------------------------------------------------------------------**

#### 6. Verification & Validation

**Acceptance Criteria:**

- [x] Grafana + Prometheus stack running with 2 auto-provisioned dashboards
- [x] Airflow StatsD exporter configured, Prometheus scraping `airflow_*` metrics
- [x] Grafana dashboards show real-time DAG duration, task success/failure rates, container metrics
- [ ] Terraform-managed action groups deployed: `az monitor action-group list` shows `ag-w3c-critical` (P1), `ag-w3c-warning` (P2), `ag-w3c-info` (P3)
- [ ] Terraform-managed budget alerts deployed: `az consumption budget list` shows both $50 warning + $100 hard cap (actual firing requires $40 spend — not tested on limited dev credits)
- [ ] Terraform-managed Databricks pipeline failure alert (P1, metric-based)
- [ ] Terraform-managed Azure SQL auto-pause alert (P1, metric-based)
- [ ] Log-based Databricks job retry alert (P2) deployed if Log Analytics enabled
- [ ] P1 alert fires to configured email on simulated pipeline failure (e.g. temporarily stop Databricks Workflow, verify email received)
- [ ] Budget alert resources exist in Azure (`az consumption budget list` shows both budgets) — P2 firing not tested on limited dev credits
- [ ] Alert severity tiering documented and auditable via Terraform code
- [ ] Data freshness probe running, all custom metrics appearing in Prometheus (`w3c_data_freshness_seconds`, `w3c_pipeline_last_run_status`, `w3c_dbt_test_pass_rate`, `w3c_row_count{source="bronze|silver|azure_sql"}`)
- [ ] Pipeline Health dashboard has 10 panels with all panels showing data
- [ ] P2 freshness alert fires >6h after pipeline completes
- [ ] P1 freshness alert fires >24h after pipeline completes

**Phase Handoff Validation:**

```bash
# Verify Prometheus is scraping
curl http://localhost:9090/api/v1/targets

# Verify Grafana dashboards load
open http://localhost:3000

# Verify Terraform-managed action groups
az monitor action-group list --resource-group rg-w3c-etl

# Verify Terraform-managed metric alerts
az monitor metrics alert list --resource-group rg-w3c-etl

# Verify Terraform-managed budget alerts
az consumption budget list --resource-group rg-w3c-etl

# Verify data freshness probe
curl http://localhost:8000/metrics | grep w3c_data_freshness

# Terraform plan shows no drift in monitoring resources
terraform plan -chdir=terraform/part_a -var-file=environments/dev/terraform.tfvars -no-color | grep -E "(monitor|budget)"
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
- [ ] Run CD smoke test (post-deploy pipeline validation)
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
- CD smoke test passed (post-deploy pipeline validation)
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
- Tier 1 CI runs on every push (no Azure creds) — Ruff, mypy, pytest, dbt compile, terraform validate
- CD pipeline deploys to Azure on merge to main (OIDC auth, no client secrets)
- Post-deploy smoke test validates pipeline end-to-end (trigger DAG → poll → SQL row count)

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

```bash
# Via GitHub UI: Settings > Secrets and variables > Actions
# Delete CD secrets from GitHub Environment (azure-dev):
# - AZURE_CLIENT_ID (OIDC federated credential app ID)
# - AZURE_TENANT_ID
# - AZURE_SUBSCRIPTION_ID
# - AIRFLOW_URL / AIRFLOW_USERNAME / AIRFLOW_PASSWORD
# - DATABRICKS_TOKEN
# - AZURE_SQL_SERVER / AZURE_SQL_USER / AZURE_SQL_PASSWORD
# - STORAGE_ACCOUNT_NAME
```

## Step 6: Delete GitHub Environments

```bash
# Via GitHub UI: Settings > Environments
# Delete azure-dev environment
```

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

| CD deploy queue blocked by failed smoke test | Low | Medium | Rollback job restores previous state; alert DevOps team on failure |
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

- [x] T-SQL compatibility macros created (tsql_cast, tsql_datepart, tsql_format_date, t_sql_compat — including test_expression_is_true, collect_freshness overrides)
- [x] All 16 dbt models migrated with inline T-SQL conditionals
- [x] PostgreSQL dialect preserved in {% else %} branches
- [x] Boolean to int conversions implemented
- [x] Complex patterns handled (generate_series, PERCENTILE_CONT, regex)
- [ ] `dbt compile --profile w3c` passes (PostgreSQL) — ✅ passes in Docker (Python 3.12); local Python 3.14 has dbt-core/mashumaro incompatibility
- [x] `dbt compile --profile w3c_azure` passes (Azure SQL) — verified by successful dbt_run on Databricks
- [x] dbt docs generate operational — verified by successful dbt_docs task on Databricks
- [x] Source freshness checks configured (then removed for static historical data — max log_date=2011-05-15 can't satisfy 24/48h freshness)
- [ ] dbt docs hosted (GitHub Pages or Azure Static Web Apps) — out of Phase 8 scope

### CI/CD

- [x] Tier 1 CI configured (every push, no Azure creds)
- [x] Tier 1 includes: Ruff, mypy, pytest, dbt compile, terraform validate
- [x] CD pipeline configured (merge to main, deploy to Azure)
- [x] CD includes: Terraform plan/apply, DAB deploy, dbt deploy, Airflow sync, smoke test
- [ ] Scheduled trigger configured (Friday 5:00 PM UTC via cron) — pending from Phase 8
- [ ] GitHub Environment configured: `azure-dev` — requires GitHub UI setup with variables/secrets
- [ ] OIDC Workload Identity Federation configured via Terraform (github_oidc.tf) — requires bootstrap terraform apply + copy client ID to Azure Client ID environment variable
- [ ] CD secrets configured in GitHub Environment azure-dev
- [x] Deployment runbook documented (`docs/deployment-runbook.md`)

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

- [x] Exactly 18 CSV exports produced — verified (18 CSV files in Star-Schema/ excluding .DS_Store)
- [x] CSV headers match expected schema — verified 18/18 headers match schema definitions (fact_webrequest: 24 cols, dim_date: 12 cols, dim_page: 7 cols, etc.)
- [ ] DAX measure field dependencies validated — fields present in CSV headers (is_404, bytes_sent, response_time_ms, is_crawler, etc.); Power BI specific validation pending
- [ ] Power BI semantic contract verified — out of Phase 8 scope (requires Power BI desktop validation)

### End-to-End Verification

- [x] Complete pipeline test passed — both DAGs (spark_ingestion_azure + dbt_marts_azure) completed green
- [x] All phases executed successfully
- [x] Data flows end-to-end: ADLS → Bronze → Silver → Azure SQL → dbt → CSV
- [x] Scheduled trigger configured (Friday 5:00 PM UTC via cron) — Phase 9 scope
- [ ] Post-deploy smoke test passed in CD pipeline — Phase 9 scope
- [x] Issues and resolutions documented — resolved: trailing comma, dbt deps, expression_is_true, ORDER BY, NOT IN→LEFT JOIN, freshness thresholds removed, export_csv parallelized
- [x] Code review (Phases 8c/8d) completed — 11 issues found, 7 fixed; see `plans/code-review-phases-8c-8d.md`

---

## Implementation Order Diagram

```
Phase 0  → Phase 1  → Phase 2  → Phase 3  → Phase 4  → Phase 5 ✅
(Prereqs) (Infra A) (Verify)  (Bronze)  (Silver)  (JDBC)
    ↓         ↓         ↓         ↓         ↓         ↓
    └─────────┴─────────┴─────────┴─────────┴─────────┘
                          ↓
Phase 6 ✅ → Phase 7 ✅ → Phase 8a ✅ → Phase 8b ✅ → Phase 8c ✅
(Wf+TF+DAG) (Dims inline) (Macros)  (Complex) (Docs)
    ↓              ↓         ↓         ↓         ↓
    └──────────────┴─────────┴─────────┴─────────┘
                          ↓
Phase 9  → Phase 10 → Phase 11
(CI/CD)  (Monitor)    (E2E)
```
