# Azure Cloud-Native Single-Pipeline ETL Platform Implementation Plan

**Version:** v2.0  
**Status:** Phase 1 Complete ✅ | Phase 2 In Progress  
**Replaces:** v1.8 (dual-path architecture)  
**Budget:** $100 Azure credit cap (with $50 alert threshold)  
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
JDBC Export (jdbc_export_azure.py — Databricks Python task)
  - Reads directly from Silver (no Gold table)
  - Tracking table for idempotency (SQL Server error 208 handling via error-code extraction)
  - Writes to Azure SQL dbo.raw_enriched
  - mssql-jdbc Maven library on cluster
      │
      ▼
Azure SQL Database (dbo.raw_enriched)
      │
      ▼
Airflow export_dimensions_azure operator
  - Reads from Azure SQL dbo.raw_enriched
  - Builds dim_geolocation + dim_useragent
  - Writes to Azure SQL dbo. (MERGE upsert on natural key)
  - Fires Airflow Dataset outlet for dbt DAG trigger
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
  └─ spark_ingestion_azure.py DAG
       └─ DatabricksRunNowOperator → Databricks Workflows
            ├─ Task 1: DLT Bronze pipeline
            ├─ Task 2: DLT Silver pipeline
            └─ Task 3: jdbc_export_azure.py (Python task)
  └─ dbt_marts_azure.py DAG (Dataset-triggered after export_dimensions_azure fires)
       ├─ export_dimensions_azure operator (PythonOperator)
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
- Databricks secret scope creation via CLI v2+
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

3. **Databricks CLI: new v2+ CLI** (`brew install databricks` / `winget install Databricks.DatabricksCLI`). All CLI commands in the plan must use v2+ syntax:
   - Auth: `databricks auth login --host <workspace-url>` (not `databricks configure --token`)
   - Workspace import: `databricks workspace import <local-path> <remote-path>` (not `--file/--format` flags)
   - Secrets scope: `databricks secrets create-scope <name>` (not `put-scope` or `create-scope` with AKV flags)
   - Secrets put: `databricks secrets put --scope <scope> --key <key>` (same in v2+)
   - DBFS: `databricks fs cp <src> <dst>` (same in v2+)
   - Do NOT mix legacy and v2+ syntax anywhere in the plan.

### Infrastructure Constraints

- **Terraform Part A/Part B split:** Part A (core infra) must complete before Part B (DLT pipeline + Workflows) because Part B resources depend on DLT source code existing first.
- **Provider versions pinned:** `azurerm ~> 4.75.0`, `databricks ~> 1.70`, Terraform `>= 1.10.5, < 2.0`
- **ADLS Gen2 containers:** `raw-logs`, `bronze`, `silver`, `gold` (fixed naming)
- **Azure SQL Serverless:** `GP_S_Gen5`, 1 vCore, auto-pause 60 min (cost optimization)
- **Databricks Premium tier:** Required for Unity Catalog
- **Unity Catalog:** `w3c_catalog` catalog, `bronze` / `silver` / `gold` schemas (fixed naming)
- **Terraform directory structure:** `terraform/part_a/` and `terraform/part_b/` with `environments/dev/` and `modules/` subdirectories
- **Budget alerts:** $50 alert, $100 hard cap (Azure Cost Management)
- **Terraform lock file:** `terraform providers lock` and committed `terraform.lock.hcl`
- **DLT cluster lifecycle:** `lifecycle { ignore_changes = [cluster, continuous] }` on `databricks_pipeline` for dev flexibility
- **ADLS Gen2 RBAC:** `Storage Blob Data Contributor` assigned to Databricks workspace managed identity
- **Databricks secret scope:** `w3c-etl-pipeline` for credentials (storage access key, Azure SQL creds)
- **VNet configuration:** Databricks-delegated subnet + Azure SQL subnet; private endpoints disabled by default (`var.enable_private_endpoints = false`)
- **Azure SQL collation:** `SQL_Latin1_General_CP1_CI_AS`

### DLT Bronze Constraints

- **DBR version pinned:** `15.4.x-scala2.12` (Python 3.11 — avoid Python 3.12-only features)
- **Do NOT use serverless DLT** (cost); classic DLT cluster: `Standard_DS3_v2`, 1-2 workers
- **Auto Loader:** `binaryFile` format with `cloudFiles.includeExistingFiles = true`, `maxFilesPerTrigger = 10`, `maxFileSize = 209715200`
- **W3C parser:** Uses `rsplit()` field-counting to handle unquoted user-agent strings (matching authoritative `w3c_parser.py`)
- **UDF pattern:** UDF+explode pattern (NOT foreachBatch — foreachBatch returns StreamingQuery, not DataFrame)
- **Format detection:** 14-field vs 18-field IIS format detection
- **Deduplication:** ROW_NUMBER dedup CTE for full_refresh idempotency (option b — preferred)
- **Self-contained parser:** `parse_log_line` function self-contained in DLT script (ported from `01_bronze_ingestion.py` reference)
- **Sample log files:** 18-field and 14-field variants in `data/samples/`
- **Bronze table:** `w3c_catalog.bronze.bronze_raw_logs`, partitioned by `log_date`
- **Delta properties:** `delta.enableChangeDataFeed = true`, `delta.autoOptimize.optimizeWrite = true`
- **Pipeline creation:** Via Terraform Part B (NOT UI) to avoid resource conflict

### DLT Silver Constraints

- **GeoIP: MaxMind GeoLite2 ONLY** (not ip-api.com). 7 UDFs: country, region, city, latitude, longitude, postcode, isp
- **GeoLite2 databases:** GeoLite2-City.mmdb and GeoLite2-ASN.mmdb must be uploaded to DBFS `/dbfs/mnt/w3c-data/` for Silver pipeline
- **Lazy reader factory pattern:** `_make_geo_reader()` and `_make_asn_reader()` — `spark.conf.get()` called at driver level, NOT inside UDF body (UDFs run on workers where spark context is unavailable)
- **Computed UDFs:** 5 computed UDFs: `page_category`, `referrer_domain`, `traffic_type`, `is_crawler`, `size_band`
- **Plain Python function:** `_extract_domain()` is a plain Python function (NOT a UDF) used inside `traffic_type` UDF — calling a UDF inside another UDF body causes a runtime error
- **UA columns excluded:** UA columns (agent_type, browser_name, browser_version, operating_system, device_type) are NOT written to Silver DDL
- **Geo columns preserved:** 6 geo columns MUST stay in Silver: country, region, city, latitude, longitude, isp — `export_dimensions_azure` reads them to build `dim_geolocation`
- **Postcode handling:** `postcode` is a computed field, stays in Silver core columns
- **PyPI library:** `geoip2==5.0.1` installed as PyPI library on Silver DLT cluster
- **Silver table:** `w3c_catalog.silver.silver_enriched_logs`

### JDBC Export Constraints

- **Source:** Reads from Silver directly (no Gold table — Gold table was eliminated as adding no value)
- **Idempotency:** Tracking table pattern: `dbo.raw_enriched_loaded` with SQL Server error 208 handling (error-code extraction via `getErrorCode()` traversal — NOT string matching on exception message)
- **DDL execution:** `execute_ddl()` uses `spark._jvm.java.sql.DriverManager` (py4j gateway) — bare `import java.sql` is NOT valid Python
- **Retry logic:** 4 attempts, `5 * (2 ** attempt)` backoff (5s/10s/20s), covers Azure SQL serverless cold-start
- **JDBC driver:** MSSQL JDBC driver: `com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11` Maven library
- **JDBC options:** `batchsize=10000`, `numPartitions=4`, `encrypt=true`, `trustServerCertificate=false`
- **Pre-check:** `spark._jvm.Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")` before connection

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

- **DAG 1:** `spark_ingestion_azure.py`: triggers Databricks Workflow via `DatabricksRunNowOperator`
- **DAG 2:** `dbt_marts_azure.py`: Dataset-triggered; runs dbt against Azure SQL target
- **Provider version:** `apache-airflow-providers-databricks==4.6.0` installed in Dockerfile (NOT in requirements.txt — avoid pip conflict)
- **Databricks connection:** `databricks_default` in Airflow with workspace URL + PAT token
- **Environment variables:** `DATABRICKS_WORKFLOW_ID`, `AZURE_SQL_SERVER`, `AZURE_SQL_DB`, `AZURE_SQL_USER`, `AZURE_SQL_PASSWORD`

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

**Summary:** All prerequisite setup completed. Azure service principal (`w3c-etl-pipeline-sp`) created with Contributor role. Terraform remote state backend bootstrapped (`rg-tfstate` resource group, storage account with `tfstate` container). MaxMind GeoLite2 databases downloaded to `data/geoip/`. Backend configuration files created for both Part A and Part B. Shared credential documentation created in `docs/credentials.md`.

**Checklist:**

- [x] Azure account with active subscription and $100+ available credit
- [x] Create Azure service principal for Terraform backend + Tier 2 CI access
- [x] Install Azure CLI
- [x] Install Terraform
- [x] Install Databricks CLI v2+
- [x] Sign up for MaxMind GeoLite2 account (free tier)
- [x] Bootstrap Terraform remote state backend (manual, pre-Phase-1 step)
- [x] Configure Terraform backend in `terraform/part_a/backend.tf`
- [x] Configure Terraform backend in `terraform/part_b/backend.tf`
- [x] Store Azure credentials as environment variables
- [x] Document shared credential usage in `docs/credentials.md`

**Acceptance Criteria:**

| # | Criteria | Status |
|---|----------|--------|
| 1 | Azure CLI installed and authenticated (`az login` successful) | ✅ Done |
| 2 | Terraform installed and version >= 1.10.5 | ✅ Done |
| 3 | Databricks CLI v2+ installed | ✅ Done |
| 4 | MaxMind GeoLite2 databases downloaded to `data/geoip/` | ✅ Done|
| 5 | Terraform remote state storage account and container created | ✅ Done |
| 6 | Backend configuration files created in Part A and Part B directories | ✅ Done |
| 7 | Service principal created with Contributor role | ✅ Done |
| 8 | `.env.azure` file created with ARM_* credentials | ✅ Done |
| 9 | Credentials documentation created | ✅ Done |
| 10 | `.gitignore` updated with env/tfstate/geoip patterns | ✅ Done |
| 11 | Sample log files (18-field and 14-field) created in `data/samples/` | ✅ Done |

**Phase Handoff Validation:**

```bash
# Verify Azure CLI
az account show --query name -o tsv

# Verify Terraform
terraform --version

# Verify Databricks CLI
databricks --version

# Verify MaxMind files
ls -la data/geoip/GeoLite2-*.mmdb

# Verify backend files exist
cat terraform/part_a/backend.tf
cat terraform/part_b/backend.tf

# Verify env vars
source .env.azure
echo $ARM_CLIENT_ID
```

---

### Phase 1 — Terraform Part A (Core Infrastructure) (✅ Complete)

**Phase Goal:** Deploy core Azure infrastructure (resource groups, networking, storage, Databricks workspace, Azure SQL) using Terraform Part A.

**Summary:** All core Azure infrastructure deployed in `westus3`. Resources include: resource group `rg-w3c-etl`, VNet with 2 subnets (Databricks-delegated + SQL), ADLS Gen2 storage account `stw3cetlwestus3` with 4 containers (`raw-logs`, `bronze`, `silver`, `gold`), Databricks Premium workspace `w3c-etl-databricks`, and Azure SQL serverless database `w3c-etl-db` (GP_S_Gen5_1, auto-pause 60 min). NSG-based subnet isolation implemented. Network rules on storage account with `Deny` default action.

**Checklist:**

- [x] Create `terraform/part_a/main.tf`, `variables.tf`, `outputs.tf` and module files
- [x] Create `terraform/part_a/environments/dev/terraform.tfvars`
- [x] Run `terraform init`, `providers lock`, `validate`, `plan`, `apply`
- [x] Verify all resources in Azure portal

**terraform/part_a/environments/dev/terraform.tfvars (Deployed):**

```hcl
subscription_id              = "2cfbc457-25bd-4007-8585-6bfa6765ec30"
tenant_id                    = "b52c550c-05c2-4689-a595-c1e0e25d4a2e"
client_id                    = "179ff733-3af5-41f8-8009-f71c177daf01"
# client_secret: sourced from ARM_CLIENT_SECRET env var
resource_group_name          = "rg-w3c-etl"
location                     = "westus3"
storage_account_name         = "stw3cetlwestus3"
databricks_workspace_name    = "w3c-etl-databricks"
sql_server_name              = "sql-w3c-etl"
sql_administrator_login      = "sqladmin"
# sql_administrator_password: sourced from TF_VAR_sql_administrator_password env var
enable_private_endpoints     = false
```

**Acceptance Criteria — ALL PASSED ✅ (2026-06-05):**

- [x] Terraform init completes successfully
- [x] Terraform providers lock generates `.terraform.lock.hcl`
- [x] Terraform validate passes
- [x] Terraform plan shows expected resources
- [x] Terraform apply completes without errors
- [x] All resources visible in Azure portal/CLI:
  - [x] Resource group: `rg-w3c-etl` (westus3)
  - [x] VNet: `vnet-w3c-etl` with 2 subnets (`snet-databricks` 10.0.1.0/24, `snet-sql` 10.0.2.0/24)
  - [x] Storage account: `stw3cetlwestus3` with 4 containers (`raw-logs`, `bronze`, `silver`, `gold`)
  - [x] Databricks workspace: `w3c-etl-databricks` (Premium tier)
  - [x] Azure SQL server: `sql-w3c-etl`
  - [x] Azure SQL database: `w3c-etl-db` (serverless, GP_S_Gen5_1, auto-pause 60 min)
- [x] Outputs captured: storage_account_name, databricks_workspace_url, server_fqdn, database_name

**Deployed Outputs:**
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

**Phase Handoff Validation:**

```bash
# Verify Terraform state
cd terraform/part_a
terraform show

# Verify outputs
terraform output storage_account_name
terraform output databricks_workspace_url
terraform output server_fqdn
terraform output database_name

# Verify Azure resources
az resource list --resource-group rg-w3c-etl --query '[].{Name:name, Type:type, Location:location}' -o table

# Verify storage containers
az storage container list --account-name $(terraform output storage_account_name) --query '[].name' -o tsv

# Verify VNet subnets
az network vnet subnet list --vnet-name vnet-w3c-etl --resource-group rg-w3c-etl --query '[].{Name:name, Prefix:addressPrefix}' -o table
```

---

### Phase 2 — Deploy and Verify Azure Infrastructure

**Phase Goal:** Verify all Azure resources from Phase 1 are operational, configure budget alerts, and set up local environment variables.

**Checklist:**

- [ ] Verify Databricks workspace is accessible
- [ ] Authenticate to Databricks workspace using CLI v2+
- [ ] Create Databricks secret scope `w3c-etl-pipeline`
- [ ] Add storage access key to Databricks secrets
- [ ] Add Azure SQL credentials to Databricks secrets
- [ ] Verify Azure SQL database is accessible
- [ ] Create Unity Catalog `w3c_catalog`
- [ ] Create Unity Catalog schemas: `bronze`, `silver`, `gold`
- [ ] Configure Azure budget alerts ($50 warning, $100 hard cap)
- [ ] Update `.env.azure` with all connection details
- [ ] Test ADLS Gen2 container access
- [ ] Test Azure SQL connectivity

**Code Scaffolds:**

**Databricks CLI v2+ authentication:**

```bash
# Authenticate to Databricks workspace
databricks auth login --host https://<workspace-url>.azuredatabricks.net

# Verify authentication
databricks workspace list
```

**Create Databricks secret scope:**

```bash
# Create secret scope (v2+ syntax for Databricks-backed scope)
databricks secrets create-scope w3c-etl-pipeline

# Add storage access key
databricks secrets put --scope w3c-etl-pipeline --key storage-access-key

# Add Azure SQL credentials (consistent with Phase 5 spark.conf.get keys)
databricks secrets put --scope w3c-etl-pipeline --key azure.sql.server
databricks secrets put --scope w3c-etl-pipeline --key azure.sql.database
databricks secrets put --scope w3c-etl-pipeline --key azure.sql.username
databricks secrets put --scope w3c-etl-pipeline --key azure.sql.password
```

**Unity Catalog creation (via Databricks CLI or SQL):**

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS w3c_catalog;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS w3c_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS w3c_catalog.silver;
CREATE SCHEMA IF NOT EXISTS w3c_catalog.gold;

-- Grant permissions
GRANT CREATE SCHEMA ON CATALOG w3c_catalog TO `users`;
GRANT USE CATALOG w3c_catalog TO `users`;
GRANT USE SCHEMA w3c_catalog.bronze TO `users`;
GRANT USE SCHEMA w3c_catalog.silver TO `users`;
GRANT USE SCHEMA w3c_catalog.gold TO `users`;
GRANT CREATE TABLE ON SCHEMA w3c_catalog.bronze TO `users`;
GRANT CREATE TABLE ON SCHEMA w3c_catalog.silver TO `users`;
GRANT CREATE TABLE ON SCHEMA w3c_catalog.gold TO `users`;
```

**Azure budget configuration (via Azure CLI):**

```bash
# Get subscription ID
SUBSCRIPTION_ID=$(az account show --query id -o tsv)

# Create budget alert at $50
az consumption budget create \
  --name w3c-etl-budget-warning \
  --resource-group rg-w3c-etl-dev \
  --category Cost \
  --amount 50 \
  --time-grain Monthly \
  --notification '{"threshold":50,"contactEmails":["ahmedikram30@gmail.com"],"operator":"GreaterThan"}'

# Create hard cap at $100
az consumption budget create \
  --name w3c-etl-budget-cap \
  --resource-group rg-w3c-etl-dev \
  --category Cost \
  --amount 100 \
  --time-grain Monthly \
  --notification '{"threshold":100,"contactEmails":["ahmedikram30@gmail.com"],"operator":"GreaterThan"}'
```

**Updated `.env.azure`:**

```bash
# Azure credentials
ARM_CLIENT_ID=<service-principal-appId>
ARM_CLIENT_SECRET=<service-principal-password>
ARM_SUBSCRIPTION_ID=<subscription-id>
ARM_TENANT_ID=<tenant-id>

# Databricks
DATABRICKS_HOST=https://<workspace-url>.azuredatabricks.net
DATABRICKS_TOKEN=<personal-access-token>

# Azure SQL
AZURE_SQL_SERVER=<server-fqdn>
AZURE_SQL_DB=w3c-etl-db
AZURE_SQL_USER=sqladmin
AZURE_SQL_PASSWORD=<strong-password>

# Storage
STORAGE_ACCOUNT_NAME=<storage-account-name>
STORAGE_ACCESS_KEY=<storage-access-key>
```

**Test ADLS Gen2 access:**

```bash
# List containers
az storage container list --account-name $STORAGE_ACCOUNT_NAME --query '[].name' -o tsv

# Upload test file
echo "test" > /tmp/test.txt
az storage blob upload \
  --container-name raw-logs \
  --file /tmp/test.txt \
  --name test.txt \
  --account-name $STORAGE_ACCOUNT_NAME

# Verify upload
az storage blob list \
  --container-name raw-logs \
  --account-name $STORAGE_ACCOUNT_NAME \
  --query '[].name' -o tsv
```

**Test Azure SQL connectivity:**

```bash
# Using sqlcmd (install via brew/winget if needed)
sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "SELECT @@VERSION"

# Or using Python
python3 << EOF
import pyodbc
conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.getenv('AZURE_SQL_SERVER')};DATABASE={os.getenv('AZURE_SQL_DB')};UID={os.getenv('AZURE_SQL_USER')};PWD={os.getenv('AZURE_SQL_PASSWORD')};Encrypt=yes;TrustServerCertificate=no"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
cursor.execute("SELECT @@VERSION")
print(cursor.fetchone())
conn.close()
EOF
```

**Acceptance Criteria:**

- Databricks workspace accessible via browser and CLI
- Databricks authentication successful (`databricks workspace list` works)
- Secret scope `w3c-etl-pipeline` created
- Secrets added: storage-access-key, azure.sql.server, azure.sql.database, azure.sql.username, azure.sql.password
- Unity Catalog `w3c_catalog` created
- Schemas created: bronze, silver, gold
- Budget alerts configured: $50 warning, $100 hard cap
- `.env.azure` updated with all connection details
- ADLS Gen2 containers accessible via Azure CLI
- Azure SQL connectivity verified via sqlcmd or Python

**Phase Handoff Validation:**

```bash
# Source environment
source .env.azure

# Verify Databricks
databricks secrets list --scope w3c-etl-pipeline

# Verify secret keys match expected naming (azure.sql.*)
databricks secrets list --scope w3c-etl-pipeline | grep -E "(storage-access-key|azure.sql)"

# Verify Unity Catalog (via Databricks SQL)
databricks sql execute --warehouse-id <warehouse-id> --sql "SHOW CATALOGS"

# Verify budget alerts
az consumption budget list --resource-group rg-w3c-etl-dev

# Verify ADLS access
az storage container list --account-name $STORAGE_ACCOUNT_NAME

# Verify SQL connectivity
sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "SELECT DB_NAME() AS current_db"
```

**Failure Recovery Table:**

| Failure Mode | Detection | Recovery Action |
|--------------|-----------|-----------------|
| Databricks auth fails | `databricks auth login` error | Verify workspace URL, check PAT token validity, regenerate token |
| Secret scope creation fails | CLI error or permission denied | Check workspace permissions, use Azure Key Vault backend if needed |
| Unity Catalog creation fails | SQL error or permission denied | Verify Premium tier, check catalog creation permissions |
| Budget creation fails | Azure CLI error | Verify subscription permissions, check existing budget limits |
| ADLS access fails | Azure CLI authentication error | Verify storage account key, check RBAC assignments |
| SQL connectivity fails | Connection timeout or auth error | Verify firewall rules, check credentials, test from different network |

---

### Phase 3 — DLT Bronze Pipeline

**Phase Goal:** Create and deploy the DLT Bronze pipeline with Auto Loader, W3C parser UDF, and quality expectations.

**Checklist:**

- [ ] Create `airflow/spark/databricks/dlt_bronze.py`
- [ ] Implement W3C parser with rsplit field-counting
- [ ] Implement 14-field vs 18-field IIS format detection
- [ ] Implement UDF+explode pattern for parsing
- [ ] Add @dlt.expect_or_drop quality rules
- [ ] Implement ROW_NUMBER dedup CTE for idempotency
- [ ] Configure Auto Loader with binaryFile format
- [ ] Set partitioning by log_date
- [ ] Configure Delta properties (change data feed, auto optimize)
- [ ] Upload MaxMind GeoLite2 databases to DBFS
- [ ] Upload sample log files to ADLS Gen2 raw-logs container
- [ ] Create Bronze table via DLT pipeline (not manual DDL)
- [ ] Verify Bronze table schema and data

**Code Scaffolds:**

**airflow/spark/databricks/dlt_bronze.py:**

```python
import dlt
from pyspark.sql.functions import udf, col, explode, split, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType

# Bronze table definition
@dlt.table(
    name="bronze_raw_logs",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true"
    },
    partition_cols=["log_date"]
)
@dlt.expect_or_drop("valid_log_date", "log_date IS NOT NULL")
@dlt.expect_or_drop("valid_status", "status BETWEEN 100 AND 599")
@dlt.expect_or_drop("valid_client_ip", "client_ip IS NOT NULL AND client_ip != '-'")
def bronze_raw_logs():
    # Auto Loader configuration
    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "binaryFile") \
        .option("cloudFiles.includeExistingFiles", "true") \
        .option("maxFilesPerTrigger", "10") \
        .option("maxFileSize", 209715200) \
        .load(f"abfss://raw-logs@{spark.conf.get('storage.account_name')}.dfs.core.windows.net/")
    
    # Parse W3C log lines
    def parse_log_line(line: str):
        """Parse W3C IIS log line with 14 or 18 fields."""
        if not line or line.startswith("#"):
            return None
        
        # Split by space, handling quoted user-agent strings
        parts = []
        current = ""
        in_quotes = False
        
        for char in line:
            if char == '"':
                in_quotes = not in_quotes
            elif char == ' ' and not in_quotes:
                parts.append(current)
                current = ""
            else:
                current += char
        parts.append(current)
        
        # Detect format by field count
        if len(parts) == 18:
            # 18-field IIS format
            return (parts[0], parts[1], parts[2], parts[3], parts[4], parts[5],
                    parts[6], parts[7], parts[8], parts[9], parts[10], parts[11],
                    parts[12], parts[13], parts[14], parts[15], parts[16], parts[17])
        elif len(parts) == 14:
            # 14-field IIS format
            return (parts[0], parts[1], parts[2], parts[3], parts[4], parts[5],
                    parts[6], parts[7], parts[8], parts[9], parts[10], parts[11],
                    parts[12], parts[13], None, None, None, None)
        else:
            return None
    
    parse_udf = udf(parse_log_line, StructType([
        StructField("log_date", StringType(), True),
        StructField("log_time", StringType(), True),
        StructField("server_ip", StringType(), True),
        StructField("method", StringType(), True),
        StructField("uri_stem", StringType(), True),
        StructField("uri_query", StringType(), True),
        StructField("client_ip", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("cookie", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("sub_status", IntegerType(), True),
        StructField("win32_status", IntegerType(), True),
        StructField("bytes_sent", LongType(), True),
        StructField("bytes_recv", LongType(), True),
        StructField("server_port", IntegerType(), True),
        StructField("username", StringType(), True),
        StructField("time_taken", LongType(), True)
    ]))
    
    # Apply parser
    parsed_df = df.withColumn("parsed", explode(parse_udf(col("content"))))
    
    # Extract fields from parsed struct
    for field_name in ["log_date", "log_time", "server_ip", "method", "uri_stem",
                       "uri_query", "client_ip", "user_agent", "cookie", "referrer",
                       "status", "sub_status", "win32_status", "bytes_sent", "bytes_recv",
                       "server_port", "username", "time_taken"]:
        parsed_df = parsed_df.withColumn(field_name, col(f"parsed.{field_name}"))
    
    parsed_df = parsed_df.drop("parsed", "content", "path")
    
    # Add source_file column for tracking
    parsed_df = parsed_df.withColumn("source_file", col("input_file_name"))
    
    # Deduplication using ROW_NUMBER (for full_refresh idempotency)
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window_spec = Window.partitionBy("source_file", "log_date", "log_time", "client_ip").orderBy("source_file")
    parsed_df = parsed_df.withColumn("row_num", row_number().over(window_spec))
    parsed_df = parsed_df.filter(col("row_num") == 1).drop("row_num")
    
    return parsed_df
```

**Upload MaxMind databases to DBFS:**

```bash
# Upload GeoLite2 databases
databricks fs cp data/geoip/GeoLite2-City.mmdb dbfs:/mnt/w3c-data/GeoLite2-City.mmdb
databricks fs cp data/geoip/GeoLite2-ASN.mmdb dbfs:/mnt/w3c-data/GeoLite2-ASN.mmdb

# Verify upload
databricks fs ls dbfs:/mnt/w3c-data/
```

**Upload sample log files to ADLS:**

```bash
# Upload sample logs
az storage blob upload \
  --container-name raw-logs \
  --file data/samples/18-field-sample.log \
  --name samples/18-field-sample.log \
  --account-name $STORAGE_ACCOUNT_NAME

az storage blob upload \
  --container-name raw-logs \
  --file data/samples/14-field-sample.log \
  --name samples/14-field-sample.log \
  --account-name $STORAGE_ACCOUNT_NAME
```

**Acceptance Criteria:**

- `dlt_bronze.py` created with W3C parser
- Parser handles both 14-field and 18-field IIS formats
- Auto Loader configured with binaryFile format
- Quality expectations added: valid_log_date, valid_status, valid_client_ip
- ROW_NUMBER deduplication implemented
- Bronze table partitioned by log_date
- Delta properties configured: change data feed, auto optimize
- MaxMind databases uploaded to DBFS
- Sample logs uploaded to ADLS
- Bronze table created via DLT pipeline
- Bronze table schema matches expected 18 fields
- Sample data visible in Bronze table

**Phase Handoff Validation:**

```bash
# Verify DLT pipeline creation via Databricks UI or CLI
databricks pipelines list

# Verify Bronze table exists
databricks sql execute --warehouse-id <warehouse-id> --sql "DESCRIBE w3c_catalog.bronze.bronze_raw_logs"

# Verify data in Bronze table
databricks sql execute --warehouse-id <warehouse-id> --sql "SELECT COUNT(*) FROM w3c_catalog.bronze.bronze_raw_logs"

# Verify partitioning
databricks sql execute --warehouse-id <warehouse-id> --sql "SHOW PARTITIONS w3c_catalog.bronze.bronze_raw_logs"
```

**Failure Recovery Table:**

| Failure Mode | Detection | Recovery Action |
|--------------|-----------|-----------------|
| DLT pipeline creation fails | Databricks API error | Verify cluster configuration, check storage access, retry |
| Parser fails on sample logs | Data quality violations | Review log format, adjust parser logic, add more test samples |
| Auto Loader doesn't detect files | No data in Bronze table | Verify container path, check file permissions, test with includeExistingFiles |
| Deduplication removes valid rows | Row count too low | Review window spec, adjust partition columns, test with known duplicates |
| Partitioning fails | DLT error | Verify log_date format, check partition column type |

---

### Phase 4 — DLT Silver Pipeline

**Phase Goal:** Create and deploy the DLT Silver pipeline with MaxMind GeoIP enrichment and computed fields.

**Checklist:**

- [ ] Create `airflow/spark/databricks/dlt_silver.py`
- [ ] Implement lazy reader factory pattern for GeoIP
- [ ] Implement 7 MaxMind GeoLite2 UDFs (country, region, city, lat, lon, postcode, isp)
- [ ] Implement 5 computed field UDFs (page_category, referrer_domain, traffic_type, is_crawler, size_band)
- [ ] Implement _extract_domain plain Python function
- [ ] Exclude UA columns from Silver DDL
- [ ] Preserve 6 geo columns in Silver (country, region, city, lat, lon, isp)
- [ ] Add @dlt.expect_or_drop quality rules
- [ ] Configure Silver table properties
- [ ] Install geoip2==5.0.1 as PyPI library on cluster
- [ ] Test Silver pipeline with Bronze data
- [ ] Verify Silver table schema and data quality

**Code Scaffolds:**

**airflow/spark/databricks/dlt_silver.py:**

```python
import dlt
from pyspark.sql.functions import udf, col, lit, when, lower, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
import geoip2.database
import geoip2.errors

# Lazy reader factory pattern (called at driver level, not inside UDF) with error handling
def _make_geo_reader():
    """Create GeoIP2 database reader (called at driver level) with error handling."""
    try:
        geo_db_path = spark.conf.get("geoip.city_db_path", "/dbfs/mnt/w3c-data/GeoLite2-City.mmdb")
        return geoip2.database.Reader(geo_db_path)
    except Exception as e:
        print(f"WARNING: GeoLite2-City database could not be loaded: {e}. Falling back to Unknown.")
        return None

def _make_asn_reader():
    """Create ASN database reader (called at driver level) with error handling."""
    try:
        asn_db_path = spark.conf.get("geoip.asn_db_path", "/dbfs/mnt/w3c-data/GeoLite2-ASN.mmdb")
        return geoip2.database.Reader(asn_db_path)
    except Exception as e:
        print(f"WARNING: GeoLite2-ASN database could not be loaded: {e}. Falling back to Unknown.")
        return None

# Initialize readers at driver level
geo_reader = _make_geo_reader()
asn_reader = _make_asn_reader()

# GeoIP UDFs
@udf(StringType())
def get_country(ip):
    """Get country from IP address."""
    try:
        response = geo_reader.city(ip)
        return response.country.name or "Unknown"
    except:
        return "Unknown"

@udf(StringType())
def get_region(ip):
    """Get region from IP address."""
    try:
        response = geo_reader.city(ip)
        return response.subdivisions.most_specific.name or "Unknown"
    except:
        return "Unknown"

@udf(StringType())
def get_city(ip):
    """Get city from IP address."""
    try:
        response = geo_reader.city(ip)
        return response.city.name or "Unknown"
    except:
        return "Unknown"

@udf(FloatType())
def get_latitude(ip):
    """Get latitude from IP address."""
    try:
        response = geo_reader.city(ip)
        return response.location.latitude
    except:
        return None

@udf(FloatType())
def get_longitude(ip):
    """Get longitude from IP address."""
    try:
        response = geo_reader.city(ip)
        return response.location.longitude
    except:
        return None

@udf(StringType())
def get_postcode(ip):
    """Get postcode from IP address."""
    try:
        response = geo_reader.city(ip)
        return response.postal.code or "Unknown"
    except:
        return "Unknown"

@udf(StringType())
def get_isp(ip):
    """Get ISP from IP address."""
    try:
        response = asn_reader.asn(ip)
        return response.autonomous_system_organization or "Unknown"
    except:
        return "Unknown"

# Plain Python function (NOT a UDF)
def _extract_domain(url):
    """Extract domain from URL (plain Python, not UDF)."""
    if not url or url == "-":
        return "Direct"
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except:
        return "Unknown"

# Computed field UDFs
@udf(StringType())
def get_page_category(uri_stem):
    """Categorize page by URI pattern."""
    uri_stem_lower = lower(uri_stem) if uri_stem else ""
    if any(ext in uri_stem_lower for ext in [".css", ".js", ".png", ".jpg", ".gif", ".ico"]):
        return "Static Asset"
    elif "/api/" in uri_stem_lower:
        return "API"
    elif "/admin/" in uri_stem_lower:
        return "Admin"
    elif uri_stem_lower.endswith("/"):
        return "Homepage"
    else:
        return "Content"

@udf(StringType())
def get_referrer_domain(referrer):
    """Extract domain from referrer using plain Python function."""
    return _extract_domain(referrer)

@udf(StringType())
def get_traffic_type(referrer_domain):
    """Classify traffic type based on referrer domain."""
    if referrer_domain == "Direct":
        return "Direct"
    elif any(search in referrer_domain.lower() for search in ["google", "bing", "yahoo", "duckduckgo"]):
        return "Search"
    elif any(social in referrer_domain.lower() for social in ["facebook", "twitter", "linkedin", "reddit"]):
        return "Social"
    else:
        return "Referral"

@udf(BooleanType())
def get_is_crawler(user_agent):
    """Detect if user agent is a crawler."""
    if not user_agent or user_agent == "-":
        return False
    ua_lower = lower(user_agent)
    crawler_keywords = ["bot", "crawler", "spider", "scraper", "curl", "wget", "python-requests"]
    return any(keyword in ua_lower for keyword in crawler_keywords)

@udf(StringType())
def get_size_band(bytes_sent):
    """Categorize response size into bands."""
    if not bytes_sent or bytes_sent < 0:
        return "Unknown"
    elif bytes_sent < 1024:
        return "< 1KB"
    elif bytes_sent < 10240:
        return "1KB-10KB"
    elif bytes_sent < 102400:
        return "10KB-100KB"
    elif bytes_sent < 1048576:
        return "100KB-1MB"
    else:
        return "> 1MB"

# Silver table definition
@dlt.table(
    name="silver_enriched_logs",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
@dlt.expect_or_drop("valid_country", "country IS NOT NULL")
@dlt.expect_or_drop("valid_traffic_type", "traffic_type IN ('Direct', 'Search', 'Social', 'Referral')")
@dlt.expect_or_drop("valid_page_category", "page_category IS NOT NULL")
def silver_enriched_logs():
    # Read from Bronze
    bronze_df = dlt.read("bronze_raw_logs")
    
    # Apply GeoIP enrichment
    silver_df = bronze_df \
        .withColumn("country", get_country(col("client_ip"))) \
        .withColumn("region", get_region(col("client_ip"))) \
        .withColumn("city", get_city(col("client_ip"))) \
        .withColumn("latitude", get_latitude(col("client_ip"))) \
        .withColumn("longitude", get_longitude(col("client_ip"))) \
        .withColumn("postcode", get_postcode(col("client_ip"))) \
        .withColumn("isp", get_isp(col("client_ip")))
    
    # Apply computed fields
    silver_df = silver_df \
        .withColumn("page_category", get_page_category(col("uri_stem"))) \
        .withColumn("referrer_domain", get_referrer_domain(col("referrer"))) \
        .withColumn("traffic_type", get_traffic_type(col("referrer_domain"))) \
        .withColumn("is_crawler", get_is_crawler(col("user_agent"))) \
        .withColumn("size_band", get_size_band(col("bytes_sent")))
    
    # Note: UA columns (agent_type, browser_name, browser_version, os, device_type) are excluded
    # They are computed but not materialized in Silver DDL to reduce storage
    
# Select final Silver columns (25 core + 6 geo = 31 total)
    silver_df = silver_df.select(
        "log_date", "log_time", "server_ip", "method", "uri_stem",
        "uri_query", "client_ip", "user_agent", "cookie", "referrer",
        "status", "sub_status", "win32_status", "bytes_sent", "bytes_recv",
        "server_port", "username", "time_taken", "source_file",
        "postcode", "page_category", "referrer_domain", "traffic_type",
        "is_crawler", "size_band",
        "country", "region", "city", "latitude", "longitude", "isp"
    )

    return silver_df
```

**Acceptance Criteria:**

- `dlt_silver.py` created with GeoIP enrichment
- Lazy reader factory pattern implemented correctly
- 7 GeoIP UDFs implemented and tested
- 5 computed field UDFs implemented
- _extract_domain is plain Python function (not UDF)
- UA columns excluded from Silver DDL
- 6 geo columns preserved in Silver
- Quality expectations added: valid_country, valid_traffic_type, valid_page_category
- geoip2==5.0.1 installed as cluster PyPI library
- Silver table created via DLT pipeline
- Silver table schema matches 31 columns
- GeoIP enrichment verified (country, region, city populated)
- Computed fields verified (page_category, traffic_type, is_crawler)

**Phase Handoff Validation:**

```bash
# Verify Silver table exists
databricks sql execute --warehouse-id <warehouse-id> --sql "DESCRIBE w3c_catalog.silver.silver_enriched_logs"

# Verify data in Silver table
databricks sql execute --warehouse-id <warehouse-id> --sql "SELECT COUNT(*) FROM w3c_catalog.silver.silver_enriched_logs"

# Verify GeoIP enrichment
databricks sql execute --warehouse-id <warehouse-id> --sql "SELECT country, region, city, COUNT(*) FROM w3c_catalog.silver.silver_enriched_logs GROUP BY country, region, city LIMIT 10"

# Verify computed fields
databricks sql execute --warehouse-id <warehouse-id> --sql "SELECT page_category, traffic_type, is_crawler, COUNT(*) FROM w3c_catalog.silver.silver_enriched_logs GROUP BY page_category, traffic_type, is_crawler"
```

**Failure Recovery Table:**

| Failure Mode | Detection | Recovery Action |
|--------------|-----------|-----------------|
| GeoIP reader fails | UDF returns "Unknown" for all IPs | Verify DBFS path, check database file integrity, re-upload databases |
| Lazy reader pattern error | Spark context error in UDF | Move reader initialization to driver level, use spark.conf.get |
| Computed field UDF errors | Null values or incorrect categorization | Test UDF logic with sample data, adjust regex patterns |
| Silver table creation fails | DLT pipeline error | Verify Bronze table exists, check column compatibility, review expectations |

---

### Phase 5 — JDBC Export from Silver to Azure SQL

**Phase Goal:** Create and deploy the JDBC export task that reads from Silver and writes to Azure SQL with idempotency tracking.

**Checklist:**

- [ ] Create `airflow/spark/databricks/jdbc_export_azure.py`
- [ ] Implement execute_ddl using py4j gateway
- [ ] Implement tracking table pattern (dbo.raw_enriched_loaded)
- [ ] Implement SQL Server error 208 handling via error-code extraction
- [ ] Implement retry logic (4 attempts, exponential backoff)
- [ ] Configure JDBC options (batchsize, numPartitions, encrypt)
- [ ] Add MSSQL JDBC driver as Maven library
- [ ] Implement is_crawler BIT cast from string
- [ ] Use exact EXPORT_COLUMNS list (31 columns)
- [ ] Use exact RAW_ENRICHED_DDL schema
- [ ] Test export with sample Silver data
- [ ] Verify Azure SQL table creation and data

**Code Scaffolds:**

**airflow/spark/databricks/jdbc_export_azure.py:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import time

# Exact column list from specification
EXPORT_COLUMNS = [
    "log_date", "log_time", "server_ip", "method", "uri_stem",
    "uri_query", "client_ip", "user_agent", "cookie", "referrer",
    "status", "sub_status", "win32_status", "bytes_sent", "bytes_recv",
    "server_port", "username", "time_taken", "source_file",
    "postcode", "page_category", "referrer_domain", "traffic_type",
    "is_crawler", "size_band",
    "country", "region", "city", "latitude", "longitude", "isp",
]

# Exact DDL from specification
RAW_ENRICHED_DDL = """
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
"""

TRACKING_DDL = """
CREATE TABLE dbo.raw_enriched_loaded (
    source_file VARCHAR(255) PRIMARY KEY
);
"""

def execute_ddl(spark, jdbc_url, username, password, ddl):
    """Execute DDL using py4j gateway (not bare java.sql import)."""
    try:
        # Get JDBC driver via py4j
        driver_class = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        spark._jvm.Class.forName(driver_class)
        
        # Create connection
        connection = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, username, password)
        statement = connection.createStatement()
        
        # Execute DDL
        statement.execute(ddl)
        
        # Close resources
        statement.close()
        connection.close()
        
        print(f"Successfully executed DDL")
    except Exception as e:
        print(f"DDL execution failed: {str(e)}")
        raise

def get_error_code(exception):
    """Extract SQL Server error code from exception via traversal."""
    try:
        # Traverse exception chain to find SQLServerException
        current = exception
        while current is not None:
            if hasattr(current, 'getErrorCode'):
                return current.getErrorCode()
            current = current.__cause__ if hasattr(current, '__cause__') else None
        return None
    except:
        return None

def export_to_azure_sql(spark, silver_table_path, jdbc_url, username, password):
    """Export Silver data to Azure SQL with idempotency tracking."""
    
    # Pre-check: load JDBC driver
    try:
        spark._jvm.Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
        print("JDBC driver loaded successfully")
    except Exception as e:
        print(f"Failed to load JDBC driver: {str(e)}")
        raise
    
    # Create tables if not exist (with error 208 handling)
    for ddl_name, ddl in [("raw_enriched", RAW_ENRICHED_DDL), ("raw_enriched_loaded", TRACKING_DDL)]:
        max_attempts = 4
        for attempt in range(max_attempts):
            try:
                execute_ddl(spark, jdbc_url, username, password, ddl)
                break
            except Exception as e:
                error_code = get_error_code(e)
                if error_code == 208:  # Object already exists
                    print(f"{ddl_name} already exists, skipping DDL")
                    break
                elif attempt < max_attempts - 1:
                    backoff = 5 * (2 ** attempt)
                    print(f"DDL attempt {attempt + 1} failed, retrying in {backoff}s: {str(e)}")
                    time.sleep(backoff)
                else:
                    print(f"DDL failed after {max_attempts} attempts: {str(e)}")
                    raise
    
    # Read from Silver table
    silver_df = spark.read.format("delta").load(silver_table_path)
    
    # Select export columns
    export_df = silver_df.select(EXPORT_COLUMNS)
    
    # Cast is_crawler from string to BIT (Azure SQL expects 0/1)
    export_df = export_df.withColumn(
        "is_crawler",
        when(col("is_crawler") == "true", lit(1)).otherwise(lit(0))
    )
    
    # Get list of already loaded files from tracking table
    try:
        loaded_files_df = spark.read.jdbc(
            url=jdbc_url,
            table="dbo.raw_enriched_loaded",
            properties={"user": username, "password": password}
        )
        loaded_files = set(row.source_file for row in loaded_files_df.collect())
    except Exception as e:
        error_code = get_error_code(e)
        if error_code == 208:  # Table doesn't exist yet
            loaded_files = set()
        else:
            print(f"Failed to read tracking table: {str(e)}")
            raise
    
    # Filter out already loaded files
    new_data_df = export_df.filter(~col("source_file").isin(list(loaded_files)))
    
    # Get unique source files in new data
    new_files = new_data_df.select("source_file").distinct().collect()
    new_file_list = [row.source_file for row in new_files]
    
    if not new_file_list:
        print("No new files to export")
        return
    
    print(f"Exporting {len(new_file_list)} new files")
    
    # Write to Azure SQL with retry logic
    jdbc_properties = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "batchsize": "10000",
        "numPartitions": "4",
        "encrypt": "true",
        "trustServerCertificate": "false"
    }
    
    max_attempts = 4
    for attempt in range(max_attempts):
        try:
            new_data_df.write.jdbc(
                url=jdbc_url,
                table="dbo.raw_enriched",
                mode="append",
                properties=jdbc_properties
            )
            print(f"Successfully exported {new_data_df.count()} rows")
            break
        except Exception as e:
            if attempt < max_attempts - 1:
                backoff = 5 * (2 ** attempt)
                print(f"Export attempt {attempt + 1} failed, retrying in {backoff}s: {str(e)}")
                time.sleep(backoff)
            else:
                print(f"Export failed after {max_attempts} attempts: {str(e)}")
                raise
    
    # Update tracking table
    for source_file in new_file_list:
        max_attempts = 4
        for attempt in range(max_attempts):
            try:
                spark.createDataFrame([(source_file,)], ["source_file"]) \
                    .write.jdbc(
                        url=jdbc_url,
                        table="dbo.raw_enriched_loaded",
                        mode="append",
                        properties=jdbc_properties
                    )
                break
            except Exception as e:
                if attempt < max_attempts - 1:
                    backoff = 5 * (2 ** attempt)
                    print(f"Tracking update attempt {attempt + 1} failed, retrying in {backoff}s: {str(e)}")
                    time.sleep(backoff)
                else:
                    print(f"Tracking update failed after {max_attempts} attempts: {str(e)}")
                    raise
    
    print(f"Successfully updated tracking table with {len(new_file_list)} files")

# Main execution
if __name__ == "__main__":
    spark = SparkSession.builder.appName("JDBC Export to Azure SQL").getOrCreate()
    
    # Configuration from Databricks secrets
    jdbc_url = f"jdbc:sqlserver://{spark.conf.get('azure.sql.server')}:1433;database={spark.conf.get('azure.sql.database')};encrypt=true;trustServerCertificate=false"
    username = spark.conf.get("azure.sql.username")
    password = spark.conf.get("azure.sql.password")
    
    # Silver table path
    silver_table_path = spark.conf.get("silver.table.path", "dbfs:/mnt/w3c-data/silver")
    
    # Execute export
    export_to_azure_sql(spark, silver_table_path, jdbc_url, username, password)
    
    spark.stop()
```

**Maven library configuration (to be added in Terraform Part B):**

```hcl
# In databricks job cluster configuration
maven_libraries = [
  {
    coordinates = "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11"
  }
]
```

**Acceptance Criteria:**

- `jdbc_export_azure.py` created with exact EXPORT_COLUMNS
- execute_ddl uses py4j gateway (not bare java.sql import)
- Tracking table pattern implemented with error 208 handling
- Retry logic implemented (4 attempts, exponential backoff)
- JDBC options configured correctly
- MSSQL JDBC driver specified as Maven library
- is_crawler cast from string to BIT implemented
- Exact RAW_ENRICHED_DDL used
- Exact TRACKING_DDL used
- Export tested with sample Silver data
- Azure SQL tables created: dbo.raw_enriched, dbo.raw_enriched_loaded
- Data exported successfully to Azure SQL
- Tracking table updated with source files
- Idempotency verified (re-run doesn't duplicate data)

**Phase Handoff Validation:**

```bash
# Verify Azure SQL tables exist
sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo'"

# Verify dbo.raw_enriched schema
sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "EXEC sp_help 'dbo.raw_enriched'"

# Verify data in dbo.raw_enriched
sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "SELECT COUNT(*) AS row_count FROM dbo.raw_enriched"

# Verify tracking table
sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "SELECT * FROM dbo.raw_enriched_loaded"

# Verify is_crawler is BIT type
sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'raw_enriched' AND COLUMN_NAME = 'is_crawler'"

# Verify Airflow container can query Azure SQL (integration checkpoint)
docker compose exec airflow-triggerer python -c "
import pyodbc, os
conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.getenv(\"AZURE_SQL_SERVER\")};DATABASE={os.getenv(\"AZURE_SQL_DB\")};UID={os.getenv(\"AZURE_SQL_USER\")};PWD={os.getenv(\"AZURE_SQL_PASSWORD\")};Encrypt=yes;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
cursor.execute('SELECT TOP 1 * FROM dbo.raw_enriched')
print(\"Airflow-to-AzureSQL Connection successful. Rows found:\", len(cursor.fetchall()))
conn.close()
"
```

**Failure Recovery Table:**

| Failure Mode | Detection | Recovery Action |
|--------------|-----------|-----------------|
| JDBC driver not found | Class.forName fails | Verify Maven library installed, check cluster configuration |
| DDL execution fails | execute_ddl raises exception | Check error code, handle 208 gracefully, verify permissions |
| Export fails on retry | All 4 attempts exhausted | Check Azure SQL serverless cold-start, verify network connectivity |
| Tracking table update fails | Duplicate key or permission error | Verify PRIMARY KEY constraint, check INSERT permissions |
| is_crawler cast fails | Data type mismatch | Verify source data format, adjust CASE WHEN logic |

---



### Phase 6 — Databricks Workflows + Terraform Part B

**Phase Goal:** Deploy Databricks Workflows orchestration with Bronze, Silver, and JDBC export tasks using Terraform Part B.

**Checklist:**

- [ ] Create `terraform/part_b/main.tf` with Databricks resources
- [ ] Create `terraform/part_b/variables.tf` with input variables
- [ ] Create `terraform/part_b/outputs.tf` with output values
- [ ] Create Databricks pipeline resource for Bronze
- [ ] Create Databricks pipeline resource for Silver
- [ ] Create Databricks job resource with 3 tasks
- [ ] Configure Task 1: DLT Bronze pipeline
- [ ] Configure Task 2: DLT Silver pipeline
- [ ] Configure Task 3: JDBC export Python task
- [ ] Add cluster configuration (Standard_DS3_v2, 1-2 workers)
- [ ] Add PyPI library: geoip2==5.0.1
- [ ] Add Maven library: mssql-jdbc
- [ ] Configure lifecycle ignore_changes for dev flexibility
- [ ] Run terraform init in Part B directory
- [ ] Run terraform providers lock
- [ ] Run terraform validate
- [ ] Run terraform plan
- [ ] Run terraform apply
- [ ] Verify Databricks Workflow created
- [ ] Test workflow run with sample data

**Code Scaffolds:**

**terraform/part_b/main.tf:**

```hcl
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.75.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.70"
    }
  }
  
  required_version = ">= 1.10.5, < 2.0"
}

provider "databricks" {
  host = var.databricks_host
  token = var.databricks_token
}

# Bronze DLT Pipeline
resource "databricks_pipeline" "bronze" {
  name        = "w3c-bronze-pipeline"
  description = "Bronze DLT pipeline for W3C log ingestion"
  
  cluster {
    label = "bronze_cluster"
    autoscale {
      min_workers = 1
      max_workers = 2
    }
    spark_version = "15.4.x-scala2.12"
    node_type_id  = "Standard_DS3_v2"
    data_security_mode = "SINGLE_USER"
  }
  
  libraries {
    notebook {
      path = "/Repos/w3c-etl-pipeline/airflow/spark/databricks/dlt_bronze.py"
    }
  }
  
  configuration = {
    "storage.account_name" = var.storage_account_name
  }
  
  lifecycle {
    ignore_changes = [cluster, continuous]
  }
}

# Silver DLT Pipeline
resource "databricks_pipeline" "silver" {
  name        = "w3c-silver-pipeline"
  description = "Silver DLT pipeline for GeoIP enrichment and computed fields"
  
  cluster {
    label = "silver_cluster"
    autoscale {
      min_workers = 1
      max_workers = 2
    }
    spark_version = "15.4.x-scala2.12"
    node_type_id  = "Standard_DS3_v2"
    data_security_mode = "SINGLE_USER"
    custom_library {
      pypi {
        package = "geoip2==5.0.1"
      }
    }
  }
  
  libraries {
    notebook {
      path = "/Repos/w3c-etl-pipeline/airflow/spark/databricks/dlt_silver.py"
    }
  }
  
  configuration = {
    "storage.account_name" = var.storage_account_name
    "geoip.city_db_path"   = "/dbfs/mnt/w3c-data/GeoLite2-City.mmdb"
    "geoip.asn_db_path"    = "/dbfs/mnt/w3c-data/GeoLite2-ASN.mmdb"
  }
  
  lifecycle {
    ignore_changes = [cluster, continuous]
  }
}

# Databricks Workflow Job
resource "databricks_job" "w3c_etl_workflow" {
  name = "w3c-etl-workflow"
  description = "W3C ETL workflow: Bronze -> Silver -> JDBC Export"
  
  workflow_tasks {
    task_key {
      description = "DLT Bronze Pipeline"
      pipeline_task {
        pipeline_id = databricks_pipeline.bronze.id
      }
    }
    depends_on {
      task_key = []
    }
  }
  
  workflow_tasks {
    task_key {
      description = "DLT Silver Pipeline"
      pipeline_task {
        pipeline_id = databricks_pipeline.silver.id
      }
    }
    depends_on {
      task_key = ["bronze"]
    }
  }
  
  workflow_tasks {
    task_key {
      description = "JDBC Export to Azure SQL"
      notebook_task {
        notebook_path = "/Repos/w3c-etl-pipeline/airflow/spark/databricks/jdbc_export_azure.py"
        base_parameters = {
          "azure.sql.server"   = var.azure_sql_server
          "azure.sql.database" = var.azure_sql_database
          "silver.table.path"  = "dbfs:/mnt/w3c-data/silver"
        }
      }
    }
    depends_on {
      task_key = ["silver"]
    }
  }
  
  job_cluster {
    job_cluster_key = "w3c_etl_cluster"
    new_cluster {
      spark_version = "15.4.x-scala2.12"
      node_type_id  = "Standard_DS3_v2"
      autoscale {
        min_workers = 1
        max_workers = 2
      }
      data_security_mode = "SINGLE_USER"
      custom_library {
        pypi {
          package = "geoip2==5.0.1"
        }
      }
      custom_library {
        maven {
          coordinates = "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11"
        }
      }
    }
  }
  
  schedule {
    quartz_cron_expression = "0 2 * * *"  # Daily at 2 AM UTC
    timezone_id            = "UTC"
  }
}
```

**terraform/part_b/variables.tf:**

```hcl
variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
  sensitive   = true
}

variable "databricks_token" {
  description = "Databricks PAT token"
  type        = string
  sensitive   = true
}

variable "storage_account_name" {
  description = "Storage account name"
  type        = string
}

variable "azure_sql_server" {
  description = "Azure SQL server FQDN"
  type        = string
}

variable "azure_sql_database" {
  description = "Azure SQL database name"
  type        = string
}
```

**terraform/part_b/outputs.tf:**

```hcl
output "bronze_pipeline_id" {
  description = "Bronze DLT pipeline ID"
  value       = databricks_pipeline.bronze.id
}

output "silver_pipeline_id" {
  description = "Silver DLT pipeline ID"
  value       = databricks_pipeline.silver.id
}

output "workflow_job_id" {
  description = "Databricks Workflow job ID"
  value       = databricks_job.w3c_etl_workflow.id
}
```

**terraform/part_b/environments/dev/terraform.tfvars:**

```hcl
databricks_host     = "https://<workspace-url>.azuredatabricks.net"
databricks_token    = "<personal-access-token>"
storage_account_name = "<storage-account-name-from-part-a>"
azure_sql_server    = "<server-fqdn-from-part-a>"
azure_sql_database  = "w3c-etl-db"
```

**Acceptance Criteria:**

- Terraform Part B directory structure created
- Bronze DLT pipeline resource configured
- Silver DLT pipeline resource configured
- Databricks Workflow job resource configured with 3 tasks
- Task dependencies: Bronze → Silver → JDBC Export
- Cluster configuration: Standard_DS3_v2, 1-2 workers
- PyPI library: geoip2==5.0.1
- Maven library: mssql-jdbc
- Lifecycle ignore_changes configured
- Terraform init, validate, plan, apply successful
- Databricks Workflow visible in workspace
- Workflow run tested with sample data
- All 3 tasks execute successfully
- Data flows end-to-end: Bronze → Silver → Azure SQL

**Phase Handoff Validation:**

```bash
# Verify pipelines
databricks pipelines list

# Capture workflow job ID to root .env.azure for Airflow DAGs and Tier 2 CI usage
WORKFLOW_ID=$(terraform output -raw workflow_job_id)
echo "DATABRICKS_WORKFLOW_ID=$WORKFLOW_ID" >> ../../.env.azure
source ../../.env.azure

# Verify workflow job
databricks jobs list

# Trigger workflow run
databricks jobs run-now --job-id <workflow-job-id>

# Monitor run status
databricks runs list --job-id <workflow-job-id>

# Verify Azure SQL data after run
sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "SELECT COUNT(*) FROM dbo.raw_enriched"
```

**Failure Recovery Table:**

| Failure Mode | Detection | Recovery Action |
|--------------|-----------|-----------------|
| Terraform apply fails | Resource creation error | Verify dependencies from Part A, check Databricks workspace access |
| Pipeline creation fails | Databricks API error | Verify notebook paths exist, check cluster configuration |
| Workflow job creation fails | Task dependency error | Verify task keys match, check depends_on configuration |
| Workflow run fails | Task execution error | Review task logs, verify library installations, check data paths |
| Task 3 (JDBC export) fails | Connection or write error | Verify Azure SQL credentials, check network connectivity |

---

### Phase 7 — export_dimensions_azure

**Phase Goal:** Create Airflow operator to build dimensional tables (dim_geolocation, dim_useragent) from Azure SQL and trigger dbt DAG via Dataset outlet.

**Checklist:**

- [ ] Create `airflow/dags/operators/export_dimensions_azure.py`
- [ ] Implement _build_dim_geolocation function
- [ ] Implement client_ip → ip column rename
- [ ] Implement _build_dim_useragent function
- [ ] Implement user_agents library integration
- [ ] Implement _write_dim_to_azure function
- [ ] Implement MERGE upsert on natural key
- [ ] Implement IF NOT EXISTS DDL pattern
- [ ] Implement SET IDENTITY_INSERT for -1 unknown rows
- [ ] Create Airflow Dataset outlet for dbt trigger
- [ ] Test dim table creation with sample data
- [ ] Verify MERGE upsert idempotency
- [ ] Verify Dataset outlet fires correctly

**Code Scaffolds:**

**airflow/dags/operators/export_dimensions_azure.py:**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import pyodbc
import sqlalchemy
from sqlalchemy import text
from user_agents import parse as ua_parse

# Dataset for triggering dbt DAG
DIMENSIONS_DATASET = Dataset("azure://w3c-etl/dimensions_ready")

EXPORT_COLUMNS = [
    "log_date", "log_time", "server_ip", "method", "uri_stem",
    "uri_query", "client_ip", "user_agent", "cookie", "referrer",
    "status", "sub_status", "win32_status", "bytes_sent", "bytes_recv",
    "server_port", "username", "time_taken", "source_file",
    "postcode", "page_category", "referrer_domain", "traffic_type",
    "is_crawler", "size_band",
    "country", "region", "city", "latitude", "longitude", "isp",
]

DIM_GEOLOCATION_DDL = """
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'dim_geolocation' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.dim_geolocation (
        geolocation_sk INT IDENTITY(1,1) PRIMARY KEY,
        ip VARCHAR(45) NOT NULL,
        country VARCHAR(100),
        region VARCHAR(100),
        city VARCHAR(100),
        latitude FLOAT,
        longitude FLOAT,
        postcode VARCHAR(20),
        isp VARCHAR(200),
        created_at DATETIME DEFAULT GETDATE(),
        updated_at DATETIME DEFAULT GETDATE()
    );
    
    CREATE INDEX idx_dim_geolocation_ip ON dbo.dim_geolocation(ip);
END
"""

DIM_USERAGENT_DDL = """
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'dim_useragent' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.dim_useragent (
        user_agent_sk INT IDENTITY(1,1) PRIMARY KEY,
        user_agent NVARCHAR(MAX) NOT NULL,
        browser_name VARCHAR(100),
        browser_version VARCHAR(50),
        operating_system VARCHAR(100),
        device_type VARCHAR(50),
        is_bot BIT DEFAULT 0,
        created_at DATETIME DEFAULT GETDATE(),
        updated_at DATETIME DEFAULT GETDATE()
    );
    
    CREATE INDEX idx_dim_useragent_user_agent ON dbo.dim_useragent(user_agent);
END
"""

def _build_dim_geolocation(engine):
    """Build dim_geolocation from dbo.raw_enriched."""
    with engine.connect() as conn:
        # Create table if not exists
        conn.execute(text(DIM_GEOLOCATION_DDL))
        conn.commit()
        
        # Insert -1 unknown row
        conn.execute(text("""
            IF NOT EXISTS (SELECT 1 FROM dbo.dim_geolocation WHERE geolocation_sk = -1)
            BEGIN
                SET IDENTITY_INSERT dbo.dim_geolocation ON;
                INSERT INTO dbo.dim_geolocation (geolocation_sk, ip, country, region, city, latitude, longitude, postcode, isp)
                VALUES (-1, '0.0.0.0', 'Unknown', 'Unknown', 'Unknown', NULL, NULL, NULL, 'Unknown');
                SET IDENTITY_INSERT dbo.dim_geolocation OFF;
            END
        """))
        conn.commit()
        
        # Extract unique geolocation data from raw_enriched
        # CRITICAL: Rename client_ip → ip (PK column is ip not client_ip)
        conn.execute(text("""
            INSERT INTO dbo.dim_geolocation (ip, country, region, city, latitude, longitude, postcode, isp)
            SELECT DISTINCT
                client_ip AS ip,
                country,
                region,
                city,
                latitude,
                longitude,
                postcode,
                isp
            FROM dbo.raw_enriched
            WHERE client_ip IS NOT NULL AND client_ip != '-'
        """))
        conn.commit()
        
        print(f"Inserted geolocation data")

def _build_dim_useragent(engine):
    """Build dim_useragent from dbo.raw_enriched."""
    with engine.connect() as conn:
        # Create table if not exists
        conn.execute(text(DIM_USERAGENT_DDL))
        conn.commit()
        
        # Insert -1 unknown row
        conn.execute(text("""
            IF NOT EXISTS (SELECT 1 FROM dbo.dim_useragent WHERE user_agent_sk = -1)
            BEGIN
                SET IDENTITY_INSERT dbo.dim_useragent ON;
                INSERT INTO dbo.dim_useragent (user_agent_sk, user_agent, browser_name, browser_version, operating_system, device_type, is_bot)
                VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 0);
                SET IDENTITY_INSERT dbo.dim_useragent OFF;
            END
        """))
        conn.commit()
        
        # Extract unique user agents from raw_enriched
        result = conn.execute(text("""
            SELECT DISTINCT user_agent
            FROM dbo.raw_enriched
            WHERE user_agent IS NOT NULL AND user_agent != '-'
        """))
        
        for row in result:
            user_agent = row[0]
            parsed_ua = ua_parse(user_agent)
            
            browser_name = parsed_ua.browser.family if parsed_ua.browser else 'Unknown'
            browser_version = parsed_ua.browser.version_string if parsed_ua.browser else 'Unknown'
            operating_system = parsed_ua.os.family if parsed_ua.os else 'Unknown'
            device_type = parsed_ua.device.family if parsed_ua.device else 'Unknown'
            is_bot = 1 if parsed_ua.is_bot else 0
            
            conn.execute(text("""
                INSERT INTO dbo.dim_useragent (user_agent, browser_name, browser_version, operating_system, device_type, is_bot)
                VALUES (:user_agent, :browser_name, :browser_version, :operating_system, :device_type, :is_bot)
            """), {
                "user_agent": user_agent,
                "browser_name": browser_name,
                "browser_version": browser_version,
                "operating_system": operating_system,
                "device_type": device_type,
                "is_bot": is_bot
            })
        
        conn.commit()
        print(f"Inserted user agent data")

def _write_dim_to_azure(engine, table_name, natural_key, data_dict):
    """MERGE upsert on natural key."""
    with engine.connect() as conn:
        # Build MERGE statement dynamically
        columns = list(data_dict.keys())
        values = list(data_dict.values())
        
        set_clause = ", ".join([f"target.{col} = source.{col}" for col in columns if col != natural_key])
        
        merge_sql = f"""
            MERGE INTO dbo.{table_name} AS target
            USING (SELECT :{', :'.join(columns)}) AS source ({', '.join(columns)})
            ON target.{natural_key} = source.{natural_key}
            WHEN MATCHED THEN
                UPDATE SET {set_clause}, updated_at = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(columns)}) VALUES ({', '.join([f':{col}' for col in columns])});
        """
        
        conn.execute(text(merge_sql), data_dict)
        conn.commit()

def export_dimensions_azure(**context):
    """Export dimensions from Azure SQL to dim tables."""
    # Build connection string
    server = context["azure_sql_server"]
    database = context["azure_sql_database"]
    username = context["azure_sql_user"]
    password = context["azure_sql_password"]
    
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
    
    engine = sqlalchemy.create_engine(conn_str)
    
    try:
        # Build dim_geolocation
        _build_dim_geolocation(engine)
        
        # Build dim_useragent
        _build_dim_useragent(engine)
        
        print("Dimensions export completed successfully")
        
    finally:
        engine.dispose()

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="export_dimensions_azure",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Export dimensions from Azure SQL to dim tables"
) as dag:
    
    export_dimensions_task = PythonOperator(
        task_id="export_dimensions",
        python_callable=export_dimensions_azure,
        op_kwargs={
            "azure_sql_server": "{{ var.value.AZURE_SQL_SERVER }}",
            "azure_sql_database": "{{ var.value.AZURE_SQL_DB }}",
            "azure_sql_user": "{{ var.value.AZURE_SQL_USER }}",
            "azure_sql_password": "{{ var.value.AZURE_SQL_PASSWORD }}",
        },
        outlets=[DIMENSIONS_DATASET]
    )
```

**Acceptance Criteria:**

- `export_dimensions_azure.py` created with operator
- _build_dim_geolocation implements client_ip → ip rename
- _build_dim_useragent implements user_agents parsing
- _write_dim_to_azure implements MERGE upsert
- IF NOT EXISTS DDL pattern implemented
- SET IDENTITY_INSERT for -1 unknown rows implemented
- Airflow Dataset outlet configured
- dim_geolocation table created with correct schema
- dim_useragent table created with correct schema
- -1 unknown rows inserted in both tables
- MERGE upsert tested for idempotency
- Dataset outlet fires and is visible in Airflow UI

**Phase Handoff Validation:**

```bash
# Verify dim tables exist
sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'dim_%'"

# Verify dim_geolocation schema
sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "EXEC sp_help 'dbo.dim_geolocation'"

# Verify dim_useragent schema
sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "EXEC sp_help 'dbo.dim_useragent'"

# Verify -1 unknown rows
sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "SELECT * FROM dbo.dim_geolocation WHERE geolocation_sk = -1"
sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "SELECT * FROM dbo.dim_useragent WHERE user_agent_sk = -1"

# Verify data in dim tables
sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "SELECT COUNT(*) FROM dbo.dim_geolocation"
sqlcmd -S $AZURE_SQL_SERVER -d $AZURE_SQL_DB -U $AZURE_SQL_USER -P $AZURE_SQL_PASSWORD -Q "SELECT COUNT(*) FROM dbo.dim_useragent"

# Test idempotency (re-run operator)
# Verify no duplicate rows
```

**Failure Recovery Table:**

| Failure Mode | Detection | Recovery Action |
|--------------|-----------|-----------------|
| Connection fails | pyodbc error | Verify credentials, check firewall rules, test connectivity |
| DDL execution fails | SQL error | Check table existence, verify permissions, review DDL syntax |
| client_ip rename fails | Column name error | Verify column rename in SELECT statement, check target schema |
| user_agents parsing fails | Import error or parse error | Verify user-agents library installed, handle parse exceptions |
| MERGE upsert fails | SQL syntax error | Verify MERGE syntax, check natural key, review column mapping |
| Dataset outlet doesn't fire | DAG not triggered | Verify Dataset URI, check Airflow configuration, review outlet definition |

---

### Phase 8a — dbt T-SQL Macros + Simple Model Migration

**Phase Goal:** Create T-SQL compatibility macros and migrate simple dbt models with casts, EXTRACT, TO_CHAR, and boolean-to-int conversions.

**Checklist:**

- [ ] Create `macros/t_sql_compat.sql` macro file
- [ ] Implement tsql_cast macro (replace :: casts)
- [ ] Implement tsql_datepart macro (replace EXTRACT)
- [ ] Implement tsql_month_name macro (replace TO_CHAR for month)
- [ ] Implement tsql_day_name macro (replace TO_CHAR for day)
- [ ] Implement tsql_dow macro (replace EXTRACT dow)
- [ ] Implement tsql_format_date macro (replace TO_CHAR)
- [ ] Migrate staging models with inline T-SQL conditionals
- [ ] Replace :: casts with tsql_cast macro
- [ ] Replace EXTRACT with tsql_datepart macro
- [ ] Replace TO_CHAR with tsql_format_date macro
- [ ] Handle boolean to int conversions
- [ ] Preserve PostgreSQL dialect in {% else %} branch
- [ ] Test dbt compile with w3c profile (PostgreSQL)
- [ ] Test dbt compile with w3c_azure profile (Azure SQL)

**Code Scaffolds:**

**macros/t_sql_compat.sql:**

```sql
{% macro tsql_cast(field, type) -%}
  {% if target.type == 'sqlserver' -%}
    CAST({{ field }} AS {{ type }})
  {%- else -%}
    {{ field }}::{{ type }}
  {%- endif %}
{%- endmacro %}

{% macro tsql_datepart(part, field) -%}
  {% if target.type == 'sqlserver' -%}
    DATEPART({{ part }}, {{ field }})
  {%- else -%}
    EXTRACT({{ part }} FROM {{ field }})
  {%- endif %}
{%- endmacro %}

{% macro tsql_month_name(field) -%}
  {% if target.type == 'sqlserver' -%}
    DATENAME(month, {{ field }})
  {%- else -%}
    TO_CHAR({{ field }}, 'Month')
  {%- endif %}
{%- endmacro %}

{% macro tsql_day_name(field) -%}
  {% if target.type == 'sqlserver' -%}
    DATENAME(weekday, {{ field }})
  {%- else -%}
    TO_CHAR({{ field }}, 'Day')
  {%- endif %}
{%- endmacro %}

{% macro tsql_dow(field) -%}
  {% if target.type == 'sqlserver' -%}
    DATEPART(weekday, {{ field }}) - 1
  {%- else -%}
    EXTRACT(dow FROM {{ field }})
  {%- endif %}
{%- endmacro %}

{% macro tsql_format_date(field, format) -%}
  {% if target.type == 'sqlserver' -%}
    {% if format == 'YYYY-MM-DD' -%}
      FORMAT({{ field }}, 'yyyy-MM-dd')
    {%- elif format == 'YYYY-MM' -%}
      FORMAT({{ field }}, 'yyyy-MM')
    {%- elif format == 'YYYY' -%}
      FORMAT({{ field }}, 'yyyy')
    {%- else -%}
      FORMAT({{ field }}, '{{ format }}')
    {%- endif %}
  {%- else -%}
    TO_CHAR({{ field }}, '{{ format }}')
  {%- endif %}
{%- endmacro %}

{% macro tsql_split_part(field, delimiter, part) -%}
  {% if target.type == 'sqlserver' -%}
    CASE
      WHEN CHARINDEX('{{ delimiter }}', {{ field }}) = 0 THEN {{ field }}
      ELSE SUBSTRING(
        {{ field }},
        CASE {{ part }}
          WHEN 1 THEN 1
          ELSE CHARINDEX('{{ delimiter }}', {{ field }}) + 1
        END,
        CASE {{ part }}
          WHEN 1 THEN CHARINDEX('{{ delimiter }}', {{ field }}) - 1
          WHEN 2 THEN LEN({{ field }}) - CHARINDEX('{{ delimiter }}', {{ field }})
          ELSE 0
        END
      )
    END
  {%- else -%}
    SPLIT_PART({{ field }}, '{{ delimiter }}', {{ part }})
  {%- endif %}
{%- endmacro %}

{% macro tsql_regexp_replace(field, pattern, replacement) -%}
  {% if target.type == 'sqlserver' -%}
    -- Azure SQL doesn't have REGEXP_REPLACE, use manual string operations
    REPLACE(REPLACE({{ field }}, '{{ pattern }}', '{{ replacement }}'), '{{ pattern }}', '{{ replacement }}')
  {%- else -%}
    REGEXP_REPLACE({{ field }}, '{{ pattern }}', '{{ replacement }}')
  {%- endif %}
{%- endmacro %}
```

**Example model migration (staging/fact_webrequest.sql):**

```sql
{{ config(
    materialized='table',
    schema='dbt_staging'
) }}

WITH source AS (
    SELECT * FROM {{ source('raw_enriched', 'raw_enriched') }}
),

renamed AS (
    SELECT
        -- Date/time fields
        log_date,
        log_time,
        {{ tsql_cast('log_time', 'TIME') }} AS log_time_parsed,
        
        -- Server fields
        server_ip,
        server_port,
        method,
        
        -- URI fields
        uri_stem,
        uri_query,
        
        -- Client fields
        client_ip,
        user_agent,
        cookie,
        referrer,
        
        -- Response fields
        {{ tsql_cast('status', 'INT') }} AS status,
        {{ tsql_cast('sub_status', 'INT') }} AS sub_status,
        {{ tsql_cast('win32_status', 'INT') }} AS win32_status,
        {{ tsql_cast('bytes_sent', 'BIGINT') }} AS bytes_sent,
        {{ tsql_cast('bytes_recv', 'BIGINT') }} AS bytes_recv,
        {{ tsql_cast('time_taken', 'BIGINT') }} AS time_taken,
        
        -- User fields
        username,
        
        -- Enrichment fields
        postcode,
        page_category,
        referrer_domain,
        traffic_type,
        {{ tsql_cast('is_crawler', 'INT') }} AS is_crawler,
        size_band,
        
        -- Geo fields
        country,
        region,
        city,
        latitude,
        longitude,
        isp,
        
        -- Metadata
        source_file
    FROM source
)

SELECT * FROM renamed
```

**Example model migration (staging/dim_date.sql):**

```sql
{{ config(
    materialized='table',
    schema='dbt_staging'
) }}

WITH date_range AS (
    SELECT DISTINCT log_date FROM {{ source('raw_enriched', 'raw_enriched') }}
),

date_dimensions AS (
    SELECT
        log_date AS date,
        {{ tsql_datepart('year', 'log_date') }} AS year,
        {{ tsql_datepart('month', 'log_date') }} AS month,
        {{ tsql_month_name('log_date') }} AS month_name,
        {{ tsql_datepart('day', 'log_date') }} AS day,
        {{ tsql_day_name('log_date') }} AS day_name,
        {{ tsql_dow('log_date') }} AS day_of_week,
        CASE
            WHEN {{ tsql_dow('log_date') }} IN (0, 6) THEN 1
            ELSE 0
        END AS is_weekend,
        {{ tsql_format_date('log_date', 'YYYY-MM-DD') }} AS date_id,
        {{ tsql_format_date('log_date', 'YYYY-MM') }} AS month_id,
        {{ tsql_format_date('log_date', 'YYYY') }} AS year_id
    FROM date_range
)

SELECT * FROM date_dimensions
```

**Example boolean to int conversion:**

```sql
-- In staging models
{{ tsql_cast('is_crawler', 'INT') }} AS is_crawler,

-- Or using CASE WHEN for more control
CASE
    WHEN is_crawler = 'true' THEN 1
    WHEN is_crawler = 'false' THEN 0
    ELSE 0
END AS is_crawler_int,
```

**Acceptance Criteria:**

- `macros/t_sql_compat.sql` created with all required macros
- tsql_cast macro implemented
- tsql_datepart macro implemented
- tsql_month_name macro implemented
- tsql_day_name macro implemented
- tsql_dow macro implemented
- tsql_format_date macro implemented
- tsql_split_part macro implemented
- tsql_regexp_replace macro implemented
- All staging models migrated with inline T-SQL conditionals
- :: casts replaced with tsql_cast
- EXTRACT replaced with tsql_datepart
- TO_CHAR replaced with tsql_format_date
- Boolean to int conversions implemented
- PostgreSQL dialect preserved in {% else %} branches
- `dbt compile --profile w3c` passes (PostgreSQL)
- `dbt compile --profile w3c_azure` passes (Azure SQL)

**Phase Handoff Validation:**

```bash
# Test PostgreSQL compile
dbt compile --profile w3c

# Test Azure SQL compile
dbt compile --profile w3c_azure

# Verify compiled SQL for Azure SQL
cat target/compiled/dbt_staging/fact_webrequest.sql

# Verify macros are being used
grep -r "tsql_cast" target/compiled/
grep -r "tsql_datepart" target/compiled/
```

**Failure Recovery Table:**

| Failure Mode | Detection | Recovery Action |
|--------------|-----------|-----------------|
| Macro not found | dbt compile error | Verify macro file path, check macro naming, ensure macros/ directory exists |
| T-SQL syntax error | dbt compile fails | Review macro implementation, test T-SQL syntax manually |
| PostgreSQL branch breaks | w3c profile fails | Verify {% else %} branch preserves original syntax |
| Boolean conversion fails | Type mismatch error | Use CASE WHEN instead of cast, verify source data format |

---

### Phase 8b — dbt Complex Model Migration

**Phase Goal:** Migrate complex dbt models with regex, generate_series, PERCENTILE_CONT, and advanced T-SQL patterns.

**Checklist:**

- [ ] Implement generate_series T-SQL equivalent
- [ ] Implement PERCENTILE_CONT T-SQL equivalent
- [ ] Implement CREATE INDEX IF NOT EXISTS T-SQL equivalent
- [ ] Migrate mart models with complex aggregations
- [ ] Handle regex patterns with manual string operations
- [ ] Handle window functions with explicit OVER clauses
- [ ] Test complex model compilation
- [ ] Verify T-SQL syntax compatibility

**Code Scaffolds:**

**Additional T-SQL macros for complex patterns:**

```sql
-- Add to macros/t_sql_compat.sql

{% macro tsql_generate_series(start, end, step=1) -%}
  {% if target.type == 'sqlserver' -%}
    -- Azure SQL compat level 160 supports GENERATE_SERIES
    GENERATE_SERIES({{ start }}, {{ end }}, {{ step }})
  {%- else -%}
    generate_series({{ start }}, {{ end }}, {{ step }})
  {%- endif %}
{%- endmacro %}

{% macro tsql_percentile_cont(percent, field, within_group) -%}
  {% if target.type == 'sqlserver' -%}
    PERCENTILE_CONT({{ percent }}) WITHIN GROUP (ORDER BY {{ field }}) OVER ()
  {%- else -%}
    PERCENTILE_CONT({{ percent }}) WITHIN GROUP (ORDER BY {{ field }}) OVER ({{ within_group }})
  {%- endif %}
{%- endmacro %}

{% macro tsql_create_index_if_not_exists(table_name, index_name, columns, unique=false) -%}
  {% if target.type == 'sqlserver' -%}
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{{ index_name }}' AND object_id = OBJECT_ID('{{ table_name }}'))
    BEGIN
      CREATE {% if unique %}UNIQUE {% endif %}INDEX {{ index_name }} ON {{ table_name }} ({{ columns }})
    END
  {%- else -%}
    CREATE INDEX IF NOT EXISTS {{ index_name }} ON {{ table_name }} ({{ columns }})
  {%- endif %}
{%- endmacro %}

{% macro tsql_case_insensitive_like(field, pattern) -%}
  {% if target.type == 'sqlserver' -%}
    {{ field }} LIKE '{{ pattern }}' COLLATE SQL_Latin1_General_CP1_CI_AS
  {%- else -%}
    {{ field }} ~* '{{ pattern }}'
  {%- endif %}
{%- endmacro %}
```

**Example complex model migration (marts/mart_page_performance.sql):**

```sql
{{ config(
    materialized='table',
    schema='dbt_marts'
) }}

WITH page_stats AS (
    SELECT
        uri_stem,
        {{ tsql_cast('status', 'INT') }} AS status,
        {{ tsql_cast('bytes_sent', 'BIGINT') }} AS bytes_sent,
        {{ tsql_cast('time_taken', 'BIGINT') }} AS time_taken,
        {{ tsql_cast('is_crawler', 'INT') }} AS is_crawler
    FROM {{ ref('fact_webrequest') }}
),

page_metrics AS (
    SELECT
        uri_stem,
        COUNT(*) AS request_count,
        SUM(CASE WHEN status = 404 THEN 1 ELSE 0 END) AS error_count,
        AVG(bytes_sent) AS avg_bytes_sent,
        {{ tsql_percentile_cont('0.5', 'bytes_sent', '') }} AS median_bytes_sent,
        AVG(time_taken) AS avg_time_taken,
        {{ tsql_percentile_cont('0.5', 'time_taken', '') }} AS median_time_taken,
        SUM(CASE WHEN is_crawler = 1 THEN 1 ELSE 0 END) AS crawler_count
    FROM page_stats
    GROUP BY uri_stem
),

performance_bands AS (
    SELECT
        uri_stem,
        request_count,
        error_count,
        avg_bytes_sent,
        median_bytes_sent,
        avg_time_taken,
        median_time_taken,
        crawler_count,
        CASE
            WHEN avg_time_taken < 100 THEN 'Fast'
            WHEN avg_time_taken < 500 THEN 'Medium'
            WHEN avg_time_taken < 1000 THEN 'Slow'
            ELSE 'Very Slow'
        END AS performance_band
    FROM page_metrics
)

SELECT * FROM performance_bands
```

**Example generate_series usage (for time buckets):**

```sql
WITH time_buckets AS (
    SELECT
        value AS hour_bucket
    FROM {{ tsql_generate_series(0, 23) }}
),

hourly_stats AS (
    SELECT
        tb.hour_bucket,
        COUNT(*) AS request_count
    FROM time_buckets tb
    LEFT JOIN {{ ref('fact_webrequest') }} f
        ON {{ tsql_datepart('hour', 'log_time_parsed') }} = tb.hour_bucket
    GROUP BY tb.hour_bucket
)

SELECT * FROM hourly_stats
```

**Acceptance Criteria:**

- generate_series T-SQL equivalent implemented
- PERCENTILE_CONT T-SQL equivalent implemented
- CREATE INDEX IF NOT EXISTS T-SQL equivalent implemented
- Case-insensitive LIKE macro implemented
- All mart models migrated with complex patterns
- Regex patterns handled with manual string operations
- Window functions use explicit OVER clauses
- `dbt compile --profile w3c_azure` passes for all models
- T-SQL syntax verified as compatible with Azure SQL

**Phase Handoff Validation:**

```bash
# Compile all models
dbt compile --profile w3c_azure

# Verify complex model compilation
cat target/compiled/dbt_marts/mart_page_performance.sql

# Test generate_series macro
dbt run-operation test_generate_series --profile w3c_azure
```

**Failure Recovery Table:**

| Failure Mode | Detection | Recovery Action |
|--------------|-----------|-----------------|
| generate_series not supported | SQL syntax error | Verify Azure SQL compat level 160, use manual CTE if needed |
| PERCENTILE_CONT syntax error | Window function error | Add explicit OVER () clause, verify field order |
| Index creation fails | Permission error | Check CREATE INDEX permissions, use separate migration script |
| Regex pattern fails | String function error | Replace with manual string operations (REPLACE, CHARINDEX, SUBSTRING) |

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

**Hosting options (document both, implement simpler one):**

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

**Option 2: Azure Static Web Apps (more complex)**

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

**Failure Recovery Table:**

| Failure Mode | Detection | Recovery Action |
|--------------|-----------|-----------------|
| dbt docs generate fails | Subprocess error | Verify dbt-core installation, check profile configuration, review model syntax |
| Source freshness fails | Freshness check error | Verify source configuration, check loaded_at_field, review freshness thresholds |
| Docs export fails | File copy error | Verify output path permissions, check disk space, retry export |
| CSV export fails | connection/query error | Verify pyodbc configuration, check table schema mismatch, confirm exact 18 tables exist |
| GitHub Pages deployment fails | GitHub Actions error | Verify GITHUB_TOKEN, check publish_dir, review branch permissions |
| Azure Static Web Apps fails | Azure CLI error | Verify resource group, check source configuration, review location |

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
          az storage blob upload \
            --container-name raw-logs \
            --file data/samples/18-field-sample.log \
            --name ci-test/18-field-sample.log \
            --account-name ${{ secrets.STORAGE_ACCOUNT_NAME }}
      
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

**Failure Recovery Table:**

| Failure Mode | Detection | Recovery Action |
|--------------|-----------|-----------------|
| Tier 1 fails on push | GitHub Actions error | Review lint/test failures, fix code, push again |
| Tier 2 approval not granted | Workflow stuck | Review approver permissions, add additional approvers |
| Tier 2 secrets missing | Authentication error | Add secrets to GitHub repository, verify secret names |
| Databricks Workflow trigger fails | API error | Verify DATABRICKS_TOKEN, check WORKFLOW_ID, review API endpoint |
| Azure SQL query fails | Connection error | Verify SQL credentials, check firewall rules, test connectivity |
| CSV export assertion fails | File count mismatch | Review export logic, verify directory path, check file generation |
| catalog.json validation fails | JSON error | Review dbt docs generation, verify output path, check JSON structure |

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

**Failure Recovery Table:**

| Failure Mode | Detection | Recovery Action |
|--------------|-----------|-----------------|
| Prometheus not scraping | No targets in Prometheus UI | Verify StatsD exporter configuration, check network connectivity |
| Grafana dashboard not loading | Dashboard error | Verify datasource configuration, check Prometheus connection |
| Budget alerts not firing | No email received | Verify alert configuration, check notification email, test with manual spend |
| Databricks alert not firing | Pipeline failure not detected | Verify metric name, check alert condition, review resource scope |

---

### Phase 11 — Cost Management and Teardown Documentation

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
- Classic DLT clusters (not serverless)
- Auto-scale: 1-2 workers (Standard_DS3_v2)
- Cluster auto-termination after 30 minutes idle
- Use spot instances for non-critical workloads (future enhancement)

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

**Failure Recovery Table:**

| Failure Mode | Detection | Recovery Action |
|--------------|-----------|-----------------|
| Terraform destroy fails | Resource dependency error | Manually delete stuck resources via Azure portal, retry destroy |
| Service principal deletion fails | Permission error | Verify Azure AD permissions, use Global Admin account |
| GitHub secrets deletion fails | UI error | Verify repository admin permissions, delete secrets via API |

---

### Phase 12 — Documentation and README Update

**Phase Goal:** Update project README with architecture diagram, deployment instructions, badges, and links to documentation.

**Checklist:**

- [ ] Update README.md with single-pipeline architecture diagram
- [ ] Add deployment section with Phase 0-14 summary
- [ ] Add badges (CI status, docs, license)
- [ ] Add technology stack section
- [ ] Add cost management section link
- [ ] Add teardown section link
- [ ] Update architecture diagram (remove dual-path reference)
- [ ] Add troubleshooting section
- [ ] Add contributing guidelines
- [ ] Verify all links are valid

**Code Scaffolds:**

**README.md updates (key sections):**

```markdown
# W3C ETL Pipeline

[![CI Tier 1](https://github.com/username/w3c-etl-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/username/w3c-etl-pipeline/actions/workflows/ci.yml)
[![CI Tier 2](https://github.com/username/w3c-etl-pipeline/actions/workflows/ci-integration.yml/badge.svg)](https://github.com/username/w3c-etl-pipeline/actions/workflows/ci-integration.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Architecture

This project implements a cloud-native ETL pipeline for processing W3C web logs using Databricks Delta Live Tables, Unity Catalog, Azure SQL, and dbt.

\`\`\`
W3C Log Files → ADLS Gen2 → DLT Bronze → DLT Silver → Azure SQL → dbt → Power BI
\`\`\`

See [plans/azure-cloud-native-single-pipeline.md](plans/azure-cloud-native-single-pipeline.md) for detailed architecture.

## Technology Stack

- **Azure Data Lake Storage Gen2** - Raw log storage and Delta tables
- **Databricks** - ETL execution engine with Delta Live Tables
- **Unity Catalog** - Governance layer
- **Azure SQL Database (Serverless)** - Analytics warehouse
- **Apache Airflow** - Pipeline orchestration
- **dbt** - Transformation layer
- **MaxMind GeoLite2** - IP geolocation

- **Grafana + Prometheus** - Monitoring

## Deployment

See [Deployment Guide](docs/deployment.md) for step-by-step instructions.

Quick start:
1. Complete [Phase 0 Prerequisites](plans/azure-cloud-native-single-pipeline.md#phase-0--prerequisites)
2. Deploy infrastructure with [Phase 1-2](plans/azure-cloud-native-single-pipeline.md#phase-1--terraform-part-a-core-infrastructure)
3. Deploy DLT pipelines with [Phase 3-4](plans/azure-cloud-native-single-pipeline.md#phase-3--dlt-bronze-pipeline)
4. Configure JDBC export with [Phase 5](plans/azure-cloud-native-single-pipeline.md#phase-5--jdbc-export-from-silver-to-azure-sql)
5. Complete remaining phases 6-14

## Cost Management

See [Cost Management Guide](docs/cost-management.md) for cost optimization strategies and budget alert configuration.

## Teardown

See [Teardown Guide](docs/teardown.md) for complete resource cleanup instructions.

## Troubleshooting

See [Troubleshooting Guide](docs/troubleshooting.md) for common issues and solutions.
```

**Acceptance Criteria:**

- README.md updated with single-pipeline architecture
- Deployment section added with Phase 0-14 summary
- Badges added (CI status, docs, license)
- Technology stack section added
- Cost management section link added
- Teardown section link added
- Architecture diagram updated (no dual-path reference)
- Troubleshooting section added
- Contributing guidelines added
- All links verified as valid

**Phase Handoff Validation:**

```bash
# Verify README updates
cat README.md

# Verify links
markdown-link-check README.md
```

**Failure Recovery Table:**

| Failure Mode | Detection | Recovery Action |
|--------------|-----------|-----------------|
| Link check fails | Broken link error | Update link URLs, verify file paths |
| Badge not displaying | Badge URL error | Verify GitHub Actions workflow names, update badge URLs |

---

### Phase 13 — Final Verification

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
az storage blob upload \
  --container-name raw-logs \
  --file data/samples/18-field-sample.log \
  --name e2e-test/18-field-sample.log \
  --account-name $STORAGE_ACCOUNT_NAME

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

**Failure Recovery Table:**

| Failure Mode | Detection | Recovery Action |
|--------------|-----------|-----------------|
| Bronze pipeline fails | DLT error | Review logs, check Auto Loader configuration, verify data format |
| Silver pipeline fails | DLT error | Review GeoIP UDFs, check database files, verify computed field logic |
| JDBC export fails | Connection error | Verify Azure SQL credentials, check network connectivity, review retry logic |
| dbt run fails | Compilation error | Review T-SQL macros, check model syntax, verify Azure SQL compatibility |
| CSV export count mismatch | File count error | Review export logic, verify directory path, check dbt model outputs |
| Header validation fails | Schema mismatch | Compare headers to baseline, adjust model column ordering |

---

## Risk Register

| Risk | Impact | Probability | Mitigation Strategy |
|------|--------|-------------|---------------------|
| Azure credit exhaustion before completion | High | Medium | Budget alerts at $50/$100, daily cost monitoring, auto-pause on Azure SQL |
| Databricks Premium tier cost overruns | High | Medium | Cluster auto-termination, limit worker count, monitor cluster runtime |
| DLT pipeline idempotency issues | Medium | Medium | ROW_NUMBER deduplication, tracking table pattern, full_refresh mode testing |
| GeoIP database license expiration | Medium | Low | Monitor license validity, set renewal reminders, use free tier |
| T-SQL migration syntax errors | High | Medium | Comprehensive testing of all macros, Azure SQL compat level verification |
| dbt docs generation failure | Low | Low | Separate task in workflow, error handling, manual fallback |

| CI/CD Tier 2 approval delays | Low | Medium | Multiple approvers, clear documentation, automated retry logic |
| Service principal credential compromise | High | Low | Regular rotation (quarterly), monitoring for unusual activity, immediate revocation if compromised |
| Terraform state corruption | High | Low | Azure Blob Storage backend, regular state backups, state locking |
| Power BI semantic contract violations | High | Medium | Header validation, DAX field dependency checks, baseline comparison |
| MaxMind GeoIP API rate limits | Medium | Low | Local database files (not API), no rate limit concerns |
| Azure SQL serverless cold-start delays | Low | High | Retry logic with exponential backoff, warm-up queries, monitoring |
| Databricks CLI v2+ syntax errors | Medium | Low | Documentation review, CLI command validation, testing in dev environment |

---

## Acceptance Criteria (Definition of Done)

### Infrastructure

- [ ] Terraform Part A deployed successfully (resource groups, networking, storage, Databricks workspace, Azure SQL)
- [ ] Terraform Part B deployed successfully (DLT pipelines, Databricks Workflows)
- [ ] Terraform remote state backend configured (Azure Blob Storage)
- [ ] Unity Catalog created with bronze/silver/gold schemas
- [ ] Databricks secret scope `w3c-etl-pipeline` configured
- [ ] Budget alerts configured ($50 warning, $100 hard cap)
- [ ] All resources visible and accessible in Azure portal

### DLT Pipelines

- [ ] Bronze DLT pipeline deployed and operational
- [ ] Silver DLT pipeline deployed and operational
- [ ] W3C parser handles both 14-field and 18-field IIS formats
- [ ] Auto Loader configured with binaryFile format
- [ ] Quality expectations (@dlt.expect_or_drop) implemented
- [ ] ROW_NUMBER deduplication for idempotency
- [ ] Bronze table partitioned by log_date
- [ ] Silver table with 31 columns (25 core + 6 geo)
- [ ] MaxMind GeoLite2 enrichment (7 UDFs) working
- [ ] Computed fields (5 UDFs) working


### Azure Integration

- [ ] JDBC export from Silver to Azure SQL operational
- [ ] Tracking table pattern implemented (dbo.raw_enriched_loaded)
- [ ] SQL Server error 208 handling via error-code extraction
- [ ] Retry logic (4 attempts, exponential backoff)
- [ ] is_crawler BIT cast from string
- [ ] Exact RAW_ENRICHED_DDL schema (31 columns)
- [ ] export_dimensions_azure operator functional
- [ ] dim_geolocation table created with client_ip → ip rename
- [ ] dim_useragent table created with UA parsing
- [ ] MERGE upsert on natural key
- [ ] -1 unknown rows in both dim tables
- [ ] Airflow Dataset outlet firing correctly

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
Phase 0  → Phase 1  → Phase 2  → Phase 3  → Phase 4  → Phase 5
(Prereqs) (Infra A) (Verify)  (Bronze)  (Silver)  (JDBC)
    ↓         ↓         ↓         ↓         ↓         ↓
    └─────────┴─────────┴─────────┴─────────┴─────────┘
                          ↓
Phase 6  → Phase 7  → Phase 8a → Phase 8b → Phase 8c
(Workflows) (Dims)   (Macros) (Complex) (Docs)
    ↓         ↓         ↓         ↓         ↓
    └─────────┴─────────┴─────────┴─────────┘
                          ↓
Phase 9  → Phase 10 → Phase 11 → Phase 12 → Phase 13
(CI/CD)  (Monitor) (Cost)    (Docs)    (E2E)
```

---

## Resume Lines

### Primary Data Engineer Variant

"Designed and implemented a cloud-native W3C log ETL platform on Azure using Databricks Delta Live Tables, Unity Catalog, and Azure SQL. Built Bronze/Silver DLT pipelines with custom W3C parsing, MaxMind GeoIP enrichment, and data quality checks. Orchestrated via Airflow with Databricks Workflows integration, deployed dimensional models with dbt (T-SQL migration), and automated CI/CD with split-tier GitHub Actions. Delivered 18 Power BI-ready CSV exports with end-to-end monitoring and $100 cost controls."

### IaC/DevOps Angle Variant

"Architected Azure infrastructure for a W3C log ETL platform using Terraform with remote state backend (Azure Blob Storage). Deployed Databricks Premium workspace with Unity Catalog, Azure SQL serverless database, and ADLS Gen2 storage. Implemented Databricks Workflows orchestration with Bronze/Silver DLT pipelines and JDBC export. Built dbt transformation layer with T-SQL macros for Azure SQL compatibility. Configured split-tier CI/CD with GitHub Actions (Tier 1: every push, Tier 2: nightly integration with manual approval). Implemented cost controls with $100 budget cap and automated teardown procedures."

### Full-Stack Data Platform Engineer Variant

"End-to-end data platform ownership: from raw W3C log ingestion to Power BI analytics. Built cloud-native ETL on Azure (Databricks DLT, Unity Catalog, Azure SQL) with custom W3C parsing, MaxMind GeoIP enrichment, and data quality checks. Implemented dimensional modeling with dbt (T-SQL migration for Azure SQL), Airflow orchestration with Dataset triggers, and Grafana/Prometheus monitoring. Delivered 18 Power BI-ready CSV exports with semantic contract validation. Automated CI/CD with split-tier GitHub Actions and implemented cost management with $100 budget controls and automated teardown procedures."
