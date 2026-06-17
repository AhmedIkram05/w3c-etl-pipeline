# W3C Web Logs ETL Pipeline

> Serverless Databricks DLT ingests 93 W3C IIS log files through a Bronze → Silver medallion architecture in Unity Catalog, exports 153,377 enriched rows to Azure SQL in 45 seconds, transforms via dbt (16 models, dual-dialect T-SQL/PostgreSQL) into a star schema, and serves 18 Power BI‑ready CSV exports - all orchestrated by Apache Airflow with Terraform‑managed infrastructure, OIDC‑secured CI/CD, and Grafana observability. A Docker Compose stack mirrors the pipeline locally for development and CI.

<p align="center">
  <img src="https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&labelColor=000000&logo=microsoftazure" alt="Azure">
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&labelColor=000000&logo=databricks" alt="Databricks">
  <img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&labelColor=000000&logo=dbt" alt="dbt">
  <img src="https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&labelColor=000000&logo=apache-airflow" alt="Airflow">
  <img src="https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&labelColor=000000&logo=terraform" alt="Terraform">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&labelColor=000000&logo=python" alt="Python">
  <img src="https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&labelColor=000000&logo=powerbi" alt="Power BI">
  <img src="https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&labelColor=000000&logo=apachespark" alt="Spark">
  <img src="https://img.shields.io/badge/Delta_Lake-4AB197?style=for-the-badge&labelColor=000000&logo=delta" alt="Delta Lake">
  <img src="https://img.shields.io/badge/SQL_Server-CC2927?style=for-the-badge&labelColor=000000&logo=microsoftsqlserver" alt="SQL Server">
  <img src="https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&labelColor=000000&logo=postgresql" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&labelColor=000000&logo=redis" alt="Redis">
  <img src="https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&labelColor=000000&logo=grafana" alt="Grafana">
  <img src="https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&labelColor=000000&logo=prometheus" alt="Prometheus">
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&labelColor=000000&logo=docker" alt="Docker">
  <img src="https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&labelColor=000000&logo=githubactions" alt="GitHub Actions">
  <img src="https://img.shields.io/badge/pytest-0A9EDC?style=for-the-badge&labelColor=000000&logo=pytest" alt="pytest">
</p>

<p align="center">
  <a href="https://github.com/AhmedIkram05/w3c-etl-pipeline/actions/workflows/ci.yml"><img src="https://github.com/AhmedIkram05/w3c-etl-pipeline/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://github.com/AhmedIkram05/w3c-etl-pipeline/actions/workflows/cd.yml/badge.svg" alt="CD">
  <a href="https://codecov.io/gh/AhmedIkram05/w3c-etl-pipeline"><img src="https://codecov.io/gh/AhmedIkram05/w3c-etl-pipeline/branch/main/graph/badge.svg" alt="Codecov"></a>
</p>

<p align="center">
  <a href="https://app.powerbi.com/reportEmbed?reportId=41d525b8-b808-4750-88ba-cb31dbbba958&autoAuth=true&ctid=ae323139-093a-4d2a-81a6-5d334bcd9019">
    <img src="docs/media/summary.png" alt="W3C ETL Dashboard" width="800"/>
  </a>
  <br/>
  <em>Click the preview above to open the live 7-page Power BI dashboard</em>
</p>

---

<details>
<summary><b>Table of Contents</b> (click to expand)</summary>

- [Architecture Overview](#architecture-overview)
- [Engineering Highlights](#engineering-highlights)
- [Key Metrics at a Glance](#key-metrics-at-a-glance)
- [Demos](#demos)
- [Component Deep Dives](#component-deep-dives)
  - [Azure Databricks DLT (Bronze → Silver)](#1-azure-databricks-dlt-bronze--silver)
  - [Azure SQL & JDBC Export](#2-azure-sql--jdbc-export)
  - [Apache Airflow Orchestration](#3-apache-airflow-orchestration)
  - [dbt & the T-SQL Migration](#4-dbt--the-t-sql-migration)
  - [Power BI & Semantic Contract](#5-power-bi--semantic-contract)
  - [Terraform Infrastructure as Code](#6-terraform-infrastructure-as-code)
  - [Unity Catalog & Governance](#7-unity-catalog--governance)
  - [CI/CD Pipeline](#8-cicd-pipeline)
  - [Monitoring & Observability](#9-monitoring--observability)
  - [Testing Strategy](#10-testing-strategy)
- [Design Decisions](#design-decisions)
- [Quick Start](#quick-start)
- [Related Projects](#related-projects)

</details>

---

## Architecture Overview

The pipeline follows a **Bronze → Silver → Azure SQL → dbt → Power BI** medallion architecture on Azure, with a Databricks DLT serverless pipeline doing the heavy lifting.

```mermaid
flowchart LR
    classDef source fill:#3b82f6,color:#fff,stroke:#1e40af
    classDef ingest fill:#10b981,color:#fff,stroke:#047857
    classDef dlt fill:#8b5cf6,color:#fff,stroke:#6d28d9
    classDef sql fill:#f59e0b,color:#fff,stroke:#d97706
    classDef dbtclass fill:#ef4444,color:#fff,stroke:#dc2626
    classDef bi fill:#ec4899,color:#fff,stroke:#db2777

    source["93 W3C IIS log files<br/>2009–2011"]:::source

    adls["ADLS Gen2<br/>raw-logs container"]:::ingest

    bronze["DLT Bronze (serverless)<br/>w3c_etl_databricks.bronze.bronze_raw_logs<br/>Custom W3C parser UDF • 14/18-field detection<br/>7 @dlt.expect_or_drop quality checks<br/>153,380 rows • 0 dropped<br/>Partitioned by log_date"]:::dlt

    silver["DLT Silver (serverless)<br/>w3c_etl_databricks.silver.silver_enriched_logs<br/>7 MaxMind GeoIP fields (maxminddb pure Python)<br/>5 computed fields • 31 columns total<br/>153,377 rows • 30+ countries<br/>Dedup: left_anti join on source_file"]:::dlt

    jdbc["JDBC Export (notebook_task)<br/>pymssql batch executemany<br/>BATCH_SIZE=5000 • 4-attempt retry<br/>~45s for 153,377 rows"]:::sql

    azsql["Azure SQL (serverless GP_S_Gen5, 1 vCore)<br/>dbo.raw_enriched - 31 columns<br/>Auto-pause 60 min idle"]:::sql

    dims["Airflow: export_dimensions<br/>MERGE upsert on geo_hash / ua_hash<br/>→ dim_geolocation (1,585 rows)<br/>→ dim_useragent (2,040 rows)<br/>Fires Dataset trigger"]:::dbtclass

    dbt["dbt - 16 models • 121 tests<br/>10 staging + 6 marts<br/>Dual-dialect T-SQL / PostgreSQL<br/>Runs on Databricks serverless via<br/>self-bootstrapping notebooks"]:::dbtclass

    csv["18 CSV exports<br/>Star-Schema/ directory"]:::bi

    powerbi["Power BI<br/>7-page dashboard<br/>Weekly auto-refresh"]:::bi

    source -->|"ABFSS path"| adls
    adls -->|"Auto Loader"| bronze
    bronze -->|"spark.table()"| silver
    silver -->|"collect() + pymssql"| jdbc
    jdbc --> azsql
    azsql --> dims
    dims -->|"Dataset trigger"| dbt
    dbt --> csv
    csv --> powerbi
```

<details>
<summary><b>Docker Dev Environment</b> - A Docker Compose stack (PySpark 4.0.2 + PostgreSQL 13 + Airflow 2.10.2) mirrors this pipeline locally for development and CI testing. 18 services, one `docker compose up -d`. <a href="#quick-start">→ Quick Start</a></summary>
&nbsp;
The Docker pipeline follows the same Bronze → Silver → Warehouse pattern using PySpark jobs (SparkSubmitOperator), local Delta Lake directories, a PostgreSQL warehouse, and the same dbt models (PostgreSQL dialect). It is not a production data platform - it exists for fast iteration and CI verification without cloud costs.

```mermaid
flowchart LR
    classDef airflow fill:#017CEE,color:#fff,stroke:#005bb5
    classDef spark fill:#E25A1C,color:#fff,stroke:#b83d0e
    classDef storage fill:#10b981,color:#fff,stroke:#047857
    classDef monitor fill:#F46800,color:#fff,stroke:#d45500
    classDef db fill:#f59e0b,color:#fff,stroke:#d97706
    classDef oneshot fill:#8b5cf6,color:#fff,stroke:#6d28d9

    subgraph Orchestration["Apache Airflow (CeleryExecutor)"]
        ws("webserver :8080"):::airflow
        sched("scheduler"):::airflow
        wk("worker"):::airflow
    end

    subgraph Compute["Spark Cluster (4.0.2)"]
        sm("spark-master :7077"):::spark
        sw("spark-worker<br/>2C · 4GB"):::spark
    end

    subgraph Data["Data Layer"]
        pg("postgres:13 :5432<br/>metastore + warehouse"):::db
        delta("Delta Lake<br/>bronze/ → silver/"):::storage
        logs("W3C log files"):::storage
        geoip("GeoLite2 .mmdb"):::oneshot
    end

    subgraph Monitoring["Observability Stack"]
        statsd("statsd-exporter"):::monitor
        cadvisor("cadvisor"):::monitor
        prom("prometheus :9090"):::monitor
        graf("grafana :3000"):::monitor
        am("alertmanager :9093"):::monitor
        probe("freshness-probe :8000"):::monitor
    end

    logs --> sw
    geoip --> sw
    sw -->|"bronze → silver"| delta
    wk -->|"SparkSubmitOperator"| sm
    sm --> sw
    sw -->|"JDBC export"| pg
    wk -->|"dbt run (staging → marts)"| pg

    ws --> sched --> wk
    wk & ws & sched -.->|"StatsD UDP"| statsd
    cadvisor -.->|"container metrics"| prom
    statsd -.->|"airflow metrics"| prom
    probe -.->|"data freshness"| prom
    prom -.-> graf & am
```
</details>

---

## Engineering Highlights

| Area | Highlight | Why It Matters |
|---|---|---|
| **Serverless DLT** | All Databricks compute runs on serverless - no VMs, no clusters, auto-scales to zero when idle. | **Zero infrastructure management.** The pipeline costs ~$0 when idle and never requires cluster tuning. |
| **GeoIP Enrichment** | 7 MaxMind fields (country → ISP) from a single consolidated struct UDF using `maxminddb` pure Python. 3.5× faster than 7 separate UDFs. | **Serverless DLT can't install compiled C libraries.** Pure Python `maxminddb` side-steps this limitation while a lazy singleton pattern avoids PicklingError in distributed execution. |
| **45-Second JDBC Export** | 153,377 rows from Silver to Azure SQL in 45 seconds - 8–9× faster than the initial 413s implementation. | **Databricks serverless only supports JDBC reads, not writes.** Pure Python `pymssql` + `tuple(row)` (not `asDict()`) + Spark-side pre-filter before `collect()` were the breakthrough optimisations. |
| **T-SQL Dual-Dialect dbt** | All 16 models compile against both PostgreSQL (dev/CI) and T-SQL (Azure SQL/prod) via inline `{% if target.type == 'sqlserver' %}` branches - no separate `_azure.sql` files. | **One model, two databases.** 18 macros + 2 dispatch overrides abstract PostgreSQL syntax (`::casts`, `EXTRACT`, `SPLIT_PART`, `ILIKE`, `MD5`) behind Jinja wrappers. |
| **121 dbt Data Tests** | 48 `not_null` · 18 `unique` · 21 `accepted_values` · 10 `relationships` (FK) · 24 `expression_is_true` - enforcing business invariants across all 16 models. | **Production-grade data quality.** Tests catch referential integrity failures, negative response times, out-of-range percentages, and dedup key collisions before data reaches Power BI. |
| **Terraform with OIDC** | Part A (4 modules: networking, datalake, databricks, warehouse) + Part B (24 resources: DLT pipelines, Workflows, UC schemas, secrets). Full OIDC Workload Identity Federation - no static Azure credentials. | **Zero touch deployment.** One `terraform apply` provisions the entire Azure estate including the GitHub→Azure auth chain. The CI/CD pipeline authenticates via token exchange, not client secrets. |
| **3 Grafana Dashboards** | 23 panels across Airflow ETL Overview (7), Container System Metrics (6), and Pipeline Health (10) - with 8 Prometheus alert rules, 2 Azure Monitor alerts, and 3 action groups (P1/P2/P3). | **Observability from day one.** Airflow StatsD → Prometheus → Grafana pipeline means every DAG run, task duration, and data freshness metric is tracked. |

---

## Key Metrics at a Glance

| Category | Metric | Value |
|---|---|---|
| **Source** | W3C IIS log files | **93 files** (2009–2011) |
| **Bronze** | Rows ingested | **153,380** (7 quality expectations, **0 dropped**) |
| **Silver** | Enriched rows | **153,377** (31 columns, 7 GeoIP + 5 computed) |
| **GeoIP** | Countries resolved | **30+** across 6 continents |
| **GeoIP** | Known-country coverage | **99.99%** |
| **Export** | Silver → Azure SQL | **~45 seconds** (pymssql batch executemany) |
| **Warehouse** | Azure SQL database | GP_S_Gen5 serverless, 1 vCore, auto-pause 60 min |
| **dbt models** | Total | **16** (10 staging + 6 marts) |
| **dbt macros** | T-SQL compatibility | **18** macros + **2** dispatch overrides |
| **dbt data tests** | All models | **121** (48 not_null + 18 unique + 21 accepted_values + 10 relationships + 24 expression_is_true) |
| **pytest** | Total / CI | **305 tests** / **275 in CI** (17 files, 81+ classes) |
| **Terraform** | HCL assertions | **9** (3 Part A + 6 Part B) + **39** Python tests |
| **CI/CD** | Workflow files | **7** (4 CI + 1 CD + 1 CodeQL + 1 auto-merge) |
| **CI/CD** | Job stages | **9 CI + 3 CD** |
| **IaC** | Terraform modules | **4** (networking, datalake, databricks, warehouse) |
| **IaC** | Azure resources managed | **30+** across 2 Terraform parts |
| **Observability** | Grafana dashboards | **3** (23 panels) |
| **Observability** | Alert rules | **8** Prometheus + **2** Azure Monitor + **3** action groups |
| **Cost** | Budget controls | **$50 warning / $100 hard cap** |
| **Cost** | Monthly estimate | **~$0–100/mo** (serverless auto-scales to zero) |
| **CSV exports** | Power BI-ready | **18 files** (~36 MB) |
| **Power Automate** | Refresh schedule | **Friday 17:30** weekly with success/failure email |

---

## Demos

### Power BI Dashboard - 7-Page Analytics Report

> The final deliverable: 156K requests across 88 countries, 18 Power BI-ready CSV exports. Each page answers a specific business question built entirely from the Bronze → Silver → dbt star schema pipeline.

![At a Glance](docs/media/summary.png)
*Page 1 - At a Glance: 156K requests, 88 countries, human vs bot breakdown, busiest days, top countries*

![What Are People Accessing?](docs/media/file-access.png)
*Page 2 - What Are People Accessing?: file types by volume, content type treemap, server errors, broken pages*

![Where are Visitors Coming From?](docs/media/geo.png)
*Page 3 - Where are Visitors Coming From?: world map, referral sources, countries by unique visitors (US 533, China 159)*

![Who's Hitting the Site?](docs/media/traffic-overview.png)
*Page 4 - Who's Hitting the Site?: 155.6K requests, 62% human / 38% bot, request volume over time*

![Who Are The Visitors?](docs/media/visitors.png)
*Page 5 - Who Are The Visitors?: browser breakdown (IE 49K, Firefox 23K, Googlebot 16K), device types, OS split*

![How Fast is the Server?](docs/media/performance.png)
*Page 6 - How Fast is the Server?: avg 470ms, P95 1149ms, top expensive files, status type speed*

![When is the Site Busiest?](docs/media/temporal.png)
*Page 7 - When is the Site Busiest?: hourly × day heatmap, day-of-week bars, AM vs PM patterns*

### Grafana Monitoring - Pipeline Health & Observability

![Grafana Pipeline Health](docs/media/grafana_health_dashboard.png)
*Pipeline Health - Data Freshness (5 min), Pipeline Status (Healthy), dbt Test Pass Rate (100%), Row Counts (153K each)*

![Grafana Container Metrics](docs/media/grafana_containers.png)
*Container System Metrics - CPU, memory, network I/O per container (cAdvisor → Prometheus)*

![Grafana ETL Overview](docs/media/grafana_etl_overview.png)
*Airflow ETL Overview - DAG run duration, task states, dataset events, scheduling delay*

### Pipeline Run - End-to-End Execution

![Airflow DAG Graph View](docs/media/spark_ingestion_azure_airflow_graph.png)
*Airflow DAG graph - `w3c_spark_ingestion_azure` task dependencies and Dataset trigger flow*

![Databricks Workflow Graph](docs/media/databricks-jobs-graph.png)
*Bronze → Silver → JDBC Export - 3 serverless tasks, ~7 min total*

![dbt DAG Graph View](docs/media/dbt_marts_azure_airflow_graph.png)
*dbt DAG graph - 16 models with ref() dependencies across 3 schemas*

### CI/CD Pipeline

![CI Pipeline](docs/media/ci.png)
*4 parallel jobs: lint, test, dbt-compile, terraform - all green*

![CD Pipeline](docs/media/cd.png)
*terraform-plan → terraform-apply → smoke-test*

![CD Rollback](docs/media/cd_rollback.png)
*Automatic rollback on health check failure - zero-downtime recovery*

---

## Component Deep Dives

### 1. Azure Databricks DLT (Bronze → Silver)

#### Bronze DLT Pipeline

The Bronze layer ingests **93 real W3C IIS log files** from ADLS Gen2 via Auto Loader (`binaryFile` format, `maxFilesPerTrigger=10`, `includeExistingFiles=true`). A custom per-file UDF reads the full binary content, detects the `#Fields:` header to determine **14-field vs 18-field IIS format**, and parses every non-comment data line using `rsplit()` field-counting to handle unquoted user-agent strings.

```mermaid
flowchart LR
    classDef source fill:#3b82f6,color:#fff,stroke:#1e40af
    classDef dlt fill:#8b5cf6,color:#fff,stroke:#6d28d9
    classDef uc fill:#10b981,color:#fff,stroke:#047857

    adls["ADLS Gen2<br/>raw-logs container<br/>93 W3C IIS files"]:::source
    autoloader["Auto Loader<br/>binaryFile format<br/>maxFilesPerTrigger=10<br/>includeExistingFiles=true<br/>CloudFiles Incremental"]:::dlt
    bronzeuc["Unity Catalog<br/>w3c_etl_databricks.bronze.bronze_raw_logs<br/>19 columns · 153,380 rows<br/>Partitioned by log_date<br/>ChangeDataFeed enabled"]:::uc

    adls -->|"ABFSS path notification"| autoloader
    autoloader -->|"@dlt.table append<br/>7 expect_or_drop checks"| bronzeuc

    style adls stroke-width:2px
    style bronzeuc stroke-width:2px
```

![ADLS Gen2 Data Lake](docs/media/adls.png)
*ADLS Gen2 `raw-logs` container - 93 W3C IIS log files ingested by Auto Loader*

![Azure Portal Overview](docs/media/azure.png)
*Azure portal - resource group with Databricks, ADLS, SQL, and monitoring resources*

**7 quality expectations** (`@dlt.expect_or_drop`):

| Expectation | Expression | Rows Dropped |
|---|---|---|
| `valid_log_date` | `log_date IS NOT NULL` | 0 |
| `valid_status` | `status BETWEEN 100 AND 599` | 0 |
| `valid_client_ip` | `client_ip IS NOT NULL AND client_ip != '-'` | 0 |
| `valid_method` | `method IN ('GET','POST','HEAD','PUT','DELETE','OPTIONS','TRACE')` | 0 |
| `valid_uri_stem` | `uri_stem IS NOT NULL` | 0 |
| `valid_user_agent` | `user_agent IS NOT NULL AND user_agent != '-'` | 0 |
| `valid_bytes` | `(bytes_sent IS NULL OR bytes_sent >= 0) AND (bytes_recv IS NULL OR bytes_recv >= 0)` | 0 |

**Result:** **153,380 rows**, **0 dropped** - all 7 quality expectations pass on real production IIS data.

![Databricks DLT Pipelines](docs/media/dlt-pipelines.png)
*Bronze and Silver DLT pipelines in Databricks - both serverless, both green*

| Attribute | Value |
|---|---|
| Pipeline ID | `a6ea62d3-5f3a-4f53-ae8b-4bfb156703ad` |
| Type | Serverless DLT (Advanced) |
| Channel | `PREVIEW` |
| Table | `w3c_etl_databricks.bronze.bronze_raw_logs` |
| Partition | `log_date` |
| Delta properties | ChangeDataFeed, autoOptimize, autoCompact, DeletionVectors |
| Storage | Unity Catalog managed (not external) |

#### Silver DLT Pipeline

The Silver layer reads from Bronze via `spark.table()`, enriches each row with MaxMind GeoIP data, computes 5 derived fields, and applies quality expectations.

**GeoIP Enrichment - Key Decisions:**

- **Library:** `maxminddb==2.8.*` (pure Python) - NOT `geoip2` which requires compiled `libmaxminddb` unavailable on serverless DLT
- **Performance:** 1 consolidated struct UDF (6 fields from 1 City DB call) + 1 scalar UDF (ISP from ASN DB) - **3.5× faster** than 7 separate UDFs
- **Pattern:** Lazy singleton `_ensure_geo_reader()` to avoid PicklingError - `maxminddb.Reader` is not serialisable, so UDFs reference helper *functions* (serialisable by name), not reader *instances*
- **Path:** `/Volumes/w3c_etl_databricks/bronze/w3c_data/` - NOT `/dbfs/Volumes/` (FUSE mount inaccessible on serverless executors)

**7 GeoIP Fields:**

| Field | Source | Method |
|---|---|---|
| `country` | City DB → `country.names.en` | Consolidated struct UDF |
| `region` | City DB → `subdivisions[0].names.en` | Consolidated struct UDF |
| `city` | City DB → `city.names.en` | Consolidated struct UDF |
| `latitude` | City DB → `location.latitude` | Consolidated struct UDF |
| `longitude` | City DB → `location.longitude` | Consolidated struct UDF |
| `postcode` | City DB → `postal.code` | Consolidated struct UDF |
| `isp` | ASN DB → `autonomous_system_organization` | Separate scalar UDF |

**5 Computed Fields:**

| Field | Logic | Example Values |
|---|---|---|
| `page_category` | URI stem pattern matching | Static Asset / API / Admin / Homepage / Content |
| `referrer_domain` | URL domain extraction | google.com / facebook.com / (direct) |
| `traffic_type` | Referrer domain classification | Direct / Search / Social / Referral |
| `is_crawler` | UA keyword matching | bot, spider, crawler, curl, python-requests |
| `size_band` | Response size bucketing | < 1KB / 1–10KB / 10–100KB / 100KB–1MB / > 1MB |

**Dedup:** `left_anti` join on `source_file` - wrapped in `try/except` for the first pipeline run when Silver doesn't exist yet.

**Result:** **153,377 rows** (3 dropped by `valid_country` - private/reserved IPs with no GeoIP match), **31 columns**, **30+ countries**.

---

### 2. Azure SQL & JDBC Export

The JDBC export bridges Databricks Silver → Azure SQL. This is the most performance-critical stage because **Databricks serverless (Spark Connect) only supports JDBC reads, not writes**. Spark's `df.write.jdbc` is unavailable.

**Why pymssql:**
- Pure Python driver - no JVM libraries or Maven coordinates
- Works as a job environment dependency on serverless
- `cursor.executemany()` with `BATCH_SIZE=5000`

**Performance Journey - 413s → 45s (8–9× improvement):**

| # | Issue | Before | After | Impact |
|---|---|---|---|---|
| 1 | `.cache()` unsupported on serverless | `new_data_df.cache()` failed with `[NOT_SUPPORTED_WITH_SERVERLESS]` | `collect()` first, then `len(rows)` - removes both `.cache()` and redundant `.count()` scan | Fixed initial crash + saved one full scan |
| 2 | `collect()` before Spark-side filter | All 153K rows collected to driver, then filtered in Python | Filter `~col("source_file").isin(loaded_files)` **before** `collect()` | On incremental: ~0 rows vs 153K. Prevents OOM. |
| 3 | Wasteful `export_df.count()` | `total_rows = export_df.count()` scanned entire Silver table | Removed | ~10s saved per run |
| 4 | `asDict()` serialization | `row.asDict()` - 31 keys × 153K rows = 4.7M dict allocations | `tuple(row)` - no dict overhead | ~50s saved on initial run |

**Architecture:**
1. Connect to Azure SQL with 4-attempt exponential backoff (`15 × 2ⁿ` - covers serverless cold-start)
2. Ensure `dbo.raw_enriched` (31-column DDL) and `dbo.raw_enriched_loaded` (tracking table) exist via `IF OBJECT_ID(...) IS NULL`
3. Read Silver → select 31 export columns → cast `is_crawler` string → BIT
4. Query tracking table for already-loaded `source_file` values
5. **Filter in Spark BEFORE `collect()`** - critical for incremental idempotency
6. Batch INSERT via `cursor.executemany()` with `BATCH_SIZE=5000`
7. Update tracking table with new source files

![Azure SQL Schema](docs/media/azure-sql-schema.png)
*31-column `dbo.raw_enriched` table schema in Azure SQL*

**31 Export Columns:**
- **25 warehouse core:** `log_date`, `log_time`, `server_ip`, `method`, `uri_stem`, `uri_query`, `client_ip`, `user_agent`, `cookie`, `referrer`, `status`, `sub_status`, `win32_status`, `bytes_sent`, `bytes_recv`, `server_port`, `username`, `time_taken`, `source_file`, `postcode`, `page_category`, `referrer_domain`, `traffic_type`, `is_crawler`, `size_band`
- **6 GeoIP:** `country`, `region`, `city`, `latitude`, `longitude`, `isp`

**Dimension Export (Airflow PythonOperator):**

After the JDBC export, Airflow's `export_dimensions` task builds dimensional tables using `MERGE` upsert:

| Table | Natural Key | Rows | Sentinel |
|---|---|---|---|
| `dim_geolocation` | `geo_hash` - SHA-256 of `country\|region\|city\|latitude\|longitude` | **1,585** | `-1`: Unknown |
| `dim_useragent` | `ua_hash` - SHA-256 of parsed UA fields | **2,040** | `-1`: Unknown |

The geo hash is computed **in SQL** via `HASHBYTES('SHA2_256', ...)` inside the MERGE subquery - efficient batch computation. The UA hash is computed **in Python** via `hashlib.sha256()` alongside parsing to avoid extra SQL round-trips. Sentinels use `SET IDENTITY_INSERT ON/OFF` for FK integrity.

![Azure SQL - Table Row Counts](docs/media/azure-sql-row-counts.png)
*Azure SQL table row counts - `raw_enriched` fact table with 153K rows alongside dimension tables (geolocation: 1,585, useragent: 2,040)*

---

### 3. Apache Airflow Orchestration

Airflow owns **all orchestration** - 4 DAGs across 2 pipelines (Docker dev + Azure prod) with zero data processing logic in DAG files.

**DAG Architecture:**

| DAG | Schedule | Tasks | Operators | Triggers |
|---|---|---|---|---|
| `w3c_spark_ingestion` | Sat 06:00 UTC | 4 | `SparkSubmitOperator` (3) + `PythonOperator` (1) | Cron |
| `w3c_spark_ingestion_azure` | Fri 17:00 UTC | 3 | `DatabricksRunNowOperator` (1) + `PythonOperator` (2) | Cron |
| `w3c_dbt_marts` | Dataset-triggered | 5 | `BashOperator` (5) | `Dataset("postgres://...")` |
| `w3c_dbt_marts_azure` | Dataset-triggered | 6 | `DatabricksSubmitRunOperator` (4) + `PythonOperator` (2) | `Dataset("mssql://...")` |

**Dataset-Driven Decoupling (Azure):**

```mermaid
flowchart LR
    classDef dag fill:#017CEE,color:#fff,stroke:#005bb5
    classDef dataset fill:#f59e0b,color:#fff,stroke:#d97706

    ingestion["w3c_spark_ingestion_azure<br/>Friday 17:00 UTC"]:::dag
    dataset["Dataset<br/>mssql://azure-sql/dbo/<br/>raw_enriched_loaded"]:::dataset
    marts["w3c_dbt_marts_azure<br/>Dataset-triggered"]:::dag

    ingestion -->|"outlet (create_indexes task)"| dataset
    dataset -->|"inlet (schedule=[...])"| marts

    style dataset stroke-width:2px,stroke-dasharray: 5 5
```

![Airflow DAG Graph View](docs/media/spark_ingestion_azure_airflow_graph.png)
*Airflow DAG graph - `w3c_spark_ingestion_azure` task dependencies and Dataset trigger flow*

The two DAGs are **intentionally decoupled** - no DAG-to-DAG imports, no `ExternalTaskSensor`, no hard-coded DAG IDs. The Dataset mechanism allows ingestion and transformation to be developed, tested, and monitored independently.

![Airflow Gantt Chart](docs/media/spark_ingestion_azure_airflow_gantt.png)
*Gantt view: `w3c_spark_ingestion_azure` - Bronze, Silver, and JDBC Export task durations*

![Airflow Dashboard](docs/media/airflow_dashboard.png)
*Airflow UI - DAG runs, task states, and scheduling history*

**4 Operator Types Used:**

| Operator | Purpose | Used In |
|---|---|---|
| `DatabricksRunNowOperator` | Triggers `w3c-etl-workflow` (Bronze → Silver → JDBC) as a single atomic run | `spark_ingestion_azure` |
| `DatabricksSubmitRunOperator` | Submits dbt notebooks to serverless compute (source freshness, run, test, docs) | `dbt_marts_azure` |
| `PythonOperator` | Dimension export (`pyodbc` + `MERGE`), index creation, CSV export, dbt docs sync | Both |
| `SparkSubmitOperator` | Bronze/Silver/Export PySpark jobs against local Spark cluster (Docker dev) | `spark_ingestion` (Docker) |

---

### 4. dbt & the T-SQL Migration

dbt owns **all SQL transformations** - nothing else writes to the star schema. The project comprises **16 models** across **3 schemas** with **121 data tests**.

**Schema Isolation:**

| Schema | Purpose | Managed By | Number of Tables |
|---|---|---|---|
| `dbo` (Azure) / `public` (PostgreSQL) | Raw ingestion + enrichment dimensions | Airflow + JDBC Export | 4 |
| `dbt_staging` | Core warehouse star schema | dbt | 10 |
| `dbt_marts` | Pre-aggregated BI analytics | dbt | 6 |

**The Dual-Dialect Strategy:**

Every model file uses inline `{% if target.type == 'sqlserver' %}...{% else %}...{% endif %}` branches. There are **no separate `_azure.sql` files** - dbt would parse both as independent models.

```sql
-- Real example from a dbt model (simplified):
SELECT
    {% if target.type == 'sqlserver' %}
        CAST(field AS BIGINT) AS metric
    {% else %}
        field::BIGINT AS metric
    {% endif %}
FROM {{ source('w3c', 'raw_enriched') }}
```

**18 T-SQL Compatibility Macros + 2 Dispatch Overrides:**

| Macro | PostgreSQL Pattern | T-SQL Replacement |
|---|---|---|
| `tsql_cast` | `field::type` | `CAST(field AS type)` |
| `tsql_datepart` | `EXTRACT(YEAR FROM field)` | `DATEPART(year, field)` |
| `tsql_month_name` | `TO_CHAR(field, 'FMMonth')` | `DATENAME(month, field)` |
| `tsql_split_part` | `SPLIT_PART(field, delim, n)` | `CHARINDEX`/`SUBSTRING` expression |
| `tsql_case_insensitive_like` | `field ~* 'pattern'` | `field LIKE 'pattern' COLLATE SQL_Latin1_General_CP1_CI_AS` |
| `tsql_hash_md5` | `MD5(CONCAT(...))` | `CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(...)), 2)` |
| `tsql_generate_series` | `generate_series(0, 23)` | `GENERATE_SERIES(0, 23)` |
| `tsql_percentile_cont` | `PERCENTILE_CONT(...) OVER (...)` | Same syntax (used in separate DISTINCT CTE) |
| `tsql_extract_domain` | `REGEXP_REPLACE(url, pattern)` | Nested `CHARINDEX`/`SUBSTRING` |
| `tsql_true_val` / `tsql_false_val` | `TRUE` / `FALSE` | `1` / `0` |

**Dispatch overrides:** `sqlserver__test_expression_is_true` and `sqlserver__collect_freshness` implement `dbt_utils` functionality for T-SQL.

**Model Lineage:**

```
raw_enriched (source)
  ├── dim_date (93 rows, 22 UK holidays)
  ├── dim_time (1,440 rows, full day in minutes)
  ├── dim_page (~14K paths, file_extension classification)
  ├── dim_status (~1,145 triples, severity labelling)
  ├── dim_method (~5 methods, safe/unsafe classification)
  ├── dim_referrer (~2K URLs, traffic source classification)
  ├── dim_visit_buckets (6 visit frequency buckets)
  ├── dim_visitortype (3 types: Human / Crawler / Unknown)
  ├── crawler_ips (crawler IP tracking table)
  └── fact_webrequest (INCREMENTAL, 12-component MD5 dedup key)
        ├── mart_page_performance (1 row/page_sk, P95 latency)
        ├── mart_daily_aggregates (1 row/date, 15+ KPIs)
        ├── mart_crawler_analysis (1 row/date, crawler-only metrics)
        ├── mart_timeofday_analysis (1 row/date×hour, hourly breakdown)
        ├── mart_browser_analysis (browser market share over time)
        └── mart_country_browser_share (top browser per country per day)
```

![dbt DAG Graph View](docs/media/dbt_marts_azure_airflow_graph.png)
*dbt DAG graph - 16 models with ref() dependencies across 3 schemas*

![dbt DAG Execution Gantt](docs/media/dbt_marts_azure_airflow_gantt.png)
*dbt DAG execution Gantt - source freshness, run, test, and docs generation on Databricks serverless*

![Star Schema Dimensions](docs/media/star-schema-dimensions.png)
*dbt model lineage - 10 staging tables feeding 6 mart tables in the star schema*

![dbt Full Lineage Graph](docs/media/dbt_lineage_graph.png)
*dbt full lineage graph - source freshness → staging → marts → tests across PostgreSQL and Azure SQL targets*

**Star Schema Entity-Relationship Diagram:**

The fact table sits at the center with 10 surrounding dimension tables - 8 managed by dbt, 2 (`dim_geolocation`, `dim_useragent`) maintained by Airflow's dimension export. The diagram below shows the surrogate key relationships between each dimension and `fact_webrequest`.

```mermaid
erDiagram
    fact_webrequest {
        string raw_log_id PK
        int date_sk FK
        int time_sk FK
        int page_sk FK
        int method_sk FK
        int status_sk FK
        int referrer_sk FK
        int geolocation_sk FK
        int user_agent_sk FK
        int visitor_sk FK
        int visit_bucket_sk FK
        bigint bytes_sent
        bigint bytes_received
        int response_time_ms
        int request_count
        boolean is_404
        boolean is_crawler
        string size_band
        string page_category
        string referrer_domain
        string traffic_type
        int request_hour
    }

    dim_date {
        int date_sk PK
        date date UK
        int year
        int month
        string month_name
        int day_number
        string day_name
        int day_of_week
        int quarter
        string is_weekend
        string holiday_flag
    }

    dim_time {
        int time_sk PK
        int hour
        int minute
        string am_pm
        string time_band
        int shift_id
    }

    dim_page {
        int page_sk PK
        string page_path UK
        string query_string
        string directory
        string file_extension
        string page_category
    }

    dim_method {
        int method_sk PK
        string http_method UK
        string description
        string is_safe
    }

    dim_status {
        int status_sk PK
        int status_code
        int sub_status
        int win32_status
        string status_category
        string status_label
        string severity
    }

    dim_referrer {
        int referrer_sk PK
        string referrer_url UK
        string referrer_domain
        string traffic_source
    }

    dim_visitortype {
        int visitor_sk PK
        string crawler_flag
        string visitor_type
    }

    dim_visit_buckets {
        int visit_bucket_sk PK
        string visit_bucket UK
        int visit_bucket_order
        int user_count
    }

    dim_geolocation {
        int geolocation_sk PK
        string geo_hash UK
        string country
        string region
        string city
        float latitude
        float longitude
        string isp
    }

    dim_useragent {
        int user_agent_sk PK
        string ua_hash UK
        string user_agent
        string browser_name
        string browser_version
        string operating_system
        string device_type
    }

    dim_date ||--o{ fact_webrequest : "date_sk"
    dim_time ||--o{ fact_webrequest : "time_sk"
    dim_page ||--o{ fact_webrequest : "page_sk"
    dim_method ||--o{ fact_webrequest : "method_sk"
    dim_status ||--o{ fact_webrequest : "status_sk"
    dim_referrer ||--o{ fact_webrequest : "referrer_sk"
    dim_visitortype ||--o{ fact_webrequest : "visitor_sk"
    dim_visit_buckets ||--o{ fact_webrequest : "visit_bucket_sk"
    dim_geolocation |o--o{ fact_webrequest : "geolocation_sk"
    dim_useragent |o--o{ fact_webrequest : "user_agent_sk"
```

> **Legend:** `||--o{` = one-to-many (INNER JOIN). `|o--o{` = zero-or-one-to-many (LEFT JOIN - geolocation and useragent may be Unknown/-1). PK = primary key. UK = unique key. FK = foreign key.

**Fact Table Dedup - 12-Component MD5 Key:**

The `fact_webrequest` incremental model deduplicates using a 12-component MD5 hash:

```sql
raw_log_id = MD5(CONCAT(
    source_file, '|', log_time, '|', client_ip, '|',
    user_agent, '|', referrer, '|', uri_stem, '|',
    uri_query, '|', method, '|', status, '|',
    sub_status, '|', win32_status, '|', time_taken
))
```

| Category | Count | Description |
|---|---|---|
| Byte-identical duplicates | 202 | Exact row clones |
| Byte-similar (bytes differ only) | 11 | Same key, different payload - tiebreaker keeps max `time_taken` |
| **Total deduped** | **213** | 155,570 raw → 155,357 unique fact rows |

A singular test (`fact_webrequest_dedup_safety.sql`) regression-tests that no hash bucket contains >1 distinct value for any of the 3 disambiguating fields.

---

### 5. Power BI & Semantic Contract

_The semantic contract defines exactly what data leaves dbt and enters Power BI - no surprises, no undocumented columns, no type mismatches._

**7-Page Dashboard:**

| Page | Title | Key Visuals |
|---|---|---|
| 1 | At a Glance | 156K requests, 88 countries, human vs bot donut (62%/38%), busiest days bar, top 10 countries, 4 insight cards |
| 2 | What Are People Accessing? | 3.6K unique pages, file types by volume & data served, content type treemap (Image 68K, Dynamic Page 60K), server errors (404: 15,111), broken pages |
| 3 | Where are Visitors Coming From? | Azure Maps world choropleth, top referral sources, countries by unique visitors (US 533, China 159), most active cities |
| 4 | Who's Hitting the Site? | 155.6K requests, 62% human vs 38% crawler, request volume by year (2009–2011), human vs bot traffic shift over time |
| 5 | Who Are The Visitors? | 4.0K unique IPs, browser breakdown (IE 49K, Firefox 23K), device types (Desktop 83K, Bot 59K), OS split, visit frequency, top 10 IPs |
| 6 | How Fast is the Server? | Avg 470ms, P95 1,149ms, 6.5% over 1s, top 5 expensive files, response speed by content type (12 months), response size distribution |
| 7 | When is the Site Busiest? | 9.4K peak hour, 77.7% weekday traffic, hour × day-of-week heatmap, day-of-week bar (Monday 33K), AM vs PM patterns |

**Dashboard Pages:**

![At a Glance - Executive Summary](docs/media/summary.png)
*Page 1: At a Glance - 156K total requests, 88 active countries, 9.7% 404 rate, human vs bot split (62% / 38%), busiest days, top 10 countries by traffic volume, 4 insight cards*

![What Are People Accessing? - Content Analysis](docs/media/file-access.png)
*Page 2: What Are People Accessing? - 3.6K unique pages, file types by request volume & data served (aspx 38.6%, jpeg 29.6%), content type treemap (Image 68K, Dynamic Page 60K), server errors by status code (404: 15,111), top pages by hit count, broken pages*

![Where are Visitors Coming From? - Geographic](docs/media/geo.png)
*Page 3: Where are Visitors Coming From? - 88 countries reached, global visitor footprint map (Azure Maps), top referral sources (darwinsbeagleplants.org 27,989), countries by unique visitor count (US 533, China 159, UK 159), most active cities by request volume*

![Who's Hitting the Site? - Crawler vs Human](docs/media/traffic-overview.png)
*Page 4: Who's Hitting the Site? - 155.6K total requests, 62% human vs 38% crawler, request volume by year (2009: 17K, 2010: 113K, 2011: 25K), human vs bot traffic shift over time*

![Who Are The Visitors? - Browser & Device](docs/media/visitors.png)
*Page 5: Who Are The Visitors? - 4.0K unique IPs, browser breakdown (IE 49K, Firefox 23K, Googlebot 16K), device types (Desktop 83K, Bot 59K), OS split (Windows 78K), visit frequency distribution, top 10 most active IPs*

![How Fast is the Server? - Performance](docs/media/performance.png)
*Page 6: How Fast is the Server? - Avg 470ms, P95 1,149ms, 6.5% requests over 1 second, top 5 most expensive files (all cache JPEGs), response speed by content type over time (12 months), response size distribution*

![When is the Site Busiest? - Temporal](docs/media/temporal.png)
*Page 7: When is the Site Busiest? - 9.4K peak hour requests, 77.7% weekday traffic, requests by hour × day-of-week heatmap, day-of-week bar chart (Monday 33K), AM vs PM traffic patterns*

**18 CSV Files (~36 MB total):**

| Directory | Files | Description |
|---|---|---|
| `dbt_staging/` | 10 | `dim_date`, `dim_time`, `dim_page`, `dim_status`, `dim_method`, `dim_referrer`, `dim_visit_buckets`, `dim_visitortype`, `crawler_ips`, `fact_webrequest` |
| `dbt_marts/` | 6 | `mart_page_performance`, `mart_daily_aggregates`, `mart_crawler_analysis`, `mart_timeofday_analysis`, `mart_browser_analysis`, `mart_country_browser_share` |
| `public/` | 2 | `dim_geolocation` (1,585 rows), `dim_useragent` (2,040 rows) |

**Key DAX Measures (defined in Power BI):**

| Measure | Formula Logic | Purpose |
|---|---|---|
| Total Requests | `COUNTROWS(fact_webrequest)` | Overall traffic volume |
| Unique Visitors | `DISTINCTCOUNT(fact_webrequest[client_ip])` | Audience reach |
| Avg Response Time | `AVERAGE(fact_webrequest[time_taken])` | Performance baseline |
| P95 Response Time | `PERCENTILEX.INC(fact_webrequest, fact_webrequest[time_taken], 0.95)` | Tail latency |
| Total Bandwidth (MB) | `SUM(fact_webrequest[bytes_sent]) / 1048576` | Data transfer volume |
| Crawler Traffic % | `DIVIDE(COUNTROWS(FILTER(fact_webrequest, fact_webrequest[is_crawler] = TRUE())), COUNTROWS(fact_webrequest))` | Bot traffic ratio |
| Peak Hour | `CALCULATE(MAX('Time'[hour]), TOPN(1, VALUES('Time'[hour]), COUNTROWS(fact_webrequest), DESC))` | Peak traffic hour |

**Refresh Schedule:**

| Step | Schedule | Mechanism |
|---|---|---|
| Pipeline run | Friday 17:00 UTC | Airflow triggers Databricks Workflow |
| CSV export | After dbt completes | `export_csv_azure.py` writes 18 files to ADLS |
| Power BI refresh | Friday 17:30 UTC | Power Automate cloud flow with success/failure email notification |

The 30-minute buffer between pipeline start (17:00) and refresh (17:30) comfortably accommodates the full ~15-minute ETL run plus a Slack-style grace period.

![Power Automate - Scheduled Power BI Refresh](docs/media/power-automate.png)
*Power Automate cloud flow that triggers the weekly Power BI dataset refresh every Friday at 17:30 UTC, with success/failure email notification*

**Why weekly, not daily or hourly?** The source is historical (2009–2011) with no new data arriving. Weekly reprocessing validates the pipeline end-to-end, detects drift in upstream dependencies (maxminddb, dbt, Airflow, Azure SQL), and keeps Power BI fresh for the portfolio audience - without incurring unnecessary compute costs.

---

### 6. Terraform Infrastructure as Code

The entire Azure estate is managed as code in **2 Terraform parts** with a shared Azure Blob Storage backend (`tfstatew3cetl`).

**Part A - Core Azure Infrastructure** (4 modules):

| Module | Resources |
|---|---|
| **networking** | Resource Group, VNet (10.0.0.0/16), 2 subnets (Databricks-delegated + SQL), 2 NSGs with security rules |
| **datalake** | ADLS Gen2 Storage Account (Standard LRS, hierarchical namespace), 4 containers (raw-logs, bronze, silver, gold), RBAC assignments |
| **databricks** | Premium Databricks workspace, access connector (SystemAssigned MI), RBAC, 60s propagation wait |
| **warehouse** | Azure SQL Server (v12.0), serverless database (GP_S_Gen5_1, auto-pause 60 min, `prevent_destroy`), firewall rules |

**Part B - Databricks Resources** (24 resources):

| Category | Resources |
|---|---|
| **DLT Pipelines** | Bronze pipeline, Silver pipeline |
| **Workflows** | `w3c-etl-workflow` (3 serverless tasks, daily 2 AM UTC) |
| **Unity Catalog** | 3 schemas (bronze, silver, gold), 1 storage credential, 1 external location, grants |
| **Secrets** | 1 secret scope, 5 secrets (SQL server/db/user/pass, storage key) |
| **Workspace** | 7 notebooks, 2 workspace files (dbt_common.py + dbt project ZIP) |

**Network Topology:**

```
VNet: 10.0.0.0/16 (westus3)
├── snet-databricks: 10.0.1.0/24 → Delegated to Databricks
│   └── NSG: nsg-databricks (5 rules: SSH/HTTPS from AzureCloud, VNet-in, Azure-out, deny Internet)
└── snet-sql: 10.0.2.0/24
    └── NSG: nsg-sql (4 rules: Databricks subnet TCP 1433, VNet-in, Azure-out, deny Internet)
```

**OIDC Workload Identity Federation:**

All Azure authentication uses OIDC - no static secrets:

```mermaid
sequenceDiagram
    participant GH as GitHub Actions Runner
    participant OIDC as token.actions.githubusercontent.com
    participant AAD as Azure AD
    participant SP as Service Principal<br/>(w3c-etl-sp)
    participant ARM as Azure ARM API

    GH->>OIDC: Request OIDC JWT<br/>(subject: repo:AhmedIkram05/w3c-etl-pipeline:environment:azure-dev)
    OIDC-->>GH: Signed JWT
    GH->>AAD: Exchange JWT for access token<br/>(via Federated Identity Credential)
    AAD-->>GH: Azure AD access token<br/>(Contributor on rg-w3c-etl)
    GH->>ARM: terraform apply<br/>(with OIDC token)
    ARM-->>GH: Resource deployment
```

The entire OIDC chain (Azure AD app, service principal, federated credential, role assignment) is managed by `github_oidc.tf` - one `terraform apply` creates the complete auth chain without a single CLI command.

![Databricks Workflow Timeline](docs/media/databricks-jobs-timeline.png)
*Workflow execution timeline - Bronze → Silver → JDBC Export with task-level durations*

**Cost Controls:**

| Control | Threshold | Action |
|---|---|---|
| Budget alert | $50 | Email warning to alert_email_critical |
| Budget hard cap | $100 | Email notification |
| Azure SQL serverless | 60 min idle | Auto-pauses database |
| Databricks | Serverless DLT | Auto-scales to zero - no idle VM costs |

**Terraform Validation (CI):**

| Check | Tool | Runs on Every Push |
|---|---|---|
| HCL formatting | `terraform fmt --check` | ✅ |
| Module resolution | `terraform init -backend=false` | ✅ |
| Config validation | `terraform validate` | ✅ |
| HCL assertions | `terraform test` (9 assertions) | ✅ |
| Python tests | `pytest -m terraform` (39 tests) | ✅ |

<!-- MEDIA: Silicon of `terraform test` passing for Part A (3 assertions: credentials provided, resource names non-empty, alerts configured) and Part B (6 assertions: pipelines exist, workflow exists, UC schemas exist, secret scope exists, outputs defined) -->

---

### 7. Unity Catalog & Governance

All Databricks data assets are managed through **Unity Catalog** (`w3c_etl_databricks` metastore), providing centralized governance, cross-pipeline lineage, and volume-based file access - no raw DBFS paths in production code.

**Catalog Structure:**

| Schema | Purpose | Tables / Assets |
|---|---|---|
| `bronze` | Raw W3C ingested data | `bronze_raw_logs` (19 columns, 153,380 rows, partitioned by `log_date`) |
| `silver` | Enriched + deduplicated data | `silver_enriched_logs` (31 columns, 153,377 rows) |
| `gold` | Reserved for future aggregate views | Currently empty - available for curated analytics datasets |

![Unity Catalog Structure](docs/media/unity_catalog.png)
*Unity Catalog: bronze, silver, gold schemas with storage credential and external location*

**Key Unity Catalog Resources (managed by Terraform Part B):**

| Resource | Name | Purpose |
|---|---|---|
| Storage credential | `w3c-storage-credential` | Azure Managed Identity → ADLS Gen2 authentication for external locations |
| External location | `raw-logs` | ABFSS path `abfss://raw-logs@w3cdatalake.dfs.core.windows.net/` - read-only access for Auto Loader |
| Volume | `w3c_data` | Mounts GeoIP `.mmdb` files at `/Volumes/w3c_etl_databricks/bronze/w3c_data/` - the only path accessible by serverless DLT executors for non-tabular data |
| Grants | `USE_SCHEMA` + `SELECT` | Granular permissions per schema, ensuring Silver can read Bronze but not vice versa |

**Why Unity Catalog over DBFS paths:**

| Requirement | DBFS Approach | Unity Catalog Approach |
|---|---|---|
| Cross-pipeline table reads | Hard-coded `dbfs:/user/hive/...` paths | `spark.table('w3c_etl_databricks.bronze.bronze_raw_logs')` - self-documenting, no path guessing |
| GeoIP file access | `/dbfs/Volumes/...` - fails on serverless executors | `/Volumes/w3c_etl_databricks/bronze/w3c_data/` - supported FUSE mount path |
| Permission granularity | Workspace-level ACLs | Table-level grants (`GRANT SELECT ON bronze_raw_logs TO ...`) |
| Data lineage | Manual | Built-in column-level lineage tracking via System Tables |
| Terraform integration | Outside Terraform scope | `databricks_grants`, `databricks_schema`, `databricks_external_location` - full lifecycle via HCL |

---

### 8. CI/CD Pipeline

**7 workflow files** provide **4 CI jobs every push + 3 CD jobs on merge to main** (plus rollback via `workflow_dispatch`), with 3 reusable workflows shared across both.

**CI - Every Push (4 parallel jobs, zero cloud credentials):**

```mermaid
flowchart TD
    ci["Push to any branch or PR to main"] --> lint["lint (reusable)<br/>ruff, mypy, bandit, SQLFluff"]
    ci --> test["test (reusable)<br/>275 pytest + coverage + Codecov"]
    ci --> dbtc["dbt-compile (inline)<br/>PostgreSQL + T-SQL compile<br/>+ 12 output validators"]
    ci --> tf["terraform (reusable, matrix)<br/>Part A + Part B: fmt, init, validate, test"]
```

| Job | What It Validates |
|---|---|
| **lint** | ruff lint + format (PEP8), mypy type checking (19 files), bandit security scan, SQLFluff dbt SQL lint |
| **test** | 275 pytest unit tests across all pipeline layers (DLT, JDBC, dimensions, W3C parser, DAG integrity, Terraform) with Codecov coverage |
| **dbt-compile** | dbt compile against PostgreSQL + T-SQL/Azure SQL in dual-service CI containers (PostgreSQL 13 + SQL Server 2022 side-by-side), plus 12 T-SQL output validators |
| **terraform** | `fmt --check`, `init`, `validate`, `terraform test` (9 HCL assertions) across both Part A + Part B matrix |

![CI Pipeline](docs/media/ci.png)
*4 parallel jobs: lint, test, dbt-compile, terraform - all green*

**CD - Merge to Main (OIDC-scoped deployment):**

```mermaid
flowchart TD
    cd["Push to main (auto-deploy)"] --> plan["terraform-plan<br/>Part A + Part B<br/>(read-only)"]
    plan --> apply["terraform-apply<br/>Part A (OIDC) + Part B (OIDC + PAT)"]
    cd -->|"manual workflow_dispatch"| rollback["rollback<br/>Checkout HEAD~1 →<br/>terraform apply previous commit"]
```

| CD Job | Key Details |
|---|---|---|
| **terraform-plan** | Read-only plan. Part A via OIDC. Part B via PAT (skipped for Dependabot). Plan artifacts uploaded for review. |
| **terraform-apply** | Deploys Azure infra (Part A) + Databricks resources (Part B). All auth via OIDC - no `ARM_CLIENT_SECRET`. |
| **rollback** | Manual `workflow_dispatch`. Checks out `HEAD~1` with `fetch-depth: 0`, then terraform apply the previous commit. |

![CD Pipeline](docs/media/cd.png)
*terraform-plan → terraform-apply*

![CD Rollback Workflow](docs/media/cd_rollback.png)
*CD rollback: manual workflow_dispatch reverts to previous commit and re-applies Terraform*

**Pre-commit Hooks (15 local quality gates):**

| Category | Hooks | Purpose |
|---|---|---|
| **Python** | ruff (lint + format), mypy | Style, types, PEP8 compliance |
| **Security** | detect-private-key, debug-statements | Catch secrets, security hotspots, stray breakpoints |
| **Infrastructure** | check-yaml, check-json, check-toml, check-ast | Syntax validation for config files |
| **Git hygiene** | trailing-whitespace, end-of-file-fixer, check-merge-conflict, check-added-large-files, name-tests-test | Keep commits clean |

**Security Scanning:**
- **bandit** - Every push (CI `lint` job)
- **CodeQL** - Every push + PR + weekly Monday (Python + GitHub Actions)
- **GitGuardian** - Every push (GitHub org-level)

**Dependabot:** 5 ecosystems (pip, GitHub Actions, Terraform Part A, Terraform Part B, Docker) with patch auto-merge for pip dependencies.

---

### 9. Monitoring & Observability

**4-layer observability stack** covering infrastructure, application, data, and cost.

```mermaid
flowchart BT
    classDef infra fill:#ef4444,color:#fff
    classDef app fill:#f59e0b,color:#fff
    classDef data fill:#10b981,color:#fff
    classDef viz fill:#8b5cf6,color:#fff

    AM[Azure Monitor<br/>SQL auto-pause · Job retries]:::infra
    PROM[Prometheus + Alertmanager<br/>Airflow StatsD · cAdvisor metrics<br/>8 alert rules]:::app
    DFP[Data Freshness Probe<br/>4 Prometheus gauges<br/>Bronze/Silver/SQL/Status]:::data
    GRAF[Grafana<br/>3 dashboards · 23 panels<br/>Airflow ETL · Containers · Pipeline]:::viz

    AM --> GRAF
    PROM --> GRAF
    DFP --> GRAF
```

| Layer | Technology | Scope |
|---|---|---|
| **Infrastructure** | Azure Monitor | SQL auto-pause alerts, Databricks job retry alerts |
| **Application** | Prometheus + Alertmanager | Airflow StatsD metrics, container health, Spark executor stats |
| **Data** | Data Freshness Probe | 4 Prometheus gauges: Bronze row count, Silver row count, Azure SQL row count, pipeline status |
| **Visualisation** | Grafana | 3 auto-provisioned dashboards (23 panels total) |

**Grafana Dashboards:**

| Dashboard | Panels | Key Metrics |
|---|---|---|
| **Airflow ETL Overview** | 7 | DAG run duration, task states, dataset events, scheduling delay |
| **Container System Metrics** | 6 | CPU, memory, network I/O per container (cAdvisor → Prometheus) |
| **Pipeline Health** | 10 | Bronze row count (153,380), Silver row count (153,377), Azure SQL row count (153,377), JDBC export latency, 8 Prometheus alert rules, data staleness status |

![Container System Metrics Dashboard](docs/media/grafana_containers.png)
*Grafana Container System Metrics - CPU, memory, network I/O per container (cAdvisor → Prometheus)*

![Pipeline Health Dashboard](docs/media/grafana_health_dashboard.png)
*Grafana Pipeline Health - Data Freshness, Pipeline Status, dbt Test Pass Rate, DAG Run Duration, Duration Percentiles (p50 / p95 / p99), Task Success/Failure Rates, Row Counts across Bronze → Silver → Azure SQL*

![Grafana ETL Overview](docs/media/grafana_etl_overview.png)
*Airflow ETL Overview - DAG run duration, task states, dataset events, scheduling delay*

**Alert Rules (8 Prometheus + 2 Azure Monitor):**

| Rule | Condition | Severity | Action |
|---|---|---|---|
| DAG duration > threshold | `airflow_dag_duration > 600` | P2 | Slack + email |
| Task failure rate | `rate(task_failures[1h]) > 0` | P1 | Slack + email |
| Data staleness warning | No pipeline completion > 6h | P2 | Slack + email |
| Data staleness critical | No pipeline completion > 24h | P1 | Slack + email |
| SQL auto-pause | `cpu_percent < 0.1` for 1 hour | P1 | Email to critical action group |

![Prometheus Alert Rules](docs/media/prometheus_alerts.png)
*Prometheus Alertmanager - 8 alert rules covering DAG duration, task failures, data staleness, and SQL auto-pause*

![Prometheus Scrape Targets](docs/media/prometheus_targets.png)
*Prometheus targets - Airflow StatsD exporter, cAdvisor, Data Freshness Probe, and Pushgateway*

**Data Freshness Probe:**

A custom Python service (port 8000, `/metrics` endpoint) exposes 4 Prometheus gauges:

| Gauge | Source Query | Purpose |
|---|---|---|
| `w3c_bronze_row_count` | `SELECT COUNT(*) FROM w3c_etl_databricks.bronze.bronze_raw_logs` | Bronze ingestion health |
| `w3c_silver_row_count` | `SELECT COUNT(*) FROM w3c_etl_databricks.silver.silver_enriched_logs` | Silver enrichment health |
| `w3c_azuresql_row_count` | `SELECT COUNT(*) FROM dbo.raw_enriched` | JDBC export health |
| `w3c_pipeline_status` | Composite of above three | 0=no data, 1=running, 2=healthy, 3=stale |

The probe queries each layer on a configurable interval (default 30s), caches results in memory, and serves them via `prometheus_client` `Gauge` metric families - no database dependencies during scrape. Scraped by Prometheus every 15 seconds, these metrics feed the Pipeline Health dashboard's primary panels and drive 4 alert rules.

---

### 10. Testing Strategy

**5 layers of testing across 6 frameworks:**

| Layer | Framework | Count | Runs In |
|---|---|---|---|
| **Unit tests** | pytest | **305** (275 in CI) | Every push |
| **Data tests** | dbt test | **121** (48 not_null, 18 unique, 21 accepted_values, 10 relationships, 24 expression_is_true) | Merge to main (CD) |
| **IaC validation** | Terraform HCL + Python | **9** assertions + **39** pytest tests | Every push |
| **Static analysis** | ruff, mypy, bandit, SQLFluff | - | Every push (CI `lint`) |
| **Security SAST** | CodeQL, GitGuardian | - | Every push + weekly |

**All Test Suites Passing:**

The pipeline validates across **6 distinct test suites**, each targeting a different layer of the stack. Below the image is a breakdown of what each suite covers - all passing with zero failures:

![All Tests Passing - clean output, zero failures](docs/media/tests-all-passing.png)
*W3C ETL Pipeline — 490 passed, 123 skipped, 0 failed across pytest + dbt data tests*
*Skipped tests: 115 deselected by marker (Terraform, Integration, Silver enrichment require cloud/Databricks); 12 additional skipped due to dependencies. All tests pass in CI and full cloud environments.* 

**Suite breakdown:**

| Suite | Tool | Tests | What It Validates |
|---|---|---|---|
| **Unit tests** | pytest | 305 (17 files) | Bronze/Silver ingestion, JDBC export, dbt T-SQL macros, dimension export, UA parsing, Terraform config |
| **DAG integrity** | pytest | 23 | All 4 DAG files load, task graphs match, required args pass, import paths resolve |
| **Terraform** | pytest + HCL | 39 + 6 | Part A (Azure infra) + Part B (Databricks) via mocks; 6 native HCL assertions for resources + outputs |
| **Integration** | pytest | 18 | Cross-layer E2E: Spark → PostgreSQL → dbt, real file I/O and database writes in Docker |
| **dbt data tests** | dbt test | 121 | 48 not_null, 18 unique, 21 accepted_values, 10 relationships, 24 expression_is_true |

**Key Test Design Decisions:**

- **Marker-based filtering:** Tests are tagged (`@integration`, `@dbt_compile`, `@dag_integrity`, `@terraform`) so CI runs only environment-appropriate tests. CI runs **275 tests** (excludes 18 integration + 12 dbt-compile - expected environment gaps).
- **conftest.py** solves PEP 420 namespace shadowing (Airflow's missing `__init__.py`) by surgically adding only specific subdirectories to `sys.path`. Also builds `utils.zip` for PySpark worker serialization - mirroring the production `py_files` pattern.
- **Dual-dialect dbt compile:** CI validates both PostgreSQL + T-SQL compilation in a single job using side-by-side PostgreSQL 13 + SQL Server 2022 containers.
- **Mock-based Terraform testing:** Tests use `unittest.mock` to simulate Databricks/Terraform provider responses - validating config structure and resource attributes without real cloud credentials or network calls.

---

## Design Decisions

| Decision | Alternative | Why This Won |
|---|---|---|
| **Serverless DLT over classic clusters** | Classic job clusters with fixed VMs | Zero infrastructure management. Serverless auto-scales to zero when idle - costs $0 between runs. No cluster tuning or VM sizing needed. |
| **`maxminddb` pure Python over `geoip2`** | `geoip2==5.0.1` with compiled `libmaxminddb` | Serverless DLT cannot install compiled C extensions. `maxminddb` is pure Python and works as a simple `environment.dependencies` entry. |
| **Inline `{% if target.type == 'sqlserver' %}` over `_azure.sql` files** | Separate model files per dialect | dbt would parse both `dim_date.sql` and `dim_date_azure.sql` as independent models, creating duplicate DAG entries. Inline branches keep one source of truth. |
| **dbt runs on Databricks serverless, not Airflow** | Run dbt inside Airflow container with ODBC | Airflow container lacks ODBC 18 driver for Azure SQL. Running dbt on Databricks serverless via `DatabricksSubmitRunOperator` keeps Azure SQL traffic in the Databricks runtime. |
| **pymssql over `df.write.jdbc`** | Spark JDBC writer | Databricks serverless only supports JDBC reads, not writes. `pymssql` is pure Python with no JVM dependencies. |
| **OIDC over static secrets** | `ARM_CLIENT_SECRET`, long-lived PAT tokens | Zero static Azure credentials. The runner never stores or retrieves secrets - it assumes an Azure AD identity via token exchange at runtime. |
| **Terraform-managed OIDC auth chain** | Manual Azure AD CLI commands | One `terraform apply` creates the Azure AD app, service principal, federated credential, and role assignment - no manual CLI steps. |
| **`tuple(row)` over `row.asDict()`** | Dictionary serialization for INSERT | `tuple(row)` saves ~50s per 153K-row export (4.7M fewer dict allocations) via Spark `Row.__iter__` with zero overhead. |
| **Unity Catalog over DBFS paths** | Direct DBFS/ABFSS path references | UC provides governance, cross-catalog reads (`spark.table()` from Bronze → Silver across pipelines), and volume access for GeoIP databases. |
| **Staging + Marts schema isolation** | Single flat schema | `dbt_staging` for the atomic star schema, `dbt_marts` for pre-aggregated BI - prevents naming collisions and enforces the staging/mart boundary in dbt refs. |
| **Datasets over polling or sensors** | Airflow sensors, external task sensors | Dataset-triggered DAGs decouple ingestion from transformation without polling overhead or hard-coded DAG IDs. A `Dataset("mssql://...")` outlet auto-triggers `dbt_marts_azure`. |
| **Single `azure-dev` environment** | Staging + production | Staging/prod adds complexity without portfolio value for a CV project. Auto-approve on merge to main. dbt runs via Dataset trigger after ingestion completes. |
| **`prevent_destroy` on storage + SQL** | Allow destroy on `terraform destroy` | Prevents accidental loss of the fully configured SQL database and storage account during development iteration. `terraform destroy` intentionally fails for ADLS Gen2 and Azure SQL - requiring manual intervention to remove the `prevent_destroy` lifecycle guard first. |
| **Consolidated GeoIP struct UDF over 7 separate UDFs** | 7 PySpark UDFs (one per GeoIP field) | Single struct UDF opens `maxminddb.Reader` once per partition, returns all 6 City DB fields in one call - 3.5× fewer reader instantiations and 7× fewer Spark expression evaluations. |
| **Weekly Power BI refresh over real-time streaming** | Real-time or daily refresh | Source is historical (2009–2011) with no new data arriving. Weekly cadence validates pipeline health end-to-end and detects drift in 5 upstream dependency layers without unnecessary compute spend. |

---

## Quick Start

### Prerequisites

- Docker Desktop (for local dev - the 17-container stack)
- `uv` for Python dependency management
- MaxMind license key (free) in `airflow/.env` as `MAXMIND_LICENSE_KEY`

### Local Development

![Docker Compose Architecture](docs/media/docker.png)
*Docker Compose stack - 17 containers: Airflow, Spark, PostgreSQL, Prometheus, Grafana, and StatsD*

```bash
# Start the 17-container Airflow + Spark + Observability stack
docker compose -f airflow/docker-compose.yaml up -d

# Run dbt (PostgreSQL dialect - local dev)
dbt deps --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt
dbt run  --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt
dbt test --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt

# Run Python tests (without Docker-dependent suites)
uv run pytest tests/ -v --tb=short -m "not integration and not dbt_compile"

# Run linters
uv run ruff check --output-format=github .
uv run mypy --ignore-missing-imports tests/

# Terraform validation (no cloud credentials needed)
cd terraform/part_a && terraform init -backend=false && terraform test
cd terraform/part_b && terraform init -backend=false && terraform test
```

### Production (Azure)

The production pipeline is deployed via GitHub Actions CD on merge to `main`:

```bash
# dbt against Azure SQL (requires AZURE_SQL_* env vars)
dbt compile --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt --profile w3c_azure
dbt run    --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt --profile w3c_azure
dbt test   --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt --profile w3c_azure
```

> **Note:** The full production pipeline (Bronze → Silver → JDBC Export → Dimensions → dbt → CSV) runs on a weekly schedule: Airflow triggers the Databricks Workflow on Fridays at 17:00 UTC. The CD pipeline deploys infrastructure and DAGs only.

---

## Related Projects

- [**LAAD**](https://github.com/AhmedIkram05/laad) - ATM log aggregation & diagnostics platform with Kafka streaming, 3-layer ML/heuristic anomaly detection, and an Agentic RAG diagnostic assistant with multi-signal confidence fusion.
- [**DevSync**](https://github.com/AhmedIkram05/devsync) - Full-stack project management platform with real-time collaboration, GitHub OAuth 2.0 integration, bidirectional Issue/PR sync, and 1,452 tests - deployed on AWS ECS Fargate with OIDC CI/CD.
- [**StockLens**](https://github.com/AhmedIkram05/StockLens) - React Native mobile FinTech app that scans receipts via OCR and projects missed investment opportunities using Alpha Vantage data + ARIMA/regression forecasting, with biometric auth and AES-256 encryption.

---

<p align="center">
  <sub>Built with Azure, Databricks, dbt, Airflow, Terraform, Python, SQL, and a lot of pain.</sub>
</p>
