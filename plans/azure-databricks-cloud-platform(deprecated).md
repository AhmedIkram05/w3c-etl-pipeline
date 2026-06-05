# Plan: Azure Cloud-Native Data Platform on Databricks

> **Status:** v1.8 (sixth-review follow-up resolved: SQL Server error 208 handling now uses error-code extraction only; Azure SQL CSV export is in scope with a pyodbc implementation path; Power BI CSV/semantic contract is explicit and must be validated before handoff)
> **Replaces:** `plans/databricks-hybrid-wiring.md` (Priority 3a — hybrid toggle plan, superseded)
> **Budget:** ~$50–100 of $200 Azure credits (pipeline runs + storage + serverless SQL)
> **Estimated effort:** 20–30 hours (Phase 4 expanded for 5 computed UDFs, Phase 6 expanded for connection/docs) | **CV impact:** +Azure, +Terraform, +Databricks DLT, +Databricks Workflows, +Auto Loader, +Cloud Warehouse, +T-SQL/dbt Migration
> **Target roles:** DE (primary), SWE, AI Eng, ML Eng

---

## 1. Goal

Build a cloud-native Azure data platform alongside the existing Docker-based ETL pipeline, provisioning infrastructure via Terraform and replacing the current PySpark scripts with Databricks Delta Live Tables (DLT) pipelines using Auto Loader for ingestion. The existing Docker→PostgreSQL→dbt path remains intact for local dev and CI; the Azure path adds a production-grade cloud deployment option with Databricks Workflows orchestration and Azure SQL Database as the analytics warehouse.

**Key principle:** Add, don't replace. Every technology already in the project (Airflow, dbt, Spark, Delta Lake, Grafana, Prometheus) stays. The Azure path sits alongside it.

### Resume Line (Target)

> *"Designed a cloud-native data platform on Azure using Terraform, provisioning ADLS Gen2 storage, Databricks with Unity Catalog, and Azure SQL Database. Built a Delta Live Tables medallion pipeline with Auto Loader ingestion and Databricks Workflows orchestration, alongside an existing Airflow + dbt + Docker stack — demonstrating hybrid cloud/on-prem data engineering, IaC, and declarative pipeline quality."*

---

## 2. Architecture Overview

### 2.1 Existing Stack (Unchanged)

```
Docker Spark ──► PostgreSQL ──► dbt ──► CSVs
    ▲
Airflow (scheduler + dbt DAG)
    ▲
Grafana / Prometheus (monitoring)
```

### 2.2 New Azure Stack

```
Terraform ──► Azure Resources (IaC)

Auto Loader ──► ADLS Gen2 ──► DLT Bronze ──► DLT Silver ──► DLT Gold
                        ▲                                    │
                    (cloud storage)                    Databricks SQL / JDBC
                                                               │
                                                          Azure SQL DB
                                                               │
                                                          dbt marts
                                                               │
                                                     Databricks Workflows
                                                          (orchestrates DLT)
                                                               │
                                                     Airflow triggers Workflows
                                                         (via DatabricksRunNowOperator)
```

### 2.3 Technology Coexistence

| Component | Docker Path (local/CI) | Azure Path (cloud) |
|---|---|---|
| **Storage** | Local Delta Lake files | ADLS Gen2 + Unity Catalog |
| **Ingestion** | Python file discovery + custom parser | Auto Loader Binary File + parser UDF |
| **Processing** | PySpark scripts (`jobs/*.py`) | DLT Pipelines (declarative medallion) |
| **Orchestration** | Airflow `SparkSubmitOperator` | Databricks Workflows + Airflow trigger |
| **Warehouse** | PostgreSQL (`public.raw_enriched`) | Azure SQL Database |
| **Analytics** | dbt (PostgreSQL target) + CSV export for Power BI | dbt (Azure SQL target) + CSV export for Power BI |
| **IaC** | Docker Compose | Terraform (Azure) |
| **GeoIP** | MaxMind GeoLite2 .mmdb files (local) | MaxMind .mmdb on ADLS / UC Volume |
| **Dimension building** | `export_dimensions` (reads Delta local) | `export_dimensions` rewritten (reads UC or Azure SQL) |

---

## 3. Current State Audit

### 3.1 What already exists ✅

| Asset | Status | Notes |
|---|---|---|
| `airflow/dags/w3c/spark_ingestion.py` | ✅ Stable | 4-task DAG with `SparkSubmitOperator` chain + Dataset outlet |
| `airflow/dags/w3c/dbt_marts.py` | ✅ Stable | Dataset-triggered dbt DAG |
| `airflow/spark/jobs/bronze_ingestion.py` | ✅ Stable | Docker PySpark bronze — custom W3C parser |
| `airflow/spark/jobs/silver_enrichment.py` | ✅ Stable | Docker PySpark silver — 17 UDFs (7 GeoIP + 5 UA + 5 computed) |
| `airflow/spark/jobs/export_warehouse.py` | ✅ Stable | Docker PySpark JDBC export with tracking table pattern |
| `airflow/spark/databricks/01_bronze_ingestion.py` | ✅ Scaffolding | Unity Catalog script — self-contained W3C parser with per-file format detection |
| `airflow/spark/databricks/02_silver_enrichment.py` | ✅ Scaffolding | Unity Catalog script — already omits UA columns from Silver DDL |
| `airflow/spark/databricks/03_export_warehouse.py` | ✅ Scaffolding | Unity Catalog script — JDBC export pattern |
| `airflow/plugins/operators/export_dimensions.py` | ✅ Stable | PythonOperator building dims — reads Silver Delta from **local path** |
| `airflow/dbt/w3c/` | ✅ Stable | 16 dbt models, ~62 tests — 32 PostgreSQL `::` cast expressions |
| `.github/workflows/ci.yml` | ✅ Stable | Ruff, mypy, pytest, dbt compile |
| `airflow/spark/jobs/utils/geoip.py` | ✅ Stable | 7 MaxMind GeoLite2 UDFs (offline .mmdb, no network calls) |
| `airflow/spark/jobs/utils/ua_parser.py` | ✅ Stable | 5 UA-parsing UDFs |
| `airflow/spark/jobs/utils/transformations.py` | ✅ Stable | 5 computed-field UDFs |
| `airflow/spark/jobs/utils/w3c_parser.py` | ✅ Stable | W3C line parser — handles unquoted user-agent via rsplit field counting |
| Prometheus / Grafana / Alertmanager | ✅ Stable | Full monitoring stack |
| Test suite (6 test files, 136+ tests) | ✅ Strong | unit, DAG integrity, integration |

### 3.2 What is missing ❌

| Gap | Impact |
|---|---|
| No Terraform configuration | No IaC for cloud resources |
| No Azure subscription configured | Cannot provision cloud resources |
| No ADLS Gen2 storage account | No cloud data lake foundation |
| No Databricks workspace in Azure | No cloud Spark execution environment |
| No Databricks Workflows definitions | No cloud-native orchestration |
| No DLT pipeline code | No declarative medallion pipeline |
| No Auto Loader ingestion | No cloud-native incremental ingestion |
| No Azure SQL Database | No cloud warehouse target |
| No dbt profile for Azure SQL | ✅ w3c_azure profile already in profiles.yml |
| No Airflow→Databricks Workflows operator | Airflow cannot trigger cloud path |
| No cost-management config | Risk of burning Azure credits |
| `export_dimensions.py` reads local Delta path | **Broken in Azure path** — Silver is in ADLS/UC, not local filesystem |
| `apache-airflow-providers-databricks==4.6.0` installed in Dockerfile | ✅ Already done |
| ODBC Driver 18 for SQL Server in Dockerfile | ✅ Already done |

### 3.3 Constraints & Decisions

- **Azure credits:** $200, 30-day expiry. Must tear down after verification or set budget alerts ($50 alert, $100 hard cap).
- **Databricks Premium tier** required for Unity Catalog. Do NOT use the free trial workspace — use the credit-backed subscription.
- **Auto Loader** requires cloud storage (ADLS Gen2) — local dev cannot use it. The Docker path stays for offline work.
- **DLT pipelines** cost per DBU-hour. With small W3C data (~100MB), expect 2–5 DBUs per full pipeline run (~$1–3).
- **Azure SQL Database serverless** with auto-pause minimizes cost (~$5–15/month if left running).
- **dbt supports Azure SQL** via the `sqlserver` adapter (`dbt-sqlserver`). Models are mostly portable (minor dialect differences: `TEXT` vs `VARCHAR(MAX)`, `::` casts vs `CAST()`).
- **Airflow `DatabricksRunNowOperator`** (available in `apache-airflow-providers-databricks`) can trigger an existing Databricks Workflow job. This is the integration point.
- **Existing Databricks scripts** (`01_*/02_*/03_*`) will be **replaced** by DLT pipelines, not wired in. They served as scaffolding to prove the Unity Catalog DDL; DLT is the production-grade evolution. The existing `01_bronze_ingestion.py` parser logic (`parse_log_line` + `detect_format`) is the reference implementation for Phase 3.
- **Python version compatibility:** The project standard is Python 3.12, but Databricks Runtime (DBR) 15.4 LTS ships with Python 3.11. DLT pipeline code must avoid Python 3.12-only features (`match`/`case` outside limited contexts, `except*`, `from __future__ import annotations` with `|` unions in older DBR).
- **GeoIP: MaxMind GeoLite2, not ip-api.com.** The existing codebase uses `airflow/spark/jobs/utils/geoip.py` with 7 offline MaxMind GeoLite2 UDFs. The `.mmdb` database files must be uploaded to Unity Catalog Volume or DBFS for the DLT pipelines. There is zero `ip-api.com` usage anywhere in the project.
- **Dead columns status:** The `plans/fact-webrequest-dead-col-cleanup.md` cleanup removed 11 columns from the JDBC DDL only. In the Silver Delta table:
  - **6 geo columns MUST stay** in Silver: `country`, `region`, `city`, `latitude`, `longitude`, `isp` — the Airflow `export_dimensions` operator reads these from Silver to build `dim_geolocation`. (`postcode` is also a GeoIP-derived column but is classified as a computed field in the 25-column core section.)
  - **5 UA columns CAN be skipped** from Silver: `agent_type`, `browser_name`, `browser_version`, `operating_system`, `device_type` — `dim_useragent` is built from `user_agent` string in PostgreSQL, not Silver.
  - The existing `02_silver_enrichment.py` scaffold already omits UA columns from its Silver DDL, consistent with this.
- **`export_dimensions.py` local path dependency:** The `export_dimensions` PythonOperator reads Silver Delta parquet files from `/opt/spark/delta/silver` (a local path). In the Azure path, Silver data lives in Unity Catalog / ADLS Gen2. Two approaches: (a) rewrite `export_dimensions` to read from Azure SQL or Unity Catalog instead, or (b) run it inside Databricks as a Workflow task. This plan uses approach (a) — the `export_dimensions` operator is refactored to conditionally read from the cloud warehouse when in Azure mode.
- **Databricks provider dependency:** `apache-airflow-providers-databricks==4.6.0` must be added to both `requirements.txt` and the Dockerfile for the `DatabricksRunNowOperator` import.
- **Azure SQL ODBC driver:** The `dbt-sqlserver` adapter uses `pyodbc` which requires the Microsoft ODBC Driver 18 for SQL Server. This must be installed in the Docker image or dbt must be run outside Docker.

### 3.4 Power BI Semantic Contract (Must Preserve)

The current Power BI dashboard is fed by CSV extracts under `airflow/data/Star-Schema/`, generated by `airflow/dags/w3c/dbt_marts.py`. The Azure path must preserve this BI contract, not just load Azure SQL. DAX measures and friendly display names live in the Power BI semantic model outside this repo; the repo-owned contract is the exported CSV table list, stable column headers, row-level grain, and supporting dbt mart fields those measures depend on.

> **Existing bug to fix:** the repo currently has 18 BI-ready models/tables to export, but `airflow/dags/w3c/dbt_marts.py` exports only 17 CSVs because `dbt_marts.mart_country_browser_share` is missing from `MART_TABLES`. Fix this in the Docker path and carry the same 18-file export contract into the Azure path.

**Required CSV table export list:**

```python
STAGING_TABLES = [
    "dbt_staging.fact_webrequest",
    "dbt_staging.dim_date",
    "dbt_staging.dim_time",
    "dbt_staging.dim_page",
    "dbt_staging.dim_status",
    "dbt_staging.dim_referrer",
    "dbt_staging.dim_method",
    "dbt_staging.dim_visitortype",
    "dbt_staging.dim_visit_buckets",
    "dbt_staging.crawler_ips",
]

MART_TABLES = [
    "dbt_marts.mart_page_performance",
    "dbt_marts.mart_daily_aggregates",
    "dbt_marts.mart_crawler_analysis",
    "dbt_marts.mart_browser_analysis",
    "dbt_marts.mart_timeofday_analysis",
    "dbt_marts.mart_country_browser_share",
]

PUBLIC_TABLES = [
    "public.dim_geolocation",  # Azure SQL equivalent: dbo.dim_geolocation
    "public.dim_useragent",    # Azure SQL equivalent: dbo.dim_useragent
]
```

> **Required correction:** add `dbt_marts.mart_country_browser_share` to the Docker `MART_TABLES` list, Azure `MART_TABLES` list, CSV export validation, and `tests/test_dag_integrity.py` expected export assertions. The final Power BI export count is **18 CSV files**, not 17.

**Required exported headers for the user's listed Power BI tables:**

| Power BI table | CSV source | Required headers |
|---|---|---|
| Date | `dbt_staging.dim_date.csv` | `date_sk`, `date`, `year`, `month`, `month_name`, `day_number`, `day_name`, `day_of_week`, `quarter`, `week_of_year`, `is_weekend`, `holiday_flag` |
| Geolocation | `public.dim_geolocation.csv` / `dbo.dim_geolocation.csv` | `geolocation_sk`, `ip`, `country`, `region`, `city`, `postcode`, `latitude`, `longitude`, `isp` |
| Method | `dbt_staging.dim_method.csv` | `method_sk`, `http_method`, `description` (`is_safe` may remain as an extra non-breaking column) |
| Page | `dbt_staging.dim_page.csv` | `page_sk`, `page_path`, `query_string`, `directory`, `file_name`, `file_extension`, `page_category` |
| Referrer | `dbt_staging.dim_referrer.csv` | `referrer_sk`, `referrer_url`, `referrer_domain`, `traffic_source` |
| Status | `dbt_staging.dim_status.csv` | `status_sk`, `status_code`, `sub_status`, `win32_status`, `status_category`, `status_label`, `description`, `severity` |
| Time | `dbt_staging.dim_time.csv` | `time_sk`, `hour`, `minute`, `am_pm`, `time_band`, `shift_id` |
| User Agent | `public.dim_useragent.csv` / `dbo.dim_useragent.csv` | `user_agent_sk`, `user_agent`, `agent_type`, `browser_name`, `browser_version`, `operating_system`, `device_type` |
| Visit Buckets | `dbt_staging.dim_visit_buckets.csv` | `visit_bucket`, `visit_bucket_order`, `user_count` (`visit_bucket_sk` may remain as an extra key column) |
| Visitor Type | `dbt_staging.dim_visitortype.csv` | `visitor_sk`, `crawler_flag`, `visitor_type` |
| Web Requests | `dbt_staging.fact_webrequest.csv` | `raw_log_id`, `date_sk`, `time_sk`, `page_sk`, `method_sk`, `status_sk`, `referrer_sk`, `geolocation_sk`, `user_agent_sk`, `visitor_sk`, `bytes_sent`, `bytes_received`, `response_time_ms`, `request_count`, `is_404`, `is_crawler`, `is_direct_traffic`, `size_band`, `time_band` (`source_file`, `visit_bucket_sk`, `page_category`, `referrer_domain`, `traffic_type` may remain as extra non-breaking columns) |

**Required DAX/measure support check:** the Azure SQL + dbt path must preserve the source fields used to calculate all current Power BI measures: 404 request counts/rates (`is_404`, mart `total_404`/`pct_404`), active countries (`dim_geolocation.country`, `mart_daily_aggregates.active_countries`), bytes and response time measures (`bytes_sent`, `bytes_received`, `response_time_ms`, mart average/P95 fields), crawler/human/direct traffic rates (`is_crawler`, `is_direct_traffic`, `mart_daily_aggregates.pct_crawler`), desktop/top-browser share (`dim_useragent.device_type`, `mart_browser_analysis.pct_of_daily_traffic`, `mart_daily_aggregates.top_browser_share`), peak-hour metrics (`mart_timeofday_analysis.total_requests`, `mart_daily_aggregates.peak_hour_requests`, `peak_traffic_hour`), unique hosts/pages (`geolocation_sk`, `page_sk`), and weekday traffic (`dim_date.day_name`, `is_weekend`, `holiday_flag`).

**Handoff validation before declaring Power BI compatibility:**

1. Export CSVs from Docker and Azure paths into separate directories.
2. Compare headers for every file in `STAGING_TABLES + MART_TABLES + PUBLIC_TABLES`; header order should match existing CSVs unless Power BI is confirmed to bind by column name only.
3. Regenerate Docker CSVs before using them as the baseline. The committed `dbt_marts.mart_daily_aggregates.csv` can be stale; the current SQL model includes `peak_hour_requests`, `peak_traffic_hour`, and `top_browser_share`, and those headers must exist in the regenerated baseline and Azure export.
4. Compare row counts for all dimension tables and marts; allow only documented differences caused by Azure SQL type/dialect behavior.
5. Confirm exactly 18 CSV files are exported, including `dbt_marts.mart_country_browser_share.csv`.
6. Manually refresh the PBIX/Power BI dataset against the Azure CSV export directory and verify all visuals/measures listed by the user refresh without missing-column, relationship, or DAX errors.

---

## 4. Phased Plan

### Prerequisites: Azure Account

Before starting Phase 1, ensure you have:

1. **Azure subscription with credits** — Sign up at <https://azure.microsoft.com/en-gb/free/> ($200 credit, expires 30 days from account creation)
2. **Azure CLI installed** — `brew install azure-cli` (macOS) or `winget install Microsoft.AzureCLI` (Windows)
3. **Azure CLI authenticated**:

   ```bash
   az login --use-device-code
   az account show --query id -o tsv   # Copy as ARM_SUBSCRIPTION_ID
   ```

4. **Check available credits** — Azure Portal → Cost Management → Credits
5. **⚠️ Credit expiry warning** — The $200 credit expires 30 days from account creation. If this plan spans > 30 days, Azure resources will incur real costs. Set a calendar reminder 25 days from creation.
6. **Databricks CLI installed + authenticated:**

   > **⚠️ CLI version:** `brew install databricks` installs the **new Databricks CLI (v2+)** which uses different syntax from the legacy `databricks-cli` pip package. The plan uses **legacy CLI syntax** (`databricks configure --token`, `databricks secrets create-scope`, `databricks workspace import --format`) because it's more widely documented and compatible with existing Databricks workflows.
   >
   > **Option A (legacy — recommended for this plan):** `pip install databricks-cli`
   > **Option B (new CLI v2+):** `brew install databricks` then translate commands:
   > - `databricks configure --token` → `databricks auth login --host <workspace-url>`
   > - `databricks workspace import --file <local> --path <remote> --format SOURCE` → `databricks workspace import <local> <remote>`
   > - `databricks secrets create-scope --scope <name>` → `databricks secrets put-scope <name>`
   > - `databricks fs cp <local> <remote>` → `databricks fs cp <local> <remote>` (mostly same)
   >
   > **Choose one CLI version and use it consistently throughout all phases.**

   ```bash
   # Option A (legacy — matches this plan's commands):
   pip install databricks-cli
   databricks configure --token   # Will prompt for workspace URL + PAT token
   
   # Option B (new CLI — see translation table above):
   # brew install databricks
   # databricks auth login --host <workspace-url>
   ```

   Note: The PAT token is created in Phase 6 (Databricks workspace must be provisioned first). For Phase 3 GeoIP uploads, you'll return to configure the CLI after Phase 2 provisions the workspace.
7. **⚠️ Register for a MaxMind GeoLite2 account** — The GeoIP enrichment at Phase 4 requires MaxMind GeoLite2 City and ASN databases. These are free but require registration:

   ```bash
   # Create account at https://www.maxmind.com/en/geolite2/signup
   # After login, download:
   #   GeoLite2-City.mmdb  (geo enrichment: country, region, city, lat, lon, postcode)
   #   GeoLite2-ASN.mmdb   (ISP lookup)
   # Save both files to ./data/geoip/ for the Docker path, or note path for Phase 4 upload.
   #
   # This is a MANUAL step — cannot be automated. Register now to avoid blocking Phase 4.
   ```

### Phase 1 — Terraform IaC Modules (4 hours)

**Goal:** Production-grade Terraform Part A modules that provision the core Azure stack. `terraform validate` + `terraform plan` green.

- [ ] Create `terraform/` directory structure with Part A/B split:

  ```text
  terraform/
  ├── part_a/
  │   ├── environments/
  │   │   └── dev/
  │   │       ├── main.tf              # Root module composition
  │   │       ├── terraform.tfvars      # Dev-specific values
  │   │       └── provider.tf          # Azure provider config
  │   ├── modules/
  │   │   ├── datalake/
  │   │   │   ├── main.tf              # ADLS Gen2 storage account + containers
  │   │   │   ├── variables.tf
  │   │   │   └── outputs.tf
  │   │   ├── databricks/
  │   │   │   ├── main.tf              # Databricks workspace + Unity Catalog + workflows + secret scope
  │   │   │   ├── variables.tf
  │   │   │   └── outputs.tf
  │   │   ├── networking/
  │   │   │   ├── main.tf              # VNet, subnets, NSG (simplified for MVP)
  │   │   │   ├── variables.tf
  │   │   │   └── outputs.tf
  │   │   └── warehouse/
  │   │       ├── main.tf              # Azure SQL Database serverless + firewall rule
  │   │       ├── variables.tf
  │   │       └── outputs.tf
  │   └── versions.tf                  # Provider version constraints (see below)
  ├── part_b/
  │   ├── environments/
  │   │   └── dev/
  │   │       ├── main.tf
  │   │       └── terraform.tfvars
  │   ├── modules/databricks-pipelines/
  │   └── versions.tf
  ```

- [ ] **`versions.tf`** — pin provider versions explicitly:

  ```hcl
  terraform {
    required_version = ">= 1.10.5, < 2.0"
    required_providers {
      azurerm = {
        source  = "hashicorp/azurerm"
        version = "~> 4.75.0"  # Pinned to ~> 4.75 to avoid unexpected breaking changes in 4.x minor releases
      }
      databricks = {
        source  = "databricks/databricks"
        version = "~> 1.70"  # >= 1.70, < 2.0 — Part B also uses ~> 1.70 to ensure consistent provider features
      }
    }
  }
  ```

  > **Rationale:** `azurerm ~> 4.75.0` is pinned to the latest stable 4.x minor to avoid unexpected breaking changes — the 4.x series has 75+ releases with property migrations. `databricks ~> 1.70` ensures Part A and Part B use the same provider version range (Part B uses `~> 1.70`), avoiding feature-gap issues where Part A resolves to pre-1.70 and Part B requires 1.70+ resources. Both providers have stable APIs for Unity Catalog. After `terraform init`, run `terraform providers lock` to generate a `terraform.lock.hcl` file committed to the repo for deterministic provider versioning.

- [ ] **Module: `datalake`**
  - Azure Resource Group (output: name, location)
  - Storage account with `is_hns_enabled = true` (Data Lake Gen2 gen2)
  - Properties: `account_tier = "Standard"`, `account_replication_type = "LRS"` (cheapest)
  - 4 containers: `raw-logs`, `bronze`, `silver`, `gold`
  - RBAC: `azurerm_role_assignment` assigning `Storage Blob Data Contributor` to the Databricks workspace managed identity (principal_id from databricks module)
  - Output: storage account name, primary access key (sensitive), container URLs, principal_id of Databricks workspace MI

- [ ] **Module: `databricks`**
  - Databricks workspace (Premium tier, `sku = "premium"`)
  - Unity Catalog metastore (auto-provisioned if using Premium workspace; if not, create `databricks_metastore` resource)
  - Catalog: `w3c_catalog` (via `databricks_catalog`)
  - Schemas: `bronze`, `silver`, `gold` (via `databricks_schema`)
  - Output: workspace URL, workspace ID, workspace host

- [ ] **Module: `networking` (simplified for MVP)**
  - VNet with address space (e.g., `10.0.0.0/16`)
  - 2 subnets: Databricks (delegated) + Azure SQL
  - NSG with rules for Azure SQL (allow Azure services + Databricks workspace IP)
  - Skip private endpoints for cost — make them conditional via `var.enable_private_endpoints` (default: `false`)
  - Output: subnet IDs, VNet ID

- [ ] **Module: `warehouse`**
  - Azure SQL Database serverless (`GP_S_Gen5`, 1 vCore, `auto_pause_delay_in_minutes = 60`)
  - Server-level firewall rules: `start_ip_address = "0.0.0.0"`, `end_ip_address = "0.0.0.0"` for "Allow Azure services" + at least one rule for your dev IP
  - Database: `w3c_warehouse`
  - Collation: `SQL_Latin1_General_CP1_CI_AS` (default, matches dbt conventions)
  - Output: server FQDN, database name, connection string template (sensitive)

- [ ] **Root `main.tf`** — compose all modules, wire outputs to inputs
- [ ] **`provider.tf`** — `azurerm` provider with `features {}` block (empty features is fine)
- [ ] **Root `outputs.tf`** — export key resource IDs for CI and downstream use:
  
  > **CRITICAL:** Part A `module.databricks-pipelines` does NOT exist (pipelines are in Part B). The `databricks_job_id` output is defined in Part B's `outputs.tf` instead. Part A exports only infrastructure-level outputs.
  
  ```hcl
  output "storage_account_name" {
    description = "ADLS Gen2 storage account name (for Azure SQL JDBC export URIs)"
    value       = module.datalake.storage_account_name
  }
  output "databricks_host" {
    description = "Databricks workspace URL (for Airflow connection)"
    value       = module.databricks.workspace_url
  }
  output "azure_sql_server_fqdn" {
    description = "Azure SQL Database server FQDN (for dbt profiles and JDBC URL)"
    value       = module.warehouse.server_fqdn
  }
  output "databricks_workspace_id" {
    description = "Databricks workspace ID (for CI workflows requiring workspace context)"
    value       = module.databricks.workspace_id
  }
  ```

  > **Why outputs matter:** Airflow's `DatabricksRunNowOperator` needs the Databricks job ID. dbt profiles need the Azure SQL FQDN. CI needs the workspace ID. Without these outputs, each deployment requires manual copy-paste of resource IDs. See Phase 10 for CI integration.
- [ ] **`terraform/README.md`** — how to init, plan, apply, destroy, plus cost estimate

**Acceptance:** `cd terraform/part_a && terraform init && terraform validate && terraform plan` succeeds with a complete resource list. No actual Azure resources created yet (Phase 2 applies). Provider versions pinned to avoid breaking changes.

---

### Phase 2 — Deploy Azure Infrastructure (30 min)

**Goal:** Run `terraform apply` to provision the Azure stack.

- [ ] Install Azure CLI, authenticate: `az login --use-device-code`
- [ ] Set environment variables (or write to `environments/dev/terraform.tfvars`):

  ```bash
  export ARM_SUBSCRIPTION_ID="<your-subscription-id>"
  # If using terraform.tfvars instead:
  cat > terraform/part_a/environments/dev/terraform.tfvars << 'EOF'
  resource_group_name = "w3c-etl-dev"
  location           = "westeurope"
  enable_private_endpoints = false
  EOF
  ```

- [ ] Run `cd terraform/part_a && terraform init` (backend = local for MVP; `azurerm` backend recommended for team collaboration — configure in `terraform/part_a/versions.tf` before any team member runs `terraform apply`, as state migration is required once non-local backends are used)
- [ ] Run `cd terraform/part_a && terraform plan` — review resources to be created
- [ ] Run `cd terraform/part_a && terraform apply --auto-approve`
- [ ] Verify resources in Azure Portal:
  - Resource Group exists with all resources
  - ADLS Gen2 containers accessible (Storage Browser → containers visible)
  - Databricks workspace accessible via URL (launch workspace)
  - Azure SQL Database reachable:

    ```bash
    sqlcmd -S <server>.database.windows.net -d w3c_warehouse -U w3c_pipeline -P '<password>' -Q "SELECT COUNT(*) AS tables FROM sys.tables"
    ```

    (Requires `sqlcmd` — install via `brew install mssql-tools` on macOS or `apt install mssql-tools` on Ubuntu/Debian)
- [ ] Run `terraform output` and save outputs to `.env.azure` (for later phases):

  ```bash
  mkdir -p databricks/dlt databricks/jobs
  cd terraform/part_a && terraform output -json > ../../.env.azure.json
  ```

- [ ] Create an Azure SQL Database login/user for pipeline access:

  ```sql
  -- Connect to w3c_warehouse via sqlcmd or Azure Data Studio
  CREATE LOGIN w3c_pipeline WITH PASSWORD = '<generate-a-password>';
  CREATE USER w3c_pipeline FOR LOGIN w3c_pipeline;
  ALTER ROLE db_datareader ADD MEMBER w3c_pipeline;
  ALTER ROLE db_datawriter ADD MEMBER w3c_pipeline;
  -- Grant CREATE TABLE + ALTER on dbo schema instead of broad db_ddladmin
  GRANT CREATE TABLE TO w3c_pipeline;
  GRANT ALTER ON SCHEMA::dbo TO w3c_pipeline;
  ```

  Save credentials to `.env.azure` or a password manager.

**Acceptance:** All Azure resources provisioned and verified. Total Terraform apply time < 5 minutes. Cost-to-date: ~$0 (storage + workspace are free until used). SQL login created for pipeline access.

#### Failure Recovery — Phase 2

| Failure Scenario | Recovery Action |
|---|---|
| `terraform apply` fails after partial resource creation | Run `terraform destroy -target=azurerm_resource_group.w3c_etl` to clean up, fix the error, re-run `terraform apply` |
| State lock contention | `terraform force-unlock <LOCK_ID>` after verifying no other apply is running |
| Azure quota exceeded | Request quota increase via Azure Portal → Support → Quota increase; or reduce cluster sizes in variables |

---

### Phase 3 — Auto Loader + DLT Bronze Pipeline (2.5 hours)

**Goal:** Auto Loader incrementally ingests W3C log files from ADLS Gen2 into a Bronze Delta table, using the existing W3C parser logic in a custom UDF.

> **CRITICAL:** W3C Extended Log Format uses space-delimited fields with **unquoted user-agent strings** that contain spaces. Auto Loader's built-in CSV parser **cannot** handle this correctly — it will split every token, corrupting the `user_agent` column. The existing `airflow/spark/jobs/utils/w3c_parser.py` handles this by using `rsplit()` from the right, preserving the UA. **The DLT Bronze must use Auto Loader Binary File format** to read raw file content, then apply the parser logic via a UDF that uses the same `rsplit()` pattern. The UDF returns an array of structs per file, which is exploded to produce individual rows. This avoids `foreachBatch` (which returns `StreamingQuery`, not `DataFrame`) and eliminates the undefined `spark` reference bug in callbacks.

- [ ] Create `databricks/dlt/dlt_bronze.py`:

  ```python
  import dlt
  import os
  from pyspark.sql import functions as F
  from pyspark.sql.types import (
      StructType, StructField, StringType, IntegerType, DateType, ArrayType,
  )

  # ── Schema matching authoritative airflow/spark/jobs/utils/schemas.py ──
  # 19 fields: log_date as DateType (parsed from "YYYY-MM-DD" log filenames),
  # bytes_sent/bytes_recv/time_taken as IntegerType (schema.py uses IntegerType,
  # not LongType — LogLine namedtuple fields are ints, and none exceed 2^31-1
  # for the W3C 2009-2011 dataset of ~155K requests).
  BRONZE_SCHEMA = StructType([
      StructField("log_date", DateType(), True),
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
      StructField("bytes_sent", IntegerType(), True),
      StructField("bytes_recv", IntegerType(), True),
      StructField("server_port", IntegerType(), True),
      StructField("username", StringType(), True),
      StructField("time_taken", IntegerType(), True),
      StructField("source_file", StringType(), True),
  ])

  # Same schema wrapped in ArrayType for the per-file UDF return
  PARSE_SCHEMA = ArrayType(BRONZE_SCHEMA)


  def _safe_int(v: str):
      """Safely convert a string to int, returning None on failure.
      
      CRITICAL: Matches authoritative w3c_parser.py::safe_int() which
      rejects negative values (time_taken should never be negative).
      """
      try:
          v = int(v)
          if v < 0:
              return None  # Reject negative values (matches authoritative parser)
          return v
      except (ValueError, TypeError):
          return None


  def _detect_format(text: str) -> int:
      """Detect 14 or 18-field W3C format from #Fields: header line.
      
      Returns the format (14 or 18), RAISES ValueError for unknown formats
      to prevent silent field corruption (the default-to-18 fallback can
      silently misalign columns).
      
      Raises:
          ValueError: If header found with field count not in (14, 18).
      """
      for line in text.splitlines():
          if line.startswith("#Fields:"):
              count = len(line.replace("#Fields:", "").strip().split())
              if count in (14, 18):
                  return count
              raise ValueError(
                  f"Unknown W3C format: {count} fields. "
                  f"Expected 14 or 18. File may be corrupted or a different log format."
              )
      raise ValueError(
          "No #Fields: header found in W3C log file. "
          "Cannot determine field format (14 or 18-field)."
      )


  @F.udf(returnType=PARSE_SCHEMA)
  def parse_w3c_content(content: bytes, source_file_path: str) -> list:
      """Parse raw W3C binary file content into an array of parsed row structs.
      
      CRITICAL: Uses line.rsplit() on trailing fixed-width fields instead of
      line.split(), matching the authoritative airflow/spark/jobs/utils/w3c_parser.py.
      This preserves user-agent strings with spaces (e.g. "Mozilla/5.0 (Windows NT 10.0)").
      
      Field ordering (authoritative w3c_parser.py):
        0=date, 1=time, 2=s-ip, 3=cs-method, 4=cs-uri-stem, 5=cs-uri-query,
        6=s-port, 7=cs-username, 8=c-ip, 9=cs(User-Agent),
        10=cs(Cookie), 11=cs(Referer)     ← 18-field only
        12=sc-status, 13=sc-substatus, 14=sc-win32-status,
        15=sc-bytes, 16=cs-bytes, 17=time-taken  ← 18-field only
        (14-field: 10=status, 11=substatus, 12=win32-status, 13=time-taken)
      """
      text = content.decode("utf-8", errors="replace")
      source_file = os.path.basename(source_file_path)
      
      file_format = _detect_format(text)
      rows = []
      
      for line in text.splitlines():
          stripped = line.strip()
          if not stripped or stripped.startswith("#"):
              continue
          
          try:
              if file_format == 14:
                  # rsplit on 4 trailing fixed-width fields: status, substatus, win32_status, time_taken
                  rs = line.rsplit(" ", 4)
                  if len(rs) < 5:
                      continue
                  rs[0] = rs[0].strip()
                  ls = rs[0].split(" ", 9)
                  if len(ls) < 10:
                      continue
                  
                  rows.append((
                      ls[0],              # log_date
                      ls[1],              # log_time
                      ls[2],              # server_ip (s-ip)
                      ls[3],              # method
                      ls[4],              # uri_stem
                      ls[5],              # uri_query
                      ls[8],              # client_ip (c-ip)
                      ls[9],              # user_agent (rsplit preserves spaces)
                      None,               # cookie
                      None,               # referrer
                      _safe_int(rs[1]),   # status
                      _safe_int(rs[2]),   # sub_status
                      _safe_int(rs[3]),   # win32_status
                      None,               # bytes_sent
                      None,               # bytes_recv
                      _safe_int(ls[6]),   # server_port
                      ls[7],              # username
                      _safe_int(rs[4]),   # time_taken
                      source_file,
                  ))
              else:  # 18-field format
                  # rsplit on 6 trailing fixed-width fields: status, substatus, win32_status,
                  # bytes_sent, bytes_recv, time_taken
                  rs = line.rsplit(" ", 6)
                  if len(rs) < 7:
                      continue
                  rs[0] = rs[0].strip()
                  ls = rs[0].split(" ", 11)
                  if len(ls) < 12:
                      continue
                  
                  rows.append((
                      ls[0],              # log_date
                      ls[1],              # log_time
                      ls[2],              # server_ip (s-ip)
                      ls[3],              # method
                      ls[4],              # uri_stem
                      ls[5],              # uri_query
                      ls[8],              # client_ip (c-ip)
                      ls[9],              # user_agent (rsplit preserves spaces)
                      ls[10],             # cookie
                      ls[11],             # referrer
                      _safe_int(rs[1]),   # status
                      _safe_int(rs[2]),   # sub_status
                      _safe_int(rs[3]),   # win32_status
                      _safe_int(rs[4]),   # bytes_sent
                      _safe_int(rs[5]),   # bytes_recv
                      _safe_int(ls[6]),   # server_port
                      ls[7],              # username
                      _safe_int(rs[6]),   # time_taken
                      source_file,
                  ))
          except Exception:
              continue
      
      return rows


  @dlt.table(
      name="bronze_raw_logs",
      comment="Raw W3C logs ingested via Auto Loader (Binary File) with UDF using rsplit for UA preservation",
      table_properties={
          "delta.enableChangeDataFeed": "true",
          "delta.autoOptimize.optimizeWrite": "true",
      },
      partition_cols=["log_date"],
  )
  @dlt.expect_or_drop("valid_log_date", "log_date IS NOT NULL")
  @dlt.expect_or_drop("valid_source_file", "source_file IS NOT NULL")
  def bronze_raw_logs():
      """Bronze DLT table: Auto Loader reads binary files, parses W3C content via UDF.
      
      CRITICAL: Returns a DataFrame (not .start() on a stream).
      DLT table functions MUST return a DataFrame — the function returns the
      exploded result of parsing binary file content. DLT handles materialization.
      
      The UDF uses rsplit() matching authoritative w3c_parser.py to preserve
      user-agent strings with spaces.
      """
      raw_path = spark.conf.get("pipeline.raw_path")
      checkpoint_path = spark.conf.get("pipeline.checkpoint_path")
      
      # Step 1: Auto Loader reads raw binary files
      # CRITICAL: binaryFile loads the ENTIRE file content as a single column into
      # executor memory. Without bounds, a single large file (>200MB) can OOM the
      # executor. maxFilesPerTrigger limits files per micro-batch; maxFileSize
      # skips oversized files entirely.
      raw = (
          spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "binaryFile")
          .option("cloudFiles.includeExistingFiles", "true")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .option("maxFilesPerTrigger", "10")       # Bound memory per trigger
          .option("maxFileSize", "209715200")         # 200MB max per file (skip larger)
          .load(raw_path)
      )
      
      # Step 2: Parse each binary file via UDF that returns an array of structs,
      # then explode to produce one row per log line.
      # This avoids foreachBatch (which returns StreamingQuery, not DataFrame)
      # and avoids undefined `spark` references in callbacks.
      parsed = (
          raw.select(
              F.explode(parse_w3c_content("content", "path")).alias("row")
          )
          .select("row.*")
      )
      
       # Step 3: Filter out empty files (only header lines, no data rows).
       # NOTE: No dead-letter-queue implemented for this MVP. Empty-file logs
       # (only header lines) are simply excluded. A future enhancement could
       # write rejected files to a DLQ Delta table.
      parsed_with_content = parsed.filter(
          F.col("client_ip").isNotNull() & (F.length(F.col("client_ip")) > 0)
      )
      
      return parsed_with_content
  ```

  > **Why UDF + explode instead of foreachBatch?** DLT table functions MUST return a `DataFrame`. `writeStream.foreachBatch(...).start()` returns `StreamingQuery`, causing DLT to materialize 0 rows. The UDF+explode pattern: each file is parsed by a single UDF call (returns `ArrayType(BRONZE_SCHEMA)`), then `F.explode()` flattens it to individual rows. This also eliminates the undefined `spark` bug — UDFs don't need a spark reference since they operate on the inputs directly. The parser uses `rsplit()` matching authoritative `w3c_parser.py` instead of `split()`, preserving user-agent strings with spaces in unquoted W3C format.

- [ ] Configure Auto Loader:
  - Uses `binaryFile` format — reads raw file bytes + metadata (path, modification time)
  - `cloudFiles.includeExistingFiles = "true"` — picks up existing files on first run
  - Checkpoint location in bronze container for idempotent re-runs
  - The W3C parser logic is self-contained in the `parse_log_line` function (ported from `w3c_parser.py::parse_log_line` — note function name differs from `01_bronze_ingestion.py::parse_file_to_df` because the DLT signature receives a whole file's content, not an RDD)

  > **⚠️ full_refresh dedup:** When `full_refresh=True` (common during development), DLT re-reads ALL files from the source directory. Auto Loader does NOT deduplicate by `source_file` on re-read — it processes every available file as if first seen. To avoid duplicate rows:
  > - **(a) Truncate before re-run:** `DROP TABLE` or `DELETE FROM` the bronze table in Unity Catalog, then re-run with `full_refresh=True`.
  > - **(b) Dedup in the DLT view (✅ RECOMMENDED):** Add `ROW_NUMBER() OVER (PARTITION BY source_file, log_date, log_time ORDER BY source_file) AS rn` after the explode, then filter `rn = 1`. This keeps the Bronze table idempotent even during full refreshes — the dedup is part of the DLT view definition. Add it as a CTE step before the final return.
  > - **(c) Production mode:** Use `full_refresh=False` (incremental) after initial backfill. Only new files are processed.
  >
  > **Recommendation:** Use option **(b)** during development — it's the safest approach because it works regardless of how many times you trigger a full refresh. Option (a) requires manual DROP TABLE between runs. Option (c) is for production only.
  > **Full refresh is a development tool, not a production mode.** In production, DLT runs with `full_refresh=False` and Auto Loader's checkpoint tracks already-ingested files.

- [ ] Create a sample W3C log file and upload to ADLS Gen2:

  ```bash
  # Create a sample W3C log file with representative data
  mkdir -p data/samples
  cat > data/samples/u_ex250324.log << 'LOGEOF'
  #Software: Microsoft Internet Information Services 8.5
  #Version: 1.0
  #Date: 2025-03-24 00:00:00
  #Fields: date time s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs(User-Agent) cs(Cookie) cs(Referer) sc-status sc-substatus sc-win32-status sc-bytes cs-bytes time-taken
  2025-03-24 00:00:01 10.0.0.1 GET /index.html - 80 - 192.168.1.1 Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm) - https://www.google.com/ 200 0 0 1234 567 100

2025-03-24 00:00:02 10.0.0.1 GET /robots.txt - 80 - 192.168.1.2 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 - <https://www.google.com/> 200 0 0 500 300 50
2025-03-24 00:01:00 10.0.0.1 GET /api/v1/data?page=1 - 80 - 10.0.0.2 Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) - - 200 0 0 2048 1024 200
2025-03-24 00:02:00 10.0.0.1 GET /assets/style.css - 80 - 10.0.0.3 Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) - - 304 0 0 0 256 50
2025-03-24 00:03:00 10.0.0.2 GET / - 80 - 192.168.1.4 - - - 200 0 0 0 0 0
2025-03-24 00:04:00 10.0.0.1 GET /search?q=ultra+long+query+string+with+many+parts 80 - 10.0.0.5 Mozilla/5.0 (Windows NT 10.0) - - 200 0 0 5000 300 1500
   LOGEOF
   > **Edge cases covered:** Line 1-4: normal UAs with spaces. Line 5: empty UA (`-`), empty referrer, zero-size response. Line 6: complex URI query with `+` encoding, UA with minimum detail.
   >
   > **⚠️ 14-field format note:** The sample above is in 18-field format (the `#Fields:` line has 18 tokens). To test 14-field format detection and parsing, create a separate sample `data/samples/u_ex250324_14field.log` with the 14-field header:
>
   > ```
   > #Fields: date time s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs(User-Agent) sc-status sc-substatus sc-win32-status time-taken
   > 2025-03-24 00:00:01 10.0.0.1 GET /index.html - 80 - 192.168.1.1 Mozilla/5.0 200 0 0 100
   > 2025-03-24 00:00:02 10.0.0.1 GET /robots.txt - 80 - 192.168.1.2 - 200 0 0 50
   > ```
>
   > Upload this separately to `raw-logs/w3c-logs/u_ex250324_14field.log` and verify the Bronze pipeline parses it correctly (bytes_sent, bytes_recv, cookie, and referrer should be NULL).
  
  az storage fs file upload \
    --file-system raw-logs \
    --path "w3c-logs/u_ex250324.log" \
    --source "data/samples/u_ex250324.log" \
    --account-name "$(jq -r '.storage_account_name.value' .env.azure.json 2>/dev/null || az storage account list --query "[0].name" -o tsv)"

  ```
  Or use Azure Portal Storage Browser → `raw-logs` container → Upload

  > **🔶 GeoIP database upload moved to Phase 4** — The `.mmdb` files are needed by the Silver DLT pipeline (GeoIP enrichment), not the Bronze pipeline. See Phase 4 for download and upload steps.

- [ ] **DLT pipeline creation strategy:**

  > Do NOT create the pipeline via the Databricks UI for production. Instead:
  > 1. Use `databricks pipeline create` CLI command to temporarily test the pipeline during Phase 3 development
  > 2. Let Terraform Part B (Phase 6) own the permanent `databricks_pipeline` resource
  > 3. The Phase 6 Terraform resource should use `name = "w3c-{bronze,silver}-pipeline"` with `lifecycle { ignore_changes = [cluster, continuous] }` for development flexibility
  >
  > This avoids a conflict where manual pipeline creation and Terraform both try to manage the same resource.

  - Name: `w3c-bronze-pipeline`
  - Source code: Databricks repo path to `databricks/dlt/dlt_bronze.py`
  - Target: `w3c_catalog.bronze`
  - Pipeline mode: `TRIGGERED` (cost control)
  - Cluster: 1–2 workers, `Standard_DS3_v2` or `Standard_D3_v2`
  - DBR version: `15.4.x-scala2.12` (pinned for Python 3.11 compatibility; DBR 16+ uses Python 3.12)
  - Auto-stop: 10 min idle
  - Serverless: **Do NOT use** (serverless DLT costs more; classic DLT is cheaper)

- [ ] Run DLT pipeline, verify:
  - Rows ingested match sample files
  - Partitioned by `log_date`
  - Bronze table populated in `w3c_catalog.bronze.bronze_raw_logs`
  - Re-run: only processes new files (checkpoint tracked)

**Acceptance:** DLT Bronze pipeline runs successfully using Auto Loader Binary File format + custom W3C parser UDF. Produces a partitioned Delta table. Re-running is idempotent (only new files processed). W3C log format quirks (unquoted UA strings, 14/18 field formats) handled correctly.

#### Phase Handoff: Bronze → Silver Validation

Before proceeding to Phase 4, validate the Bronze output:

```sql
-- Verify row counts match expectations
SELECT COUNT(*) AS total_rows FROM bronze_raw_logs;
SELECT DISTINCT log_date, COUNT(*) AS rows_per_date FROM bronze_raw_logs GROUP BY log_date ORDER BY log_date;
-- Check format detection distribution
-- Check null distribution to validate both formats present
SELECT
    COUNT(*) AS total_rows,
    COUNT(bytes_sent) AS rows_with_bytes_sent,
    COUNT(bytes_recv) AS rows_with_bytes_recv,
    COUNT(cookie) AS rows_with_cookie,
    COUNT(referrer) AS rows_with_referrer
FROM bronze_raw_logs;
```

Expected: Row count should approximately match input file line count. Log dates should span expected range. Both 14-field and 18-field formats should be represented.

#### Failure Recovery — Phase 3

| Failure Scenario | Recovery Action |
|---|---|
| DLT pipeline fails during execution | Inspect `@dlt.expect_or_drop` violations in pipeline event log; inspect checkpoint at `abfss://bronze@<storage>/checkpoints/bronze/`; fix code; re-run with `full_refresh=True` |
| Auto Loader detects schema mismatch | Check for malformed log lines; update UDF or add `RESCUE_DATA_COLUMN` for troubleshooting |
| Duplicate rows after re-run | Use `full_refresh=True` or truncate Bronze target table before re-run |

---

### Phase 4 — DLT Silver Pipeline (2.5 hours)

**Goal:** DLT Silver pipeline reads from Bronze, applies enrichment (GeoIP via MaxMind GeoLite2, computed fields, data quality expectations), and writes to a Silver table in Unity Catalog.

> **Key decisions:**
>
> - **GeoIP** uses **7 offline MaxMind GeoLite2 UDFs** (country, region, city, latitude, longitude, postcode, isp) — identical to the Docker version. The `.mmdb` files must be accessible from DBFS or UC Volume.
> - **UA columns** (agent_type, browser_name, browser_version, operating_system, device_type) are **NOT included** in the Silver DDL — consistent with `02_silver_enrichment.py` scaffold and the dead columns plan. The `user_agent` raw string column is retained.
> - **6 geo columns MUST stay in Silver** — the Airflow `export_dimensions` operator reads these from the Silver table to build `dim_geolocation`. Removing them would break that dimension.
> - **17 enrichment UDFs total** (7 GeoIP + 5 computed + 5 UA source functions imported but only geo applied to Silver output — the UA functions are used for the raw UA string dimension, not materialized in Silver).

- [ ] Create `databricks/dlt/dlt_silver.py`:

  ```python
  import dlt
  from typing import Callable
  from pyspark.sql.functions import pandas_udf
  from pyspark.sql.types import StringType, BooleanType
  import pandas as pd
  import os
  import geoip2.database

  # ── GeoIP lazy initialization (parameterised) ──
  # CRITICAL: spark.conf.get() is NOT available on worker processes when called
  # inside Pandas UDFs. Instead, read the paths at the driver level and pass them
  # to the reader factory. The paths are captured in the closure so each worker
  # initialises its own reader on first UDF invocation.
  #
  # Pattern: spark.conf.get() must be called OUTSIDE the UDF body (in the @dlt.table
  # function or at pipeline parameter resolution time). The UDF receives the paths
  # via a factory function.

  def _make_geo_reader(geoip_city_path: str):
      """Create a lazy GeoIP reader factory closed over the city DB path.
      
      Returns a zero-arg callable that lazily initialises the reader.
      """
      reader = None
      def get_reader():
          nonlocal reader
          if reader is None and os.path.exists(geoip_city_path):
              reader = geoip2.database.Reader(geoip_city_path)
          return reader
      return get_reader

  def _make_asn_reader(geoip_asn_path: str):
      """Create a lazy ASN GeoIP reader factory closed over the ASN DB path."""
      reader = None
      def get_reader():
          nonlocal reader
          if reader is None and os.path.exists(geoip_asn_path):
              reader = geoip2.database.Reader(geoip_asn_path)
          return reader
      return get_reader

  # ── GeoIP UDFs (parameterised factory pattern) ──
  # 7 enrichment functions, matching airflow/spark/jobs/utils/geoip.py.
  # Instead of calling spark.conf.get() inside the UDF (which fails on workers),
  # the factory takes a lazy reader Callable captured in the closure.
  # Paths are resolved at the driver level in the @dlt.table function.

  def _make_geoip_udf(
      field: str,
      get_reader_fn: Callable,
  ):
      """Create a GeoIP pandas_udf for the given field.
      
      Args:
          field: One of 'country', 'region', 'city', 'latitude',
                 'longitude', 'postcode', 'isp'.
          get_reader_fn: Zero-arg Callable that returns a geoip2.Reader or None.
      """
      import pandas as pd
      from pyspark.sql.functions import pandas_udf
      from pyspark.sql.types import StringType
      
      lookup_fns = {
          "country": lambda ip, reader: reader.city(ip).country.name,
          "region": lambda ip, reader: reader.city(ip).subdivisions.most_specific.name,
          "city": lambda ip, reader: reader.city(ip).city.name,
          "latitude": lambda ip, reader: str(reader.city(ip).location.latitude),
          "longitude": lambda ip, reader: str(reader.city(ip).location.longitude),
          "postcode": lambda ip, reader: reader.city(ip).postal.code,
          "isp": lambda ip, reader: reader.asn(ip).autonomous_system_organization,
      }
      lookup_fn = lookup_fns[field]
      
      @pandas_udf(StringType())
      def geo_udf(ips: pd.Series) -> pd.Series:
          reader = get_reader_fn()
          def lookup(ip):
              if reader is None:
                  return "Unknown"
              try:
                  return lookup_fn(ip, reader) or "Unknown"
              except Exception:
                  return "Unknown"
          return ips.apply(lookup)
      
      return geo_udf

  # ── Computed field UDFs ──
  # Replicate the 5 UDFs from airflow/spark/jobs/utils/transformations.py:
  #   page_category, referrer_domain, traffic_type, is_crawler, size_band

  @udf(returnType=StringType())
  def page_category(uri_stem):
      """Classify a URI stem into a page category.
      
      Matches the logic in airflow/spark/jobs/utils/transformations.py.
      """
      if not uri_stem or uri_stem.strip() in ("-", "Unknown", ""):
          return "Unknown"
      stem = uri_stem.split("?")[0]
      if "." in stem:
          ext = stem.rsplit(".", 1)[-1].lower().strip()
          categories = {
              "aspx": "Dynamic Page", "asp": "Dynamic Page",
              "html": "Static Page", "htm": "Static Page", "shtml": "Static Page",
              "jpg": "Image", "jpeg": "Image", "png": "Image", "gif": "Image",
              "bmp": "Image", "ico": "Icon", "css": "Stylesheet", "js": "Script",
              "txt": "Text File", "pdf": "Document",
          }
          return categories.get(ext, "Other")
      return "Directory"

  @udf(returnType=StringType())
  def referrer_domain(referrer_url):
      """Extract the domain from a referrer URL.
      
      Returns 'Direct' for empty/dash, 'Unknown' for literal 'Unknown',
      or the parsed domain otherwise.
      """
      if not referrer_url or referrer_url.strip() == "-":
          return "Direct"
      if referrer_url == "Unknown":
          return "Unknown"
      try:
          from urllib.parse import urlparse
          candidate = referrer_url.strip()
          if "//" not in candidate:
              candidate = "http://" + candidate
          parsed = urlparse(candidate)
          domain = parsed.netloc.split("@")[-1].split(":")[0].lower().replace("www.", "")
          return domain or referrer_url
      except Exception:
          return str(referrer_url)

  def _extract_domain(referrer_url):
      """Extract domain from referrer URL (plain function, NOT a UDF).
      
      CRITICAL: This is a plain Python function used BY traffic_type UDF.
      UDFs CANNOT call other UDFs inside their body — calling a @udf-decorated
      function inside another UDF causes a runtime error (the UDF wrapper expects
      Column arguments, not string values). This function mirrors the logic of
      referrer_domain() but without the @udf decorator.
      
      Returns the domain string or None. On parse failure, returns the original
      URL string (matching authoritative transformations.py::_extract_domain behavior).
      """
      if not referrer_url or referrer_url == "-":
          return None
      try:
          from urllib.parse import urlparse
          candidate = referrer_url.strip()
          if "//" not in candidate:
              candidate = "http://" + candidate
          parsed = urlparse(candidate)
          domain = parsed.netloc.split("@")[-1].split(":")[0].lower().replace("www.", "")
          return domain or None
      except Exception:
          return str(referrer_url)  # Return original on parse failure (matches authoritative source)

  @udf(returnType=StringType())
  def traffic_type(referrer_url):
      """Classify a referrer into a traffic-source category.
      
      Categories: Direct, Internal, Search Engine, Social, Referral, Unknown.
      
      CRITICAL: Uses _extract_domain (a plain Python function), NOT
      referrer_domain (a UDF). Calling a UDF inside another UDF body
      causes a runtime error in PySpark.
      """
      if not referrer_url or referrer_url.strip() == "-":
          return "Direct"
      if referrer_url == "Unknown":
          return "Unknown"
      domain = _extract_domain(referrer_url)
      if domain is None:
          return "Unknown"
      search_engines = ["google", "bing", "yahoo", "baidu", "duckduckgo", "yandex", "ask"]
      social_domains = ["facebook", "twitter", "linkedin", "reddit", "t.co", "instagram"]
      internal_domains = ["darwinsbeagleplants", "134.36.36.75"]
      if any(s in domain.lower() for s in internal_domains):
          return "Internal"
      if any(s in domain.lower() for s in search_engines):
          return "Search Engine"
      if any(s in domain.lower() for s in social_domains):
          return "Social"
      return "Referral"

  def make_crawler_udf(crawler_ips):
      """Factory: create an is_crawler UDF closed over a broadcast set of IPs.
      
      Pattern matches airflow/spark/jobs/utils/transformations.py::make_crawler_udf().
      The crawler IP set is discovered from Bronze data and broadcast before
      the UDF is created. The frozenset ensures immutability in the closure.
      """
      ips = frozenset(crawler_ips)
      @udf(returnType=StringType())
      def is_crawler_impl(ip):
          if not ip or ip.strip() in ("-", "Unknown", ""):
              return "Unknown"
          return "true" if ip.strip() in ips else "false"
      return is_crawler_impl

  @udf(returnType=StringType())
  def size_band(bytes_sent, bytes_recv):
      """Bucket total bytes (sent + received) into a size band.
      
      Bands: 0→Zero, 1-1023→Tiny, 1024-10239→Small, 10240-102399→Medium,
      102400-1048575→Large, >=1048576→Huge.
      """
      total = (bytes_sent or 0) + (bytes_recv or 0)
      bands = [(1, "Zero"), (1024, "Tiny"), (10240, "Small"),
               (102400, "Medium"), (1048576, "Large")]
      for threshold, label in bands:
          if total < threshold:
              return label
      return "Huge"

  # ── DLT Silver Table ──
  @dlt.table(
      name="silver_enriched_logs",
      comment=(
          "Enriched W3C logs with GeoIP (MaxMind GeoLite2), computed fields, "
          "and data quality expectations. "
          "Geo columns retained for dim_geolocation consumption."
      ),
      table_properties={
          "delta.enableChangeDataFeed": "true",
      },
      partition_cols=["log_date"],
  )
  @dlt.expect("valid_status_code", "win32_status IS NULL OR win32_status >= 0")
  @dlt.expect("valid_timestamp", "log_date IS NOT NULL AND log_time IS NOT NULL")
  @dlt.expect_or_drop("non_null_uri", "uri_stem IS NOT NULL")
  @dlt.expect("valid_method", "method IS NULL OR method IN ('GET', 'POST', 'HEAD', 'PUT', 'DELETE', 'OPTIONS')")
  def silver_enriched_logs():
      # CRITICAL: Bronze and Silver are SEPARATE DLT pipelines defined as distinct
      # Terraform databricks_pipeline resources. dlt.read() only works within the
      # same pipeline. Cross-pipeline reads MUST use spark.table() with the full
      # three-level Unity Catalog path.
      bronze = spark.table("w3c_catalog.bronze.bronze_raw_logs")
      
      # ── Discover crawler IPs (from ALL bronze data) ──
      # Uses a broadcast set rather than collect() to avoid driver memory issues.
      # The temp view is registered from the bronze DataFrame.
      bronze.createOrReplaceTempView("bronze_vw")
      crawler_df = spark.sql("""
          SELECT DISTINCT client_ip
          FROM bronze_vw
          WHERE LOWER(uri_stem) = '/robots.txt'
      """)
      crawler_ips = {row.client_ip for row in crawler_df.collect()}
      is_crawler_udf = make_crawler_udf(crawler_ips)
      
      # ── Resolve GeoIP DB paths (read from pipeline config at driver level) ──
      geoip_city_path = spark.conf.get("pipeline.geoip_db_path",
          "/dbfs/mnt/w3c-data/GeoLite2-City.mmdb")
      geoip_asn_path = spark.conf.get("pipeline.geoip_asn_db_path",
          "/dbfs/mnt/w3c-data/GeoLite2-ASN.mmdb")
      
      # Create lazy reader factories with paths captured in closure
      get_geo_reader = _make_geo_reader(geoip_city_path)
      get_asn_reader = _make_asn_reader(geoip_asn_path)
      
      # ── Pre-filter: skip private/reserved IPs before GeoIP lookup ──
      # Private IPs (10.x.x.x, 192.168.x.x, 172.16-31.x.x) and loopback
      # (127.x.x.x) will return "Unknown" from MaxMind anyway. Filtering
      # them out avoids unnecessary DB lookups and reduces pipeline cost.
      # The authoritative w3c_parser.py's _is_usable_ip uses the same logic.
      @udf("boolean")
      def _is_usable_ip(ip: str) -> bool:
          if ip is None:
              return False
          # Detect private ranges by first octet
          first = ip.split(".")[0]
          if first == "10":
              return False
          if first == "127":
              return False
          if first == "192":
              # 192.168.x.x
              try:
                  second = ip.split(".")[1]
                  if second == "168":
                      return False
              except (IndexError, ValueError):
                  pass
          if first == "172":
              # 172.16.x.x through 172.31.x.x
              try:
                  second = int(ip.split(".")[1])
                  if 16 <= second <= 31:
                      return False
              except (IndexError, ValueError):
                  pass
          return True

      # ── Apply enrichment ──
      enriched = (bronze
          .filter(_is_usable_ip("client_ip"))
          # 7 GeoIP columns (MUST stay — consumed by export_dimensions dim_geolocation)
          .withColumn("country", _make_geoip_udf("country", get_geo_reader)("client_ip"))
          .withColumn("region", _make_geoip_udf("region", get_geo_reader)("client_ip"))
          .withColumn("city", _make_geoip_udf("city", get_geo_reader)("client_ip"))
          .withColumn("latitude", _make_geoip_udf("latitude", get_geo_reader)("client_ip"))
          .withColumn("longitude", _make_geoip_udf("longitude", get_geo_reader)("client_ip"))
          .withColumn("postcode", _make_geoip_udf("postcode", get_geo_reader)("client_ip"))
          .withColumn("isp", _make_geoip_udf("isp", get_asn_reader)("client_ip"))
          # 5 computed field columns
          .withColumn("page_category", page_category("uri_stem"))
          .withColumn("referrer_domain", referrer_domain("referrer"))
          .withColumn("traffic_type", traffic_type("referrer"))
          .withColumn("is_crawler", is_crawler_udf("client_ip"))
          .withColumn("size_band", size_band("bytes_sent", "bytes_recv"))
      )
      
      return enriched
  ```

  > **CRITICAL — Lazy init pattern (parameterised):** The GeoIP readers (`geoip2.database.Reader`) open file handles that are **not serializable** to Spark workers. Module-level initialization would fail because `spark.conf.get()` is not available at import time on workers. Unlike the Docker path (which uses `global _geo_reader` with `spark.conf.get()` inside the UDF), **the DLT version must pass paths as parameters** because Pandas UDFs execute on worker processes where `spark` is not available. The factory functions (`_make_geo_reader`, `_make_asn_reader`) close over the resolved paths and return a lazy initialiser. The `_make_geoip_udf` factory creates each UDF closed over a get_reader callable. Paths are resolved once in the `@dlt.table` function body where `spark.conf.get()` is available.
  >
  > **⚠️ Micro-batch performance:** The `_make_geoip_udf` factory creates a new reader closure on every `@dlt.table` invocation. For pipelines processing thousands of micro-batches, this re-opens the `.mmdb` file on every trigger. Consider: (a) using `SparkFiles.get()` with a module-level singleton cache on executors to share the reader across UDF invocations, or (b) broadcasting the GeoIP database path via Spark broadcast variable and initialising the reader once per executor JVM. The current approach is correct for correctness but may benefit from this optimisation at scale.

- [ ] Install `geoip2` library on the DLT cluster (cluster library or init script):

  ```
  # In Databricks cluster config → Libraries → Install PyPI:
  geoip2
  ```

- [ ] **Prerequisite: Download GeoLite2 databases (requires free MaxMind account)**

  1. Register at <https://www.maxmind.com/en/geolite2/signup> (free account)
  2. After login, go to <https://www.maxmind.com/en/accounts/current/license-key> → Generate New License Key
  3. Download GeoLite2-City and GeoLite2-ASN databases:

     ```bash
     curl -L -o /tmp/GeoLite2-City.tar.gz "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=YOUR_LICENSE_KEY&suffix=tar.gz"
     curl -L -o /tmp/GeoLite2-ASN.tar.gz "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-ASN&license_key=YOUR_LICENSE_KEY&suffix=tar.gz"
     tar -xzf /tmp/GeoLite2-City.tar.gz -C /tmp/
     tar -xzf /tmp/GeoLite2-ASN.tar.gz -C /tmp/
     ```

  4. Upload to DBFS:

     ```bash
     databricks fs cp /tmp/GeoLite2-City_*/GeoLite2-City.mmdb dbfs:/mnt/w3c-data/GeoLite2-City.mmdb
     databricks fs cp /tmp/GeoLite2-ASN_*/GeoLite2-ASN.mmdb dbfs:/mnt/w3c-data/GeoLite2-ASN.mmdb
     ```

  5. **⏰ Manual step** — requires MaxMind account registration, cannot be automated by AI agent. Perform before Phase 3 implementation.

- [ ] Add DLT expectations (data quality):
  - `@dlt.expect("valid_status_code", ...)` — range check on win32_status
  - `@dlt.expect("valid_timestamp", ...)` — non-null date/time
  - `@dlt.expect_or_drop("non_null_uri", ...)` — drop rows without URI
  - `@dlt.expect("valid_method", ...)` — allow only standard HTTP methods

- [ ] Create the Silver DLT pipeline in Databricks UI (or Terraform):
  - Name: `w3c-silver-pipeline`
  - Source code: `databricks/dlt/dlt_silver.py`
  - Target: `w3c_catalog.silver`
  - Pipeline mode: `TRIGGERED`
  - Depends on: `w3c-bronze-pipeline` running first

- [ ] Test:
  - Run DLT Silver pipeline (ensure Bronze pipeline ran first)
  - Verify enrichment columns populated correctly
  - Check DLT expectation violation stats in UI
  - Verify geo columns (country, region, city, etc.) have non-"Unknown" values

**Acceptance:** DLT Silver pipeline runs successfully, enriching Bronze data with 7 GeoIP UDFs (MaxMind GeoLite2) and 5 computed field UDFs. Geo columns preserved for downstream `dim_geolocation`. DLT expectations report violations. Silver table populated in `w3c_catalog.silver.silver_enriched_logs`.

#### Phase Handoff: Silver → JDBC Validation

```sql
-- Verify schema: expect 31 columns (19 raw W3C fields + 6 GeoIP + 5 computed + 1 log_date)
-- The authoritative column list is defined in dim_geolocation (6 geo) and
-- the 5 computed UDFs (page_category, referrer_domain, traffic_type, is_crawler, size_band).
-- Expected column names are in the .withColumn() chain below.
SELECT COUNT(*) AS column_count FROM information_schema.columns
WHERE table_catalog = 'w3c_catalog' AND table_schema = 'silver' AND table_name = 'silver_enriched_logs';
DESCRIBE silver_enriched_logs;
-- If column_count != 31, check the .withColumn() chain matches the 25+ core columns definition.

-- Verify data moved from Bronze to Silver without loss
SELECT COUNT(*) AS silver_rows FROM silver_enriched_logs;
SELECT COUNT(*) AS bronze_rows FROM bronze_raw_logs;
-- Verify GeoIP enrichment worked (at least some rows)
SELECT COUNT(*) AS geo_resolved FROM silver_enriched_logs WHERE country != 'Unknown';
-- Check date coverage
SELECT MIN(log_date), MAX(log_date), COUNT(DISTINCT log_date) FROM silver_enriched_logs;
```

#### Failure Recovery — Phase 4

| Failure Scenario | Recovery Action |
|---|---|
| GeoIP `.mmdb` file not found at pipeline runtime | DLT pipeline completes with "Unknown" for all geo columns (graceful degradation). Fix: upload `.mmdb` files to the correct DBFS/UC path and re-run. |
| `geoip2` library not installed on DLT cluster | Pipeline fails with `ModuleNotFoundError`. Fix: Add `geoip2` to cluster PyPI libraries (manual: Cluster → Libraries → Install New → PyPI → `geoip2==5.0.1`). |
| Pandas UDF serialisation error | Verify all UDFs use closure-captured parameters (not `spark.conf.get()` inside UDF body). The `_make_geoip_udf` factory pattern handles this correctly. |
| `dlt.read()` → `spark.table()` path resolution fails | Check the 3-level Unity Catalog path `w3c_catalog.bronze.bronze_raw_logs` exists. `SHOW TABLES IN w3c_catalog.bronze` to verify. |
| DLT expectation violations exceed acceptable threshold | Inspect violation stats in pipeline event log. Adjust `@dlt.expect_or_drop` vs `@dlt.expect` based on data quality requirements. |
| Silver→Bronze row count mismatch | Row count can differ if Bronze has invalid-log-line rows that were dropped by DLT expectations. Verify using `COUNT(*)` without filters. |

---

### Phase 5 — Azure SQL Export via JDBC (1.5 hours)

**Goal:** Export Silver data from Unity Catalog to Azure SQL Database via JDBC with idempotent tracking. **No separate Gold DLT table is needed** — the JDBC export reads from Silver directly (a Gold table would add storage and pipeline cost for a simple column projection; Silver already has the enriched data).

> **Schema boundary:** Azure `dbo.raw_enriched` is an internal bridge table with 31 columns (25 warehouse core columns + 6 GeoIP columns) so `export_dimensions_azure` can build `dbo.dim_geolocation` without reading Unity Catalog directly. This is intentionally wider than the current PostgreSQL `public.raw_enriched` 25-column warehouse table. The Power BI-facing contract is preserved at the dbt/CSV layer: `dbt_staging.fact_webrequest.csv` remains the fact export, and `dbo.dim_geolocation`/`dbo.dim_useragent` are exported under the existing `public.*.csv` filenames.

> **Critical: Idempotency pattern** — mirror the existing `export_warehouse.py` 7-step pattern:
>
> 1. Read Silver from Unity Catalog
> 2. Query tracking table for already-exported `source_file` values
> 3. Filter to only new source files
> 4. Apply type casts (is_crawler→BIT, lat/lon→DOUBLE)
> 5. Create target tables if not exist
> 6. Write new data to `dbo.raw_enriched` (Azure SQL)
> 7. Update tracking table `dbo.raw_enriched_loaded` with new source_file values
>
> **CRIT-5: MSSQL JDBC driver is NOT bundled with DBR.** The `mssql-jdbc` Maven library must be installed on the cluster. Install via:
>
> - **During pipeline execution:** Add `com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11` as a Maven library on the Databricks cluster or DLT pipeline
> - **In Terraform:** Add `library { maven { coordinates = "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11" } }` to the cluster configuration
> - **Manual:** Cluster → Libraries → Install New → Maven → `com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11`

- [ ] Create `databricks/jobs/jdbc_export_azure.py` (Databricks Python task — **not** DLT):

  ```python
  """
  JDBC Export: Unity Catalog Silver → Azure SQL Database.
  
  Reads from the DLT-generated Silver table directly (no Gold table).
  Mirrors the 7-step pattern from airflow/spark/jobs/export_warehouse.py:
  1. Read Silver from Unity Catalog
  2. Query tracking table for already-exported source_file values
  3. Filter to only new source files
  4. Apply type casts (is_crawler→BIT)
  5. CREATE TABLE IF NOT EXISTS for both target + tracking tables
  6. Write new data to Azure SQL
  7. Update tracking table
  
  Requires mssql-jdbc Maven library on the cluster:
    com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11
  """
  import sys
  
  from pyspark.sql.functions import col, when, lower, trim
  from pyspark.sql.types import StructType, StructField, StringType
  
  # ── Config (from Databricks secrets or pipeline parameters) ──
  # NOTE: spark.conf.get() MUST NOT be called at module level in Databricks
  # Python tasks. The spark session is injected after module import, so all
  # config reads happen inside main() below.
  CATALOG = "w3c_catalog"
  SILVER_TABLE = f"{CATALOG}.silver.silver_enriched_logs"
  
  RAW_ENRICHED_TABLE = "dbo.raw_enriched"
  TRACKING_TABLE = "dbo.raw_enriched_loaded"
  
  # Export columns (31 cols: 25 warehouse core + 6 geo columns for Azure dim build)
  EXPORT_COLUMNS = [
      "log_date", "log_time", "server_ip", "method", "uri_stem",
      "uri_query", "client_ip", "user_agent", "cookie", "referrer",
      "status", "sub_status", "win32_status", "bytes_sent", "bytes_recv",
      "server_port", "username", "time_taken", "source_file",
      "postcode", "page_category", "referrer_domain", "traffic_type",
      "is_crawler", "size_band",
      "country", "region", "city", "latitude", "longitude", "isp",
  ]
  
  # DDL for Azure SQL (NVARCHAR(MAX) for Unicode columns, VARCHAR for ASCII-only, BIT instead of BOOLEAN)
  RAW_ENRICHED_DDL = f"""
  CREATE TABLE {RAW_ENRICHED_TABLE} (
      log_date DATE,
      log_time VARCHAR(20),
      server_ip VARCHAR(45),
      method VARCHAR(10),
      uri_stem NVARCHAR(MAX),
      uri_query NVARCHAR(MAX),
      client_ip VARCHAR(45),
      user_agent NVARCHAR(MAX),
      cookie NVARCHAR(MAX),
      referrer NVARCHAR(MAX),
      status INT,
      sub_status INT,
      win32_status INT,
      bytes_sent BIGINT,
      bytes_recv BIGINT,
      server_port INT,
      username VARCHAR(100),
      time_taken BIGINT,
      source_file VARCHAR(255),
      postcode VARCHAR(20),
      page_category VARCHAR(50),
      referrer_domain VARCHAR(255),
      traffic_type VARCHAR(50),
      is_crawler BIT,
      size_band VARCHAR(20),
      country VARCHAR(100),
      region VARCHAR(100),
      city VARCHAR(100),
      latitude FLOAT,
      longitude FLOAT,
      isp VARCHAR(200)
  );
  """
  
  TRACKING_DDL = f"""
  CREATE TABLE {TRACKING_TABLE} (
      source_file VARCHAR(255) PRIMARY KEY
  );
  """
  
  def main():
      """Main entry point — reads config, executes 7-step JDBC export."""
      # Read credentials from Databricks secret scope (NOT spark.conf.get()).
      # In spark_python_task (non-notebook), spark.conf.get() won't resolve
      # {{secrets/scope/key}} — only notebook tasks support that syntax.
      # dbutils.secrets.get() is the correct API for Python wheel/task scripts.
      #
      # CRITICAL: dbutils is NOT auto-injected in spark_python_task (it's a
      # notebook-only global). Must import DBUtils explicitly.
      from pyspark.dbutils import DBUtils
      dbutils = DBUtils(spark)
      
      server = dbutils.secrets.get("w3c-etl-pipeline", "azure-sql-server")
      database = dbutils.secrets.get("w3c-etl-pipeline", "azure-sql-database")
      user = dbutils.secrets.get("w3c-etl-pipeline", "azure-sql-user")
      password = dbutils.secrets.get("w3c-etl-pipeline", "azure-sql-password")
      
      # CRITICAL: Azure SQL Database REQUIRES TLS encryption. Connections
      # without encrypt=true are rejected at the network level.
      # trustServerCertificate=false ensures the server's TLS certificate
      # is validated against a trusted CA.
      # Serverless auto-pause recovery: Azure SQL Database serverless
      # auto-pauses after 60 min of inactivity (Phase 1 variable).
      # The first connection after idle time takes 1-3 minutes to resume.
      # connectRetryCount + connectRetryInterval handles this at the
      # JDBC driver level without application-level retry code.
      jdbc_url = (
          f"jdbc:sqlserver://{server}.database.windows.net:1433;"
          f"databaseName={database};"
          f"encrypt=true;trustServerCertificate=false;"
          f"connectTimeout=30;queryTimeout=300;"
          f"connectRetryCount=5;connectRetryInterval=30;"
      )
      jdbc_props = {
          "user": user,
          "password": password,
          "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
          "encrypt": "true",
          "trustServerCertificate": "false",
      }
      
      # ── Step 1: Read Silver ──
      silver_df = spark.table(SILVER_TABLE)
      
      # ── Step 2: Query tracking table for already-exported files ──
      # NOTE: dbutils.secrets.get() throws an exception if the secret scope or
      # key does not exist. Ensure the w3c-etl-pipeline secret scope is created
      # and all required keys (azure-sql-server, azure-sql-database, azure-sql-user,
      # azure-sql-password) are populated before the first run.
      # See Phase 6 "Create Databricks secret scope" for setup instructions.
      #
      # ⚠️ Dependency: Phase 5 test execution requires Phase 6 secret scope. The
      # w3c-etl-pipeline Databricks secret scope (with keys azure-sql-server,
      # azure-sql-database, azure-sql-user, azure-sql-password) is created in
      # Phase 6. To test Phase 5's JDBC export before Phase 6 completes, either:
      # (a) temporarily hardcode credentials in the script for testing only, or
      # (b) create the secret scope manually in Phase 5:
      #   databricks secrets create-scope --scope w3c-etl-pipeline
      # and populate it with the Terraform outputs from Phase 2.
      #
      # CRITICAL — NOT thread-safe for parallel runs: If two pipeline instances
      # run concurrently, both could read the same unexported files and both
      # write duplicates. The Airflow DAG uses max_active_runs=1 and the
      # Databricks Workflow runs triggered (not continuous), so this is safe.
      # For true parallel-safety, add a WITH (UPDLOCK, TABLOCK) hint.
      try:
          tracked_df = (spark.read.format("jdbc")
              .option("url", jdbc_url)
              .option("dbtable", TRACKING_TABLE)
              .option("user", jdbc_props["user"])
              .option("password", jdbc_props["password"])
              .option("driver", jdbc_props["driver"])
              .option("encrypt", jdbc_props["encrypt"])
              .option("trustServerCertificate", jdbc_props["trustServerCertificate"])
              .load())
          tracked_files = {row.source_file for row in tracked_df.select("source_file").collect()}
      except Exception as e:
          # SQL Server error code 208 = "Invalid object name" (table not found).
          # Use getErrorCode() via the JVM SQLException chain only. Do not
          # string-match str(e): "208" can appear in line numbers, ports, or
          # nested messages unrelated to SQL Server's invalid-object error.
          def get_sql_error_code(exc):
              seen = set()
              candidates = [exc]
              while candidates:
                  current = candidates.pop(0)
                  if current is None or id(current) in seen:
                      continue
                  seen.add(id(current))
                  
                  if hasattr(current, "getErrorCode"):
                      try:
                          return int(current.getErrorCode())
                      except Exception:
                          pass
                  
                  for attr in ("java_exception", "exception", "cause"):
                      try:
                          nested = getattr(current, attr, None)
                      except Exception:
                          nested = None
                      if nested is not None:
                          candidates.append(nested)
                  
                  if hasattr(current, "getCause"):
                      try:
                          candidates.append(current.getCause())
                      except Exception:
                          pass
              return None
          
          if get_sql_error_code(e) == 208:
              print(f"Tracking table {TRACKING_TABLE} does not exist — first run")
              tracked_files = set()
          else:
              print(f"Warning: Could not query tracking table: {e}")
              print("Proceeding without idempotency — may re-export already-loaded files")
              tracked_files = set()
      
      # ── Step 3: Filter to new source files ──
      all_sources = {row.source_file for row in silver_df.select("source_file").distinct().collect()}
      new_source_files = all_sources - tracked_files
      
      if not new_source_files:
          print("No new source files to export. Exiting.")
          sys.exit(0)
      
      print(f"Found {len(new_source_files)} new source file(s)")
      new_data = silver_df.filter(col("source_file").isin(new_source_files))
      
      # ── Step 4: Apply type casts ──
      new_data = new_data.withColumn(
          "is_crawler",
          when(trim(lower(col("is_crawler"))) == "true", True).otherwise(False)
      )
      
      # ── Step 5: Create tables if not exist ──
      def execute_ddl(ddl: str) -> None:
          """Execute a DDL statement via JDBC (no wasteful dummy DataFrame).
          
          NOTE: Uses spark._jvm (py4j gateway) — bare `import java.sql` is NOT
          valid Python. spark._jvm gives access to the JVM's java.sql.DriverManager.
          The mssql-jdbc JAR must be on the Spark classpath via spark.jars.packages
          or cluster library (com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11).
          
          Includes retry with aggressive exponential backoff for transient SQL
          errors (e.g., Azure SQL throttling at DTU/SKU limits) AND serverless
          cold-start recovery (auto-pause after 60 min inactivity).
          
          CRITICAL: Uses try/finally to ensure the connection is closed even on
          exception — the old version left connections open on failure.
          """
          import time
          
          # Pre-check: verify the MSSQL JDBC driver is on the classpath
          try:
              spark._jvm.Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
          except Exception:
              raise RuntimeError(
                  "MSSQL JDBC driver not found on classpath. "
                  "Install com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11 "
                  "via cluster Maven library or spark.jars.packages."
              )
          
          MAX_RETRIES = 4
          last_exception = None
          for attempt in range(MAX_RETRIES):
              conn = None
              try:
                  conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, jdbc_props["user"], jdbc_props["password"])
                  stmt = conn.createStatement()
                  stmt.execute(ddl)
                  return  # success
              except Exception as e:
                  last_exception = e
                  if attempt < MAX_RETRIES - 1:
                      # Aggressive backoff: 5s, 10s, 20s — total 35s before final raise.
                      # Combined with JDBC connectRetryCount=5 (driver-level, 30s each),
                      # the total window covers Azure SQL serverless cold-start (~1-3 min).
                      time.sleep(5 * (2 ** attempt))
              finally:
                  if conn is not None:
                      conn.close()
          raise last_exception
      
      try:
          execute_ddl(RAW_ENRICHED_DDL)
          print(f"Created table {RAW_ENRICHED_TABLE}")
      except Exception as e:
          if "already exists" in str(e).lower():
              print(f"Table {RAW_ENRICHED_TABLE} already exists")
          else:
              raise
      
      try:
          execute_ddl(TRACKING_DDL)
          print(f"Created table {TRACKING_TABLE}")
      except Exception as e:
          if "already exists" in str(e).lower():
              print(f"Table {TRACKING_TABLE} already exists")
          else:
              raise
      
      # ── Step 6: Write new data to Azure SQL ──
      export_data = new_data.select(*EXPORT_COLUMNS)
      export_data.write.format("jdbc") \
          .mode("append") \
          .option("url", jdbc_url) \
          .option("dbtable", RAW_ENRICHED_TABLE) \
          .option("user", jdbc_props["user"]) \
          .option("password", jdbc_props["password"]) \
          .option("driver", jdbc_props["driver"]) \
          .option("encrypt", jdbc_props["encrypt"]) \
          .option("trustServerCertificate", jdbc_props["trustServerCertificate"]) \
          .option("batchsize", "10000") \
          .option("numPartitions", "4") \
          .save()
      
      print(f"Wrote {export_data.count()} rows to {RAW_ENRICHED_TABLE}")
      
      # ── Step 7: Update tracking table ──
      tracking_schema = StructType([StructField("source_file", StringType(), True)])
      tracking_rows = [[sf] for sf in new_source_files]
      tracking_df = spark.createDataFrame(tracking_rows, schema=tracking_schema)
      
      tracking_df.write.format("jdbc") \
          .mode("append") \
          .option("url", jdbc_url) \
          .option("dbtable", TRACKING_TABLE) \
          .option("user", jdbc_props["user"]) \
          .option("password", jdbc_props["password"]) \
          .option("driver", jdbc_props["driver"]) \
          .option("encrypt", jdbc_props["encrypt"]) \
          .option("trustServerCertificate", jdbc_props["trustServerCertificate"]) \
          .option("batchsize", "1000") \
          .option("numPartitions", "1") \
          .save()
      
      print(f"Updated tracking table with {len(new_source_files)} source_file(s)")
  
  
  if __name__ == "__main__":
      main()
  ```

- [ ] Install `mssql-jdbc` Maven library on the Databricks cluster:

  ```python
  # Option A: Terraform (add to databricks_cluster or databricks_pipeline resource)
  # library {
  #   maven {
  #     coordinates = "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11"
  #   }
  # }
  
  # Option B: Manual — Cluster → Libraries → Install New → Maven
  #   Coordinates: com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11
  
  # Option C: DLT pipeline automatic — add to pipeline config UI
  ```

- [ ] Create target tables in Azure SQL Database (first run creates them automatically; manual fallback):
  - Connect via Azure Data Studio or `sqlcmd`
  - Run the `RAW_ENRICHED_DDL` and `TRACKING_DDL` statements if automatic creation fails

- [ ] Test:
  - Run `jdbc_export_azure.py` as a Databricks Python task
  - Verify data in Azure SQL Database via `sqlcmd` or Azure Data Studio
  - Re-run: verify no duplicate rows (idempotency confirmed by tracking table)
  - Compare row counts between Unity Catalog Silver and Azure SQL `raw_enriched`

**Acceptance:** Azure SQL Database `dbo.raw_enriched` populated with same schema as PostgreSQL version. Re-running is idempotent — tracking table prevents duplicates. No separate Gold DLT table (reads from Silver directly). MSSQL JDBC driver installed via Maven on the cluster.

#### Phase Handoff: JDBC → dbt Validation

```sql
-- On Azure SQL:
SELECT COUNT(*) AS raw_enriched_count FROM dbo.raw_enriched;
SELECT COUNT(*) AS tracked_files FROM dbo.raw_enriched_loaded;
-- Verify tracking table matches source (no duplicates)
SELECT COUNT(DISTINCT source_file) FROM dbo.raw_enriched_loaded;
```

#### Failure Recovery — Phase 5

| Failure Scenario | Recovery Action |
|---|---|
| JDBC write fails halfway (Azure SQL DTU throttling) | Use a staging table pattern: write to `dbo.raw_enriched_staging`, then `MERGE INTO dbo.raw_enriched` on success. Only update tracking table after merge completes. |
| Connection timeout during export | Increase `socketTimeout` in JDBC URL; reduce batch size in `option("batchsize", 10000)` |
| Tracking table query fails on first run | The `try/except` already handles missing table; verify exception handling is specific to "table not found" (SQL Server error 208), not a bare `except Exception` |

---

### Phase 6 — Databricks Workflows Orchestration + Airflow Integration (2 hours)

> **🔶 Splitting suggestion:** Phase 6 bundles 6 concerns (code upload, Terraform Part B, Airflow DAG, connections, export_dimensions_azure, Dockerfile changes, secret scope). If 2 hours is tight, pre-split into:
>
> - **Phase 6a (1h):** Code upload + Terraform Part B apply (pipeline + workflow creation)
> - **Phase 6b (1h):** Airflow DAG + connections + export_dimensions_azure + secret scope

**Goal:** Upload DLT code to Databricks, define Databricks Workflows that orchestrate the pipelines and JDBC export. Airflow triggers the workflow via `DatabricksRunNowOperator`.

> **CRIT-3: Terraform ordering — pipelin resources must be created AFTER source code exists.**
> The `databricks_pipeline` resource requires `library { notebook { path } }` or `library { file { path } }` pointing to source code that must already exist on Databricks. **If the DLT Python files don't exist yet, Terraform apply fails.**
>
> **Solution:** Split Terraform into two phases:
>
> - **Terraform Part A (Phases 1-2):** Azure infrastructure only — resource group, storage, Databricks workspace, Unity Catalog, Azure SQL Database. Does NOT include `databricks_pipeline` or `databricks_job` resources.
> - **Terraform Part B (Phase 6):** Code-dependent resources — uploaded DLT scripts, `databricks_pipeline` (DLT pipelines), `databricks_job` (Workflows). Applied AFTER Phases 3-5 write the code.

- [ ] Add Databricks provider to dependencies (already present — single installation only):

  > **NOTE:** The Databricks provider (`apache-airflow-providers-databricks==4.6.0`) is installed in the Dockerfile only — NOT in `requirements.txt`. Installing it in both places is redundant and can cause pip version conflicts during iterative Docker builds. The Dockerfile RUN layer approach (separate layer after main pip install) version-bumps efficiently without reinstalling all base dependencies.
  >
  > **Note:** Unlike `apache-airflow-providers-apache-spark` (which previously used `--no-deps` but was updated to a full install to avoid pyspark import path issues), the Databricks provider is installed **without** `--no-deps` because it requires `databricks-sdk` as a runtime dependency for `DatabricksRunNowOperator`. Install `databricks-sdk` pinned in `requirements.txt` if needed.
  
  Already present in `airflow/Dockerfile`:

  ```dockerfile
  RUN pip install --no-cache-dir apache-airflow-providers-databricks==4.6.0
  ```

- [ ] **Upload DLT code to Databricks workspace** (prerequisite for pipeline creation):

  ```bash
  # Upload DLT pipeline scripts to Databricks workspace (as files, not notebooks)
  # These will be referenced by databricks_pipeline library blocks
  databricks workspace import \
    --file databricks/dlt/dlt_bronze.py \
    --path /Workspace/Shared/w3c-etl/dlt/dlt_bronze.py \
    --format SOURCE
  
  databricks workspace import \
    --file databricks/dlt/dlt_silver.py \
    --path /Workspace/Shared/w3c-etl/dlt/dlt_silver.py \
    --format SOURCE
  
  # Upload JDBC export script
  databricks workspace import \
    --file databricks/jobs/jdbc_export_azure.py \
    --path /Workspace/Shared/w3c-etl/jobs/jdbc_export_azure.py \
    --format SOURCE
  ```

- [ ] **Create Terraform Part B** (`terraform/modules/databricks-pipelines/`):

  > **Note:** Part B is a new Terraform module directory, separate from Part A. It's applied AFTER DLT code is uploaded.

  ```hcl
  # terraform/modules/databricks-pipelines/main.tf
  
  resource "databricks_pipeline" "w3c_bronze" {
    name = "w3c-bronze-pipeline"
    catalog = "w3c_catalog"
    target = "bronze"
    
    library {
      file {
        path = "/Workspace/Shared/w3c-etl/dlt/dlt_bronze.py"
      }
    }
    
    cluster {
      label         = "default"
      num_workers   = 1
      node_type_id  = "Standard_DS3_v2"
      spark_version = "15.4.x-scala2.12"  # Pinned for Python 3.11 compatibility
    }
    
    configuration = {
      "pipeline.raw_path"           = "abfss://raw-logs@${var.storage_account}.dfs.core.windows.net/w3c-logs/"
      "pipeline.checkpoint_path"    = "abfss://bronze@${var.storage_account}.dfs.core.windows.net/checkpoints/bronze/"
      # NOTE: No bronze_target_path needed — DLT manages the table in
      # w3c_catalog.bronze.bronze_raw_logs. The table is returned as a DataFrame
      # from the DLT function, not written via foreachBatch.
      
      # CRITICAL: ADLS Gen2 authentication for Auto Loader.
      # Auto Loader uses Spark's abfss:// filesystem client which needs
      # explicit credentials. Without these configs, the pipeline fails with
      # "abfss://... does not have a Spark Hadoop configuration property set".
      # Options:
      #   (a) Storage account access key (simplest — used here)
      #   (b) Service principal with Storage Blob Data Contributor RBAC
      #   (c) Databricks secret scope with Azure AD token
      #
      # The access key is fetched from Databricks secrets by the pipeline.
      # Store the key beforehand:
      #   databricks secrets put --scope w3c-etl-pipeline --key storage-access-key
      "fs.azure.account.key.${var.storage_account}.dfs.core.windows.net"
        = "{{secrets/w3c-etl-pipeline/storage-access-key}}"
    }
    
    continuous = false
    photon     = false
    channel    = "CURRENT"
  }
  
  resource "databricks_pipeline" "w3c_silver" {
    name    = "w3c-silver-pipeline"
    catalog = "w3c_catalog"
    target  = "silver"
    
    library {
      file {
        path = "/Workspace/Shared/w3c-etl/dlt/dlt_silver.py"
      }
    }
    
    cluster {
      label         = "default"
      num_workers   = 1
      node_type_id  = "Standard_DS3_v2"
      spark_version = "15.4.x-scala2.12"  # Pinned for Python 3.11 compatibility
    }
    
    configuration = {
      "pipeline.geoip_db_path"     = "/dbfs/mnt/w3c-data/GeoLite2-City.mmdb"
      "pipeline.geoip_asn_db_path" = "/dbfs/mnt/w3c-data/GeoLite2-ASN.mmdb"
      # NOTE: pipeline.silver_target_path is defined here but NOT consumed by the
      # Python DLT code — DLT manages the Silver table location internally in
      # Unity Catalog (w3c_catalog.silver.silver_enriched_logs). This config exists
      # for future use if explicit path control is needed.
      "pipeline.silver_target_path" = "abfss://silver@${var.storage_account}.dfs.core.windows.net/tables/silver_enriched_logs"
    }
    
    # Install geoip2 (pinned to 5.0.1, matching requirements_spark.txt) and mssql-jdbc on the cluster
    library {
      pypi {
        package = "geoip2==5.0.1"
      }
    }
    
    continuous = false
    photon     = false
    channel    = "CURRENT"
  }
  ```

  **Provider configuration for Part B:**

  Part B is a **separate root directory**. Create `terraform/part_b/versions.tf` and `terraform/part_b/provider.tf`:

  ```hcl
  # terraform/part_b/versions.tf
  terraform {
    required_version = ">= 1.9, < 2.0"
    required_providers {
      databricks = {
        source  = "databricks/databricks"
        version = "~> 1.70"
      }
    }
  }

  # terraform/part_b/provider.tf
  provider "databricks" {
    # Explicit authentication: uses Azure Service Principal or Azure CLI
    # Part B runs in a separate root directory, so auth cannot be inherited
    # from Part A's provider chain. Configure ONE of the following:
    #
    # Option 1 (Azure SPN — recommended for CI):
    #   export ARM_CLIENT_ID="<spn-app-id>"
    #   export ARM_CLIENT_SECRET="<spn-password>"
    #   export ARM_TENANT_ID="<tenant-id>"
    #   export ARM_SUBSCRIPTION_ID="<subscription-id>"
    #
    # Option 2 (Databricks token):
    #   export DATABRICKS_HOST="https://<workspace-url>"
    #   export DATABRICKS_TOKEN="<personal-access-token>"
    #
    # Option 3 (Azure CLI — local dev only):
    #   az login && az account set --subscription "<subscription-id>"
    #   then the Databricks provider picks up Azure CLI tokens automatically.
    #
    # The provider uses the same authentication as Part A via environment
    # variables — see terraform/part_a/provider.tf.
  }
  ```

  > **No Gold DLT pipeline** — the Gold table was eliminated in Phase 5 as it added no transformation value (was just a column projection from Silver). The JDBC export reads from Silver directly.
  >
  > **Part B `outputs.tf`:** Create `terraform/part_b/outputs.tf` to export the Databricks job ID needed by Airflow. This replaces the Part A `module.databricks-pipelines.databricks_job_id` output that was removed in Phase 1 (that module doesn't exist in Part A):
>
  > ```hcl
  > output "databricks_job_id" {
  >   description = "ID of the Databricks Workflow job (for Airflow DatabricksRunNowOperator)"
  >   value       = databricks_job.w3c_etl_pipeline.id
  > }
  > output "databricks_host" {
  >   description = "Databricks workspace URL (for Airflow connection)"
  >   value       = data.databricks_current_user.me.workspace_url
  > }
  > ```

  ```hcl
  # Databricks Workflow (databricks_job) — orchestrates Bronze DLT → Silver DLT → JDBC Export
  
  resource "databricks_job" "w3c_etl_pipeline" {
    name = "w3c-etl-pipeline"
    
    # Schedule: Saturday 6 AM UTC (matching Airflow schedule)
    schedule {
      quartz_cron_expression = "0 0 6 ? * SAT"
      timezone_id           = "UTC"
      pause_status          = "PAUSED"  # Airflow triggers it; start paused
    }
    
    task {
      task_key = "dlt_bronze"
      pipeline_task {
        pipeline_id = databricks_pipeline.w3c_bronze.id
      }
    }
    
    task {
      task_key = "dlt_silver"
      pipeline_task {
        pipeline_id = databricks_pipeline.w3c_silver.id
      }
      depends_on {
        task_key = "dlt_bronze"
      }
    }
    
    task {
      task_key = "jdbc_export"
      spark_python_task {
        python_file = "/Workspace/Shared/w3c-etl/jobs/jdbc_export_azure.py"
      }
      # Use new_cluster for production (auto-creates + auto-terminates); fall back to
      # existing_cluster_id for dev. The new_cluster block is the default.
      new_cluster {
        num_workers   = 1
        node_type_id  = "Standard_DS3_v2"
        spark_version = "15.4.x-scala2.12"
        spark_conf = {
          "spark.jars.packages" : "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11"
        }
      }
      depends_on {
        task_key = "dlt_silver"
      }
    }
  }
  ```

  > **Why only 3 tasks (not 4)?** The Gold DLT pipeline was eliminated (see Phase 5). The JDBC export reads from Silver directly. This saves a DLT pipeline run and its associated cost.
  >
  > **Note on `new_cluster` vs `existing_cluster_id`:** The job uses `new_cluster` by default so Terraform manages the cluster lifecycle (auto-creation + auto-termination). For debugging, switch to `existing_cluster_id` pointing to a persistent dev cluster. The MSSQL JDBC driver is installed via `spark.jars.packages` in `new_cluster.spark_conf`.
  
- [ ] Add `terraform/modules/databricks-pipelines/variables.tf`:

  ```hcl
  variable "export_cluster_id" {
    description = "Optional: existing Databricks cluster ID for the JDBC export job task. If omitted, new_cluster is used (auto-created, auto-terminated)."
    type        = string
    default     = null
  }
  
  variable "storage_account" {
    description = "ADLS Gen2 storage account name (for pipeline configuration)"
    type        = string
  }
  ```
  
- [ ] Define variables in the root `terraform/environments/dev/terraform.tfvars`:

  ```hcl
  # Databricks cluster for the JDBC export spark_python_task.
  # Default: new_cluster (auto-created + auto-terminated).
  # For dev debugging, set to an existing cluster ID:
  # export_cluster_id = "<your-databricks-cluster-id>"
  storage_account   = "<your-storage-account-name>"
  ```

  **Cluster Configuration for JDBC Export Task**

  The JDBC export Spark task requires a cluster with the MSSQL JDBC driver. Configuration:

  | Setting | Value | Rationale |
  |---------|-------|-----------|
  | Cluster Mode | Job Cluster (Task uses `new_cluster` for production; `existing_cluster_id` for dev) | Lower cost for production |
  | DBR Version | 15.4 LTS (Spark 3.5.0, Scala 2.12) | Matches DLT runtime for ABI compatibility |
  | Node Type | Standard_DS3_v2 | Same as DLT clusters for consistent performance baseline |
  | Runtime Engine | Standard (not Photon) | Photon not available for Python tasks |
  | Libraries (Maven) | `com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11` | Required for `Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")` |
  | Auto-termination | 10 minutes | Cost control for dev clusters |

- [ ] Apply Terraform Part B:

  ```bash
  cd terraform/part_b
  terraform init   # Will pick up the new module
  terraform apply  # Creates DLT pipelines + Workflow
  ```

- [ ] **Airflow DAG integration — Approach B (recommended): Create a separate DAG for Azure path**

  Instead of complex branching logic in the existing `spark_ingestion.py`, create a new DAG `spark_ingestion_azure.py`:

  ```python
  """
  W3C Spark Ingestion DAG — Azure Databricks Path.
  
  Separate DAG from the Docker-based spark_ingestion.py.
  Only one DAG is active at a time based on execution mode.
  
  Flow:
    1. Trigger Databricks Workflow (Bronze → Silver → JDBC Export)
    2. Run export_dimensions_azure (reads from Azure SQL, not local Delta)
    3. Fire Dataset outlet → triggers dbt_marts DAG (which targets Azure SQL)
  """
  from __future__ import annotations
  
  import datetime as dt
  import os
  
  from airflow import DAG
  from airflow.datasets import Dataset
  from airflow.operators.python import PythonOperator
  from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
  
  from operators.export_dimensions_azure import export_dimensions_azure as _export_dimensions_azure
  
  # Use same Dataset URI as the Docker path so that the existing dbt_marts DAG triggers
  # (approach A from the review: produce the same PostgreSQL-style Dataset).
  WAREHOUSE_LOADED = Dataset("postgres://postgres:5432/w3c_warehouse/public/raw_enriched_loaded")
  
  default_args = {
      "owner": "w3c-team",
      "depends_on_past": False,
      # email_on_failure intentionally omitted: no SMTP configured.
      # Alerts via existing Prometheus + Grafana stack (Airflow exporter).
      "retries": 2,
      "retry_delay": dt.timedelta(minutes=5),
  }
  
  dag = DAG(
      dag_id="w3c_spark_ingestion_azure",
      schedule="0 6 * * 6",
      start_date=dt.datetime(2026, 3, 1),
      catchup=False,
      max_active_runs=1,
      default_args=default_args,
      tags=["w3c", "databricks", "azure"],
  )
  
  trigger_databricks = DatabricksRunNowOperator(
      task_id="trigger_azure_pipeline",
      databricks_conn_id="databricks_default",
      job_id=os.environ.get("DATABRICKS_WORKFLOW_ID", "0"),
      dag=dag,
  )
  
  # export_dimensions_azure reads from Azure SQL instead of local Delta
  export_dimensions = PythonOperator(
      task_id="export_dimensions",
      python_callable=_export_dimensions_azure,
      outlets=[WAREHOUSE_LOADED],
      dag=dag,
  )
  
  trigger_databricks >> export_dimensions
  ```

  > **Why approach B (separate DAG)?** The existing `spark_ingestion.py` has 4 sequential `SparkSubmitOperator` tasks that tightly couple to the Docker Spark path. Adding Azure branching with `BranchPythonOperator` would add complexity, and the `SparkSubmitOperator` tasks would still run (and fail) without a Docker Spark cluster. A separate DAG is cleaner, easier to test, and follows the single-responsibility principle. Only one DAG is active at a time based on the execution mode.

- [ ] **Address `export_dimensions` local path dependency (A2 fix):**

  Create `airflow/plugins/operators/export_dimensions_azure.py` — a full implementation that reads from Azure SQL Database instead of local Delta:

  ```python
  """
  export_dimensions_azure.py — Azure SQL version of export_dimensions.
  
  Replaces export_dimensions.py for the Azure execution path.
  
  Key differences from the Docker version:
  - Reads raw_enriched from Azure SQL Database (via pyodbc) instead of
    local Delta parquet files (pd.read_parquet())
  - Reads dim_useragent source from Azure SQL raw_enriched.user_agent
    instead of PostgreSQL public.raw_enriched
  - Writes dim_geolocation and dim_useragent to BOTH PostgreSQL and
    Azure SQL. The PostgreSQL write maintains backward compatibility
    with the Docker path. The Azure SQL write ensures dbt models
    (fact_webrequest, mart_daily_aggregates, mart_browser_analysis,
    mart_country_browser_share) can reference dbo.dim_geolocation and
    dbo.dim_useragent when running against the Azure SQL target.
  """
  import os
  from urllib.parse import unquote_plus
  
  import pandas as pd
  from sqlalchemy import create_engine, text
  
  # PostgreSQL connection (target for dimension tables — same as Docker path)
  PG_CONN_STR = os.environ.get(
      "AIRFLOW_CONN_POSTGRES_DEFAULT",
      "postgresql://w3c:w3c@postgres:5432/w3c_warehouse"
  )
  
  def get_azure_engine() -> object:
      """Create SQLAlchemy engine for Azure SQL Database."""
      server = os.environ["AZURE_SQL_SERVER"]
      database = os.environ["AZURE_SQL_DB"]
      user = os.environ["AZURE_SQL_USER"]
      password = os.environ["AZURE_SQL_PASSWORD"]
      conn_str = (
          f"mssql+pyodbc://{user}:{password}"
          f"@{server}.database.windows.net:1433/{database}"
          f"?driver=ODBC+Driver+18+for+SQL+Server"
      )
      return create_engine(conn_str)
  
  
  def _build_dim_geolocation(raw_df: pd.DataFrame) -> pd.DataFrame:
      """Build dim_geolocation from raw_enriched data.
      
      Matches the logic in export_dimensions.py::_build_dim_geolocation()
      but reads from Azure SQL DataFrame instead of Delta parquet.
      
      CRITICAL: Renames client_ip → ip to match the PostgreSQL
      dim_geolocation table's column name (the PK column is `ip`, not
      `client_ip`). Without this rename, to_sql() maps by column name
      and produces NULL in the PK column, causing NOT NULL violation.
      """
      geo_df = raw_df[[
          "client_ip", "country", "region", "city",
          "latitude", "longitude", "postcode", "isp"
      ]].dropna(subset=["client_ip"]).drop_duplicates(subset=["client_ip"])
      
      geo_df = geo_df.rename(columns={"client_ip": "ip"})
      geo_df["country"] = geo_df["country"].fillna("Unknown").replace("", "Unknown")
      geo_df["region"] = geo_df["region"].fillna("Unknown").replace("", "Unknown")
      geo_df["city"] = geo_df["city"].fillna("Unknown").replace("", "Unknown")
      geo_df["postcode"] = geo_df["postcode"].fillna("-").replace("", "-")
      geo_df["isp"] = geo_df["isp"].fillna("-").replace("", "-")
      
      return geo_df
  
  
  def _build_dim_useragent(raw_df):
      """Build dim_useragent dimension from raw data, using user_agents library for UA parsing.
      
      Matches the existing Docker export_dimensions.py pattern for consistency.
      """
      from user_agents import parse as ua_parse
      
      ua_records = raw_df[["user_agent"]].dropna().drop_duplicates()
      
      rows = []
      for _, row in ua_records.iterrows():
          ua_str = str(row["user_agent"])[:1000]
          ua = ua_parse(unquote_plus(ua_str))
          
          # Match export_dimensions.py::_parse_user_agent exactly.
          agent_type = "Crawler" if getattr(ua, "is_bot", False) else "Browser"
          browser_name = ua.browser.family or "Unknown"
          browser_version = ua.browser.version_string or "Unknown"
          operating_system = ua.os.family or "Unknown"
          
          if getattr(ua, "is_mobile", False):
              device_type = "Mobile"
          elif getattr(ua, "is_tablet", False):
              device_type = "Tablet"
          elif getattr(ua, "is_pc", False):
              device_type = "Desktop"
          elif getattr(ua, "is_bot", False):
              device_type = "Bot"
          else:
              device_type = "Other"
          
          rows.append({
              "user_agent": ua_str,
              "agent_type": agent_type,
              "browser_name": browser_name,
              "browser_version": browser_version,
              "operating_system": operating_system,
              "device_type": device_type,
          })
      
      ua_df = pd.DataFrame(rows)
      if ua_df.empty:
          ua_df = pd.DataFrame(columns=["user_agent", "agent_type", "browser_name",
                                         "browser_version", "operating_system", "device_type"])
      
      return ua_df


  def _ensure_azure_dim_tables(azure_engine):
      """Create dim_geolocation and dim_useragent tables in Azure SQL if they don't exist.
      
      These tables are referenced by dbt models (fact_webrequest, mart_daily_aggregates,
      mart_browser_analysis, mart_country_browser_share) when running against Azure SQL.
      """
      with azure_engine.connect() as conn:
          conn.execute(text("""
              IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_geolocation')
              CREATE TABLE dbo.dim_geolocation (
                  geolocation_sk INT IDENTITY(1,1) PRIMARY KEY,
                  ip VARCHAR(45) NOT NULL UNIQUE,
                  country NVARCHAR(100),
                  region NVARCHAR(100),
                  city NVARCHAR(100),
                  postcode VARCHAR(20),
                  latitude FLOAT,
                  longitude FLOAT,
                  isp NVARCHAR(200)
              )
          """))
          conn.execute(text("""
              IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_useragent')
              CREATE TABLE dbo.dim_useragent (
                  user_agent_sk INT IDENTITY(1,1) PRIMARY KEY,
                  user_agent NVARCHAR(1000) NOT NULL UNIQUE,
                  agent_type VARCHAR(20),
                  browser_name NVARCHAR(200),
                  browser_version VARCHAR(50),
                  operating_system NVARCHAR(200),
                  device_type NVARCHAR(100)
              )
          """))
          # Preserve the existing -1 unknown rows for FK integrity and Power BI relationships.
          conn.execute(text("""
              IF NOT EXISTS (SELECT 1 FROM dbo.dim_geolocation WHERE geolocation_sk = -1)
              BEGIN
                  SET IDENTITY_INSERT dbo.dim_geolocation ON;
                  INSERT INTO dbo.dim_geolocation
                      (geolocation_sk, ip, country, region, city, postcode, latitude, longitude, isp)
                  VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', '-', NULL, NULL, '-');
                  SET IDENTITY_INSERT dbo.dim_geolocation OFF;
              END
          """))
          conn.execute(text("""
              IF NOT EXISTS (SELECT 1 FROM dbo.dim_useragent WHERE user_agent_sk = -1)
              BEGIN
                  SET IDENTITY_INSERT dbo.dim_useragent ON;
                  INSERT INTO dbo.dim_useragent
                      (user_agent_sk, user_agent, agent_type, browser_name,
                       browser_version, operating_system, device_type)
                  VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown');
                  SET IDENTITY_INSERT dbo.dim_useragent OFF;
              END
          """))
          conn.commit()


  def _write_dim_to_azure(azure_engine, table_name, df, natural_key):
      """Write a dimension DataFrame to Azure SQL using MERGE (upsert).
      
      Uses the natural key (`ip` or `user_agent`), not the surrogate key,
      so reruns do not create duplicate dimension members and existing
      surrogate keys remain stable.
      """
      with azure_engine.connect() as conn:
          for _, row in df.iterrows():
              col_names = ", ".join(f"[{c}]" for c in df.columns)
              source_cols = ", ".join(f":{c} AS [{c}]" for c in df.columns)
              
              stmt = text(f"""
                  MERGE INTO dbo.{table_name} AS target
                  USING (SELECT {source_cols}) AS source
                  ON target.[{natural_key}] = source.[{natural_key}]
                  WHEN NOT MATCHED THEN
                      INSERT ({col_names})
                      VALUES ({", ".join(f"source.[{c}]" for c in df.columns)});
              """)
              conn.execute(stmt, row.to_dict())
          conn.commit()


  def export_dimensions_azure(**context):
      """Entry point: read from Azure SQL, build dimensions, write to PostgreSQL and Azure SQL.
      
      Two-target write strategy:
      1. PostgreSQL: maintains backward compatibility with Docker path (canonical dim store)
      2. Azure SQL: ensures dbt models on Azure SQL target can find dbo.dim_geolocation
         and dbo.dim_useragent (4 dbt models depend on these tables)
      """
      azure_engine = get_azure_engine()
      
      with azure_engine.connect() as azure_conn:
          raw_df = pd.read_sql(
              "SELECT * FROM dbo.raw_enriched", azure_conn
          )
      
      dim_geo = _build_dim_geolocation(raw_df)
      dim_ua = _build_dim_useragent(raw_df)
      
      # ── Ensure Azure SQL dimension tables exist ──
      _ensure_azure_dim_tables(azure_engine)
      
      # ── Target 1: PostgreSQL (canonical dimension store, Docker path compatibility) ──
      import psycopg2
      from psycopg2.extras import execute_values
      
      pg_conn = psycopg2.connect(PG_CONN_STR)
      try:
          with pg_conn.cursor() as cur:
              cur.execute("""
                  INSERT INTO dim_geolocation
                      (geolocation_sk, ip, country, region, city, postcode, latitude, longitude, isp)
                  VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', '-', NULL, NULL, '-')
                  ON CONFLICT DO NOTHING
              """)
              cur.execute("""
                  INSERT INTO dim_useragent
                      (user_agent_sk, user_agent, agent_type, browser_name,
                       browser_version, operating_system, device_type)
                  VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown')
                  ON CONFLICT DO NOTHING
              """)
              pg_conn.commit()
          
          # dim_geolocation: batch INSERT ... ON CONFLICT (ip) DO NOTHING
          geo_values = [
              (
                  row["ip"],
                  row.get("country"), row.get("region"), row.get("city"),
                  row.get("postcode"),
                  row.get("latitude"), row.get("longitude"),
                  row.get("isp"),
              )
              for _, row in dim_geo.iterrows()
          ]
          execute_values(
              pg_conn.cursor(),
              """
              INSERT INTO dim_geolocation
                  (ip, country, region, city, postcode, latitude, longitude, isp)
              VALUES %s
              ON CONFLICT (ip) DO NOTHING
              """,
              geo_values
          )
          pg_conn.commit()
          
          # dim_useragent: batch INSERT ... ON CONFLICT (user_agent) DO NOTHING
          ua_values = [
              (
                  row["user_agent"],
                  row.get("agent_type"), row.get("browser_name"),
                  row.get("browser_version"), row.get("operating_system"),
                  row.get("device_type"),
              )
              for _, row in dim_ua.iterrows()
          ]
          execute_values(
              pg_conn.cursor(),
              """
              INSERT INTO dim_useragent
                  (user_agent, agent_type, browser_name, browser_version,
                   operating_system, device_type)
              VALUES %s
              ON CONFLICT (user_agent) DO NOTHING
              """,
              ua_values
          )
          pg_conn.commit()
      finally:
          pg_conn.close()
      
      # ── Target 2: Azure SQL (dbt Azure SQL models need these tables) ──
      _write_dim_to_azure(azure_engine, "dim_geolocation", dim_geo, "ip")
      _write_dim_to_azure(azure_engine, "dim_useragent", dim_ua, "user_agent")
      
      print(f"Built dim_geolocation: {len(dim_geo)} rows (written to PostgreSQL + Azure SQL)")
      print(f"Built dim_useragent: {len(dim_ua)} rows (written to PostgreSQL + Azure SQL)")
  ```

- [ ] **Set up Airflow connections for the Azure path:**
  
  Create the `databricks_default` connection in Airflow UI (Admin → Connections) or via CLI:

  ```bash
  # Generate a Databricks PAT token from: User Settings → Developer → Access Tokens
  airflow connections add databricks_default \
    --conn-type databricks \
    --conn-host https://<workspace-url>.cloud.databricks.com \
    --conn-extra '{"token": "<personal-access-token>"}'
  ```
  
  > **Databricks PAT token:** Generate in Databricks workspace: User Settings (top-right avatar) → Developer → Access Tokens → Generate New Token. Use a descriptive name like `airflow-integration`. Store the token securely — it is shown only once.
  
  The following Airflow environment variables / connections are also needed for the Azure path:
  
  | Variable | Source | Purpose |
  |---|---|---|
  | `DATABRICKS_WORKFLOW_ID` | Terraform output `databricks_job.w3c_etl_pipeline.id` | Workflow ID for `DatabricksRunNowOperator` |
  | `AZURE_SQL_SERVER` | Terraform output `server_fqdn` | Azure SQL server hostname |
  | `AZURE_SQL_DB` | Terraform variable `database_name` | Azure SQL database name |
  | `AZURE_SQL_USER` | Terraform output / manual | Pipeline login username |
  | `AZURE_SQL_PASSWORD` | Terraform output (sensitive) | Pipeline login password |
  
- [ ] **Create Databricks secret scope for pipeline credentials:**
  
  ```bash
  # Create a Databricks secret scope (backed by Databricks, not Azure Key Vault for MVP)
  databricks secrets create-scope --scope w3c-etl-pipeline
  
  # Load Azure SQL credentials into the scope
  databricks secrets put --scope w3c-etl-pipeline --key azure-sql-user
  databricks secrets put --scope w3c-etl-pipeline --key azure-sql-password
  databricks secrets put --scope w3c-etl-pipeline --key azure-sql-server
  databricks secrets put --scope w3c-etl-pipeline --key azure-sql-database
  
  # Reference the scope from .env.azure (sourced during Terraform or manually)
  while IFS='=' read -r key value; do
    echo "$value" | databricks secrets put --scope w3c-etl-pipeline --key "$key"
  done < .env.azure
  ```
  
  > **Why Databricks secrets?** DLT pipelines and Databricks Workflows can read secrets via `dbutils.secrets.get("w3c-etl-pipeline", "azure-sql-user")` (or `{{secrets/w3c-etl-pipeline/azure-sql-user}}` in DLT pipeline configuration) instead of storing credentials in code. The `.env.azure` file from Phase 2 is the source of truth.
  
- [x] Add `pyodbc` and `sqlalchemy` to `airflow/requirements.txt` (already done):

  ```
  pyodbc>=5.1,<6.0
  sqlalchemy>=2.0,<3.0
  user-agents>=2.2,<3.0
  psycopg2-binary>=2.9,<3.0
  ```

- [x] Update `airflow/Dockerfile` to install ODBC Driver 18 for SQL Server (already done):

  ```dockerfile
  # Microsoft ODBC Driver 18 for SQL Server (needed for dbt-sqlserver + pyodbc)
  # Base image is Debian-based (apache/airflow:2.10.2 uses Debian Bookworm)
  # Use apt-get install -y curl because curl may not be in the base image
  RUN apt-get update && apt-get install -y curl gnupg2 && rm -rf /var/lib/apt/lists/* \
      && curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
      && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
      && apt-get update \
      && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
      && rm -rf /var/lib/apt/lists/*
  ```

**Acceptance:** Databricks Workflow runs all 3 tasks (Bronze → Silver → JDBC Export) in sequence. DLT pipeline resources created AFTER code is uploaded (CRIT-3 resolved). Airflow separate DAG triggers the workflow via `DatabricksRunNowOperator`. `export_dimensions_azure` reads from Azure SQL with full dimension-building logic. Downstream dbt DAG triggers correctly via generic Dataset URI.

#### Phase 6 Handoff: Airflow DAG Import Check

Before proceeding to Phase 7, verify the new Airflow DAG loads correctly:

```bash
# Check DAG integrity (no import errors)
python -c "
from airflow.models import DagBag
dagbag = DagBag(dag_folder='airflow/dags/w3c')
assert 'w3c_spark_ingestion_azure' in dagbag.dag_ids, \
    f'Azure DAG not found. Errors: {dagbag.import_errors}'
print('Azure DAG loaded successfully')
"
```

Also verify the dimension export operator imports correctly:

```bash
python -c "
from operators.export_dimensions_azure import export_dimensions_azure
print('export_dimensions_azure import OK')
"
```

Expected: No import errors. The DAG appears in Airflow UI with all 2 tasks (trigger_databricks, export_dimensions). The Databricks connection `databricks_default` is configured.

#### Phase 6 → 7 Handoff: dbt Profile Switching

> **⚠️ dbt DAG profile switching:** The existing `dbt_marts.py` DAG uses a hardcoded `--profile w3c` flag for PostgreSQL. When triggered by the Azure DAG's Dataset outlet, it must switch to `--profile w3c_azure` to target Azure SQL.
>
> **Required change:** Update `dbt_marts.py` to use `{{ env_var('DBT_PROFILE', 'w3c') }}` or a Dataset-aware branch:
>
> - **Option A (env_var):** Use `{{ env_var('DBT_PROFILE', 'w3c') }}` in dbt_project.yml (already done), then set `DBT_PROFILE=w3c_azure` as an Airflow environment variable in the Azure DAG's task context. The existing `dbt_marts.py` DAG reads `os.environ.get('DBT_PROFILE', 'w3c')` to decide the `--profile` flag.
> - **Option B (separate dbt DAG):** Create `dbt_marts_azure.py` — a copy of `dbt_marts.py` with `--profile w3c_azure --target dev` hardcoded, listening to the same Dataset URI. Simpler but duplicates DAG code.
> - **Option C (BranchPythonOperator):** Use a single DAG with a branch that checks a global Airflow Variable (`azure_mode`) to decide which profile to run.
>
> **Recommendation:** Option A (env_var) is the cleanest — it requires only a single-line change to `dbt_marts.py` (read profile from env var) and setting the env var on the Airflow task. The `dbt_project.yml` already uses `{{ env_var('DBT_PROFILE', 'w3c') }}`, so dbt itself picks up the env var.
>
#### Phase 6 → 7 Handoff: Azure SQL CSV Export for Power BI

> **Required:** CSV export is in scope. The Azure path must produce the same 18 BI CSV files as the Docker/PostgreSQL path. Do not rely on `psql \copy`; it only works against PostgreSQL.

1. Fix the existing Docker export list first:
   - Add `dbt_marts.mart_country_browser_share` to `MART_TABLES` in `airflow/dags/w3c/dbt_marts.py`.
   - Update `tests/test_dag_integrity.py::test_export_csv_generates_correct_script` so it expects 18 exported tables (10 staging + 6 marts + 2 public dims).
   - Verify `airflow/data/Star-Schema/dbt_marts.mart_country_browser_share.csv` is generated on the Docker path.

2. Create `airflow/plugins/operators/export_csv_azure.py`:

   ```python
   """
   Export Azure SQL dbt/star-schema tables to Power BI CSV files.
   
   Mirrors dbt_marts.py's PostgreSQL psql/copy export, but uses pyodbc
   because Azure SQL cannot be exported with psql.
   """
   
   from __future__ import annotations
   
   import csv
   import os
   from pathlib import Path
   
   import pyodbc
   
   STAR_SCHEMA_DIR = Path(os.environ.get("STAR_SCHEMA_DIR", "/opt/airflow/data/Star-Schema"))
   
   # logical_name preserves the existing Power BI CSV filename.
   # source_name is the actual Azure SQL object to query.
   EXPORT_TABLES = [
       ("dbt_staging.fact_webrequest", "dbt_staging.fact_webrequest"),
       ("dbt_staging.dim_date", "dbt_staging.dim_date"),
       ("dbt_staging.dim_time", "dbt_staging.dim_time"),
       ("dbt_staging.dim_page", "dbt_staging.dim_page"),
       ("dbt_staging.dim_status", "dbt_staging.dim_status"),
       ("dbt_staging.dim_referrer", "dbt_staging.dim_referrer"),
       ("dbt_staging.dim_method", "dbt_staging.dim_method"),
       ("dbt_staging.dim_visitortype", "dbt_staging.dim_visitortype"),
       ("dbt_staging.dim_visit_buckets", "dbt_staging.dim_visit_buckets"),
       ("dbt_staging.crawler_ips", "dbt_staging.crawler_ips"),
       ("dbt_marts.mart_page_performance", "dbt_marts.mart_page_performance"),
       ("dbt_marts.mart_daily_aggregates", "dbt_marts.mart_daily_aggregates"),
       ("dbt_marts.mart_crawler_analysis", "dbt_marts.mart_crawler_analysis"),
       ("dbt_marts.mart_browser_analysis", "dbt_marts.mart_browser_analysis"),
       ("dbt_marts.mart_timeofday_analysis", "dbt_marts.mart_timeofday_analysis"),
       ("dbt_marts.mart_country_browser_share", "dbt_marts.mart_country_browser_share"),
       ("public.dim_geolocation", "dbo.dim_geolocation"),
       ("public.dim_useragent", "dbo.dim_useragent"),
   ]
   
   
   def _quote_table(source_name: str) -> str:
       schema, table = source_name.split(".", 1)
       return f"[{schema}].[{table}]"
   
   
   def _connect():
       driver = os.environ.get("AZURE_SQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
       server = os.environ["AZURE_SQL_SERVER"]
       database = os.environ["AZURE_SQL_DATABASE"]
       user = os.environ["AZURE_SQL_USER"]
       password = os.environ["AZURE_SQL_PASSWORD"]
       return pyodbc.connect(
           "DRIVER={" + driver + "};"
           f"SERVER=tcp:{server},1433;"
           f"DATABASE={database};"
           f"UID={user};"
           f"PWD={password};"
           "Encrypt=yes;"
           "TrustServerCertificate=no;"
           "Connection Timeout=30;"
       )
   
   
   def export_csv_azure(**context) -> None:
       STAR_SCHEMA_DIR.mkdir(parents=True, exist_ok=True)
       with _connect() as conn:
           for logical_name, source_name in EXPORT_TABLES:
               output_path = STAR_SCHEMA_DIR / f"{logical_name}.csv"
               cursor = conn.cursor()
               cursor.execute(f"SELECT * FROM {_quote_table(source_name)}")
               headers = [col[0] for col in cursor.description]
               with output_path.open("w", newline="", encoding="utf-8") as f:
                   writer = csv.writer(f)
                   writer.writerow(headers)
                   while True:
                       rows = cursor.fetchmany(10000)
                       if not rows:
                           break
                       writer.writerows(rows)
               cursor.close()
               print(f"Exported {logical_name} from {source_name} to {output_path}")
   ```

3. Wire the Azure dbt DAG to use `export_csv_azure` after `dbt docs generate`:
   - Use the same 18 `EXPORT_TABLES` ordering as the Docker path.
   - Keep output filenames identical to the current Power BI contract (`public.dim_geolocation.csv`, not `dbo.dim_geolocation.csv`).
   - Store Azure SQL credentials in Airflow environment variables or an Airflow connection; do not hardcode them in the DAG.

4. Add tests:
   - Unit test `_quote_table()` escapes the schema/table shape expected by SQL Server.
   - Unit test `EXPORT_TABLES` contains exactly 18 logical names and includes `dbt_marts.mart_country_browser_share`.
   - DAG integrity test confirms the Azure dbt DAG ends with `export_csv` and uses the Python callable.
   - Header regression test compares generated Azure CSV headers to the Docker CSV headers for all 18 files.

5. Manual Power BI validation:
   - Refresh the PBIX/Power BI dataset against the Azure-generated CSV directory.
   - Confirm all user-listed tables, columns, and DAX measures refresh without missing-field errors.
   - Confirm the "Top Browser Share max per Country" measure has `dbt_marts.mart_country_browser_share.csv` available as a source.

---

### Phase 7 — dbt Migration to Azure SQL (5·8 hours) ⚠️ Highest effort

> **⚠️ Split into 7a + 7b:** Phase 7 is the largest phase at 5–8 hours. Split it to reduce risk:
>
> - **Phase 7a (2–3 hours):** Create macro library, convert simple models (dim_date, dim_time, dim_status, dim_method, dim_visit_buckets, dim_visitortype, fact_webrequest, all 6 mart models). Test with `dbt compile --profile w3c_azure --target dev`.
> - **Phase 7b (3–5 hours):** Convert regex-heavy models (dim_page, dim_referrer), full `dbt run`, `dbt test`, cross-check row counts. Highest risk of T-SQL approximation errors.

**Goal:** dbt connects to Azure SQL Database as a second target, running the same models after the Azure pipeline.

> **CRIT-1: Comprehensive PostgreSQL→T-SQL migration required across ALL 16 models.**
> The dbt models use 32 `::` cast expressions plus ~57 more T-SQL-incompatible patterns (EXTRACT, TO_CHAR, SPLIT_PART, generate_series, ~* regex, CREATE INDEX IF NOT EXISTS) across 7 different SQL features. Simply converting `::` casts (as initially assumed) only covers ~35% of the problem. This section provides complete T-SQL conversions for every expression.

> **Approach: inline `{% if target.type == 'sqlserver' %}` conditionals in existing models.** Rather than creating `_azure.sql` copies (which would cause duplicate model definitions — see §7.3 for details), edit each model in-place to use the cross-database macros from §7.1. The PostgreSQL branch preserves the original SQL when `--profile w3c` is used; the T-SQL branch activates when `--profile w3c_azure` is used. The macro library below provides reusable cross-database helpers for patterns used in both targets.

#### 7.1 Cross-Database Macro Library

1. Create the macros directory:

   ```bash
   mkdir -p airflow/dbt/w3c/macros
   ```

2. Create `airflow/dbt/w3c/macros/t_sql_compat.sql` with target-type-aware macros:

```sql
-- ────────────────────────────────────────────────────────────
-- T-SQL Compatibility Macros
-- Cross-database macros providing target-type-aware SQL.
-- The {% if target.type == 'sqlserver' %} branch runs for Azure SQL;
-- the {% else %} branch preserves original PostgreSQL-compatible SQL.
-- Used by ALL models (not _azure.sql copies — see §7.3).
-- ────────────────────────────────────────────────────────────

-- Safe CAST wrapper (replaces ::type syntax)
{% macro tsql_cast(expr, as_type) -%}
  {% if target.type == 'sqlserver' -%}
    CAST({{ expr }} AS {{ as_type }})
  {% else -%}
    {{ expr }}::{{ as_type }}
  {% endif -%}
{%- endmacro %}

-- EXTRACT(YEAR/MONTH/DAY FROM ...) → DATEPART
{% macro tsql_datepart(part, expr) -%}
  {% if target.type == 'sqlserver' -%}
    DATEPART({{ part }}, {{ expr }})
  {% else -%}
    EXTRACT({{ part }} FROM {{ expr }})
  {% endif -%}
{%- endmacro %}

-- EXTRACT(DOW FROM ...) → DATEPART(WEEKDAY, ...) - 1
-- (PostgreSQL DOW: 0=Sunday..6=Saturday; SQL Server: 1=Sunday..7=Saturday)
{% macro tsql_dow(expr) -%}
  {% if target.type == 'sqlserver' -%}
    DATEPART(WEEKDAY, {{ expr }}) - 1
  {% else -%}
    EXTRACT(DOW FROM {{ expr }})
  {% endif -%}
{%- endmacro %}

-- TO_CHAR(date, 'YYYYMMDD') → FORMAT(date, 'yyyyMMdd')
{% macro tsql_format_date(expr, pg_format, tsql_format) -%}
  {% if target.type == 'sqlserver' -%}
    FORMAT({{ expr }}, '{{ tsql_format }}')
  {% else -%}
    TO_CHAR({{ expr }}, '{{ pg_format }}')
  {% endif -%}
{%- endmacro %}

-- TO_CHAR(date, 'FMMonth') → DATENAME(MONTH, date)
{% macro tsql_month_name(expr) -%}
  {% if target.type == 'sqlserver' -%}
    DATENAME(MONTH, {{ expr }})
  {% else -%}
    TO_CHAR({{ expr }}, 'FMMonth')
  {% endif -%}
{%- endmacro %}

-- TO_CHAR(date, 'FMDay') → DATENAME(WEEKDAY, date)
{% macro tsql_day_name(expr) -%}
  {% if target.type == 'sqlserver' -%}
    DATENAME(WEEKDAY, {{ expr }})
  {% else -%}
    TO_CHAR({{ expr }}, 'FMDay')
  {% endif -%}
{%- endmacro %}

-- SPLIT_PART(str, delim, position) → T-SQL workaround
-- Uses PARSENAME for 2-part splits or CHARINDEX/SUBSTRING for general case
{% macro tsql_split_part(str, delim, position) -%}
  {% if target.type == 'sqlserver' -%}
    -- Replace SPLIT_PART with CHARINDEX/SUBSTRING
    CASE WHEN {{ str }} IS NULL THEN NULL
         WHEN CHARINDEX('{{ delim }}', {{ str }}) = 0 THEN {{ str }}
         ELSE
           CASE WHEN {{ position }} = 1
             THEN LEFT({{ str }}, CHARINDEX('{{ delim }}', {{ str }}) - 1)
             ELSE SUBSTRING(
               {{ str }},
               CHARINDEX('{{ delim }}', {{ str }}) + 1,
               LEN({{ str }})
             )
           END
    END
  {% else -%}
    SPLIT_PART({{ str }}, '{{ delim }}', {{ position }})
  {% endif -%}
{%- endmacro %}

-- REVERSE of SPLIT_PART (for extracting file extension etc.)
{% macro tsql_split_part_rev(str, delim, position) -%}
  {% if target.type == 'sqlserver' -%}
    CASE WHEN {{ str }} IS NULL THEN NULL
         WHEN CHARINDEX('{{ delim }}', REVERSE({{ str }})) = 0 THEN {{ str }}
         ELSE
           CASE WHEN {{ position }} = 1
             THEN LEFT(REVERSE({{ str }}), CHARINDEX('{{ delim }}', REVERSE({{ str }})) - 1)
             ELSE SUBSTRING(
               REVERSE({{ str }}),
               CHARINDEX('{{ delim }}', REVERSE({{ str }})) + 1,
               LEN({{ str }})
             )
           END
    END
  {% else -%}
    SPLIT_PART(REVERSE({{ str }}), '{{ delim }}', {{ position }})
  {% endif -%}
{%- endmacro %}

-- Regex match: ~ → LIKE (approximate — handles simple suffix/prefix patterns)
-- For complex regex patterns, use a macro with pattern-specific T-SQL logic
{% macro tsql_like(expr, pattern_tsql) -%}
  {% if target.type == 'sqlserver' -%}
    {{ expr }} LIKE '{{ pattern_tsql }}'
  {% else -%}
    {{ expr }} ~ '{{ pattern_tsql }}'
  {% endif -%}
{%- endmacro %}

-- Case-insensitive regex match: ~* → LIKE with CI collation
{% macro tsql_ilike(expr, pattern_tsql) -%}
  {% if target.type == 'sqlserver' -%}
    {{ expr }} LIKE '{{ pattern_tsql }}' COLLATE SQL_Latin1_General_CP1_CI_AS
  {% else -%}
    {{ expr }} ~* '{{ pattern_tsql }}'
  {% endif -%}
{%- endmacro %}

-- REGEXP_REPLACE → REPLACE (simple case) or inline T-SQL logic
-- For complex regex replacement, uses manual string manipulation
{% macro tsql_regexp_replace(expr, pattern_src, replacement_tsql) -%}
  {% if target.type == 'sqlserver' -%}
    {{ replacement_tsql }}
  {% else -%}
    REGEXP_REPLACE({{ expr }}, '{{ pattern_src }}', '\1')
  {% endif -%}
{%- endmacro %}

-- generate_series(0, N) → recursive CTE
{% macro tsql_generate_series(alias, start_val, end_val) -%}
  {% if target.type == 'sqlserver' -%}
    (
      WITH {{ alias }}_cte AS (
        SELECT {{ start_val }} AS n
        UNION ALL
        SELECT n + 1 FROM {{ alias }}_cte WHERE n < {{ end_val }}
      )
      SELECT n AS {{ alias }} FROM {{ alias }}_cte
    )
  {% else -%}
    generate_series({{ start_val }}, {{ end_val }}) AS {{ alias }}({{ alias }})
  {% endif -%}
{%- endmacro %}

-- CASE for ::INT cast on boolean (is_404::INT)
{% macro tsql_bool_to_int(expr) -%}
  {% if target.type == 'sqlserver' -%}
    CASE WHEN {{ expr }} THEN 1 ELSE 0 END
  {% else -%}
    {{ expr }}::INT
  {% endif -%}
{%- endmacro %}
```

#### 7.2 Model-by-Model Conversion Guide

Below is the complete cross-reference for every PostgreSQL-only expression in all 16 models.

##### dim_page.sql — 21 PostgreSQL expressions (most affected)

| # | Line | Pattern | PG Expression | T-SQL Equivalent |
|---|---|---|---|---|
| 1 | post_hook | `CREATE INDEX IF NOT EXISTS` | `CREATE INDEX IF NOT EXISTS ...` | `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_dim_page_path') CREATE INDEX idx_dim_page_path ON ...` |
| 2-3 | directory CASE | `~ '^.*/[^/]+/[^/]+$'` | Case-sensitive regex match | `... LIKE '%/[^/]/%[^/]'` (approximate); BETTER: use `CHARINDEX('/', REVERSE(...))` to count slashes |
| 4-5 | directory CASE | `SPLIT_PART(...)` | Two split operations | Use `{{ tsql_split_part_rev(...) }}` |
| 6 | directory CASE | `~ '^/[^/]+$'` | Root-level path | `... LIKE '/[^/]%' AND ... NOT LIKE '%/%/[^/]%'` |
| 7-8 | file_name CASE | `~ '^.*/([^/]+)$'` | Extract filename | Use `REVERSE(SUBSTRING(REVERSE(...), 1, CHARINDEX('/', REVERSE(...)) - 1))` |
| 9-10 | file_name | `SPLIT_PART(...)` | Two splits | Use `{{ tsql_split_part_rev(...) }}` |
| 11-16 | ext CASE | `~* '\.(aspx\|asp)$'` | Case-insensitive ext match | `... LIKE '%.aspx' OR ... LIKE '%.asp'` (with CI collation) |
| 17-18 | ext CASE | `~* '\.(html?\|shtml)$'` | Regex with optional char | `(LIKE '%.htm' OR LIKE '%.shtml') AND NOT LIKE '%.html%'` — Note: `LIKE '%.htm%'` would match `.html.bak` or `.htmldoc`. Use `LIKE '%.htm'` (exact suffix) OR `LIKE '%.shtml'` to avoid false positives. |
| 19 | ext fallback | `SPLIT_PART(...)` | Split to get extension | Use `{{ tsql_split_part_rev(...) }}` macro |
| 20 | ext fallback | `~ '\.'` | Contains dot | `... LIKE '%.%'` |
| 21-26 | category CASE | `~*` (6 patterns) | Case-insensitive ext → category | Replace each with `LIKE` + CI collation patterns matching the ext CASE |

**Implementation approach for dim_page:** This model has too many regex patterns for macros alone. The inline `{% if target.type == 'sqlserver' %}` block wraps the `~*` and `~` operators with `LIKE` + `CHARINDEX` logic, while the `{% else %}` block preserves the original PostgreSQL regex for the Docker path.

##### dim_referrer.sql — 10 PostgreSQL expressions

| # | Line | Pattern | PG Expression | T-SQL Equivalent |
|---|---|---|---|---|
| 1 | post_hook | `CREATE INDEX IF NOT EXISTS` | Same as dim_page | Use existence check pattern |
| 2 | referrer_domain | `~* '^https?://([^/]+)'` | Extract domain from URL | `PATINDEX` or `CHARINDEX` + multi-step: `SUBSTRING(url, CHARINDEX('//', url) + 2, CHARINDEX('/', url + '/', CHARINDEX('//', url) + 2) - CHARINDEX('//', url) - 2)` |
| 3 | referrer_domain | `REGEXP_REPLACE(url, '^https?://([^/]+).*', '\1')` | Extract captured group | Manual: `CASE WHEN url LIKE 'http%' THEN LOWER(SUBSTRING(url, CHARINDEX('//', url) + 2, CHARINDEX('/', url + '/', CHARINDEX('//', url) + 2) - CHARINDEX('//', url) - 2)) END` |
| 4 | referrer_domain | `SPLIT_PART(REPLACE(...), '/', 1)` | Split on / | `LEFT(domain_str, CHARINDEX('/', domain_str + '/') - 1)` |
| 5-11 | traffic_source | `~*` (7 patterns) | Brand detection | Replace each with `LIKE '%pattern%'` CI collation |

##### dim_date.sql — 18 PostgreSQL expressions

| # | Line | Pattern | PG Expression | T-SQL Equivalent |
|---|---|---|---|---|
| 1 | source | `log_date::DATE` | `::DATE` cast | `CAST(log_date AS DATE)` via `{{ tsql_cast('log_date', 'DATE') }}` |
| 2 | date_sk | `TO_CHAR(log_date, 'YYYYMMDD')::INTEGER` | TO_CHAR + cast | `CAST(FORMAT(log_date, 'yyyyMMdd') AS INT)` via `{{ tsql_format_date('log_date', 'YYYYMMDD', 'yyyyMMdd') }}` |
| 3-8 | year/month/day/quarter/week | `EXTRACT(YEAR/MONTH/DAY/QUARTER/WEEK FROM log_date)::INTEGER` | 6 EXTRACT calls | `DATEPART(YEAR/MONTH/DAY/QUARTER/WEEK, log_date)` via `{{ tsql_datepart(...) }}` macro |
| 9 | month_name | `TO_CHAR(log_date, 'FMMonth')` | FM prefix | `DATENAME(MONTH, log_date)` via `{{ tsql_month_name(...) }}` |
| 10 | day_name | `TO_CHAR(log_date, 'FMDay')` | FM prefix | `DATENAME(WEEKDAY, log_date)` via `{{ tsql_day_name(...) }}` |
| 11 | day_of_week | `EXTRACT(DOW FROM log_date)::INTEGER` | DOW (0-6) | `DATEPART(WEEKDAY, log_date) - 1` (maps 1-7 to 0-6) via `{{ tsql_dow(...) }}` |
| 12 | holiday | `'{{ date }}'::DATE` | `::DATE` cast | `CAST('{{ date }}' AS DATE)` |

##### dim_time.sql — 2 PostgreSQL expressions

| # | Line | Pattern | PG Expression | T-SQL Equivalent |
|---|---|---|---|---|
| 1-2 | series | `generate_series(0, 23) AS h(hour)` | Set-returning function | Replace with recursive CTE or `GENERATE_SERIES(0, 23)` (Azure SQL supports `GENERATE_SERIES` in compatibility level 160) |
| 3-4 | series | `generate_series(0, 59) AS m(minute)` | Same | Same approach |

**T-SQL version for dim_time.sql (the inline `{% if %}` block for Azure SQL):**
> **Note:** Azure SQL (compatibility level 160) supports `GENERATE_SERIES` natively. This is cleaner than the `sys.all_columns` ROW_NUMBER() approach. Use `GENERATE_SERIES` when available, fall back to the CTE pattern for older compatibility levels.

```sql
WITH time_entries AS (
    SELECT
        (h.hour * 100 + m.minute) AS time_sk,
        h.hour,
        m.minute,
        CASE WHEN h.hour < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
        CASE
            WHEN h.hour < 6 THEN 'Early Morning'
            WHEN h.hour < 12 THEN 'Morning'
            WHEN h.hour < 18 THEN 'Afternoon'
            ELSE 'Evening'
        END AS time_band,
        CASE
            WHEN h.hour < 6 THEN 1
            WHEN h.hour < 12 THEN 2
            WHEN h.hour < 18 THEN 3
            ELSE 4
        END AS shift_id
    FROM
        (SELECT value AS hour FROM GENERATE_SERIES(0, 23)) h
        CROSS JOIN (SELECT value AS minute FROM GENERATE_SERIES(0, 59)) m
)
SELECT * FROM time_entries
```

> **Fallback for older compatibility levels (< 160):** Use `sys.all_columns` ROW_NUMBER() pattern:
>
> ```sql
> (SELECT TOP 24 n = ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 FROM sys.all_columns) h
> ```

##### fact_webrequest.sql — 8 PostgreSQL expressions

| # | Line | Pattern | PG Expression | T-SQL Equivalent |
|---|---|---|---|---|
| 1-3 | post_hook | `CREATE INDEX IF NOT EXISTS` (3×) | Postgres syntax | Use existence check pattern (see dim_page) |
| 4 | computed | `ei.log_date::DATE` | `::DATE` | `CAST(ei.log_date AS DATE)` |
| 5-6 | computed | `::BIGINT` (2×) | `::BIGINT` | `CAST(... AS BIGINT)` |
| 7 | computed | `::INTEGER` | `::INTEGER` | `CAST(... AS INT)` |
| 8-9 | join | `EXTRACT(HOUR FROM c.log_time::TIME)::INTEGER` | EXTRACT + ::TIME | Replace with `DATEPART(HOUR, CAST(c.log_time AS TIME))` |
| 10-11 | join | `EXTRACT(MINUTE FROM c.log_time::TIME)::INTEGER` | Same | Same pattern with MINUTE |

##### dim_status.sql — 1 PostgreSQL expression

| Line | Pattern | PG Expression | T-SQL Equivalent |
|---|---|---|---|
| 1 | `::text` | `::text` | `CAST(... AS VARCHAR(255))` |

##### Mart models — 9 PostgreSQL `::NUMERIC` expressions + 3 `PERCENTILE_CONT OVER ()` fixes

| File | Line | Pattern | PG Expression | T-SQL Equivalent |
|---|---|---|---|---|---|
| mart_page_performance.sql | 12 | `::NUMERIC(10,2)` (2×) | Numeric cast | `CAST(... AS NUMERIC(10,2))` |
| mart_timeofday_analysis.sql | 13 | `is_404::INT` | Bool to int | `CASE WHEN is_404 THEN 1 ELSE 0 END` |
| mart_timeofday_analysis.sql | 14-15 | `::NUMERIC(10,2)` (2×) | Numeric cast | `CAST(... AS NUMERIC(10,2))` |
| mart_daily_aggregates.sql | 17 | `is_404::INT` | Bool to int | `CASE WHEN is_404 THEN 1 ELSE 0 END` |
| mart_daily_aggregates.sql | 18-19 | `::NUMERIC(10,2)` (2×) | Numeric cast | `CAST(... AS NUMERIC(10,2))` |
| mart_daily_aggregates.sql | 19 | `PERCENTILE_CONT(...) WITHIN GROUP (ORDER BY ...)` | PG window syntax | Wrap in `{% if target.type == 'sqlserver' %}... OVER (){% else %}...{% endif %}` — SQL Server requires explicit `OVER ()` on PERCENTILE_CONT |
| mart_crawler_analysis.sql | 29 | `::NUMERIC` | Numeric cast | `CAST(... AS NUMERIC)` |
| mart_crawler_analysis.sql | 30 | `::NUMERIC(10,2)` | Numeric cast | `CAST(... AS NUMERIC(10,2))` |
| mart_crawler_analysis.sql | 30 | `PERCENTILE_CONT(...) WITHIN GROUP (ORDER BY ...)` | PG window syntax | Same `OVER ()` fix — wrap in `{% if target.type == 'sqlserver' %}` conditional |
| mart_browser_analysis.sql | 23 | `::NUMERIC(10,2)` | Numeric cast | `CAST(... AS NUMERIC(10,2))` |
| mart_browser_analysis.sql | 23 | `PERCENTILE_CONT(...) WITHIN GROUP (ORDER BY ...)` | PG window syntax | Same `OVER ()` fix — wrap in `{% if target.type == 'sqlserver' %}` conditional |

#### 7.3 Implementation Strategy

**Approach: inline `{% if target.type == 'sqlserver' %}...{% else %}...{% endif %}` in existing models**

Rather than creating `_azure.sql` duplicates (which would cause each `ref()` in dbt to see TWO definitions of the same model — the `.sql` file AND the `_azure.sql` file — since dbt parses ALL `.sql` files in model-paths). Instead:

1. **Edit each model in-place** to use the `{% if target.type == 'sqlserver' %}...{% else %}...{% endif %}` pattern from the §7.1 macro library. The PostgreSQL path is unchanged when `--profile w3c` is used; the Azure SQL path uses the T-SQL branch when `--profile w3c_azure` is used.
2. **Global macro file** (`macros/t_sql_compat.sql`) — defines reusable cross-database macros from §7.1, used by all models for shared patterns.
3. **Simple conversions** (cast, EXTRACT, TO_CHAR, bool→int) — use macros from `t_sql_compat.sql` directly in model files. The macros evaluate to the correct dialect based on `target.type`.
4. **Complex regex models** (dim_page.sql, dim_referrer.sql) — wrap regex-heavy portions in `{% if target.type == 'sqlserver' %}...{% else %}...{% endif %}` blocks with `LIKE` + `CHARINDEX`/`SUBSTRING` T-SQL alternatives.
5. **dim_time.sql** — wrap `generate_series` in a conditional using a `CROSS JOIN` of numbered rows for Azure SQL.
6. **Run `--profile w3c_azure`** — `target.type` evaluates to `sqlserver`, and the T-SQL branches activate. The PostgreSQL pipeline is unaffected.

> **Migration safety:** The PostgreSQL path remains fully intact — the `{% else %}` branch preserves the original SQL. If T-SQL conversion has bugs, only the Azure SQL pipeline is affected. The PostgreSQL CI (`dbt compile --profile w3c`) continues to validate the original path on every commit.
>
> **CRITICAL:** Do NOT create `_azure.sql` copies. dbt parses ALL `.sql` files in model-paths — having both `dim_date.sql` and `dim_date_azure.sql` creates two separate models named `dim_date` and `dim_date_azure`. The `ref('dim_date')` call would always resolve to the original, bypassing the Azure version entirely. The inline conditional approach avoids this entirely.

**Example: dim_date.sql with macro conversion**

```sql
WITH raw_dates AS (
    SELECT DISTINCT {{ tsql_cast('log_date', 'DATE') }} AS log_date
    FROM {{ source('w3c', 'raw_enriched') }}
    WHERE log_date IS NOT NULL
),
date_entries AS (
    SELECT
        {{ tsql_cast(tsql_format_date('log_date', 'YYYYMMDD', 'yyyyMMdd'), 'INT') }} AS date_sk,
        log_date AS date,
        {{ tsql_datepart('YEAR', 'log_date') }} AS year,
        {{ tsql_datepart('MONTH', 'log_date') }} AS month,
        {{ tsql_month_name('log_date') }} AS month_name,
        {{ tsql_datepart('DAY', 'log_date') }} AS day_number,
        {{ tsql_day_name('log_date') }} AS day_name,
        {{ tsql_dow('log_date') }} AS day_of_week,
        {{ tsql_datepart('QUARTER', 'log_date') }} AS quarter,
        {{ tsql_datepart('WEEK', 'log_date') }} AS week_of_year,
        CASE WHEN {{ tsql_dow('log_date') }} IN (0, 6) THEN 'Yes' ELSE 'No' END AS is_weekend,
        CASE WHEN log_date IN (
            {% for date in var('uk_holidays') %}{{ tsql_cast("'" + date + "'", 'DATE') }}{{ ',' if not loop.last }}{% endfor %}
        ) THEN 'Yes' ELSE 'No' END AS holiday_flag
    FROM raw_dates
)
SELECT * FROM date_entries
```

**Example: dim_referrer domain extraction with T-SQL block**

```sql
referrer_domain:
{% if target.type == 'sqlserver' %}
    CASE
        WHEN rr.referrer_url = 'Direct' THEN 'Direct'
        WHEN rr.referrer_url LIKE 'http://%' OR rr.referrer_url LIKE 'https://%'
            THEN LOWER(
                SUBSTRING(rr.referrer_url,
                    CHARINDEX('//', rr.referrer_url) + 2,
                    CHARINDEX('/', rr.referrer_url + '/',
                        CHARINDEX('//', rr.referrer_url) + 2)
                    - CHARINDEX('//', rr.referrer_url) - 2
                )
            )
        ELSE 'Unknown'
    END
{% else %}
    CASE
        WHEN rr.referrer_url = 'Direct' THEN 'Direct'
        WHEN rr.referrer_url ~* '^https?://([^/]+)'
            THEN LOWER(REGEXP_REPLACE(rr.referrer_url, '^https?://([^/]+).*', '\1'))
        ELSE 'Unknown'
    END
{% endif %}
```

#### 7.4 Additional Dialect Fixes

| Feature | PostgreSQL | Azure SQL |
|---|---|---|
| String types | `TEXT` | `NVARCHAR(MAX)` (use NVARCHAR for Unicode support; VARCHAR for ASCII-only columns like IPs, status codes, methods) |
| Boolean | `BOOLEAN`, `TRUE`/`FALSE` | `BIT`, `1`/`0` (dbt-sqlserver handles this) |
| Index creation | `CREATE INDEX IF NOT EXISTS ...` | `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_...') CREATE INDEX ... ON ...` |
| Case-insensitive match | `ILIKE` or `~*` | `LIKE` with `COLLATE SQL_Latin1_General_CP1_CI_AS` |
| Schema | `public` | `dbo` |
| Current timestamp | `CURRENT_TIMESTAMP` | `CURRENT_TIMESTAMP` (same) |
| String concat | `\|\|` (if used) | `+` or `CONCAT()` (dbt-sqlserver handles) |

#### 7.5 Profile & Source Configuration

- [ ] **Configure Azure SQL target in profiles.yml [DONE — already in codebase]**
  The `w3c_azure` profile with `dev` and `azure_sql_ci` targets is already added to `airflow/dbt/profiles.yml`.

  For reference, the existing profile configuration:

  ```yaml
  w3c_azure:
    target: dev
    outputs:
      dev:
        type: sqlserver
        driver: "ODBC Driver 18 for SQL Server"
        server: "{{ env_var('AZURE_SQL_SERVER') }}.database.windows.net"
        port: 1433
        database: "{{ env_var('AZURE_SQL_DB') }}"
        schema: dbo
        user: "{{ env_var('AZURE_SQL_USER') }}"
        password: "{{ env_var('AZURE_SQL_PASSWORD') }}"
        encrypt: true
        trust_cert: false
        authentication: sql
        threads: 4
        retries: 3
      azure_sql_ci:
        type: sqlserver
        driver: "ODBC Driver 18 for SQL Server"
        server: "{{ env_var('AZURE_SQL_SERVER') }}.database.windows.net"
        port: 1433
        database: "{{ env_var('AZURE_SQL_DB') }}"
        schema: dbo
        user: "{{ env_var('AZURE_SQL_USER') }}"
        password: "{{ env_var('AZURE_SQL_PASSWORD') }}"
        encrypt: true
        trust_cert: false
        authentication: sql
        threads: 4
        retries: 3
  ```

  > **Why `trust_cert: false`?** Azure SQL Database enforces TLS — `trust_cert: false` validates the server's TLS certificate against a trusted CA. Setting `trust_cert: true` would skip certificate validation, which is both a security risk and unnecessary when Azure SQL's certificate chain is verifiable. The `encrypt: true` option ensures the connection is encrypted.
  >
  > **Why `azure_sql_ci` target?** This separate target is used by the CI workflow for `dbt compile --profile w3c_azure --target azure_sql_ci`. Even though CI can't actually connect to Azure SQL (no running database in CI), `dbt compile` only validates SQL compilation and doesn't open a database connection — so it succeeds without real credentials. The `dev` target is used for actual `dbt run`/`dbt test` against a live Azure SQL DB.
  >
  > **Why add to `airflow/dbt/profiles.yml` instead of a separate file?** The existing CI commands use `--profiles-dir airflow/dbt` and existing DAGs pass `--profiles-dir airflow/dbt`. Adding the Azure target to the same file avoids changing all existing profile-dir paths. The profile name is `w3c_azure` (so commands use `--profile w3c_azure --target dev`), keeping it distinct from the Postgres profile.

- [ ] **Update dbt_project.yml for env_var profile [DONE — already in codebase]**
  The `profile` field already uses `{{ env_var('DBT_PROFILE', 'w3c') }}` in `airflow/dbt/w3c/dbt_project.yml`.

  For reference, the change was:

  ```yaml
  # Before: profile: 'w3c'
  # After:
  profile: "{{ env_var('DBT_PROFILE', 'w3c') }}"
  ```

  > **Why?** The DAG passes `--profile w3c_azure` for Azure SQL runs, but `dbt_project.yml` has a hardcoded `profile: 'w3c'`. dbt uses the `--profile` CLI flag as an override, so this works without changes to DAG commands — but using `env_var` makes the default explicit and allows the CI environment to set `DBT_PROFILE=w3c_azure` as an alternative.
  >
  > **⚠️ Side effect for `dbt compile` in CI:** The CI workflow runs `dbt compile --profile w3c_azure --target azure_sql_ci` with the `--profile` flag, which overrides the `dbt_project.yml` setting. The `env_var` default (`'w3c'`) is only used when no `--profile` flag is passed. Since the CI ALWAYS passes `--profile w3c_azure`, the `env_var` change has zero effect on CI. The benefit is for local development: developers can set `export DBT_PROFILE=w3c_azure` to make their default profile Azure SQL without changing CLI commands.
  >
  > **Test impact:** Running `dbt compile` without `--profile` and without `DBT_PROFILE` set defaults to the `w3c` (PostgreSQL) profile as before. The change is backward-compatible.

- [ ] **Conditional sources.yml database/schema [DONE — already in codebase]**
  The `database` and `schema` fields in `airflow/dbt/w3c/models/sources.yml` already use `{% if target.type == 'sqlserver' %}...{% endif %}` conditionals.

  For reference, the updated configuration:

  ```yaml
  sources:
    - name: w3c
      database: "{% if target.type == 'sqlserver' %}{{ env_var('AZURE_SQL_DB', 'w3c_warehouse') }}{% else %}w3c_warehouse{% endif %}"
      schema: "{% if target.type == 'sqlserver' %}dbo{% else %}public{% endif %}"
      tables:
        - name: raw_enriched
  ```

  > **Why conditional `database`?** In the PostgreSQL path, the database is `w3c_warehouse`. In the Azure SQL path, the database is set via `AZURE_SQL_DB` environment variable. Without the conditional, `dbt compile --profile w3c_azure` would fail if `w3c_warehouse` doesn't exist in Azure SQL (the database name might differ). The `env_var` fallback `'w3c_warehouse'` ensures backward compatibility when the variable is unset.

#### 7.6 Dependency & Driver Installation

- [x] Add `dbt-sqlserver` adapter to `airflow/requirements.txt` (already done):

  ```
  dbt-sqlserver==1.8.4
  ```

  > **Version rationale:** Pinned to `1.8.4` for compatibility with `dbt-core==1.8.9`. Note that later `dbt-sqlserver` releases may require `dbt-core>=1.9` — check PyPI before upgrading dbt-core. Run `pip install --dry-run dbt-sqlserver==1.8.4` in the Docker image before committing to verify no conflicts with existing `protobuf>=5.27,<6` and `dbt-common==1.27.1` pins. If upgrading dbt-core to 1.9+, upgrade `dbt-sqlserver` in lockstep.

- [x] Install ODBC Driver 18 (already done in Phase 6 and CI):
  - Dockerfile addition in Phase 6 handles this
  - For local testing: <https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server>

- [x] **Add dbt Azure SQL compile step to CI (`.github/workflows/ci.yml`) — already done:**

  ```yaml
  - name: Install dbt-sqlserver
    run: pip install "dbt-sqlserver==1.8.4"
  - name: Install ODBC Driver 18 for SQL Server (required by dbt-sqlserver adapter)
    run: |
      sudo apt-get update && sudo apt-get install -y curl gnupg2
      curl -sSL https://packages.microsoft.com/keys/microsoft.asc | sudo gpg --dearmor -o /usr/share/keyrings/microsoft.gpg
      # Detect OS and use correct Microsoft repo
      if [ -f /etc/os-release ] && grep -qi ubuntu /etc/os-release; then
        echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/ubuntu/24.04/prod noble main" | sudo tee /etc/apt/sources.list.d/mssql-release.list
      else
        echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" | sudo tee /etc/apt/sources.list.d/mssql-release.list
      fi
      sudo apt-get update
      sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
  - name: dbt compile (Azure SQL/T-SQL validation)
    run: dbt compile --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt --profile w3c_azure --target azure_sql_ci
  ```

  > **Why `--target azure_sql_ci` instead of `--target dev`?** The `azure_sql_ci` target uses placeholder credentials (no real Azure SQL connection needed — `dbt compile` never connects to the database). Using a separate target avoids leaking CI placeholder values into the `dev` target, which is used with real credentials during local development.
  >
  > **Why OS detection for the ODBC repo?** GitHub Actions runners may use either Ubuntu 24.04 (`ubuntu-latest`) or Debian 12 (Docker-based environments). The Microsoft repo path differs between Debian and Ubuntu. The `if grep -qi ubuntu /etc/os-release` check selects the correct repo for each OS.
  >
  > **Why ODBC driver is needed in CI:** `dbt-sqlserver` imports `pyodbc` at module load time, which loads the native `libmsodbcsql-18.so` shared library. Even `dbt compile` (which doesn't connect to the database) triggers this import. Without the ODBC driver installed, the CI step crashes with `ImportError: dlopen: library not loaded`. If the driver cannot be installed in your CI environment, skip this step — it's a compile-time validation only, and the models will compile correctly on deploy targets where the driver IS installed (Docker, Databricks).

#### 7.7 Testing
>
> **Note:** All dbt commands use `--profile w3c_azure --target dev` instead of `--target azure` because `--target` selects a target *within* a profile, not a profile name. The profile is named `w3c_azure` and the target within it is `dev`.
>
> **Testing order:** Always run `dbt compile` first. This catches SQL dialect errors (invalid T-SQL syntax) without needing a running Azure SQL Database. See "compile-first" pattern below.

- [ ] **Stage 0 (CI — no DB needed):** `dbt compile --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt --profile w3c_azure --target azure_sql_ci` — **CI only.** Compiles the Azure SQL models; does NOT open a database connection. Passes without a live Azure SQL instance. This is run in CI on every PR.
- [ ] **Stage 1 (compile — no DB needed):** `dbt compile --profile w3c_azure --target dev --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt` — validates SQL compilation against the `sqlserver` adapter. Catches T-SQL syntax errors, unresolved `ref()` calls, and macro expansion failures. `dbt compile` does NOT connect to the database — it only compiles the SQL. **Run this FIRST** before any `dbt run`.
- [ ] **Stage 2 (incremental runs):** `dbt run --profile w3c_azure --target dev --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt --select dim_status dim_method` — test with simplest models first
- [ ] `dbt run --profile w3c_azure --target dev --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt --select dim_date dim_time` — test date/time dimension models
- [ ] `dbt run --profile w3c_azure --target dev --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt --select dim_page dim_referrer` — test regex-heavy models
- [ ] `dbt run --profile w3c_azure --target dev --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt` — full run all 16 models
- [ ] `dbt test --profile w3c_azure --target dev --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt` — all ~62 tests pass
- [ ] `dbt docs generate --profile w3c_azure --target dev --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt` — docs generation
- [ ] Cross-check: same row counts between PostgreSQL and Azure SQL for all models

> **Why compile-first?** `dbt compile` only validates SQL compilation — it does NOT open a database connection. This makes it safe to run in CI against the `sqlserver` target without needing a live Azure SQL instance. Use this pattern: CI runs `dbt compile` (Stage 0) with the `azure_sql_ci` target on every PR; local dev runs `dbt compile` first, then progressively `dbt run --select` for specific models, then full `dbt run`, then `dbt test`.

**Acceptance:** dbt compiles and runs successfully against Azure SQL Database with all 16 models (using `--profile w3c_azure --target dev`). All 32+ PostgreSQL `::` cast expressions converted to `CAST()` via the macro library (plus ~57 more for EXTRACT→DATEPART, TO_CHAR→FORMAT/DATENAME, SPLIT_PART→CHARINDEX/SUBSTRING, generate_series→CTE/sys.all_columns, REGEXP_REPLACE→manual string ops, ~*→LIKE+COLLATE, CREATE INDEX IF NOT EXISTS→existence check). All ~62 dbt tests pass on Azure target.

---

### Phase 8 — Cost Management & Teardown (30 min)

**Goal:** Protect the $200 credits with budget alerts, and document teardown procedure.

- [ ] Set up Azure budget alerts:
  - Azure Portal → Cost Management → Budgets
  - Create alert at $50 (notification) and $100 (hard warning)
  - Email alert to self

- [ ] Configure Databricks cost controls:
  - Cluster auto-termination: 10 min idle
  - Job clusters only (no all-purpose clusters left running)
  - DLT pipeline: `TRIGGERED` mode (not continuous)
  - Budget policy: max 8 DBUs per run
  - Set `databricks_job` schedule to PAUSED initially (Airflow triggers it)

- [ ] Test full pipeline end-to-end:
  - Upload W3C sample data to ADLS Gen2 (`raw-logs` container)
  - Upload GeoLite2 `.mmdb` files to DBFS
  - Run the Databricks Workflow manually (Bronze → Silver → JDBC)
  - Run `dbt run --profile w3c_azure --target dev --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt`
  - Verify data integrity (compare row counts between Docker PostgreSQL path and Azure SQL)
  - Check Azure Portal cost dashboard: verify < $10 for test run

- [ ] Document teardown in `terraform/README.md`:
  > **⚠️ CRITICAL: Terraform destroy order** — Part B (pipeline code) resources depend on Part A (infrastructure) resources. Since they are separate root directories with separate state files, destroy Part B FIRST, then Part A. Follow this exact order:
  > 1. **Stop the DAG in Airflow** — Pause the `w3c_spark_ingestion_azure` DAG to prevent new pipeline runs
  > 2. **Destroy Part B** (Databricks pipeline assets): `cd terraform/part_b && terraform destroy`
  > 3. **Verify**: Check all Databricks jobs, pipelines, and secret scopes are removed
  > 4. **Destroy Part A** (core infrastructure): `cd terraform/part_a && terraform destroy`
  
  ```bash
  # ── Order: stop DAG → destroy Part B → destroy Part A ──
  
  # Step 1: Pause the Airflow DAG (prevents new pipeline triggers)
  airflow dags pause w3c_spark_ingestion_azure
  
  # Step 2: Destroy Databricks pipeline assets first (jobs, pipelines)
  cd terraform/part_b && terraform destroy -auto-approve
  
  # Step 3: Destroy core infrastructure (resource group, storage, SQL, workspace)
  cd ../part_a && terraform destroy -auto-approve
  
  # Manual verification:
  # 1. Azure Portal → Resource Groups — verify w3c-etl-dev is deleted
  # 2. Databricks workspace billing — check no active clusters
  # 3. Cancel any remaining Databricks workspace if charges persist
  ```

- [ ] Capture cost summary for resume context:
  - "Provisioned and ran full pipeline for under $10 in Azure credits"

**Acceptance:** Full end-to-end pipeline runs successfully on Azure. Cost dashboard shows < $10 consumed. Teardown procedure documented.

---

### Phase 9 — Documentation & Resume (1 hour)

- [ ] **`README.md` updates:**
  - Add `## Azure Cloud Deployment` section with:
    - Architecture diagram (Azure resources + DLT pipeline)
    - Prerequisites (Azure subscription, CLI, Terraform, MaxMind GeoLite2 account)
    - Quick start: `cd terraform && terraform init && terraform apply`
    - Cost estimate: "~$5-10 per full pipeline run on small datasets"
  - Add badges: Azure, Terraform, Databricks (next to existing badges)
  - Update TL;DR Architecture bullet to mention hybrid Docker/Azure deployment
  - Update Container Topology diagram note: "Docker stack for local/CI; Azure stack for production"
  - Add Databricks Workflows + DLT to Technology Badge Strip
  - Document the two-DAG setup: `w3c_spark_ingestion` (Docker), `w3c_spark_ingestion_azure` (Databricks)
  - Add MaxMind GeoLite2 setup instructions for Databricks path

- [ ] **`/plans/improvements.md` updates:**
  - Replace Priority 3a entry with this plan
  - Update Cloud platforms rating: D+ → A- (Azure + Terraform + Databricks)
  - Add Databricks DLT row to Skills Coverage Matrix
  - Add Terraform row to Skills Coverage Matrix
  - Add Azure row to Skills Coverage Matrix
  - Add Data Quality (DLT expectations) enhancement note
  - Mark Gap 3 (Cloud-Native) as RESOLVED

**Acceptance:** README renders with Azure badges, architecture diagram, and deployment section. improvements.md reflects new ratings. Changelog updated.

---

### Phase 10 — Final Verification (30 min)

- [ ] `ruff check --fix .` and `ruff format .`
- [ ] `mypy --ignore-missing-imports tests/`
- [ ] `pytest -m "not integration and not dag_integrity"` (Docker path unaffected)
- [ ] `pytest -m dag_integrity` (verifies all DAGs including the new `w3c_spark_ingestion_azure` DAG — requires Airflow to be installed)
- [ ] `dbt deps --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt` (install dependencies for both profiles)
- [ ] `dbt compile --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt` (PostgreSQL profile — validate no regressions)
- [ ] `dbt compile --profile w3c_azure --target dev --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt` (Azure SQL profile — validate T-SQL syntax)
- [ ] `terraform validate` in `terraform/part_a/` and `terraform/part_b/`
- [ ] `terraform plan` shows no drift (if infra still deployed)
- [ ] Manual: verify Docker path still works (`pytest -m integration` if stack up)
- [ ] Review Azure cost dashboard: confirm within budget
- [ ] **CI check (add to `.github/workflows/ci.yml`):**

  ```yaml
  terraform-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: 1.10.5   # must satisfy >= 1.10.5, < 2.0 from versions.tf
      - run: terraform init -backend=false && terraform validate
        working-directory: terraform
      - run: |
          # Check if terraform/part_b actually exists (not affected by working-directory)
          if [ -d "$GITHUB_WORKSPACE/terraform/part_b" ]; then
            terraform init -backend=false && terraform validate
          else
            echo "Part B not yet created — skipping validation"
          fi
        working-directory: terraform/part_b
      - run: terraform fmt --check
        working-directory: terraform
  ```

**Acceptance:** All quality gates green. Both Docker and Azure paths work. Cost within $200 budget. CI validates Terraform config on every PR.

---

## 5. Out of Scope (Future Work)

- **Priority 3 (full AWS/GCP):** This plan covers Azure. AWS/GCP would follow the same pattern with different Terraform providers.
- **Streaming (Gap 8):** Auto Loader in `TRIGGERED` mode is batch. Continuous mode / structured streaming is future work.
- **Private networking:** VNet with private endpoints adds complexity and cost. Not needed for MVP.
- **Databricks SQL / BI dashboards:** The Silver table is ready for consumption, but building Power BI/Tableau dashboards is out of scope.
- **CI for Terraform:** `terraform plan` in CI with real Azure creds is a natural next step but not included here (Phase 10 adds basic validate/fmt checks).
- **Secret management:** MVP uses Terraform outputs + `.env` files + Databricks secrets. Azure Key Vault integration is future work.
- **DLT unit tests:** DLT doesn't easily support local unit testing. Consider `dlt.assert` or test datasets as future improvement.
- **Multi-region / high availability:** Single-region dev deployment only.
- **Continuous DLT mode:** All DLT pipelines use `TRIGGERED` mode. Continuous mode would enable real-time ingestion.

---

## 6. Risk Register

| Risk | Probability | Impact | Mitigation |
|---|---|---|---|
| Azure credits exhausted mid-development | Medium | Cannot complete deployment | Set $50 budget alert in Phase 1. DLT runs cost ~$1-3 each. Limit to 10 runs total. |
| W3C log format incompatible with Auto Loader CSV parser | **High** | Bronze DLT pipeline fails | **SOLUTION:** Use Binary File format + custom parser UDF (already the plan — see Phase 3). Proven approach from existing `01_bronze_ingestion.py`. |
| MaxMind GeoLite2 DBs not available at runtime | Medium | Geo columns = Unknown | Upload `.mmdb` files to DBFS/UC Volume in Phase 3. Document download step. Pipeline completes with "Unknown" as graceful degradation (same as Docker path). **Mitigation:** Download .mmdb files offline before Phase 3 implementation begins. Store in a known location. |
| Azure SQL + dbt dialect differences block model migration | **High** | ~80+ PostgreSQL expressions across 16 models fail | **SOLUTION:** Full migration table covering ALL PostgreSQL-only features: `::`→CAST, `~*`→LIKE+COLLATE, EXTRACT→DATEPART, TO_CHAR→FORMAT/DATENAME, SPLIT_PART→CHARINDEX/SUBSTRING, generate_series→CTE, REGEXP_REPLACE→manual, CREATE INDEX→existence check. Test with `dbt compile --profile w3c_azure --target dev` early. |
| Databricks Workflows Terraform resource incomplete | Low | Cannot IaC the workflow | Fallback: define workflow in UI + export JSON. Document the manual step in Phase 6. |
| `terraform destroy` doesn't clean up all resources | Low | Unexpected residual charges | Document manual clean-up steps in teardown doc. Databricks workspaces cost nothing when idle (compute is the cost). |
| Airflow `DatabricksRunNowOperator` API mismatch | Low | Cannot trigger Workflows from Airflow | Provider `apache-airflow-providers-databricks==4.6.0` tested import in Phase 6. Fallback: trigger via REST API + `SimpleHttpOperator`. |
| `export_dimensions.py` reads local Delta path (broken in Azure) | **High** | Dimension tables not built | **SOLUTION:** Create `export_dimensions_azure.py` reading from Azure SQL. Documented in Phase 6. |
| Python 3.12 features in DLT pipeline code | Medium | DBR runtime errors | Document DBR 15.4 LTS uses Python 3.11. DLT code must avoid 3.12-only features. (Phase 3-5 code uses compatible patterns.) |

---

## 7. Acceptance Criteria (Definition of Done)

The plan is "done" when **all** of the following are true:

1. `terraform validate` and `terraform plan` succeed for the dev environment (provider versions pinned)
2. `terraform apply` provisions all Azure resources (ADLS, Databricks Premium, Azure SQL)
3. DLT Bronze pipeline ingests W3C logs from ADLS Gen2 via Auto Loader (Binary File) + custom parser UDF, correctly handling unquoted UA strings
4. DLT Silver pipeline enriches data with 7 GeoIP UDFs (MaxMind GeoLite2) + 5 computed field UDFs; 6 geo columns (postcode is in the 25-core computed section) preserved for dim_geolocation
5. *(DEPRECATED — Gold table eliminated as it added no transformation value over Silver)*
6. JDBC export populates Azure SQL Database directly from Silver (no Gold table) with idempotent tracking table (re-run produces zero duplicates)
7. Databricks Workflow orchestrates the full pipeline (Bronze → Silver → JDBC) — 3 tasks, not 4
8. Airflow has a separate DAG (`spark_ingestion_azure.py`) that triggers the Databricks Workflow
9. `export_dimensions_azure.py` reads from Azure SQL (not local Delta) and fires the Dataset outlet
10. dbt compiles and runs successfully against Azure SQL Database target; all ~80+ PostgreSQL-only expressions converted (::→CAST, ~*→LIKE+COLLATE, EXTRACT→DATEPART, TO_CHAR→FORMAT/DATENAME, SPLIT_PART→CHARINDEX/SUBSTRING, generate_series→CTE/sys.all_columns, REGEXP_REPLACE→manual string ops, CREATE INDEX IF NOT EXISTS→existence check)
11. Docker path still works: `pytest -m "not integration and not dag_integrity"` passes
12. Azure cost dashboard shows total spend < $50 for the development cycle
13. Teardown documented; `terraform destroy` successfully removes all resources
14. README has Azure + Terraform + Databricks badges and deployment section
15. `improvements.md` reflects Cloud platforms rating A-, adds DLT/Terraform/Azure rows
16. CI includes `terraform validate` and `terraform fmt --check` steps

---

## 8. Implementation Order

```
Phase 1 (Terraform IaC) ─────────────────────────────────────────┐
                                                                   ▼
Phase 2 (Deploy Azure Infra) ──────────────────────────────────► Phase 8 (Cost Mgmt)
                                                                   │
Phase 3 (DLT Bronze + Auto Loader) ──┐                            │
                                       ├──► Phase 6 (Workflows) ──┤
Phase 4 (DLT Silver) ────────────────┤                            │
                                       │                            │
Phase 5 (JDBC Export from Silver) ───┘                            │
                                                                    │
Phase 7a (dbt: macros + simple models) ─┐                         │
                                          ├──► Phase 7b (dbt: regex models + test) ───┤
                                                                    │
Phase 9 (Docs) ───────────────────────────────────────────────────┤
                                                                    │
Phase 10 (Verify) ◄───────────────────────────────────────────────┘
```

**Critical: Terraform Part A/B split:**

- **Part A (Phase 1–2):** Core infrastructure — resource group, storage, Databricks workspace, Unity Catalog, Azure SQL Database. Does NOT include `databricks_pipeline` or `databricks_job` resources (those depend on DLT source code existing).
- **Part B (Phase 6):** Code-dependent resources — `databricks_pipeline` (DLT), `databricks_job` (Workflows). Applied AFTER Phases 3–5 write the DLT code to the Databricks workspace.
>
> **Why this split?** The `databricks_pipeline` Terraform resource requires `library { file { path } }` pointing to source code that must already exist on Databricks. If the DLT Python files don't exist yet, `terraform apply` fails. Part A creates the workspace; Part B creates pipelines referencing code uploaded to that workspace.

**Parallel notes:**

- Phase 1 must complete before Phase 2
- Phases 3–5 are sequential (each builds on the DLT pipeline)
- Phase 6 depends on Phases 3–5 completing (needs pipeline IDs + uploaded code for Workflow definition)
- Phase 7a depends on Phase 5 (JDBC export populates the warehouse); Phase 7b depends on Phase 7a (macros + simple models first)
- Phase 8 runs after Phase 2 (set up budget alerts before heavy usage)
- Phase 9 depends on Phases 3–7 (docs based on working pipeline)
- Phase 10 is the final gate
- **Phase 6 → 7 handoff:** The dbt DAG profile switching mechanism (see Phase 6 handoff) must be implemented before Phase 7 testing, otherwise `dbt run --profile w3c_azure` cannot find dimension tables on Azure SQL.

---

## 9. Resume Lines

### Primary (Data Engineering - Azure/Databricks)
>
> *"Built a cloud-native data platform on Azure using Terraform, provisioning ADLS Gen2 storage, Databricks with Unity Catalog, and Azure SQL Database. Designed a Delta Live Tables medallion pipeline with Auto Loader ingestion and Databricks Workflows orchestration, replacing a Docker-based Spark pipeline — reducing infrastructure overhead and adding declarative data quality."*

### Alternative (IaC + DevOps angle)
>
> *"Provisioned a complete Azure data platform infrastructure using Terraform, including ADLS Gen2, Databricks Premium workspace with Unity Catalog, and serverless Azure SQL Database — with cost-aware cluster management keeping full-pipeline runs under $10."*

### Alternative (Full-stack DE)
>
> *"Extended an existing Airflow + dbt + Docker ETL pipeline with a cloud-native Azure deployment path, adding Databricks Delta Live Tables, Auto Loader cloud ingestion, and Databricks Workflows orchestration — supporting hybrid local/cloud execution without disrupting the downstream dbt analytics contract."*

---

## 10. References

- **Existing pipeline:** `airflow/dags/w3c/spark_ingestion.py`, `airflow/dags/w3c/dbt_marts.py`
- **New DAG:** `airflow/dags/w3c/spark_ingestion_azure.py` (Phase 6)
- **Existing Databricks scaffolding:** `airflow/spark/databricks/01_bronze_ingestion.py` → `03_export_warehouse.py` (reference for W3C parsing, replaced by DLT)
- **Docker PySpark jobs:** `airflow/spark/jobs/{bronze,silver,export}_*.py`
- **MaxMind GeoLite2:** `airflow/spark/jobs/utils/geoip.py` (7 UDFs), signup at <https://www.maxmind.com/en/geolite2/signup>
- **W3C parser:** `airflow/spark/jobs/utils/w3c_parser.py`, `airflow/spark/databricks/01_bronze_ingestion.py::parse_log_line()`
- **export_dimensions (Docker):** `airflow/plugins/operators/export_dimensions.py`
- **export_dimensions_azure (new):** `airflow/plugins/operators/export_dimensions_azure.py` (Phase 6)
- **Dead columns cleanup:** `plans/fact-webrequest-dead-col-cleanup.md`
- **Architecture:** `.agents/architecture-v3.md`
- **Improvements:** `.agents/improvements.md` (will be updated in Phase 9)
- **Terraform Azure provider:** <https://registry.terraform.io/providers/hashicorp/azurerm/latest>
- **Databricks Terraform provider:** <https://registry.terraform.io/providers/databricks/databricks/latest>
- **Delta Live Tables docs:** <https://docs.databricks.com/delta-live-tables/>
- **Auto Loader docs (Binary File):** <https://docs.databricks.com/en/ingestion/auto-loader/file-formats.html#binary-file-format>
- **Azure SQL + dbt:** <https://docs.getdbt.com/docs/core/connect-data-platform/sqlserver-setup>
- **Apache Airflow Databricks provider:** <https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/>
- **Azure free credits:** <https://azure.microsoft.com/en-gb/free/>
