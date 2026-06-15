# Key Metrics Reference

> **Purpose:** Single source of truth for the **Key Metrics at a Glance** section of the README. Consolidates every metric from both pipelines (Docker dev + Azure prod) into dual-column, recruiter-friendly tables.

---

## Section 1: Pipeline Scale Metrics

| Category | Metric | Docker Pipeline | Azure Pipeline |
|---|---|---|---|
| **Source** | Log files | 93 W3C IIS `.log` | 93 W3C IIS `.log` |
| **Source** | Timespan | 2009–2011 | 2009–2011 |
| **Bronze** | Raw requests ingested | 155,570 rows, 19 cols | 153,380 rows, 19 cols |
| **Bronze** | Rows dropped by quality expectations | N/A (no expectations) | **0** (7 expectations all pass) |
| **Bronze** | Storage format | Delta Lake (local `/opt/spark/delta/bronze`) | Unity Catalog `w3c_etl_databricks.bronze.bronze_raw_logs` |
| **Bronze** | Partitioning | `log_date` | `log_date` |
| **Bronze** | Ingestion engine | PySpark 4.0.2 `SparkSubmitOperator` | Serverless DLT Auto Loader (`binaryFile`) |
| **Silver** | Enriched rows | 155,570 rows, 35 cols | 153,377 rows, **31** cols |
| **Silver** | Bronze → Silver preservation | **100%** (155,570 → 155,570) | **99.998%** (153,380 → 153,377 — 3 dropped by `valid_country` expectation) |
| **Silver** | Enrichment UDFs | **17** (7 GeoIP + 5 UA + 5 computed) | **12** (7 GeoIP + 0 UA + 5 computed) |
| **Silver** | UA columns in Silver | ✅ agent_type, browser, os, device_type — 5 columns | ❌ UA columns excluded from Silver DDL (parsed later in `export_dimensions`) |
| **Silver** | Geo columns in Silver | 6 (country, region, city, lat, lon, postcode, isp) | 6 (country, region, city, lat, lon, isp — postcode computed) |
| **Silver** | Dedup method | ROW_NUMBER on full dataset | `left_anti` join on `source_file` |
| **Warehouse** | After dedup (fact) | 155,357 (**213 dropped** — 202 byte-identical + 11 byte-similar) | 153,377 (**3 dropped** by GeoIP `valid_country`, not by dedup) |
| **Warehouse** | Dedup key | 12-component MD5 (`source_file` + log_time + client_ip + user_agent + referrer + uri_stem + uri_query + method + status + sub_status + win32_status + time_taken) | `left_anti` on `source_file` (Silver) + `HASHBYTES` FK restoration (dbt) |
| **GeoIP** | Countries resolved | **~78** | **30+** |
| **GeoIP** | Known-country coverage | **99.99%** (155,337 / 155,357) | **~99.99%** (153,374 / 153,377) |
| **GeoIP** | Known-ISP coverage | **100%** | **100%** |
| **GeoIP** | Known-postcode coverage | **~57%** | Not tracked |
| **GeoIP** | Database | MaxMind GeoLite2-City + GeoLite2-ASN (local `.mmdb`) | MaxMind GeoLite2-City + GeoLite2-ASN (Unity Catalog volume) |
| **GeoIP** | Library | `geoip2==5.0.1` + `maxminddb==2.6.3` | `maxminddb==2.8.*` (pure Python — serverless DLT has no compiled C deps) |

**Why row counts differ between pipelines:** The Docker PySpark pipeline processes all 93 files and deduplicates across the full dataset (ROW_NUMBER), dropping 213 byte-identical/similar rows. The Azure DLT Silver pipeline processes the same 93 files but uses a `left_anti` join on `source_file` — it never sees duplicate source_file values from Auto Loader. The 3 Azure rows "dropped" are by the `valid_country` GeoIP quality expectation (GeoIP lookup returned NULL for private/reserved IPs), not by dedup logic. The 2,190 fewer total Azure rows (155,570 vs 153,380) reflect differences in how the two parsers handle edge-case log lines.

---

## Section 2: Dimension & Star Schema Metrics

| Dim / Table | Docker (rows) | Azure (rows) | Notes |
|---|---|---|---|
| `dim_geolocation` | **4,011** | **1,585** | Azure has fewer distinct IPs in Silver (different IP resolution + GeoIP difference). Both include a `-1` sentinel row. |
| `dim_useragent` | **2,276** | **~2,040** | Azure parses 2,040 unique UA strings (vs 2,276 in Docker — slight dataset difference). Phase 7 reported 219 after hash-based dedup on parsed UA fields (aggregates similar UAs). |
| `fact_webrequest` | 155,357 | 153,377 | Post-dedup fact table row count |
| `dim_date` | 93 | 93 | Static historical date range |
| `dim_time` | 1,440 | 1,440 | Full day in minutes |
| `dim_page` | 14,091 | ~14K | Unique page paths |
| `dim_status` | 1,145 | ~1,145 | HTTP status code triples |
| `dim_referrer` | 2,341 | ~2,341 | Unique referrer URLs |
| `dim_method` | 4 | 4 | GET, POST, HEAD, etc. |
| `dim_visitortype` | 3 | 3 | Human / Crawler / Unknown |
| `dim_visit_buckets` | 6 | 6 | Visit frequency buckets |
| `crawler_ips` | varies | varies | IPs requesting `robots.txt` |
| dbt staging models | **10** | **10** | Same models, dual-dialect |
| dbt mart models | **5** | **6** | Azure includes `mart_country_browser_share` |
| CSV exports | **17** | **18** | Azure includes `mart_country_browser_share` |
| Total warehouse tables | 12 (public) + 15 (dbt) = **17** | 3 (dbo) + 16 (dbt) = **19** | |

---

## Section 3: Testing Metrics

| Metric | Old (Docker-only) | New (Both Pipelines) |
|---|---|---|
| **pytest tests (total)** | **162** (6 files, 38 classes) | **305** (17 files, 81+ classes) |
| Core unit tests (unmarked) | 133 | **183** |
| Integration tests (`@integration`) | 17 | **18** |
| DAG integrity (`@dag_integrity`) | 12 | **5** |
| dbt compile tests (`@dbt_compile`) | 0 | **12** |
| Terraform tests (`@terraform`) | 0 | **12** (Part A: 5 @parametrize + Part B: 4 @parametrize) |
| **Test file count** | 6 | **17** |
| DLT Bronze tests | — | **60** (11 test classes) — `test_dlt_bronze.py` |
| DLT Silver tests | — | **91** (15 test classes) — `test_dlt_silver.py` |
| Azure export operators | — | **35** — `test_export_azure_operators.py` |
| JDBC Azure export | — | **31** — `test_jdbc_export_azure.py` |
| Azure dimension export | — | **29** — `test_export_dimensions_azure.py` |
| dbt T-SQL migration tests | — | **40** (28 unmarked config + 12 @dbt_compile) — `test_dbt_tsql_migration.py` |
| Terraform Part A tests | — | **14** (5 @parametrize) — `test_terraform_part_a.py` |
| Terraform Part B tests | — | **25** (4 @parametrize) — `test_terraform_part_b.py` |
| DAG integrity | 12 (1 file) | **23** (5 @dag_integrity + 18 unmarked) — `test_dag_integrity.py` |
| Integration tests | 17 (1 file) | **18** — `test_integration.py` |
| **dbt data tests (PostgreSQL)** | **64** | **64** (unchanged, still CI-compiled) |
| **dbt data tests (Azure SQL)** | — | **118** |
| ¬ `not_null` | 29 | 46 |
| ¬ `unique` | 14 | 18 |
| ¬ `accepted_values` | 0 (sources.yml only) | **20** |
| ¬ `relationships` (FK) | 7 | 10 |
| ¬ `dbt_utils.expression_is_true` | 6 | 24 |
| ¬ Singular tests (`.sql`) | 4 | 4 |
| ¬ Source tests (`sources.yml`) | 4 | 4 |
| **dbt_utils tests** | 6 | 24 |
| **Terraform HCL assertions** | — | **9** (3 Part A plan + 6 Part B resource existence) |
| **Pre-commit hooks** | **8** | **15** |
| **Linting tools** | **3** (ruff lint, ruff format, mypy) | **5** (+ bandit, SQLFluff) |
| **Security tools** | — | **3** (bandit, CodeQL, GitGuardian) |
| CI test run (PR) | 133 | **275** (excludes 18 integration + 12 dbt_compile) |

**Key testing architecture change:** The old `test_dag_integrity.py` had 12 tests; the new one has 23. Old had no DLT, Terraform, Azure export, or T-SQL migration tests. New suite adds all of these, bringing the total from 162 → 305.

---

## Section 4: dbt Model & Macro Metrics

| Metric | Docker (PostgreSQL) | Azure (Azure SQL) |
|---|---|---|
| **dbt version** | 1.8.9 | 1.8.9 (PostgreSQL), 1.10.8 (Azure SQL adapter) |
| **dbt adapter** | `dbt-postgres==1.8.2` | `dbt-sqlserver==1.10.0rc1` |
| **Total models** | **15** | **16** |
| Staging models | 10 | 10 |
| Mart models | **5** | **6** (+ `mart_country_browser_share`) |
| **T-SQL macros** | **0** | **17** (`t_sql_compat.sql`) |
| ¬ Cast macros | — | `tsql_cast`, `tsql_boolean_to_int` |
| ¬ Date macros | — | `tsql_datepart`, `tsql_month_name`, `tsql_day_name`, `tsql_dow`, `tsql_format_date` |
| ¬ String macros | — | `tsql_split_part`, `tsql_regexp_replace`, `tsql_case_insensitive_like`, `tsql_extract_domain` |
| ¬ Set macros | — | `tsql_generate_series` |
| ¬ Aggregate macros | — | `tsql_percentile_cont` |
| ¬ Hash macros | — | `tsql_hash_md5` |
| ¬ Bool literal macros | — | `tsql_bool_literal`, `tsql_true_val`, `tsql_false_val` |
| ¬ Other macros | — | `tsql_create_index_if_not_exists` |
| **Data tests** | 64 | **118** |
| **Packages** | `dbt_utils 1.1.1` | `dbt_utils 1.1.1` |
| **Database dialect** | PostgreSQL | T-SQL / Azure SQL + PostgreSQL (dual-profile) |
| **CSV exports** | **17** (~36 MB) | **18** (~36 MB) |
| **Profile name** | `w3c` | `w3c`, `w3c_azure` |
| **SQL dialect pattern** | Native PostgreSQL | Inline `{% if target.type == 'sqlserver' %}...{% else %}...{% endif %}` — no separate `_azure.sql` files |

---

## Section 5: CI/CD Metrics

| Metric | Old (README) | New (Reality) |
|---|---|---|
| **CI jobs per push** | 3 parallel | **4** parallel |
| **CD jobs on merge** | (none) | **7** sequential |
| CI job types | lint, test, dbt-compile | + terraform validate (Part A + B matrix) |
| CD pipeline stages | — | `terraform-plan` → `terraform-apply` → `deploy-dab` → `deploy-dbt` → `sync-airflow` → `smoke-test` → `rollback` |
| Reusable workflows | 0 | **3** (`_reusable-lint`, `_reusable-test`, `_reusable-terraform`) |
| Total workflow files | 2 (ci, dependabot) | **7** (ci, cd, codeql, 3 reusable, dependabot-auto-merge) |
| OIDC federation | ❌ | ✅ (Terraform-managed `github_oidc.tf` — 1 federated identity credential for `azure-dev`) |
| Dependabot ecosystems | 0 | **5** (pip, GitHub Actions, Terraform, Docker, Python) |
| Security tools | 0 | **3** (bandit, CodeQL weekly, GitGuardian on every push) |
| Post-deploy smoke test | ❌ | ✅ (trigger DAG via REST API → poll completion → assert Azure SQL row count) |
| Pre-commit hooks | 8 | **15** |
| Lint job tools | ruff, mypy | ruff, mypy, bandit, SQLFluff |
| Test coverage upload | ❌ | ✅ (Codecov — fail_ci_if_error: false) |
| dbt compile profiles | 1 (PostgreSQL) | **2** (PostgreSQL `w3c` + Azure SQL `w3c_azure`) |
| Deployment environments | — | 1 (`azure-dev` — auto on merge to main) |
| CD auth mechanism | — | OIDC Workload Identity Federation (no secrets) |
| Dependabot auto-merge | — | ✅ (patch auto-approve and merge) |

---

## Section 6: Infrastructure Metrics

| Metric | Docker Dev | Azure Prod |
|---|---|---|
| **Compute** | Spark 2 cores / 4 GB RAM | Databricks Serverless DLT (auto-scales to zero) |
| **Containers** | **17** services (Airflow × 7, Spark × 2, geoip-downloader init × 1, Postgres, Redis, Prometheus, Alertmanager, Grafana, cAdvisor, statsd-exporter) | N/A (serverless — Airflow orchestration only runs on Docker) |
| **Named volumes** | 5 (postgres-db, prometheus-data, grafana-data, alertmanager-data, geoip-data) | N/A |
| **Observability services** | 6 (Prometheus, Alertmanager, Grafana, cAdvisor, statsd-exporter, data-freshness-probe) | 4-layer (Grafana + Prometheus + Azure Monitor + Data Freshness Probe) |
| **Terraform modules** | 0 | **4** (networking, datalake, databricks, warehouse) |
| **Terraform parts** | 0 | **2** (Part A — core Azure infra; Part B — Databricks DLT + Workflows + UC) |
| **Azure resources managed** | 0 | **8+** (RG, VNet, NSG, ADLS Gen2, Databricks workspace, Azure SQL, action groups, budget alerts) |
| **Storage** | Local Delta dirs (`/opt/spark/delta/`) | ADLS Gen2 (4 containers: raw-logs, bronze, silver, gold) + Unity Catalog |
| **Cost controls** | None | $50 warning alert, $100 hard cap budget |
| **Monitoring dashboards** | **2** (13 panels) — Airflow ETL Overview (7) + Container System Metrics (6) | **3** (23 panels) — Airflow ETL Overview (7) + Container System Metrics (6) + Pipeline Health (**10**) |
| **Prometheus alert rules** | **6** (airflow × 2, containers × 3, prometheus × 1) | **8** (+ 2 data staleness: DataStaleWarning >6h P2, DataStaleCritical >24h P1) |
| **Azure Monitor alerts** | — | **2 metric + 3 action groups + 2 budgets** |
| **Prometheus retention** | 90 days | 90 days |
| **Scrape / eval interval** | 15 s / 30 s | 15 s / 30 s |
| **Alert routing** | Slack `#w3c-etl-alerts` | Slack + email (P1 critical, P2 warning, P3 info) |
| **Data freshness probe** | ❌ | ✅ (port 8000, 4 Prometheus gauges: Azure SQL 153,377 + Bronze 153,380 + Silver 153,377 + pipeline status) |

---

## Section 7: Performance Metrics

| Stage | Docker (first run) | Docker (subsequent) | Azure (serverless DLT) |
|---|---|---|---|
| **Bronze ingestion** | ~90 s | ~15 s | ~3 min (DLT pipeline — full Auto Loader scan) |
| **Silver enrichment** | ~75 s | ~75 s | ~3 min (DLT pipeline — GeoIP + computed UDFs) |
| **JDBC / Export warehouse** | ~40 s | ~5 s | ~45 s (pymssql `executemany` with BATCH_SIZE=5000, 4-attempt retry for DB cold-start) |
| **Dimension export** | ~30 s | ~10 s | (part of Airflow DAG — `_export_dimensions` PythonOperator) |
| **dbt deps** | ~3 s | ~3 s | ~3 s |
| **dbt run** (10 staging + 5/6 marts) | ~21 s | ~21 s | ~similar (on Databricks serverless via self-bootstrapping notebook) |
| **dbt test** | ~5 s | ~5 s | ~5 s (118 tests on Azure SQL) |
| **dbt docs generate** | ~5 s | ~5 s | ~5 s |
| **CSV export** | ~5 s | ~5 s | ~5 s (18 files) |
| **Total Spark/Workflow** | ~3–4 min | ~1.5–2 min | ~6–7 min (single Workflow: Bronze → Silver → JDBC) |
| **Total E2E (including dbt)** | ~4–5 min | ~2–3 min | ~7–8 min |
| **Pipeline schedule** | Saturday 06:00 UTC | N/A | Friday 17:00 UTC (Airflow) + 2:00 AM UTC (Workflow direct) |

**Azure sizing:** Azure SQL Serverless GP_S_Gen5 (1 vCore, auto-pause 60 min). Databricks Serverless DLT (no VM provisioning). Azure SQL cold-start adds 15–60 s to first export after idle period.

---

## Section 8: Quick Stats Summary Block

| Metric | Value |
|---|---|
| Total pytest tests | **305** (183 core + 12 dbt_compile + 12 terraform + 5 dag_integrity + 18 integration) |
| Total pytest tests in CI | **275** (excludes 18 integration + 12 dbt_compile — Docker-dependent + environment-gated) |
| Total dbt data tests | **118** (46 `not_null`, 18 `unique`, 20 `accepted_values`, 10 `relationships`, 24 `expression_is_true`) |
| CI/CD workflow files | **7** (4 reusable + 3 top-level) |
| CI/CD job stages | **16** (9 CI + 7 CD) |
| Pre-commit hooks | **15** |
| Linting tools | **5** (ruff, mypy, bandit, SQLFluff, terraform fmt) |
| Security tools | **3** (bandit, CodeQL weekly, GitGuardian per push) |
| dbt models | **16** (10 staging + 6 marts) |
| T-SQL macros | **17** |
| Terraform HCL assertions | **9** (3 Part A plan + 6 Part B resource existence) |
| Terraform Python tests | **39** (14 Part A + 25 Part B) |
| Infrastructure modules | **4** (networking, datalake, databricks, warehouse) |
| Grafana dashboards | **3** (23 panels: 7 + 6 + 10) |
| Prometheus alert rules | **8** (2 airflow, 3 containers, 1 prometheus, 2 data freshness) |
| Azure Monitor alerts | **2** metric alerts + **3** action groups (P1/P2/P3) + **2** budget alerts ($50 warning, $100 hard cap) |
| Terraform resources (Part A) | **8+** (RG, VNet, NSG, ADLS, Databricks workspace, Azure SQL, action groups, budgets) |
| Terraform resources (Part B) | **24** (DLT pipelines, Workflows, Unity Catalog schemas, OIDC, secrets) |
| Docker services | **17** |
| W3C log files | **93** (2009–2011) |
| Bronze → Silver preservation | 100% (Docker) / 99.998% (Azure — 3 rows dropped by GeoIP quality) |
| Distinct countries | 78 (Docker) / 30+ (Azure) |
| GeoIP known-country | 99.99% |

---

## Section 9: Key Differences Between Pipelines

| Aspect | Docker Dev | Azure Prod | Why It Matters |
|---|---|---|---|
| **Execution engine** | PySpark 4.0.2 (Docker Compose) | Delta Live Tables (Databricks Serverless) | Docker is for dev/test; Azure is the production platform |
| **Storage backend** | Local Delta dirs | Unity Catalog + ADLS Gen2 | Production requires governed, shared storage |
| **GeoIP library** | `geoip2` (compiled C dep via libmaxminddb) | `maxminddb` (pure Python) | Serverless DLT can't install compiled extensions |
| **UA parsing** | In Spark Silver (5 pandas UDFs, PyArrow) | In Airflow `_export_dimensions()` PythonOperator | Azure split: Spark handles compute, Airflow handles I/O |
| **Dimension dedup** | `INSERT … ON CONFLICT DO NOTHING` on raw string | MERGE upsert on SHA-256 hash of parsed fields | Hash-based is faster and handles URL encoding variance |
| **Fact FK restoration** | `COALESCE(-1)` on 8 dbt dims + 2 Airflow dims | HASHBYTES geo lookup CTE + raw-string UA join CTE | Azure needs extra CTEs because dim tables are built differently |
| **dbt execution** | Airflow container (BashOperator) | Databricks serverless (self-bootstrapping notebook) | dbt-sqlserver ODBC driver must run inside Databricks, not Airflow |
| **CSV count** | 17 | 18 (`mart_country_browser_share` added) | Fixes a v1.8 gap — Power BI gets one more analytics view |
| **Row counts** | 155,570 bronze → 155,357 fact | 153,380 bronze → 153,377 fact | Historical files are identical; differences come from parser edge-case handling and GeoIP Quality-of-Service expectations |
| **dim_geolocation rows** | 4,011 | 1,585 | Azure Silver has fewer distinct IPs (different GeoIP DB version + `valid_country` drops 3 unresolvable IPs) |
| **dim_useragent rows** | 2,276 | ~2,040 (Phase 6 E2E) / 219 (Phase 7 hash-deduped) | Docker deduplicates on raw UA string; Azure deduplicates on SHA-256 hash of parsed UA fields (browser + OS + device), collapsing nearly identical UAs |
| **Cost model** | Free (local Docker) | ~$60–100/mo (with $100 hard cap) | Azure has real cloud costs, demonstrating production budget governance |

---

*Last updated: June 2026. Cross-checked against README.md, azure-cloud-native-single-pipeline.md (1495 lines), testing-suite-reference.md (394 lines), and improvements.md (238 lines).*
