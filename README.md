# W3C Web Logs ETL Pipeline

> W3C web logs, ingested raw → served as a business-ready Power BI datamart, on a fully serverless Azure stack with zero static credentials.

<p align="center">
  <img src="https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&labelColor=000000&logo=microsoftazure" alt="Azure">
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&labelColor=000000&logo=databricks" alt="Databricks">
  <img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&labelColor=000000&logo=dbt" alt="dbt">
  <img src="https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&labelColor=000000&logo=apache-airflow" alt="Airflow">
  <img src="https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&labelColor=000000&logo=terraform" alt="Terraform">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&labelColor=000000&logo=python" alt="Python">
  <img src="https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&labelColor=000000&logo=powerbi" alt="Power BI">
  <img src="https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&labelColor=000000&logo=apachespark" alt="Spark">
  <img src="https://img.shields.io/badge/PySpark-FFD43B?style=for-the-badge&labelColor=000000&logo=python" alt="PySpark">
  <img src="https://img.shields.io/badge/Delta_Lake-4AB197?style=for-the-badge&labelColor=000000&logo=delta" alt="Delta Lake">
  <img src="https://img.shields.io/badge/SQL_Server-CC2927?style=for-the-badge&labelColor=000000&logo=microsoftsqlserver" alt="SQL Server">
  <img src="https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&labelColor=000000&logo=postgresql" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&labelColor=000000&logo=redis" alt="Redis">
  <img src="https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&labelColor=000000&logo=grafana" alt="Grafana">
  <img src="https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&labelColor=000000&logo=prometheus" alt="Prometheus">
  <img src="https://img.shields.io/badge/OpenLineage-7C3AED?style=for-the-badge&labelColor=000000&logo=openlineage" alt="OpenLineage">
  <img src="https://img.shields.io/badge/Marquez-1F6FEB?style=for-the-badge&labelColor=000000" alt="Marquez">
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&labelColor=000000&logo=docker" alt="Docker">
  <img src="https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&labelColor=000000&logo=githubactions" alt="GitHub Actions">
  <img src="https://img.shields.io/badge/pytest-0A9EDC?style=for-the-badge&labelColor=000000&logo=pytest" alt="pytest">
</p>

<p align="center">
  <a href="https://github.com/AhmedIkram05/w3c-etl-pipeline/actions/workflows/ci.yml"><img src="https://github.com/AhmedIkram05/w3c-etl-pipeline/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
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

## How It Fits Together

Bronze → Silver → Azure SQL → dbt → Power BI, on Databricks serverless, with OpenLineage → Marquez observing every stage across both engines.

```mermaid
flowchart LR
    classDef source fill:#3b82f6,color:#fff,stroke:#1e40af
    classDef ingest fill:#10b981,color:#fff,stroke:#047857
    classDef dlt fill:#8b5cf6,color:#fff,stroke:#6d28d9
    classDef sql fill:#f59e0b,color:#fff,stroke:#d97706
    classDef dbtclass fill:#ef4444,color:#fff,stroke:#dc2626
    classDef bi fill:#ec4899,color:#fff,stroke:#db2777
    classDef lineage fill:#7c3aed,color:#fff,stroke:#5b21b6

    source["93 W3C IIS log files<br/>2009–2011"]:::source

    adls["ADLS Gen2<br/>raw-logs container"]:::ingest

    bronze["DLT Bronze (serverless)<br/>w3c_etl_databricks.bronze.bronze_raw_logs<br/>Custom W3C parser UDF • 14/18-field detection<br/>7 @dlt.expect_or_drop quality checks<br/>153,380 rows • 0 dropped<br/>Partitioned by log_date"]:::dlt

    silver["DLT Silver (serverless)<br/>w3c_etl_databricks.silver.silver_enriched_logs<br/>7 MaxMind GeoIP fields (maxminddb pure Python)<br/>5 computed fields • 31 columns total<br/>153,377 rows • 30+ countries<br/>Dedup: left_anti join on source_file"]:::dlt

    jdbc["JDBC Export (notebook_task)<br/>pymssql batch executemany<br/>BATCH_SIZE=5000 • 4-attempt retry<br/>~45s for 153,377 rows"]:::sql

    azsql["Azure SQL (serverless GP_S_Gen5, 1 vCore)<br/>dbo.raw_enriched - 31 columns<br/>Auto-pause 60 min idle"]:::sql

    dims["Airflow: export_dimensions<br/>SCD Type 2 dim_geolocation · MERGE upsert dim_useragent<br/>→ dim_geolocation (1,586 current rows + history)<br/>→ dim_useragent (2,041 rows)<br/>Fires Dataset trigger"]:::dbtclass

    dbt["dbt - 16 models • 121 tests<br/>10 staging + 6 marts<br/>Dual-dialect T-SQL / PostgreSQL<br/>Runs on Databricks serverless via<br/>self-bootstrapping notebooks"]:::dbtclass

    csv["18 CSV exports<br/>Star-Schema/ directory"]:::bi

    powerbi["Power BI<br/>7-page dashboard<br/>Weekly auto-refresh"]:::bi

    marquez["OpenLineage → Marquez<br/>Cross-engine lineage<br/>DAG/task runs · dbt models ·<br/>w3cDataQuality facet"]:::lineage

    source -->|"ABFSS path"| adls
    adls -->|"Auto Loader"| bronze
    bronze -->|"spark.table()"| silver
    silver -->|"collect() + pymssql"| jdbc
    jdbc --> azsql
    azsql --> dims
    dims -->|"Dataset trigger"| dbt
    dbt --> csv
    csv --> powerbi

    dims -.->|"task events"| marquez
    dbt -.->|"dbt-ol + quality facet"| marquez
```

**Every piece, in one line:**

| Component | What it does |
|---|---|
| **[Azure Databricks DLT](docs/README-full.md#1-azure-databricks-dlt-bronze--silver)** | Serverless Bronze → Silver: custom W3C parser, GeoIP enrichment, dedup — zero cluster management |
| **[Azure SQL](docs/README-full.md#2-azure-sql--jdbc-export)** | Serverless warehouse; JDBC export tuned from 413s → 45s |
| **[Apache Airflow](docs/README-full.md#3-apache-airflow-orchestration)** | 4 DAGs wired by dataset triggers — ingestion → dimensions → dbt, no polling |
| **[dbt](docs/README-full.md#4-dbt--the-t-sql-migration)** | 16 models compiling against both T-SQL and PostgreSQL from one source |
| **[Power BI](docs/README-full.md#5-power-bi--semantic-contract)** | 7-page live dashboard on a semantic contract: logic in dbt, presentation in BI |
| **[Terraform](docs/README-full.md#6-terraform-infrastructure-as-code)** | Entire Azure estate as code, GitHub→Azure auth via OIDC — zero static secrets |
| **[Unity Catalog](docs/README-full.md#7-unity-catalog--governance)** | Governed catalogs, schemas, volumes, cross-pipeline `spark.table()` reads |
| **[CI/CD](docs/README-full.md#8-cicd-pipeline)** | 7 workflows: lint, test, dbt-compile, terraform plan→apply, smoke test |
| **[Observability](docs/README-full.md#9-monitoring--observability)** | 3 Grafana dashboards, 8 Prometheus alert rules — DAG duration to data freshness |
| **[OpenLineage → Marquez](docs/README-full.md#10-data-lineage--openlineage-marquez)** | Cross-engine lineage for every run, plus a custom `w3cDataQuality` facet |
| **[Docker dev stack](docs/README-full.md#quick-start)** | Same 18-service pipeline locally — Airflow on Celery+Redis, Spark, Postgres, Grafana — for fast iteration and CI |

---

## Why It's Interesting

| Highlight | Why It Matters |
|---|---|
| **45-second export** — 153,377 rows Silver → Azure SQL went from 413s to 45s (8–9× faster). | Databricks serverless only supports JDBC reads, not writes. Pure-Python `pymssql` + `tuple(row)` + Spark-side pre-filter before `collect()` were the breakthrough. [Deep dive](docs/README-full.md#2-azure-sql--jdbc-export) |
| **Dual-dialect dbt** — all 16 models compile against PostgreSQL (dev/CI) and T-SQL (Azure SQL/prod) via inline dialect branches, no duplicate model files. | One model, two databases, one source of truth. [Deep dive](docs/README-full.md#4-dbt--the-t-sql-migration) |
| **Terraform with OIDC** — two Terraform parts provision the whole estate, including the GitHub→Azure auth chain itself. | Zero static credentials: the runner assumes an Azure AD identity via token exchange, not client secrets. One `terraform apply` from scratch. [Deep dive](docs/README-full.md#6-terraform-infrastructure-as-code) |
| **4-layer observability** — Grafana dashboards, Prometheus alerting on a StatsD stream, Azure Monitor alerts, and OpenLineage lineage. | DAG durations, container health, data freshness, and pipeline lineage are all tracked from day one. [Deep dive](docs/README-full.md#9-monitoring--observability) |

---

## Key Metrics

| Metric | Value |
|---|---|
| Requests served | **155.6K** across **88 active countries** (BI) / 30+ GeoIP-resolved (Silver) |
| Traffic | **62% human / 38% bot**, 9.7% 404 rate |
| Bronze rows ingested | **153,380** — 0 dropped through 7 quality gates |
| Export performance | **~45 seconds** for 153,377 rows (was 413s) |
| dbt models | **16** (10 staging + 6 marts), dual-dialect T-SQL/PostgreSQL |
| dbt data tests | **121** (not_null, unique, accepted_values, relationships, expression_is_true, singular) |
| pytest | **627 total** (597 in CI: 480 unit + 92 terraform + 25 DAG integrity) |
| Orchestration | **4 Airflow DAGs**, dataset-triggered; **7 GitHub Actions workflows** |
| Observability | **3 Grafana dashboards** (23 panels), **8 Prometheus + 2 Azure Monitor alerts** |
| Cost | **~$0–100/mo** — serverless auto-scales to zero, $50 warning / $100 hard cap |

---

## Demos

**The 7-page Power BI dashboard** (live report [here](https://app.powerbi.com/reportEmbed?reportId=41d525b8-b808-4750-88ba-cb31dbbba958&autoAuth=true&ctid=ae323139-093a-4d2a-81a6-5d334bcd9019)):

![Power BI dashboard — all 7 pages](docs/media/powerbi.gif)

**Orchestration in action** — Airflow DAG graphs and gantts, Databricks workflow + DLT pipeline runs, and the Power Automate refresh schedule:

![Orchestration](docs/media/orchestration.gif)

**The Azure estate** — resource group, ADLS, the Azure SQL schema and table row counts, Unity Catalog:

![Azure estate](docs/media/azure-estate.gif)

**Observability** — Grafana dashboards and the Prometheus targets/alert rules behind them:

![Observability](docs/media/observability.gif)

**CI/CD** — the 4 parallel CI jobs, terraform plan→apply→smoke-test CD run, and the rollback story:

![CI/CD](docs/media/cicd.gif)

---

## Trade-offs That Mattered

| Decision | Alternative | Why This Won |
|---|---|---|
| **Dual-dialect dbt:** inline `{% if target.type == 'sqlserver' %}` branches | Per-dialect model files (`_azure.sql`) | dbt would parse both as independent models — duplicate DAG entries. Inline branches keep one source of truth. |
| **Serverless DLT** over classic clusters | Fixed job clusters with VMs | Zero infrastructure management: scales to zero when idle, no cluster tuning ever. |
| **SCD Type 2** for `dim_geolocation` over append-only/Type 1 | In-place overwrite | Full attribute history plus current-state performance, via a T-SQL `MERGE ... OUTPUT` pattern. |
| **Thin Power BI reports** — transforms stay in dbt/SQL | Logic embedded in Power BI DAX | The report is a presentation layer over a semantic contract; the warehouse stays the single source of truth. |

All 15 decisions, with alternatives and reasoning: [Design Decisions](docs/README-full.md#design-decisions).

---

## Quick Start

```bash
docker compose -f airflow/docker-compose.yaml up -d   # 18-service Airflow+Spark+PG+Grafana stack
dbt deps --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt
dbt run  --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt
dbt test --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt
uv run pytest tests/ -m "not integration and not dbt_compile"
cd terraform/part_a && terraform init -backend=false && terraform test
```

Full local, lineage, and production (Azure) instructions: [docs/README-full.md](docs/README-full.md#quick-start).

---

## Documentation

The **complete design document** — all 11 component deep dives, every proof image, the full design-decisions table — lives at **[docs/README-full.md](docs/README-full.md)**.

---

## Related Projects

- [**LAAD**](https://github.com/AhmedIkram05/laad) - ATM log aggregation & diagnostics platform with Kafka streaming, 3-layer ML/heuristic anomaly detection, and an Agentic RAG diagnostic assistant with multi-signal confidence fusion.
- [**DevSync**](https://github.com/AhmedIkram05/devsync) - Full-stack project management platform with real-time collaboration, GitHub OAuth 2.0 integration, bidirectional Issue/PR sync, and 1,452 tests - deployed on AWS ECS Fargate with OIDC CI/CD.
- [**StockLens**](https://github.com/AhmedIkram05/StockLens) - React Native mobile FinTech app that scans receipts via OCR and projects missed investment opportunities using Alpha Vantage data + ARIMA/regression forecasting, with biometric auth and AES-256 encryption.

---

<p align="center">
  <sub>Built with Azure, Databricks, dbt, Airflow, OpenLineage, Terraform, Python, SQL, and a lot of pain.</sub>
</p>
