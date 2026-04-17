# W3C Web Log ETL Pipeline

An automated ELT pipeline that ingests IIS W3C web server log files, transforms them into a star schema data warehouse on AWS RDS (PostgreSQL), and surfaces analytics through a 7-page Power BI dashboard — fully automated end to end with Apache Airflow and Power Automate.

---

## Live Dashboard

**Direct link (Power BI Reports):** [Open in Power BI](https://app.powerbi.com/reportEmbed?reportId=41d525b8-b808-4750-88ba-cb31dbbba958&autoAuth=true&ctid=ae323139-093a-4d2a-81a6-5d334bcd9019&actionBarEnabled=true)

**Video Walkthrough (Entire Pipeline):** [Open in sharepoint](https://dmail-my.sharepoint.com/:v:/g/personal/2571642_dundee_ac_uk/IQDarKYb4S4bTp1CU2mwRNHqAd4DaKYajEdvCQ7YxxTk3no?e=A77Xws)

---

## Architecture

```
IIS W3C .log files
        │
        ▼
┌───────────────────┐
│   Apache Airflow  │  DAG: Process_W3C_Data
│   (ELT Pipeline)  │  Schedule: Every Friday 5:00pm
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│   raw_logs table  │  Staging — incremental, deduplicates on re-run
│   (PostgreSQL)    │
└────────┬──────────┘
         │  9 parallel dimension tasks
         ▼
┌───────────────────────────────────────┐
│         Star Schema  ·  AWS RDS       │
│                                       │
│  fact_webrequest (central fact table) │
│  dim_date        dim_time             │
│  dim_page        dim_geolocation      │
│  dim_useragent   dim_status           │
│  dim_referrer    dim_method           │
│  dim_visitortype                      │
└────────┬──────────────────────────────┘
         │  PostgreSQL COPY → CSV
         ▼
┌───────────────────┐
│     Power BI      │  7-page dashboard
│                   │  Refresh: Power Automate 5:30pm
└───────────────────┘
```

---

## Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow |
| Database | PostgreSQL 14 · AWS RDS |
| Transformation | Python 3 · psycopg2 · execute_values |
| Geolocation | ip-api.com batch API |
| Holiday detection | Python `holidays` library (UK) |
| User agent parsing | `user-agents` library |
| Visualisation | Microsoft Power BI |
| Refresh automation | Power Automate |

---

## DAG Overview

The pipeline runs as a single DAG (`Process_W3C_Data`) with four sequential phases:

**Phase 1 — Create tables**
Creates all PostgreSQL tables idempotently with `CREATE TABLE IF NOT EXISTS`. Seeds default `-1` surrogate key rows in every dimension for null-safe joins.

**Phase 2 — Stage raw logs**
Loads new `.log` files from `LogFiles/` into `raw_logs` incrementally. Detects W3C column format (14-col or 18-col) per file from the `#Fields` header. Batch inserts 5,000 rows at a time.

**Phase 3 — Build dimensions (parallel)**
Nine tasks fire simultaneously:

| Task | What it builds |
|---|---|
| `makeDateDimension` | Year, month, quarter, week, weekend flag, UK holiday flag |
| `makeTimeDimension` | All 1,440 minute combinations with time bands and shift IDs |
| `makePageDimension` | URI paths with file extension and page category classification |
| `makeLocationDimension` | Country, city, ISP via ip-api.com batch API with IP caching |
| `makeUserAgentDimension` | Browser, OS, device type after URL decoding |
| `makeStatusDimension` | HTTP status severity classification (2xx/3xx/4xx/5xx) |
| `makeReferrerDimension` | Traffic source classification (Direct/Search/Social/Internal/Referral) |
| `makeMethodDimension` | HTTP verb descriptions |
| `DetectCrawlerIPs` | Bot IP detection via `robots.txt` access |

**Phase 4 — Build fact table and export**
`BuildFactTable` executes a single `INSERT INTO ... SELECT` joining `raw_logs` to all eight dimensions simultaneously via surrogate keys. `ExportCSV` then streams all star schema tables to CSV using PostgreSQL `COPY`.

---

## Key Design Decisions

**ELT over ETL**
Raw data is staged first, then transformed in-database. This preserves a full audit trail in `raw_logs` and allows transformation tasks to be re-run independently if logic changes, without re-ingesting source files.

**Parallel dimension building**
All eight dimension tasks run concurrently with no inter-task dependencies. The fact table build uses a fan-in dependency, waiting for all nine tasks before executing. This reduces phase 3 runtime by ~8x compared to sequential execution.

**Minus-one surrogate keys**
Every dimension contains a default `-1` row representing unknown/missing values. The fact table uses `LEFT JOIN` + `COALESCE(..., -1)` throughout, ensuring no raw log record is ever dropped from the fact table due to an unmatched dimension. NULL foreign keys would be silently excluded from Power BI aggregations.

**IP caching for geolocation**
`makeLocationDimension` queries existing IPs in `dim_geolocation` before calling the API, only requesting lookups for new IPs. This makes repeat pipeline runs fast and avoids unnecessary API calls against ip-api.com's rate limits.

**AWS RDS over local storage**
PostgreSQL is hosted on AWS RDS rather than a local Docker container, providing managed backups, automatic failover, and remote accessibility. All credentials are passed via environment variables.

---

## Configuration

Set the following environment variables before running:

```bash
W3C_DB_HOST=<your-rds-endpoint>
W3C_DB_PORT=5432
W3C_DB_NAME=w3c-warehouse
W3C_DB_USER=<username>
W3C_DB_PASS=<password>
```

Defaults fall back to a local `postgres` container if variables are not set.

---

## Dashboard Pages

| Page | Business question answered |
|---|---|
| Who's Hitting the Site? | Human vs crawler traffic split and trends |
| What Are People Accessing? | Top pages, file types, 404 errors, broken URLs |
| How Fast Is the Server? | Avg + P95 response time, slow request rate, slowest files |
| Where Are Visitors Coming From? | Geographic breakdown, referrer sources |
| When Is the Site Busiest? | Hour-by-day traffic matrix, peak periods, weekday vs weekend |
| Who Are the Visitors? | Browser, OS, device type, visit frequency, top IPs |
| At a Glance | Executive KPI summary with written analysis |

---

## Dependencies

```bash
pip install apache-airflow psycopg2-binary requests user-agents holidays
```

---

## Author

**Ahmed Ikram**
