# CSV Data Quality Audit

**Date:** 2026-06-12
**Scope:** All 18 CSV files in `airflow/data/Star-Schema/` (output of `spark_ingestion_azure` + `dbt_marts_azure` DAGs)
**Total Fact Rows:** 153,170
**Date Range:** Jan 2010 – Mar 2019

---

## 1. Dimension Table Health

### ✅ dim_date (93 rows)
Healthy. 93 daily rows covering Jan 2010–Mar 2019 with date_sk, full_date, year, month, day, day_of_week, quarter, is_weekend.

### ✅ dim_time (1,440 rows)
Correct. 24 hours × 60 minutes = 1,440 rows. Time bands and AM/PM labels are correct across hours 0–23.

### ✅ dim_method (4 rows)
Healthy. GET, HEAD, POST, PUT with safe/unsafe flags.

### ✅ dim_status (26 rows)
Healthy. Covers 200/206/302/304/403/404/405/406/500 with sub_status and win32_status combinations, categorized as Success/Redirect/Client Error/Server Error.

### ✅ dim_visitortype (3 rows)
Healthy. -1=Unknown, 1=Crawler, 2=Human.

### ✅ dim_visit_buckets (6 rows)
Healthy. 1 Visit through 51+ Visits with counts.

### ✅ dim_page (13,728 rows)
Healthy. Page distribution:
- 10,405 Dynamic Page (W3C IIS CMS pages)
- 2,957 Image (jpg, gif, png, ico)
- 320 Other
- 13 Static Page
- 22 Image Gallery
- 11 Document

### ✅ dim_referrer (2,334 rows)
Healthy. Referral URLs from darwinsbeagleplants.org, bing.com, google.com, msn.com, yahoo.com, live.com, aol.com, ask.com, altavista.com, duckduckgo.com, yandex.com, baidu.com.

### ✅ dim_geolocation (1,586 rows)
Healthy. Covers countries, regions, cities with lat/lon and ISP data.

### ✅ dim_useragent (2,041 rows)
Healthy overall. Browser UAs parse correctly (Chrome, Firefox, Safari, IE, Opera). Many entries show `operating_system=Other`, `browser_name=Other`, `device_type=Other` — these are crawlers, bots, and tools (curl, wget, Java, etc.) where the UA parser returns unknowns. This is expected behavior.

### ✅ crawler_ips (1,140 rows)
Healthy. IP list for crawler detection.

---

## 2. Fact Table — Critical Issues

### ❌ CRITICAL: geolocation_sk = -1 (ALL 153,170 rows)
Every row in `staging.fact_webrequest.csv` has `geolocation_sk = -1`. This means the IP-to-geolocation lookup during enrichment is failing completely — no row matched a geolocation dimension record.

### ❌ CRITICAL: user_agent_sk = -1 (ALL 153,170 rows)
Every row in `staging.fact_webrequest.csv` has `user_agent_sk = -1`. This means the User-Agent string parsing/lookup is failing completely — no row matched a user agent dimension record.

### ❌ HIGH: page_category = "Unknown" (ALL rows)
The `page_category` enrichment field (derived from URI file extension mapping) is "Unknown" for every single fact row. This is odd given that dim_page has proper categories. This is likely an enrichment-time issue, not a dimension-load issue.

### ❌ HIGH: referrer_domain = "Direct" (ALL rows)
Every fact row shows `referrer_domain = "Direct"` and `traffic_type = "Direct"`. This suggests the referrer extraction/enrichment logic in the silver layer is not populating the derived columns.

### ❌ HIGH: bytes_sent with weekend/hour flags = NULL
`fact_webrequest.is_weekend`, `fact_webrequest.request_hour`, and `fact_webrequest.day_of_week` are all NULL across all rows. The time-based enrichment (using date_sk/time_sk lookups into dim_date/dim_time) is failing.

---

## 3. Mart Health

### ✅ mart_page_performance (13,728 rows)
Healthy. 1 row per page (inner join on dim_page). Contains proper page_views, total_bytes, avg_response_size, etc.

### ✅ mart_timeofday_analysis (2,093 rows)
Healthy. Hourly aggregates across the date range.

### ✅ mart_crawler_analysis (86 rows)
Healthy. Crawler traffic breakdowns.

### ❌ mart_browser_analysis (0 rows — EMPTY)
Completely empty (only header row). This mart joins fact_webrequest → dim_useragent, and since ALL user_agent_sk = -1 (no match), every row is excluded. The join produces zero results.

### ❌ mart_country_browser_share (93 rows — but junk data)
All 93 rows show `country=Unknown`, `top_browser=Unknown`, `top_browser_share_pct=100.0`. Direct consequence of geo + UA join failures — it groups by Unknown country and Unknown browser, producing one meaningless row per day.

### ⚠️ mart_daily_aggregates (93 rows)
Columns affected by the join failures:
- `active_countries` = 0 for ALL rows (geo join failure)
- `unique_hosts` = 1 for ALL rows (should vary per day based on distinct client IPs)
- `unique_human_hosts` = 1 for ALL rows (same issue)
- `top_browser_share` is NULL/empty for many rows (UA join failure)
- `top_page` and `top_page_views` appear populated (comes from page join which works)

---

## 4. Root Cause Analysis

### 4.1 Geo + UA Join Failures (geolocation_sk = -1, user_agent_sk = -1)

The `-1` surrogate key values indicate the fact-to-dimension lookup produced no match. Possible causes:

**A) Silver enrichment inserts row-by-row, but the dimension tables are empty at that point.**
If the DLT silver pipeline processes the streaming fact data before the dimension tables are fully populated, every lookup returns NULL → -1. This is an ordering/timing issue.

**B) Enrichment JOIN condition mismatch.**
The enrichment logic may be using a different key format or column name than what the dimension tables contain. For example:
- Geo enrichment: IP-address lookup format mismatch (string vs integer, missing CIDR logic)
- UA enrichment: User-agent string hash mismatch or parsing inconsistency between enrichment code and dimension-load code

**C) Dimension SK generation is non-deterministic across runs.**
If SK values are auto-generated (e.g., ROW_NUMBER) rather than deterministic (hash-based or sequence-based with fixed mapping), a re-run could produce different SKs for the same natural key, breaking the relationship.

**D) The `spark_ingestion_azure` DAG persists fact data using tracking tables.**
The SQL export script in `dbt_marts_azure` reads from `dbo.raw_enriched_tracking`. The issue may be in the export SQL (the `export_enriched_data` stored procedure or the SQL query in the Airflow task), which may not be matching SKs correctly.

### 4.2 page_category = "Unknown"

The `page_category` column in the fact table is derived from the URI path/file extension. Since dim_page correctly categorizes pages, the enrichment logic in the bronze→silver step may not be applying the same mapping, or the mapping is applied to a different field.

### 4.3 referrer_domain = "Direct"

The referrer domain extraction and traffic_type classification seems absent or broken in the silver enrichment logic. All rows fall through to the default "Direct" value.

### 4.4 bytes_sent time enrichment = NULL

`is_weekend`, `request_hour`, `day_of_week` being NULL suggests the time dimension lookups (using date_sk/time_sk) are also failing, possibly because date_sk/time_sk themselves are NULL or not populated in the fact table.

---

## 5. Dependency Chain

The cascading failures follow this dependency chain:

```
Fact enrichment layer (Silver)
  └─ user_agent_sk = -1 ──→ mart_browser_analysis empty
  └─ geolocation_sk = -1 ──→ mart_country_browser_share: country=Unknown
  └─ both -1                → mart_daily_aggregates: active_countries=0, top_browser_share=NULL
  └─ page_category=Unknown  → (possibly from same enrichment gap)
  └─ referrer_domain=Direct → (possibly from same enrichment gap)

Marts that do NOT join dimensions (work correctly):
  └─ mart_page_performance   (joins dim_page only — works)
  └─ mart_crawler_analysis   (uses crawler_ip flag only — works)
  └─ mart_timeofday_analysis (uses time_sk directly — works)
```

---

## 6. Recommended Next Steps

| Priority | Investigation | Files to Inspect |
|----------|---------------|------------------|
| P0 | Check DLT Python enrichment code for geo + UA SK lookups | `airflow/spark/databricks/dlt_pipeline.py` or similar |
| P0 | Check the export SQL — does it re-map SKs or use tracking tables? | `airflow/spark/databricks/jdbc_export.py`, SQL in `spark_ingestion_azure.py` |
| P1 | Verify dim_useragent and dim_geolocation are populated BEFORE the fact enrichment runs | DAG definition in `airflow/dags/w3c/spark_ingestion_azure.py` |
| P1 | Check page_category mapping logic in enrichment | Same DLT pipeline scripts |
| P2 | Add data quality assertions to DLT pipeline | Pipeline config |
| P2 | Add row-level validation before export | Export scripts |
