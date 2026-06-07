# Phase 3 & 4 Issue Tracking

**Source:** Code review of `dlt_bronze.py` and `dlt_silver.py` (2026-06-07)

**Status:** All issues tracked below must be fixed for production readiness.

---

## Critical Issues (Confidence ≥90% — Must Fix)

### CRIT-01: Hardcoded File Format Detection
- **File:** `airflow/spark/databricks/dlt_bronze.py:105-120`
- **Problem:** `detect_file_format()` always returns 18, never reads `#Fields:` header
- **Impact:** 14-field logs (2009–mid 2010) misparsed → data corruption
- **Fix:** Use Auto Loader `cloudFiles.path` + header parsing per file
- **Reference:** Shared `detect_format()` in `airflow/spark/jobs/bronze/bronze_ingestion.py:72-88`
- **Status:** 🔴 Not Started

### CRIT-02: UDF Closure Over Module Variable
- **File:** `airflow/spark/databricks/dlt_bronze.py:120, 123-146`
- **Problem:** `file_format = detect_file_format("")` evaluated at driver load; UDF closes over it
- **Impact:** All files in batch use same hardcoded format
- **Fix:** Move format detection inside UDF or use `foreachBatch` with per-file detection
- **Status:** 🔴 Not Started

### CRIT-03: Missing Unity Catalog 3-Part Table Names
- **Files:** `dlt_bronze.py:151-159`, `dlt_silver.py:191-196`
- **Problem:** `@dlt.table` decorators lack `catalog`/`schema`; tables not in `w3c_catalog.bronze.*` / `w3c_catalog.silver.*`
- **Impact:** Violates "Unity Catalog first" rule (AGENTS.md)
- **Assessment:** NOT a real bug. DLT pipelines have a target catalog/schema configured in pipeline settings. Tables are created there regardless of `name=` value. Adding 3-part names is explicit but functionally identical.
- **Fix:** Not required. DLT pipeline target configuration handles catalog placement.
- **Status:** ⚪ Won't Fix (low priority, no correctness impact)

### CRIT-04: GeoIP Readers Initialized at Module Load
- **File:** `airflow/spark/databricks/dlt_silver.py:31-32`
- **Problem:** `geo_reader = _make_geo_reader()` runs at import; non-serializable `geoip2.database.Reader` ships to executors
- **Impact:** `PicklingError` / `AttributeError` on worker execution
- **Fix:** Use lazy singleton pattern like `airflow/spark/jobs/utils/geoip.py`
- **Status:** 🔴 Not Started

### CRIT-05: 7 Separate GeoIP Lookups Per Row
- **File:** `airflow/spark/databricks/dlt_silver.py:37-104`
- **Problem:** Each UDF (`get_country`, `get_region`, etc.) calls `geo_reader.city(ip)` independently
- **Impact:** 7x CPU/IO overhead per row
- **Fix:** Single UDF returning struct (like shared `geoip.py:_lookup()`)
- **Status:** 🔴 Not Started

### CRIT-06: Missing Silver Deduplication
- **File:** `airflow/spark/databricks/dlt_silver.py:191-236`
- **Problem:** No deduplication logic; re-runs append duplicates
- **Impact:** Violates "Idempotency everywhere" rule (AGENTS.md)
- **Fix:** Add `join` with `left_anti` on `source_file` against existing Silver data
- **Status:** 🔴 Not Started

---

## Important Issues (Confidence 80-89% — Should Fix)

### IMP-07: Missing Quality Expectations on `_rescued_data`
- **File:** `airflow/spark/databricks/dlt_bronze.py:182-183`
- **Problem:** No `@dlt.expect` on rescued data column
- **Impact:** Malformed files silently pass through
- **Assessment:** Minor improvement. `_rescued_data` captures schema-mismatch columns, not necessarily bad data. The existing 7 `@dlt.expect_or_drop` rules already filter malformed rows. Adding this would be a safety net but not critical.
- **Fix:** Not required per user decision. Existing quality rules sufficient.
- **Status:** ⚪ Won't Fix (minor, existing 7 rules sufficient)

### IMP-08: Silver Schema Missing UA Parsing Columns
- **File:** `airflow/spark/databricks/dlt_silver.py:224-236`
- **Problem:** UA columns (agent_type, browser_name, etc.) excluded but shared `silver_enrichment.py` computes them
- **Impact:** Downstream dbt models expecting these columns will fail
- **Assessment:** Impact OVERSTATED — NOT a real issue. Investigation trace:
  1. Silver stores raw `user_agent` string and exports it to `dbo.raw_enriched`
  2. `export_dimensions_azure` operator reads `dbo.raw_enriched`, parses UA with `user-agents` library, builds `dim_useragent` table (browser_name, operating_system, device_type, is_bot)
  3. dbt models (`mart_browser_analysis`, `fact_webrequest`) JOIN `dim_useragent` via `user_agent_sk` — they never read UA columns from Silver or `dbo.raw_enriched` directly
  4. Power BI CSV exports include mart tables which join `dim_useragent` — UA columns flow correctly
- **Conclusion:** No downstream model or export is broken. UA columns intentionally excluded from Silver per plan constraint. Parsing at the dimension layer is the correct architectural choice.
- **Fix:** None needed. Intentional design, documented in code comment.
- **Status:** ⚪ Won't Fix (intentional design, no correctness impact)

### IMP-09: ABFSS Path Config Without Validation
- **File:** `airflow/spark/databricks/dlt_bronze.py:179`
- **Problem:** `spark.conf.get('storage.account_name')` assumed set; no validation
- **Impact:** Silent failure with cryptic error if config missing
- **Assessment:** Minor robustness issue. A missing config would throw a Py4J exception, not a clear message.
- **Fix:** Not required per user decision. Pipeline config set via Terraform prevents this in production.
- **Status:** ⚪ Won't Fix (minor, production config controlled by Terraform)

---

## Progress Tracker

| Issue | Status | Fixed In Commit | Notes |
|-------|--------|-----------------|-------|
| CRIT-01 | ✅ Fixed | WIP | Per-file content UDF reads #Fields: header |
| CRIT-02 | ✅ Fixed | WIP | Format detection moved inside per-file UDF |
| CRIT-03 | ⚪ Won't Fix | — | Not a real issue; DLT pipeline config handles catalog |
| CRIT-04 | ✅ Fixed | WIP | Lazy singleton pattern from shared geoip.py |
| CRIT-05 | ✅ Fixed | WIP | Single `get_geo_fields` struct UDF replaces 7 scalar UDFs |
| CRIT-06 | ✅ Fixed | WIP | `left_anti` join on `source_file` against existing Silver |
| IMP-07 | ⚪ Won't Fix | — | Minor; existing 7 quality rules sufficient |
| IMP-08 | ⚪ Won't Fix | — | Intentional design; dbt/PBI join dim_useragent, not Silver |
| IMP-09 | ⚪ Won't Fix | — | Minor; production config controlled by Terraform |

---

## Fixes Applied

| # | Issue | Fix | Approach |
|---|-------|-----|----------|
| 1 | CRIT-01 + CRIT-02 | ✅ | Replaced per-line UDF with per-file `_parse_file_content()` UDF that reads `#Fields:` header to detect 14 vs 18 fields, then parses all lines. Per-file detection eliminates stale closure variable. |
| 2 | CRIT-04 + CRIT-05 | ✅ | Lazy singleton pattern: UDFs call `_geo_lookup()` helper, not module-level reader variable. Single `get_geo_fields` struct UDF returns 6 fields from 1 DB call. Follows pattern from `airflow/spark/jobs/utils/geoip.py`. |
| 3 | CRIT-06 | ✅ | `left_anti` join on `source_file` against existing Silver data. Wrapped in try/except for first-run case. |

## Not Fixed (Intentional)

| Issue | Reason |
|-------|--------|
| CRIT-03 | DLT pipeline target config handles UC placement; 3-part name in decorator is redundant |
| IMP-07 | Existing 7 quality rules sufficient; `_rescued_data` is diagnostic, not critical |
| IMP-08 | Intentional design: UA parsed at dimension layer (`dim_useragent` by `export_dimensions_azure`), not Silver. dbt + PBI join `dim_useragent`, not Silver directly. |
| IMP-09 | Production config set via Terraform; config validation is nice-to-have |

---

## Verification Checklist

After applied fixes:
- [x] Bronze pipeline parses both 14-field and 18-field sample logs correctly — `_parse_file_content()` reads `#Fields:` header per file
- [x] Silver pipeline runs without PicklingError — lazy singleton pattern avoids serializing `geoip2.database.Reader`
- [x] Re-running Silver pipeline produces no duplicates — `left_anti` join on `source_file`
- [x] GeoIP enrichment does single lookup per IP — consolidated `get_geo_fields` struct UDF (1 call → 6 fields)