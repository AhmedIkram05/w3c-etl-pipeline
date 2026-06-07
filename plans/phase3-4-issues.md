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
- **Fix:** Add `name="w3c_catalog.bronze.bronze_raw_logs"` and `name="w3c_catalog.silver.silver_enriched_logs"`
- **Status:** 🔴 Not Started

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
- **Fix:** Add `@dlt.expect("no_rescued_data", "_rescued_data IS NULL")`
- **Status:** 🔴 Not Started

### IMP-08: Silver Schema Missing UA Parsing Columns
- **File:** `airflow/spark/databricks/dlt_silver.py:224-236`
- **Problem:** UA columns (agent_type, browser_name, etc.) excluded but shared `silver_enrichment.py` computes them
- **Impact:** Downstream dbt models expecting these columns will fail
- **Fix:** Include UA columns in Silver or document intentional omission
- **Status:** 🔴 Not Started

### IMP-09: ABFSS Path Config Without Validation
- **File:** `airflow/spark/databricks/dlt_bronze.py:179`
- **Problem:** `spark.conf.get('storage.account_name')` assumed set; no validation
- **Impact:** Silent failure with cryptic error if config missing
- **Fix:** Add validation with clear error message
- **Status:** 🔴 Not Started

---

## Progress Tracker

| Issue | Status | Fixed In Commit | Notes |
|-------|--------|-----------------|-------|
| CRIT-01 | 🔴 Not Started | | |
| CRIT-02 | 🔴 Not Started | | |
| CRIT-03 | 🔴 Not Started | | |
| CRIT-04 | 🔴 Not Started | | |
| CRIT-05 | 🔴 Not Started | | |
| CRIT-06 | 🔴 Not Started | | |
| IMP-07 | 🔴 Not Started | | |
| IMP-08 | 🔴 Not Started | | |
| IMP-09 | 🔴 Not Started | | |

---

## Fix Order (Priority)

1. CRIT-01 (format detection) — blocks correct parsing
2. CRIT-04 (GeoIP serialization) — pipeline crashes
3. CRIT-03 (Unity Catalog names) — production requirement
4. CRIT-05 (consolidate GeoIP UDFs) — 7x performance
5. CRIT-06 (Silver dedup) — data correctness
6. CRIT-02 (UDF closure) — correctness
7. IMP-08 (UA columns) — schema consistency
8. IMP-07 (rescued data) — data quality
9. IMP-09 (config validation) — robustness

---

## Verification Checklist

After all fixes:
- [ ] Bronze pipeline parses both 14-field and 18-field sample logs correctly
- [ ] Silver pipeline runs without PicklingError
- [ ] Tables created in `w3c_catalog.bronze` and `w3c_catalog.silver`
- [ ] Re-running pipelines produces no duplicates
- [ ] GeoIP enrichment does single lookup per IP
- [ ] UA columns present in Silver (matching shared job)
- [ ] Config validation throws clear error when unset
- [ ] Quality expectations fire on test bad data