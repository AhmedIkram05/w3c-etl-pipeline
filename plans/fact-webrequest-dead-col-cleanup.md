# `fact_webrequest` Dead-Column Cleanup

| | |
|---|---|
| **Status** | v2 — spec review ✅, code review ✅ (after revisions) — **ACTIVE** |
| **Effort** | ~3.5 hrs (45 min code, 30 min tests + migration, 30 min docs, 15 min verify, 30 min DAG #1, 30 min DAG #2 + data verify, 10 min rollback doc) |
| **Author** | opencode, 2026-06-02 |
| **Supersedes** | n/a |
| **Related** | [`databricks-hybrid-wiring.md` § 3.4](databricks-hybrid-wiring.md) (finding first surfaced there) |


## 1. Context

The `fact_webrequest` dbt model carries **11 denormalized columns** that are created and populated by the silver pipeline, written to PostgreSQL `raw_enriched`, copied into the fact table, and then **never read by any mart, test, or downstream consumer**. The 11 dead cols are:

- **6 denorm geo cols** (`country`, `region`, `city`, `latitude`, `longitude`, `isp`) — already live in `dim_geolocation`, which marts `LEFT JOIN` to
- **5 denorm UA cols** (`agent_type`, `browser_name`, `browser_version`, `operating_system`, `device_type`) — already live in `dim_useragent`, which `mart_browser_analysis` `LEFT JOIN` to

**Why these are dead — a star-schema framing.** A fact table should hold **quantified, numeric measures** (e.g. `bytes_sent`, `response_time_ms`, `request_count`) plus **foreign keys to dims**. Descriptive / qualitative attributes (country names, browser names, OS names) belong in **dimension tables**, not on the fact. The 11 dead cols are *qualitative* attributes duplicated from `dim_geolocation` / `dim_useragent` — they violate the fact/dim separation by definition. The fact already has `geolocation_sk` and `user_agent_sk` FKs; readers should `JOIN` to the dims, not denorm.

The user reviewed this finding in session and chose **Option A: drop the dead cols from the fact table** rather than leave them as forward-compat weight.

**Boundary discipline (critical):** the silver pipeline **must** keep populating these cols into the silver Delta table because the Airflow `export_dimensions` operator reads them from silver to build `dim_geolocation` and `dim_useragent`. The drop only happens at the **JDBC writer → `public.raw_enriched` → `fact_webrequest` boundary**. Silver withColumn calls stay; only the silver-to-Postgres projection shrinks.

## 2. Background — the dead-column finding

| Consumer | Reads from | Cols used |
|---|---|---|
| `mart_page_performance` | `fact_webrequest` | `geolocation_sk` (FK), `is_404`, `bytes_sent`, `response_time_ms`, `date_sk`, `page_sk` |
| `mart_daily_aggregates` | `fact_webrequest` | `date_sk`, `geolocation_sk` (FK), `is_crawler`, `is_direct_traffic`, `is_404`, `bytes_sent`, `response_time_ms`, `page_sk`; then `JOIN dim_geolocation` for `country` |
| `mart_crawler_analysis` | `fact_webrequest` | `date_sk`, `page_sk`, `status_sk`, `geolocation_sk` (FKs), `bytes_sent`, `response_time_ms`, `is_crawler` |
| `mart_timeofday_analysis` | `fact_webrequest` | `date_sk`, `time_sk`, `page_sk`, `geolocation_sk` (FKs), `is_404`, `is_crawler`, `bytes_sent`, `response_time_ms` |
| `mart_browser_analysis` | `fact_webrequest` | `date_sk`, `user_agent_sk`, `geolocation_sk` (FKs), `bytes_sent`, `response_time_ms`; then `JOIN dim_useragent` for `browser_name`, `operating_system`, `device_type` |

**No mart reads any of the 11 denorm cols from the fact.** All geo/UA analytics go through dim joins.

## 3. Audit (re-verified 2026-06-02)

### 3.1 Per-column read audit (full repo, excluding build dirs)

| Col | Verdict | Note |
|---|---|---|
| `country` | ✅ dead | `mart_daily_aggregates` uses `dg.country` (from `dim_geolocation` JOIN) — not the fact |
| `region` | ✅ dead | zero reads anywhere |
| `city` | ✅ dead | zero reads anywhere |
| `latitude` | ✅ dead | zero reads anywhere; only DDL + test fixtures |
| `longitude` | ✅ dead | zero reads anywhere; only DDL + test fixtures |
| `isp` | ✅ dead | zero reads anywhere |
| `agent_type` | ✅ dead | zero reads anywhere |
| `browser_name` | ✅ dead | `mart_browser_analysis` uses `ua.browser_name` (from `dim_useragent` JOIN) — not the fact |
| `browser_version` | ✅ dead | zero reads anywhere |
| `operating_system` | ✅ dead | `mart_browser_analysis` uses `ua.operating_system` (from `dim_useragent` JOIN) — not the fact |
| `device_type` | ✅ dead | `mart_browser_analysis` uses `ua.device_type` (from `dim_useragent` JOIN) — not the fact |

**`postcode` is NOT on the dead list.** It stays in `silver_enrichment.py` L206 (withColumn), `export_warehouse.py` L78 (DDL), and `dim_geolocation` (via `export_dimensions.py:174` reading silver). Removing it would break `dim_geolocation`.

### 3.2 Touch-point manifest

#### Producers (silver scripts — KEEP withColumns, only edit Databricks DDL)

- `airflow/spark/jobs/silver_enrichment.py:201-213` — **11 withColumn calls STAY** (6 geo + 5 UA). UDFs + withColumn calls remain because `export_dimensions.py:139-185` (`_read_silver_geo_dim`) reads them from silver Delta to build `dim_geolocation`. The `postcode` withColumn at L206 also stays.
- `airflow/spark/databricks/02_silver_enrichment.py:303-308` — **6 withColumn calls (geo) STAY** for the same reason.
- `airflow/spark/databricks/02_silver_enrichment.py:123-128` — `CREATE TABLE` DDL has 6 geo cols → these **stay** (silver Delta still has them)
- `airflow/spark/databricks/02_silver_enrichment.py:129-133` — `CREATE TABLE` DDL has 5 UA cols → **drop from DDL** (already declared but unpopulated, per the upstream finding)

#### Storage (DDL — actual drop sites)

- `airflow/spark/jobs/export_warehouse.py:73-84` — `RAW_ENRICHED_DDL` 36-col → drop 11 → 25-col. `postcode` at L78 stays.
- `airflow/spark/jobs/export_warehouse.py:245-274` — `apply_type_casts` function has explicit casts for `latitude`/`longitude` at L264-267 → remove those 2 cast branches
- `airflow/spark/databricks/03_export_warehouse.py:121-131` — Unity Catalog gold DDL 35-col (no `postcode`) → drop 11 → 24-col
- `airflow/dbt/w3c/models/staging/fact_webrequest.sql:47-58` — `geo_map` CTE: KEEP `geolocation_sk` and `ip AS client_ip` (needed for the join to raw at L175); DROP the 6 denorm cols from the SELECT
- `airflow/dbt/w3c/models/staging/fact_webrequest.sql:105-110` — `computed` CTE's UA block → drop the 5 UA cols
- `airflow/dbt/w3c/models/staging/fact_webrequest.sql:141-151` — final SELECT → drop 11 cols
- `airflow/dbt/w3c/models/schema.yml:246-267` — 11 `description` blocks in `fact_webrequest.columns` → drop
- `airflow/plugins/operators/export_dimensions.py:349-371` — `dim_geolocation` and `dim_useragent` DDLs → **unchanged** (dims keep all their cols; they're the authoritative source)

#### Transport (silver → JDBC → raw_enriched → fact)

- The silver→JDBC bridge is `airflow/spark/jobs/export_warehouse.py:52-91` (DDL block) and `L275-310` (JDBC write). The 11 dead cols are removed from the projection at the DDL block; the JDBC write at L275-310 uses `mode=overwrite` and reads from silver Delta, so it transparently picks up the new 25-col schema.
- `airflow/dbt/w3c/models/sources.yml:21, 34-38` — `dim_geolocation`/`dim_useragent` column listings → **unchanged** (dims keep the cols)
- `airflow/dags/w3c/dbt_marts.py:60-63` — `EXPORT_SCRIPT` uses `psql \copy (SELECT * FROM {table})` → **unchanged** (wildcard means new col count is automatic)

#### dbt models

- `airflow/dbt/w3c/models/staging/fact_webrequest.sql:175-176` — JOIN to `dim_geolocation` for `geolocation_sk` only, no fact-side denorm cols read → unchanged (join key `ip AS client_ip` retained via the modified `geo_map` CTE)

#### Python tests

- `tests/test_export_warehouse.py:78-99` — `test_raw_enriched_ddl_has_type_casts` and `test_raw_enriched_ddl_has_all_36_columns` → update to 25 cols, remove lat/lon cast assertions
- `tests/test_export_warehouse.py:297-323, 375-402, 435-447` — 4 type-cast tests for `latitude`/`longitude` → **remove** (the cols no longer exist)
- `tests/test_integration.py:119-137` — `test_raw_enriched_has_type_cast_columns` → remove lat/lon rows (or remove the whole test if only 2 cols)
- `tests/test_export_dimensions.py` — **unchanged** (tests mock silver→dim path; the 6+5 cols remain in `dim_geolocation`/`dim_useragent`)

#### dbt tests

- `airflow/dbt/w3c/tests/test_fact_referential integrity.sql` — **unchanged** (only tests FKs, no denorm col refs)
- `airflow/dbt/w3c/tests/test_dimension_coverage.sql` — **unchanged** (tests dim row counts)
- `airflow/dbt/w3c/tests/test_row_count_consistency.sql` — **unchanged**
- `airflow/dbt/w3c/tests/singular/fact_webrequest_dedup_safety.sql` — **unchanged** (keys on `raw_enriched` cols that remain)
- **No data tests (`unique`/`not_null`/`relationships`/`accepted_values`) exist for any of the 11 dead cols** in `schema.yml` — so no test deletions needed.
- **New test assertion (recommended):** after the migration, `SELECT column_name FROM information_schema.columns WHERE table_name='fact_webrequest' AND table_schema='dbt_staging'` returns exactly 24 rows; the integration test in `test_integration.py` should be extended with a check `assert len(cols) == 24 and "country" not in [c.column_name for c in cols]`.

#### Docs (full grep list of README col-count touch points)

- `README.md:67` — "36 cols" raw_enriched claim
- `README.md:123` — "35 cols" fact claim
- `README.md:130` — Mermaid says "35 cols" (contradicts L67)
- `README.md:173` — "17 UDFs" — unchanged (UDFs stay)
- `README.md:272` — "35 cols, 17 UDFs"
- `README.md:376` — "155,570 × 35" row-col math
- `README.md:387` — "35 cols" (mermaid)
- `README.md:492` — "GeoIP (7)" UDF count — unchanged
- `README.md:493` — "User-Agent (5)" UDFs — **unchanged** (UDFs still defined; withColumns stay)
- `README.md:496` — "35 enrichment columns added"
- `README.md:532` — "36 cols" (mermaid contradicts L130)
- `README.md:541, 546, 551` — claim context
- `README.md:618` — `fact_webrequest` model description "geo fields are denormalised from `dim_geolocation`" → remove that phrase
- `README.md:655, 657` — Mermaid (35 vs 36)
- `README.md:738-758` — "What a row looks like" table rows for the 11 cols → remove
- `README.md:776-800` — Mermaid ER diagram `country` annotation → remove
- `README.md:807-808` — silver × 35 / fact 155,357 row math
- `README.md:877` — Databricks silver 35 cols
- `README.md:878` — Databricks gold 35 cols
- `README.md:1043, 1155-1156` — context references
- `databricks-hybrid-wiring.md:71, 73, 75-83, 88, 90, 221` — § 3.4 finding + L221 "Out of scope" note → update to "Done — see [`fact-webrequest-dead-col-cleanup.md`](fact-webrequest-dead-col-cleanup.md)"
- `databricks-hybrid-wiring.md:59, 100, 106, 111` — JDBC DDL plans (35 vs 36 col parity)
- `.agents/improvements.md` — no changes (Skills Matrix unaffected)

#### CI / config

- `.github/workflows/ci.yml`, `pyproject.toml`, `.pre-commit-config.yaml`, `Makefile` — no column-name references; **unchanged**

#### External dependencies

- **No Power BI `.pbix`/`.pbit` files in the repo.** README L1133 confirms Power BI reads the CSV exports (which are auto-regenerated from the new schema via `psql \copy (SELECT * FROM ...)`).
- **No Grafana / Prometheus references** to the dead cols.
- **No external schema contracts or XCom payloads** reference column names (the Airflow Dataset URI is table-level only).
- **Power BI owner sign-off required:** README L441 references a "Power BI geographic dashboard" that "clusters traffic by country" — already served by `mart_daily_aggregates.active_countries` JOIN to `dim_geolocation`, so the dim join continues to work. User should confirm with the Power BI owner that no semantic model binds directly to the denorm fact cols.

### 3.3 Subtle risks

1. **`CREATE TABLE IF NOT EXISTS` does not drop cols on existing DBs.** Any pre-existing `public.raw_enriched` table will keep all 11 cols after this change unless explicitly migrated. **An `ALTER TABLE public.raw_enriched DROP COLUMN ...` script is required.**
2. **`apply_type_casts` (`export_warehouse.py:245-274`)** explicitly casts `latitude`/`longitude` to `DOUBLE PRECISION` (lines 264-267) — these cast branches must be removed, otherwise `apply_type_casts` will try to cast non-existent cols and crash.
3. **Delta Lake is a bind mount, not a named volume.** `docker compose down -v` only removes named volumes; `./spark/delta` is a bind mount (per `docker-compose.yaml` L113, L407, L440). Phase 5 must `rm -rf ./spark/delta/silver ./spark/delta/bronze` explicitly to ensure the new silver Delta schema takes effect. Without this, the old 36-col silver Delta persists and a subsequent write with the new schema either fails or silently triggers `mergeSchema` if enabled.
4. **README is internally inconsistent** about col counts (35 vs 36, in 13+ places). All those numbers need to be updated to 25 (raw_enriched) and 24 (fact).
5. **`test_export_warehouse.py:99` count assertion** parses the DDL string — fragile but works. The new count is 25.
6. **The Databricks `02_silver_enrichment.py:129-133` DDL** declares the 5 UA cols but never populates them (intentional, per `databricks-hybrid-wiring.md` § 3.4). After the cleanup, the DDL will only have the 5 UA cols **removed from the schema definition too** — so this is no longer a "declared but unpopulated" issue. The 6 geo cols in the same DDL (L123-128) **stay** because the withColumn calls at L303-308 still populate them.
7. **No `ALTER TABLE` migration tooling exists** in the repo today. The plan adds a one-off SQL script under `airflow/scripts/migrations/`.
8. **dbt incremental strategy can't drop cols.** `on_schema_change='append_new_columns'` (fact_webrequest.sql L4) only handles adds. Column drops require `dbt run --full-refresh` on first build. The marts do not read the dead cols (verified in § 2 + § 3.1), so mart full-refresh is not strictly required, but a full refresh is the safest way to verify the rebuild. Phase 4 step 5 uses `dbt run --full-refresh` for the staging schema; mart full refresh is recommended but can be incremental in production.

### 3.4 Col counts before/after (per environment)

| Table | Docker / local dev | Databricks (future) | After |
|---|---|---|---|
| silver Delta (Docker `silver_enrichment.py`) | 36 cols (unchanged) | n/a | 36 (unchanged) |
| silver Delta (Databricks `02_silver_enrichment.py`) | n/a | 35 → 30 cols (UA DDL dropped; 6 geo + 24 base) | 30 |
| `public.raw_enriched` (Docker JDBC) | 36 → 25 | n/a | 25 |
| `w3c_catalog.gold.warehouse_enriched` (Databricks) | n/a | 35 → 24 | 24 |
| `fact_webrequest` (dbt model) | 35 → 24 | (same model) | 24 |
| `dim_geolocation` | 9 | 9 | 9 (unchanged) |
| `dim_useragent` | 7 | 7 | 7 (unchanged) |
| `dbt_staging.fact_webrequest.csv` (export) | 35 → 24 | (same) | 24 |

The 24 surviving `fact_webrequest` cols (in current SELECT order): `raw_log_id`, `source_file`, `date_sk`, `time_sk`, `page_sk`, `method_sk`, `status_sk`, `referrer_sk`, `geolocation_sk`, `user_agent_sk`, `visitor_sk`, `visit_bucket_sk`, `bytes_sent`, `bytes_received`, `response_time_ms`, `request_count`, `is_404`, `is_crawler`, `is_direct_traffic`, `size_band`, `page_category`, `referrer_domain`, `traffic_type`, `time_band`.

## 4. Phased Plan

### Phase 1 — Code changes (~45 min)

1. **`airflow/spark/jobs/silver_enrichment.py`** — **NO CHANGES** to withColumn calls or UDFs. They stay so `dim_geolocation` and `dim_useragent` keep getting built. (Confirmed via `export_dimensions.py:139-185`.)
2. **`airflow/spark/databricks/02_silver_enrichment.py`** — remove the 5 UA cols from `CREATE TABLE` DDL (L129-133). Keep the 6 geo cols in the DDL (L123-128) and the 6 geo withColumn calls (L303-308) — they stay because Databricks `export_dimensions`-equivalent (Phase 2+ of the Databricks plan) will read silver for `dim_geolocation`.
3. **`airflow/spark/jobs/export_warehouse.py`** — edit `RAW_ENRICHED_DDL` (L73-84) to drop 11 cols. `postcode` at L78 stays. Edit `apply_type_casts` (L245-274) to remove the 2 lat/lon cast branches at L264-267.
4. **`airflow/spark/databricks/03_export_warehouse.py`** — edit Unity Catalog DDL (L121-131) to drop 11 cols.
5. **`airflow/dbt/w3c/models/staging/fact_webrequest.sql`** — in `geo_map` CTE (L47-58): KEEP `geolocation_sk` and `ip AS client_ip` (the join key to `raw_enriched.client_ip` at L175); DROP the 6 denorm geo cols from the SELECT. In `computed` CTE (L105-110): drop the 5 UA cols. In final SELECT (L141-151): drop all 11.
6. **`airflow/dbt/w3c/models/schema.yml`** — remove the 11 `description` blocks from `fact_webrequest.columns` (L246-267).

**Mid-migration state note:** After Phase 1 step 3, the writer produces 25-col silver-Delta-to-Postgres projections but the on-disk Delta table still has 36 cols from the prior run. This is benign because the JDBC writer reads from a fresh DataFrame (the silver pipeline overwrites the Delta on each run), and the new 25-col Postgres `raw_enriched` will be created fresh on a clean DB. On an existing DB, the migration script (Phase 2) must run **before** the next DAG run to shrink `raw_enriched` to 25 cols. dbt's incremental model rebuild will then read the new schema.

### Phase 2 — Test updates + migration (~30 min)

1. **`tests/test_export_warehouse.py`** — update `test_raw_enriched_ddl_has_all_36_columns` → `test_raw_enriched_ddl_has_all_25_columns`; remove `test_raw_enriched_ddl_has_type_casts` (L78-88) and the 4 lat/lon type-cast tests (L297-323, L375-402, L435-447). Update the count assertion at L99 from 36 → 25.
2. **`tests/test_integration.py`** — update `test_raw_enriched_has_type_cast_columns` (L119-137) to either remove lat/lon rows or remove the test entirely (it has only 2 cols).
3. **`tests/test_integration.py`** — add a new test `test_fact_webrequest_has_no_dead_cols` that queries `information_schema.columns` for `dbt_staging.fact_webrequest` and asserts the 11 dead col names are absent. (Optional but recommended for regression coverage.)
4. **`airflow/scripts/migrations/drop_denorm_fact_cols.sql`** (NEW) — `ALTER TABLE public.raw_enriched DROP COLUMN IF EXISTS country, region, city, latitude, longitude, isp, agent_type, browser_name, browser_version, operating_system, device_type;` + `COMMENT ON TABLE public.raw_enriched IS 'W3C silver enriched log rows; 25 cols (post-dead-col cleanup, see fact-webrequest-dead-col-cleanup.md)';`. Mounted into the Postgres container via the `initdb.d` volume in `docker-compose.yaml` (see Phase 5 step 3b).
5. **`airflow/scripts/migrations/add_denorm_fact_cols.sql`** (NEW) — the reverse migration, so rollback (Phase 7) is immediately available. Same SQL body as Phase 7's step 2.

### Phase 3 — Docs (~30 min)

1. **`README.md`** — using the full grep list in § 3.2, fix col-count claims to 25 (raw_enriched) and 24 (fact); remove `fact_webrequest` "geo denormalised" description at L618; remove the 11 col rows from "What a row looks like" (L738-758); update Mermaid ER diagram (L776-800) `country` annotation. The UDF-count references (L173, L492, L493) and "17 UDFs" claims are **unchanged** because UDFs themselves are not removed.
2. **`databricks-hybrid-wiring.md`** — update L221 from "Out of scope" to "Done — see [`fact-webrequest-dead-col-cleanup.md`](fact-webrequest-dead-col-cleanup.md)"; update L100/106/111 JDBC DDL counts (36→25 raw, 35→24 gold) if those numbers appear.
3. **`.agents/improvements.md`** — no changes (the Skills Matrix is unaffected; dead-col finding is internal cleanup, not a skill gap).
4. **`fact-webrequest-dead-col-cleanup.md`** — this plan; living document, version-bump to v2.1 when implementation lands.

### Phase 4 — Verification (~15 min)

1. `ruff check` and `ruff format --check` — must be clean
2. `mypy --ignore-missing-imports tests/` — must be clean (CI-equivalent)
3. `pytest tests/ -m "not integration and not dag_integrity" -k "not TestTrafficType"` — all unit tests pass (5 pre-existing TestTrafficType failures filtered out)
4. `dbt compile` in `airflow/dbt/w3c/` — no compile errors
5. `dbt run --full-refresh --select staging` in `airflow/dbt/w3c/` — staging models rebuild from scratch (full refresh is required because column drops can't be handled by incremental)
6. `dbt run --select marts` (incremental is fine; marts don't read dead cols) — all 5 marts build
7. `dbt test` in `airflow/dbt/w3c/` — all 23 data tests pass
8. `dbt source freshness` — confirm `raw_enriched` freshness is reported (no errors)

### Phase 5 — Clean Docker setup + DAG run #1 (~30 min)

1. `cd airflow && docker compose down -v` — tear down all named volumes
2. `rm -rf ./spark/delta/silver ./spark/delta/bronze` — **explicit** (Delta is a bind mount, not a named volume; `down -v` does NOT remove it)
3. `docker compose up -d` — bring up the full 13-container stack
4. Wait for healthchecks: `docker compose ps` until all 13 are `healthy` (or `Up` for non-healthcheck services)
5. Verify Postgres is up: `docker exec airflow-postgres pg_isready -U airflow`
6. Run the migration: the init container runs `airflow/scripts/migrations/drop_denorm_fact_cols.sql` automatically via the `initdb.d` mount. Verify: `docker exec airflow-postgres psql -U airflow -d w3c_warehouse -c "\d raw_enriched"` shows 25 cols.
7. Trigger `w3c_spark_ingestion` DAG: `docker exec airflow-scheduler airflow dags trigger w3c_spark_ingestion`
8. Wait for completion (~5-10 min)
9. Verify the DAG completed successfully in the Airflow UI; spot-check the `export_dimensions` task to confirm `dim_geolocation` still has the 9 cols including `postcode`.
10. Verify `public.raw_enriched` has 25 cols: `SELECT COUNT(*) FROM information_schema.columns WHERE table_name='raw_enriched' AND table_schema='public';` → 25.

### Phase 6 — DAG run #2 + end-to-end data verification (~30 min)

1. Trigger `w3c_spark_ingestion` DAG a second time (the `dbt_marts` DAG will be triggered automatically by the Dataset signal).
2. Wait for both DAGs to complete.
3. Verify:
   - `dbt test` — all 23 tests pass on the rebuilt warehouse
   - **Data integrity check #1:** `SELECT COUNT(*) FROM public.raw_enriched` — same count on run #1 and run #2 (idempotency)
   - **Data integrity check #2:** `SELECT column_name FROM information_schema.columns WHERE table_name = 'raw_enriched' AND table_schema = 'public' ORDER BY ordinal_position` — exactly 25 columns, none of the 11 dead cols present
   - **Data integrity check #3:** `SELECT column_name FROM information_schema.columns WHERE table_name = 'fact_webrequest' AND table_schema = 'dbt_staging' ORDER BY ordinal_position` — exactly 24 columns, none of the 11 dead cols present
   - **Data integrity check #4:** `SELECT COUNT(*) FROM dbt_staging.fact_webrequest` — same count on run #1 and run #2
   - **Data integrity check #5:** For each of the 5 marts — `SELECT COUNT(*) FROM dbt_marts.<mart_name>` — same count on run #1 and run #2
   - **Data integrity check #6:** `SELECT browser_name, SUM(total_requests) FROM dbt_marts.mart_browser_analysis GROUP BY browser_name` — row count and totals match between run #1 and run #2 (idempotency); spot-check that the output is non-empty (the dim join works)
   - **Data integrity check #7:** `head -1 airflow/data/Star-Schema/dbt_staging.fact_webrequest.csv` — header is exactly 24 cols, no denorm col names
   - **Data integrity check #8:** `head -1 airflow/data/Star-Schema/dbt_marts.mart_browser_analysis.csv` — same as before (mart schema unchanged)
   - **Data integrity check #9 (NEW):** `head -1 airflow/data/Star-Schema/public.dim_geolocation.csv` — `postcode` column still present (confirms the silver→dim path is intact)
   - **Data integrity check #10 (NEW):** `dbt source freshness` — `raw_enriched` reported as fresh
   - **Data integrity check #11 (NEW):** sample 5 rows from `dbt_staging.fact_webrequest` and confirm `geolocation_sk`, `user_agent_sk`, `date_sk` are all positive integers (not 0 or -1) — verifies the dim joins are actually populating FKs
   - **Data integrity check #12 (NEW):** `SELECT COUNT(*) FROM dbt_marts.mart_daily_aggregates WHERE active_countries > 0` — non-zero count, confirming the `JOIN dim_geolocation` for country still works

### Phase 7 — Rollback runbook (always present, no time estimate)

If a Phase 4/5/6 failure requires rollback after the migration has been applied to a deployed environment:

1. **Code rollback:** `git revert <merge-commit>` restores all code changes.
2. **DB rollback:** The reverse migration is `airflow/scripts/migrations/add_denorm_fact_cols.sql` (created in Phase 2 alongside the drop script):
   ```sql
   ALTER TABLE public.raw_enriched
     ADD COLUMN IF NOT EXISTS country TEXT,
     ADD COLUMN IF NOT EXISTS region TEXT,
     ADD COLUMN IF NOT EXISTS city TEXT,
     ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION,
     ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION,
     ADD COLUMN IF NOT EXISTS isp TEXT,
     ADD COLUMN IF NOT EXISTS agent_type VARCHAR(20),
     ADD COLUMN IF NOT EXISTS browser_name VARCHAR(50),
     ADD COLUMN IF NOT EXISTS browser_version VARCHAR(50),
     ADD COLUMN IF NOT EXISTS operating_system VARCHAR(50),
     ADD COLUMN IF NOT EXISTS device_type VARCHAR(20);
   ```
   These cols will be NULL until the next DAG run re-populates them via the (now-restored) writer.
3. **Delta Lake rollback:** `rm -rf ./spark/delta/silver` to force a clean Delta rebuild from the restored writer.
4. **dbt rollback:** `dbt run --full-refresh --select staging` to rebuild the fact with the restored 35-col schema.
5. **Power BI:** re-trigger the dataset refresh.

**Recovery time:** ~15 min. The migration is forward + backward idempotent (`DROP COLUMN IF EXISTS` / `ADD COLUMN IF NOT EXISTS`).

## 5. Out of Scope

- **Power BI dataset re-binding** — user must verify with the Power BI owner that no semantic model binds directly to the denorm fact cols. The CSV exports in `airflow/data/Star-Schema/` are auto-regenerated by dbt_marts (wildcard SELECT *) so no Power BI re-config is needed IF Power BI consumes the CSVs (per README L1133).
- **Phase 1+ of the Databricks hybrid plan (JDBC export, DAG wiring, etc.)** — separate plan; this cleanup reduces the silver→Postgres payload by 11 cols, which makes the Databricks plan simpler.
- **Cleanup of other potential dead columns in marts or dims** — out of scope; only fact_webrequest is audited.

## 6. Risk Register

| # | Risk | Severity | Likelihood | Mitigation |
|---|---|---|---|---|
| 1 | `public.raw_enriched` keeps 11 cols on existing DBs because `IF NOT EXISTS` is a no-op | High | High | Phase 2 adds `ALTER TABLE ... DROP COLUMN IF EXISTS` migration script + Phase 5 step 6 runs it before DAG run #1 |
| 2 | `apply_type_casts` crashes on missing lat/lon cols | High | High | Phase 1 step 3 explicitly removes the lat/lon cast branches |
| 3 | Silver Delta table has stale 36-col schema on a clean restart | High | Medium | Phase 5 step 2 explicitly `rm -rf ./spark/delta/silver` (bind mount, not a named volume) |
| 4 | Power BI dataset binds to denorm fact cols | Medium | Low | README's "Power BI geographic dashboard" already uses the dim JOIN; user confirms with Power BI owner before merge |
| 5 | `dbt run` incremental fails because dbt's incremental strategy can't drop cols | Medium | Low | Phase 4 step 5 uses `--full-refresh` on first build; subsequent runs are incremental |
| 6 | `test_export_warehouse.py:99` col-count parser is fragile | Low | Low | The DDL will be hand-edited; the parser still works for the new 25-col layout |
| 7 | Pre-existing `TestTrafficType` worker-side import failures (5 tests) confuse the verification | Low | Low | Pre-existing on main; the verification step filters them out with `-k "not TestTrafficType"` |
| 8 | DAG run #1 fails because `geoip-downloader` init service is slow | Low | Medium | Phase 5 step 4 waits for healthchecks; geoip init runs as a separate container with a dependency condition |
| 9 | The Marts don't get rebuilt after the staging model is full-refreshed | Low | Low | `dbt run --select marts` in Phase 4 step 6 is explicit; Phase 6 verifies mart row counts |
| 10 | `geo_map` CTE join breaks if `ip AS client_ip` is removed | High | High | Phase 1 step 5 explicitly retains `ip AS client_ip` in the modified CTE |

## 7. Acceptance Criteria

1. Working tree is clean after `git restore` of the 5 reverted files (Phase 1 of the Databricks plan is gone) AND the new cleanup changes are applied.
2. `ruff check`, `ruff format --check`, `mypy tests/`, `pytest tests/`, `dbt compile`, `dbt run --full-refresh`, `dbt test`, `dbt source freshness` all pass.
3. `public.raw_enriched` has exactly 25 columns after the migration script runs (verified via `information_schema.columns`).
4. `dbt_staging.fact_webrequest` has exactly 24 columns after `dbt run` (verified via `information_schema.columns`).
5. `dim_geolocation` still has 9 columns including `postcode` (verified via CSV header and `information_schema.columns`).
6. DAG run #1 completes successfully; all 4 DAG tasks (`bronze_ingestion`, `silver_enrichment`, `export_warehouse`, `export_dimensions`) succeed.
7. DAG run #2 completes successfully with identical row counts in `raw_enriched`, `fact_webrequest`, and all 5 marts.
8. `dbt_staging.fact_webrequest.csv` has a 24-column header with no denorm col names.
9. `mart_browser_analysis` still produces non-empty output (verifying the dim join works).
10. None of the 11 dead cols appear anywhere in `fact_webrequest`, `raw_enriched`, or dbt marts (verified via `information_schema.columns` queries).
11. README and `databricks-hybrid-wiring.md` are updated; no 35/36/35 col-count contradictions remain.
12. The new `test_fact_webrequest_has_no_dead_cols` test passes on the rebuilt warehouse (if added in Phase 2 step 3).
13. Rollback runbook (Phase 7) is documented and tested on a non-prod environment before merge.

## 8. Implementation Order

```
Phase 1 (code) ─┬─→ Phase 2 (tests + migration) ─┬─→ Phase 3 (docs + rollback doc) ─→ Phase 4 (verify) ─→ Phase 5 (DAG run #1) ─→ Phase 6 (DAG run #2 + data verify)
                │                                 │
                └─→ can run in parallel with Phase 3, but Phase 2 depends on Phase 1's exports
```

- Phases 1-4 are mechanical and subagent-driven (spec + code review per the skill).
- Phases 5-6 require user-in-the-loop because the user must:
  - Confirm Docker is available and not in use
  - Trigger the DAGs and observe the UI
  - Run the verification queries themselves (or sign off on me running them)
  - Confirm the Power BI owner has been notified
- Phase 7 (rollback runbook) is created alongside Phase 3 (step 4), finalized in Phase 4, and validated on a non-prod env.

## 9. Subagent Review Plan

Per the `subagent-driven-development` skill:

1. ✅ **Spec compliance review** (subagent) — does the plan match the user's request?
2. ✅ **Code quality review** (subagent) — passed after v2 revisions (4 critical + 6 important + 5 minor addressed)
3. ⏳ **User review** (you) — does the v2 plan look right? Any concerns?

Only after the third review do I dispatch the implementation subagent.

## 10. Test Plan (recap)

| Layer | Command | Expected |
|---|---|---|
| Lint | `ruff check .` | All checks passed |
| Format | `ruff format --check .` | All files already formatted |
| Type | `mypy --ignore-missing-imports tests/` | Success: no issues found in N source files |
| Unit | `pytest tests/ -m "not integration and not dag_integrity" -k "not TestTrafficType"` | 100% of non-pre-existing tests pass |
| dbt compile | `dbt compile` (in `airflow/dbt/w3c/`) | 0 errors |
| dbt run (staging) | `dbt run --full-refresh --select staging` | 10 staging models built |
| dbt run (marts) | `dbt run --select marts` | 5 marts built |
| dbt test | `dbt test` | 23 tests passed |
| dbt source freshness | `dbt source freshness` | raw_enriched reported as fresh |
| DAG #1 | `airflow dags trigger w3c_spark_ingestion` | success |
| DAG #2 | `airflow dags trigger w3c_spark_ingestion` | success, idempotent row counts |
| Data | 12 SQL queries listed in § 4 Phase 6 | all assertions hold |
| Rollback | Apply reverse migration on a test DB | cols restored, DAG runs, fact rebuilds |
