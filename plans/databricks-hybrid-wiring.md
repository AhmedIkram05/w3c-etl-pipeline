# Plan: Databricks Hybrid Wiring (Priority 3a)

> **Status:** v1.1
> **Source doc:** [improvements.md](./improvements.md) § Priority 3a
> **Estimated effort:** 3–3.75 hours | **Expected impact:** +1 row in skills matrix (Cloud platforms D+ → B), +3 resume keywords (Databricks, Unity Catalog, hybrid execution)
> **Closes:** Gap 3 (Cloud-Native Services, currently the only 🟡 PARTIALLY RESOLVED critical gap)

---

## 1. Goal

Wire the existing `airflow/spark/databricks/01–03_*.py` scripts into the `w3c_spark_ingestion` DAG via `DatabricksSubmitRunOperator`, with an Airflow-Variable toggle that switches the entire bronze→silver→export chain between **Docker Spark** and **Databricks** execution at runtime — **without breaking the downstream dbt_marts dataset contract**.

The Databricks chain must end with a JDBC write to PostgreSQL so the existing `public.raw_enriched` source that dbt depends on stays populated. The Unity Catalog Gold table is preserved as a bonus analytics layer for BI tools that can connect directly to Databricks.

---

## 2. Architecture Decision

Three approaches were considered. **Option C is recommended.**

| Option | Bronze | Silver | Export to PG | Export to UC Gold | dbt works? | Effort |
|---|---|---|---|---|---|---|
| **A. Pure Databricks** | Databricks | Databricks | ✗ (UC only) | ✓ | ✗ broken | smallest, but breaks downstream |
| **B. Bronze+Silver on DBX, Export on Docker** | Databricks | Databricks | Docker Spark JDBC | ✗ | requires Delta → S3/ADLS sync | large infra change |
| **C. Bronze+Silver+JDBC on DBX, Docker unchanged** | Databricks | Databricks | **NEW: Databricks JDBC task** | ✓ (bonus) | ✓ | small, self-contained ✓ |

**Why C:** the dbt DAG triggers on a Dataset emitted *after* `export_dimensions` runs. As long as `public.raw_enriched` is populated before that task, the contract holds. Option C adds a single new Databricks task (JDBC export) that mirrors the Docker `export_warehouse.py` behaviour but reads from `w3c_catalog.silver.enriched_logs` and writes via the Databricks cluster's JDBC connection back to the existing PostgreSQL.

**Mode toggle:** an Airflow Variable `SPARK_EXECUTION_MODE ∈ {docker, databricks}`. The DAG builds its task list at parse time based on this variable — no runtime branching inside operators.

**Default for local dev:** `docker` (no Databricks workspace required). CI unaffected (still runs Docker path). `databricks` mode activates only when a real workspace is configured.

---

## 3. Current State Audit

### 3.1 What already exists ✅

| File | Status | Notes |
|---|---|---|
| `airflow/spark/databricks/01_bronze_ingestion.py` | ✅ ready | 285 lines, inline W3C parser, reads DBFS, writes `w3c_catalog.bronze.raw_logs` |
| `airflow/spark/databricks/02_silver_enrichment.py` | ✅ ready | 358 lines, inline GeoIP + 4 computed-field UDFs; CREATE TABLE declares 5 UA columns (left unpopulated — see § 3.4 dead-column finding) |
| `airflow/spark/databricks/03_export_warehouse.py` | ✅ ready | 263 lines, writes to `w3c_catalog.gold.warehouse_enriched` with Delta tracking table |
| README § Databricks Integration (L868) | ✅ documented | 38-line section explaining the scripts, Unity Catalog paths, Docker vs DBX differences |
| AGENTS.md / improvements.md § Priority 3a | ✅ scoped | "Scaffolding done, wiring planned" |

### 3.2 What is missing ❌

| Gap | Impact |
|---|---|
| No `DatabricksSubmitRunOperator` import / task in any DAG | scripts are not runnable from Airflow |
| `apache-airflow-providers-databricks` not in `airflow/requirements.txt` | operator would ImportError at parse time |
| No `databricks_default` Airflow Connection defined | no way for the operator to authenticate |
| No `04_jdbc_export.py` Databricks script | no path from `silver.enriched_logs` to PostgreSQL |
| No `SPARK_EXECUTION_MODE` Airflow Variable | no way to switch modes |
| No test coverage for hybrid DAG or mode toggle | regression risk |

> **Note on schema parity:** Docker silver (`airflow/spark/jobs/silver_enrichment.py`) writes 36 columns (includes `postcode`); Databricks silver writes 30 columns (6 geo cols + 24 base). The 11 denormalized geo/UA columns have been removed from the JDBC writer DDL (see [fact-webrequest-dead-col-cleanup.md](plans/fact-webrequest-dead-col-cleanup.md)). Both pipelines write to the same `public.raw_enriched` table; the Docker version's 25-col JDBC output and the Databricks version's 24-col JDBC output will agree on all columns the other has.

### 3.3 Constraints discovered

- **Airflow version:** `apache/airflow:2.10.2` base image. `DatabricksSubmitRunOperator` is in `apache-airflow-providers-databricks` ≥ 4.0 — compatible.
- **CI runs Docker mode only** — no Databricks workspace available in CI, so the Databricks branch must be importable but the operator calls must be guarded (or use mock connections in tests).
- **`mypy` ignores `airflow.dags.w3c.*`** in `pyproject.toml` L28-30 — adding new operators won't fail type checks but the imports must be valid at DAG-parse time.
- **Pytest markers:** `pytest -m "not integration and not dag_integrity"` in CI (L48 of `ci.yml`). New tests for mode toggle should NOT use those markers (they must run in CI).
- **Spark connection pattern:** Docker uses `AIRFLOW_CONN_SPARK_DEFAULT` env var injecting JSON `{"conn_type": "spark", "host": "spark://..."}` (docker-compose L80). The Databricks connection follows the same pattern: `AIRFLOW_CONN_DATABRICKS_DEFAULT`.

### 3.4 Dead-column finding (UA enrichment not required)

**Finding:** the 5 denormalized UA columns in `fact_webrequest` (`agent_type`, `browser_name`, `browser_version`, `operating_system`, `device_type`) are **write-only dead code**. They are populated by the Docker silver pipeline's UDFs and written to `raw_enriched` → `fact_webrequest`, but **no consumer reads them**:

| Consumer | How `fact_webrequest` is consumed | Denorm UA cols read? |
|---|---|---|
| `mart_page_performance` | aggregates from `fact_webrequest` + dims | ✗ |
| `mart_daily_aggregates` | aggregates from `fact_webrequest` + dim_date | ✗ |
| `mart_crawler_analysis` | aggregates by `is_crawler` boolean | ✗ |
| `mart_browser_analysis` | joins `fact_webrequest.user_agent_sk` → `dim_useragent` (parsed there by Airflow operator) | ✗ |
| `mart_timeofday_analysis` | aggregates by time_sk + dim_time | ✗ |
| Power BI reports | read CSVs of marts + dims only, never `fact_webrequest` directly | ✗ |
| dbt test `test_fact_referential integrity.sql` | asserts 9 FKs to dims | ✗ |
| dbt test `test_dimension_coverage.sql` | counts dim rows | ✗ |
| dbt test `test_fact_webrequest_dedup_safety.sql` | dedup on `uri_query/method/win32_status` | ✗ |
| dbt test `test_row_count_consistency.sql` | row counts only | ✗ |

`dim_useragent` itself is built by the Airflow `export_dimensions` operator (`airflow/plugins/operators/export_dimensions.py:286`), which uses the `user-agents` library to parse the raw `user_agent` string from `raw_enriched` directly — **independent of any silver-pipeline UDFs**.

**Implication:** Phase 1 (adding 5 UA UDFs to the Databricks silver script) was originally scoped to "close the schema gap." After this audit, the gap is revealed as write-only dead code in both Docker and Databricks modes. **Phase 1 removed from the plan** (was 15 min). The 5 UA columns in `02_silver_enrichment.py`'s `CREATE TABLE` have been removed from the DDL in the cleanup (see [fact-webrequest-dead-col-cleanup.md](plans/fact-webrequest-dead-col-cleanup.md) Phase 1 step 2).

**Cleanup done:** the 11 denormalized columns (6 geo + 5 UA) have been removed from the JDBC writer DDL, dbt fact model, and migration scripts via the cleanup plan. The silver pipeline still populates them for `dim_geolocation` / `dim_useragent` feeding — only the PostgreSQL projection shrunk. See [fact-webrequest-dead-col-cleanup.md](plans/fact-webrequest-dead-col-cleanup.md) for full details.

---

## 4. Phased Plan

Each phase is independently mergeable. Phases 1–2 are the minimum viable wiring; phases 3–5 add quality, doc, and CI.

### Phase 1 — Add JDBC export script (45 min)

**Goal:** new `04_jdbc_export.py` that reads from `w3c_catalog.silver.enriched_logs` and writes 24 columns to `public.raw_enriched` via Spark JDBC — matching the Docker `export_warehouse.py` 25-col schema (minus the 1-column `postcode` difference documented in § 3.2, and minus the 11 denormalized cols cleaned up in [fact-webrequest-dead-col-cleanup.md](plans/fact-webrequest-dead-col-cleanup.md)).

- [ ] Create `airflow/spark/databricks/04_jdbc_export.py`:
  - Reuse the incremental pattern from `03_export_warehouse.py` (tracking via `w3c_catalog.gold.export_tracking`)
  - Read from `w3c_catalog.silver.enriched_logs`
  - JDBC write to `jdbc:postgresql://postgres:5432/w3c_warehouse` (configurable via env or `--jdbc-url` arg)
  - `CREATE TABLE IF NOT EXISTS` using a 24-column DDL (Databricks gold schema — no `postcode`, no denorm cols)
  - Use `.mode("append")` to preserve incremental semantics
  - Mirror the Docker version's tracking-table approach (PostgreSQL `public.export_tracking` if the Docker version uses one, OR a Databricks-side tracking table for parity)
- [ ] Decision: which tracking table? **Recommendation:** use a separate `w3c_catalog.gold.jdbc_export_tracking` table — keeps the Gold UC tracking isolated from the JDBC export tracking. Two tracking tables, two jobs.

**Acceptance:** running the script standalone on a Databricks cluster with a populated silver table and a reachable PostgreSQL populates `public.raw_enriched` with all 24 columns. Re-running with no new source files is a no-op.

### Phase 2 — Wire into the DAG (60 min)

**Goal:** the existing `w3c_spark_ingestion` DAG branches at parse time on `SPARK_EXECUTION_MODE` and uses either the Docker `SparkSubmitOperator` chain or a new `DatabricksSubmitRunOperator` chain. Both chains converge on `export_dimensions` (PythonOperator, emits the Dataset).

- [ ] Edit `airflow/dags/w3c/spark_ingestion.py`:
  - Read `Variable.get("SPARK_EXECUTION_MODE", default_var="docker")` at module top
  - Add a `_build_databricks_tasks(mode)` helper that returns a dict of 4 tasks: `databricks_bronze_ingestion`, `databricks_silver_enrichment`, `databricks_export_warehouse` (Gold UC), `databricks_jdbc_export` (PostgreSQL)
  - If `mode == "databricks"`: instantiate the 4 DBX tasks; the Docker `bronze_ingestion`, `silver_enrichment`, `export_warehouse` tasks are NOT created (avoids dummy `EmptyOperator` placeholders, keeps the graph clean)
  - If `mode == "docker"`: keep current behaviour (4 Docker tasks: bronze, silver, export, dimensions)
  - Wire dependencies in both modes so the last task before `export_dimensions` is the JDBC-populating task
  - Document the mode in the DAG `description` field
- [ ] Add `DatabricksSubmitRunOperator` import — guard with try/except so CI (no provider installed) doesn't crash at parse time. **Better:** add the provider package to `airflow/requirements.txt` unconditionally; CI's `test` job doesn't import DAGs anyway (it runs `-m "not integration and not dag_integrity"`).

**Operator arguments for `DatabricksSubmitRunOperator`:**

```python
DatabricksSubmitRunOperator(
    task_id="databricks_bronze_ingestion",
    databricks_conn_id="databricks_default",
    existing_cluster_id=Variable.get("DATABRICKS_EXISTING_CLUSTER_ID", default_var=""),
    notebook_task={
        "notebook_path": Variable.get("DATABRICKS_NOTEBOOK_PATH_BRONZE",
                                       default_var="/Shared/w3c/01_bronze_ingestion"),
        "base_parameters": {"log_files_dir": "dbfs:/mnt/w3c-logs/LogFiles"},
    },
    new_cluster=Variable.get("DATABRICKS_NEW_CLUSTER_JSON", default_var=NEW_CLUSTER_JSON_DEFAULT),
    timeout_seconds=3600,
    dag=dag,
)
```

**Acceptance:** `airflow dags list` shows `w3c_spark_ingestion` in both modes. Switching the variable and re-parsing produces a DAG with 4 Databricks tasks (no Docker tasks) in `databricks` mode, or 4 Docker tasks in `docker` mode. Both DAGs have a `export_dimensions` terminal task emitting the WAREHOUSE_LOADED dataset.

### Phase 3 — Airflow connection + variable bootstrap (15 min)

- [ ] Add `AIRFLOW_CONN_DATABRICKS_DEFAULT` to `airflow/docker-compose.yaml` x-airflow-common env block (mirror the `SPARK_DEFAULT` pattern at L80):

  ```yaml
  AIRFLOW_CONN_DATABRICKS_DEFAULT: '{"conn_type": "databricks", "host": "${DATABRICKS_HOST:-}", "extra": {"token": "${DATABRICKS_TOKEN:-}"}}'
  ```

- [ ] Add to `.env.example` (create if absent):

  ```
  DATABRICKS_HOST=adb-xxxx.azuredatabricks.net
  DATABRICKS_TOKEN=
  DATABRICKS_EXISTING_CLUSTER_ID=
  ```

- [ ] Add Airflow Variable bootstrap script `airflow/scripts/set_spark_mode.sh`:

  ```bash
  airflow variables set SPARK_EXECUTION_MODE docker   # or databricks
  ```

  Document in README § Quick Start.

**Acceptance:** `docker-compose up` succeeds. `airflow variables get SPARK_EXECUTION_MODE` returns `docker` by default. Setting it to `databricks` and restarting the scheduler causes the DAG to re-parse with the new branch.

### Phase 4 — Provider dependency + test strategy (45 min)

- [ ] Add `apache-airflow-providers-databricks>=6.0,<7.0` to `airflow/requirements.txt` (compatible with Airflow 2.10)
- [ ] Add `apache-airflow-providers-databricks` to `tests/requirements-test.txt` so the import works in `test_dag_integrity.py`
- [ ] Write `tests/test_spark_execution_mode.py` (NEW — unit, runs in CI):
  - **Test 1:** `mode = "docker"` → DAG contains 4 `SparkSubmitOperator` tasks, 0 `DatabricksSubmitRunOperator` tasks
  - **Test 2:** `mode = "databricks"` → DAG contains 4 `DatabricksSubmitRunOperator` tasks, 0 `SparkSubmitOperator` tasks
  - **Test 3:** `mode = "databricks"` → `databricks_jdbc_export` precedes `export_dimensions` in topological order
  - **Test 4:** `mode = "invalid"` → ValueError raised at DAG parse (fail fast, not silent)
  - Use `monkeypatch.setenv` + reload of the DAG module, or refactor `spark_ingestion.py` to expose a `build_dag(mode: str)` factory function and test the factory directly
- [ ] Add a test to `tests/test_dag_integrity.py` that asserts the DAG parses in both modes (parametrize on `mode`)
- [ ] Update `test_dag_integrity.py` task-count assertion if it was hard-coded to 4

**Acceptance:** `pytest tests/test_spark_execution_mode.py -v` passes 4/4. `pytest tests/test_dag_integrity.py -v` passes. CI green.

### Phase 5 — Documentation updates (30 min)

- [ ] **README.md**:
  - Add `## Hybrid Execution (Docker ↔ Databricks)` subsection under existing `### Databricks Integration` (after L904, before `---` on L906)
  - Include: Mermaid flowchart of the Databricks branch, environment variable table (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_EXISTING_CLUSTER_ID`, `SPARK_EXECUTION_MODE`), 5-line "How to switch modes" snippet
  - Add `Databricks` to the badge strip at L7-22
  - Update TL;DR § "Databricks-ready" bullet (L75) from "Self-contained Unity Catalog scripts" → "Hybrid Docker/Databricks execution via `DatabricksSubmitRunOperator` — toggle with `SPARK_EXECUTION_MODE`"
  - Update Container Topology diagram if it shows only Docker services
- [ ] **`.agents/improvements.md`**:
  - Move "Priority 3a" from 🟡 SCAFFOLDING DONE → ✅ COMPLETED with the new wiring summary
  - Update "Gap 3" status from 🟡 PARTIALLY RESOLVED → ✅ RESOLVED
  - Update "Cloud platforms" row in Skills Coverage Matrix: D+ → B
  - Update "Spark/PySpark" row if hybrid execution is mentioned
- [ ] **`.agents/architecture-v3.md`** (if it has a Databricks section) — add the hybrid wiring
- [ ] **CHANGELOG** (if reinstated) — entry for v3.1.0

**Acceptance:** README renders correctly (mermaid). improvements.md reflects new state. Badges show Databricks.

### Phase 6 — Final verification (15 min)

- [ ] Run `ruff check --fix .` and `ruff format .`
- [ ] Run `mypy --ignore-missing-imports tests/` — must pass
- [ ] Run `pytest -m "not integration and not dag_integrity"` — must pass (133+ existing + 4 new = 137+)
- [ ] Run `pytest -m dag_integrity` locally if Docker stack up — must pass in both modes
- [ ] Run `dbt compile --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt` — must pass (unchanged contract)
- [ ] Manual: switch to `databricks` mode, parse the DAG, verify task list in Airflow UI

**Acceptance:** all quality gates green, DAG parses in both modes, documentation updated.

---

## 5. Out of Scope (Future Work)

These are intentionally **not** part of this plan — they belong to a later priority or a separate initiative:

- **Priority 3 (full AWS/GCP + Terraform):** S3, EMR/Dataproc, Redshift/BigQuery, IaC modules — 2-3 weeks of work, separate plan
- **Streaming (Gap 8):** Kafka, Kinesis, Pub/Sub, Flink — not a graduate-role deal-breaker
- **Live Databricks workspace testing:** we have no live DBX credentials in CI; the integration test is the Docker path. Manual verification on a real workspace is post-merge work
- **Notebook-based vs script-based execution:** the plan uses `notebook_task`. The Databricks scripts also work as `spark_python_task` — left as a future optimisation
- **Auto-cluster mode:** the plan uses `existing_cluster_id` (cheaper, faster). New-cluster JSON is a config var fallback for ad-hoc testing
- **CI matrix testing of both modes:** the CI test job runs once in `docker` mode (the default). Testing the `databricks` mode in CI requires a mock connection, which the `test_spark_execution_mode.py` tests effectively provide
- ~~**Cleanup of write-only denorm cols in `fact_webrequest`:** the 5 UA + 6 geo denorm columns are dead code (see § 3.4)~~ **Done.** Cleanup completed in [fact-webrequest-dead-col-cleanup.md](plans/fact-webrequest-dead-col-cleanup.md) — all 11 denorm cols removed from JDBC writer DDL, dbt fact model, and migration scripts.

---

## 6. Risk Register

| Risk | Probability | Impact | Mitigation |
|---|---|---|---|
| Databricks workspace unreachable from Airflow worker | Medium | DAG fails on first run | `existing_cluster_id` is required; `notebook_task` retries 3× by default; fail-fast logging in `04_jdbc_export.py` |
| Schema drift between Docker and Databricks silver tables | Medium | JDBC export fails | Phase 1 uses the 35-col Databricks DDL; document the 1-col `postcode` difference in § 3.2; integration test asserts parity for the 35 shared columns |
| DAG parse failure in CI when `apache-airflow-providers-databricks` missing | Low | CI broken | Add provider to `airflow/requirements.txt` AND `tests/requirements-test.txt` in Phase 4 |
| Spark submit operator has different retry semantics than Databricks | Low | Silent failure mode | Document in DAG docstring; add a one-line "If you switch modes, also review `retries` and `retry_delay` in default_args" |
| `SPARK_EXECUTION_MODE` Variable changed mid-run | Low | Confusing failure | Airflow Variables are read at DAG parse time, not per-task; document the "restart scheduler after changing" requirement in README |

---

## 7. Acceptance Criteria (Definition of Done)

The plan is "done" when **all** of the following are true:

1. `airflow dags list` shows `w3c_spark_ingestion` parses successfully in both `docker` and `databricks` modes
2. CI passes: `ruff check`, `ruff format --check`, `mypy tests/`, `pytest -m "not integration and not dag_integrity"` (137+ tests), `dbt compile`
3. `tests/test_spark_execution_mode.py` covers all 4 mode-toggle cases
4. `04_jdbc_export.py` standalone-execution verified (manual, on a real Databricks workspace — post-merge)
5. `improvements.md` reflects Gap 3 = RESOLVED, Priority 3a = COMPLETED, Cloud platforms = B
6. README has a "Hybrid Execution" subsection with Mermaid diagram + env var table
7. `DATABRICKS` badge added to README header
8. `git log` shows a single coherent commit chain (1 commit per phase preferred, but a single squashed commit acceptable)

---

## 8. Implementation Order (Suggested)

If working in a single session, this is the dependency-respecting order:

```
Phase 1 (JDBC script)    ──┐
                             ├──► Phase 4 (tests)   ──► Phase 6 (verify)
Phase 2 (DAG wiring)    ──┤
                             ├──► Phase 5 (docs)
                             │
                             │
Phase 3 (config)         ──► (parallel with Phase 2)
```

Phases 1–2 must be sequential (JDBC export → DAG wiring).
Phase 3 (config) can run in parallel with Phase 2.
Phase 4 (tests) is gated on Phase 2 being done.
Phase 5 (docs) is gated on Phase 2 + 3 being done.
Phase 6 (verify) is the final gate.

---

## 9. Resume Line (When Complete)

> *"Designed a hybrid ETL pipeline supporting both Docker-based Spark and Databricks execution with Unity Catalog governance, switchable at runtime via Airflow Variables without modifying the downstream dbt contract."*

---

## 10. References

- Source doc: `.agents/improvements.md` § Priority 3a
- Architecture: `.agents/architecture-v3.md` (Dataset-driven DAG contract)
- Current DAG: `airflow/dags/w3c/spark_ingestion.py` (250 lines, 4 tasks)
- Databricks scaffolding: `airflow/spark/databricks/01_*.py` → `03_*.py`
- Docker equivalents: `airflow/spark/jobs/{bronze,silver,export}_*.py`
- Airflow Databricks provider docs: <https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators.html>
