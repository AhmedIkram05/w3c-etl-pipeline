# Testing Suite Reference

> **Purpose:** Inventory of every test layer and framework in the W3C ETL Pipeline — designed to feed a "Testing" section in the README for recruiters.
>
> **Related:** Pre-commit hooks and CI/CD workflow details live in the [CI/CD & Pre-commit Hooks Reference](./ci-cd-reference.md).

---

## 1. Overview

| Layer | Framework | Count | CI Stage | Visibility |
|-------|-----------|-------|----------|------------|
| Python lint | ruff (lint + format) | — | `lint` | ✅ Always runs |
| Type checking | mypy | — | `lint` | ✅ Always runs |
| Security scan | bandit | — | `lint` | ✅ Always runs |
| SQL lint | SQLFluff | — | `lint` | ✅ (continue-on-error) |
| Python unit tests | pytest | 305 total / 275 CI† | `test` | ✅ Always runs |
| dbt compile (PostgreSQL) | dbt compile | — | `dbt-compile` (CI) | ✅ |
| dbt compile (T-SQL/Azure SQL) | dbt compile + pytest | 12 | `dbt-compile` (CI) | ✅ |
| dbt data tests | dbt test (YAML) | 118 | `deploy-dbt` (CD) | ✅ |
| Terraform validate/fmt | terraform | — | `terraform` (CI) | ✅ |
| Terraform tests | terraform test (HCL) | 9 assertions (1 plan + 6 resource) | `terraform` (CI) | ✅ |
| CodeQL SAST | GitHub CodeQL | — | `codeql` | ✅ Weekly |
| Secret scanning | GitGuardian | — | External (org-level) | ✅ Every push |
| Post-deploy smoke test | Bash (REST API) | 1 | `smoke-test` (CD) | ✅ On merge to main |

† 30 tests are excluded from CI runs: 18 integration tests (need Docker stack) + 12 dbt-compile validation tests (need `dbt compile` to run first). These are expected environment gaps, not failures.

---

> **Pre-commit hooks moved to [CI/CD & Pre-commit Hooks Reference](./ci-cd-reference.md#pre-commit-hooks).**

## 3. Python Unit Tests (pytest — 305 total, 275 in CI)

### 3.1. Framework & Configuration

- **pytest** with `pytest-cov` for coverage
- **conftest.py** provides a shared `spark` fixture (local PySpark `SparkSession` with Delta Lake support)
- Sophisticated `sys.path` management in `conftest.py`:
  - Strips the project root from `sys.path` to avoid PEP 420 namespace shadowing (the project's `airflow/` directory has no `__init__.py`, so Python 3.3+ treats it as a namespace package — adding it to `sys.path` would break `import airflow.models`)
  - Resolves two possible directory layouts (bare metal vs. Docker volume mounts) for cross-environment compatibility
  - Adds only the specific subdirectories needed (`spark/jobs/`, `dags/`, `plugins/`) to enable fully-qualified imports
  - Builds a `utils.zip` from `spark/jobs/utils/` and ships it to PySpark workers via `SparkContext.addPyFile()` so UDFs deserialize correctly — mirroring the production `py_files` pattern used in `SparkSubmitOperator`
- **Test markers** for selective filtering — a deliberate design that lets CI exclude environment-dependent suites while keeping them available locally:

  | Marker | Tests | Purpose |
  |--------|-------|---------|
  | _(unmarked)_ | 183 | Core unit tests — always run in CI |
  | `@integration` | 18 | Docker-dependent E2E tests (full Spark→PostgreSQL→dbt flow) |
  | `@dbt_compile` | 12 | Validates compiled T-SQL/Azure SQL output — needs `dbt compile` first |
  | `@dag_integrity` | 5 | Airflow DAG loader tests — verifies import, task graph, required args |
  | `@terraform` | 12 | Requires `terraform` binary — validates config with mock providers |
  | **Total** | **305** | |

- **Parametrized tests**: `test_terraform_part_a.py` (5 `@parametrize` decorators) and `test_terraform_part_b.py` (4) multiply individual test functions across multiple input combinations, increasing coverage without code duplication.
- Coverage uploaded to **Codecov** on every CI run (`fail_ci_if_error: false` — non-blocking; coverage trends visible to the team)

### 3.2. Test Breakdown by File

| File | Tests | Classes | Layer | What It Validates |
|------|-------|---------|-------|-------------------|
| `test_dlt_silver.py` | 91 | 15 | Silver DLT | PySpark silver enrichment: geolocation lookup, user-agent parsing, visit bucketing, dedup logic |
| `test_dlt_bronze.py` | 60 | 11 | Bronze DLT | PySpark bronze ingestion: W3C log parsing, IP→geo mapping, schema enforcement |
| `test_export_dimensions.py` | 41 | 12 | Dimension Export | `_export_dimensions` operator logic: type-casting, surrogate keys, MERGE upsert |
| `test_dbt_tsql_migration.py` | 40 | 4 | dbt T-SQL | 28 unmarked (config/parsing) + 12 `@dbt_compile` (compiled T-SQL output validation: FK columns, data types, staging CTE) |
| `test_export_azure_operators.py` | 35 | 7 | Azure Export | `ExportCSVToAzure` / `ExportToAzureSql` operator logic |
| `test_export_warehouse.py` | 33 | 8 | Warehouse Export | JDBC export: `_export_to_warehouse` UDF serialization, Spark→PostgreSQL type mapping |
| `test_jdbc_export_azure.py` | 31 | 6 | JDBC Azure | Azure SQL JDBC export: spark_row construction, connection config, type casts |
| `test_export_dimensions_azure.py` | 29 | 6 | Dim Export Azure | Azure-SQL-specific dimension export: `_parse_user_agent`, bucket assignment |
| `test_01_bronze_ingestion.py` | 29 | 7 | Bronze Ingestion | Airflow DAG task: W3C log parsing, metadata injection, dedup logic |
| `test_transformations.py` | 28 | 5 | Transformations | Utility functions: `page_category`, `extract_top_level_domain`, `map_method`, traffic-light status |
| `test_w3c_parser.py` | 27 | 5 | W3C Parser | Raw IIS W3C log line parsing: field extraction, edge cases, malformed lines |
| `test_terraform_part_b.py` | 25 | 6 | Terraform B | Mock-based Terraform config validation for Part B (Databricks pipelines, workflows, Unity Catalog) |
| `test_dag_integrity.py` | 23 | 5 | DAG Integrity | Airflow DAG loader: import errors, task graph structure, required DAG args |
| `test_02_silver_enrichment.py` | 21 | 5 | Silver Enrichment | Airflow DAG task: enrichment pipeline, GeoIP/user-agent lookups |
| `test_integration.py` | 18 | 5 | Integration | Cross-layer E2E: Spark→PostgreSQL→dbt flow (requires Docker) |
| `test_03_export_warehouse.py` | 15 | 4 | Warehouse Export | Airflow DAG task: export dimensions to warehouse, tracking table |
| `test_terraform_part_a.py` | 14 | 5 | Terraform A | Mock-based Terraform config validation for Part A (Azure infra: VNet, ADLS, SQL Server) |

### 3.3. CI Test Command

```bash
# Run everything except Docker-dependent and dbt-compile tests
pytest tests/ -v --tb=short -m "not integration and not dbt_compile"

# With coverage
pytest tests/ -v --tb=short \
  -m "not integration and not dbt_compile" \
  --cov=airflow --cov-report=xml --cov-report=term-missing
```

### 3.4. Local / Full Test Command

```bash
# Run everything (requires Docker for integration tests, dbt compile for dbt_compile tests)
pytest tests/ -v --tb=short

# Run a specific marker
pytest tests/ -v --tb=short -m integration
pytest tests/ -v --tb=short -m terraform
pytest tests/ -v --tb=short -m dag_integrity
```

---

## 4. Static Analysis & Linting (CI: `lint` job)

| Tool | Scope | What It Catches | Config |
|------|-------|-----------------|--------|
| **ruff** (lint) | All Python files | PEP8 violations, unused imports, naming conventions | `pyproject.toml` (E4/E7/E9/F/I/N/W) |
| **ruff** (format) | All Python files | Code formatting consistency | `pyproject.toml` (120 chars, preview) |
| **mypy** | `tests/` + `airflow/spark/databricks/` | Type errors, missing return types | `pyproject.toml` (Python 3.12, strict off) |
| **bandit** | `airflow/` (excl. tests, databricks) | Security hotspots: hardcoded secrets, SQL injection, command injection | `pyproject.toml` |
| **SQLFluff** | `airflow/dbt/w3c/models/` | SQL style, readability, dialect compliance | `.sqlfluff` (Postgres dialect, UPPER keywords, 120 chars) |

All run in CI on every push to any branch.

---

## 5. dbt Data Tests (YAML-defined — 118 tests)

### 5.1. Test Categories

| Type | Count | Files | Purpose |
|------|-------|-------|---------|
| `not_null` | 46 | schema.yml (39) + sources.yml (7) | Core columns must never be null (surrogate keys, metrics, critical attributes) |
| `unique` | 18 | schema.yml (14) + sources.yml (4) | Surrogate keys are unique (dimension tables) |
| `accepted_values` | 20 | schema.yml (18) + sources.yml (2) | Enumerated fields match expected values (HTTP methods, status codes, boolean flags) |
| `relationships` | 10 | schema.yml (10) | Referential integrity: fact→dimension FK paths are valid |
| `dbt_utils.expression_is_true` | 24 | schema.yml (24) | Business logic invariants (e.g. `response_time_ms >= 0`, `bytes_sent >= 0`, `request_count > 0`) |

### 5.2. Models with Tests

| Model | Tests | Coverage Focus |
|-------|-------|----------------|
| `fact_webrequest` | 22 | Referential integrity (10 FK), `not_null` on metrics, `accepted_values` on HTTP method/status |
| `dim_date` | 7 | SK uniqueness, `not_null` on derived columns (year, month, quarter), weekend/holiday accepted values |
| `mart_daily_aggregates` | 8 | `not_null` on key metrics, `expression_is_true` on count invariants (3) |
| `mart_page_performance` | 7 | `not_null` on page SK and total requests, `expression_is_true` on latency/404 invariants |
| `dim_time` | 6 | SK uniqueness, `not_null` on hour/minute, shift/band accepted values |
| `dim_status` | 4 | SK uniqueness, `not_null` on status_code, accepted values for status categories |
| `dim_method` | 3 | SK uniqueness, `not_null` on http_method, accepted values for method types |
| ... plus 9 more dimension/mart models |

### 5.3. CI Command

```bash
# Compile (PostgreSQL + Azure SQL/T-SQL)
dbt compile --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt

# Run data tests (against Azure SQL in production CD)
dbt test --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt --profile w3c_azure
```

---

## 6. Terraform Infrastructure Tests (9 HCL assertions)

### 6.1. Part A — Azure Infrastructure (3 assertions)

```
terraform/part_a/tests/default.tftest.hcl
```

- **Run mode:** `command = plan` (Azurerm provider validates resource ID formats during apply; plan mode avoids this with mock providers)
- **Mock providers:** `azurerm`, `databricks`, `azuread`, `time`
- **Variables provided:** subscription_id, tenant_id, client_id, storage_account_name, sql_server_name, alert emails
- **Assertions (3):**
  - Required Azure credentials (subscription_id, tenant_id, client_id) are provided
  - Required resource names (storage_account, sql_server) are non-empty
  - Alert email addresses (critical/warning/info) are configured

### 6.2. Part B — Databricks Configuration (6 assertions)

```
terraform/part_b/tests/default.tftest.hcl
```

- **Run mode:** `command = apply` (simple Databricks-only config; mock providers fully resolve)
- **Mock provider:** `databricks`
- **Assertions (6):**
  - `databricks_pipeline.bronze` exists (bronze DLT pipeline)
  - `databricks_pipeline.silver` exists (silver DLT pipeline)
  - `databricks_job.w3c_etl_workflow` exists (orchestration workflow)
  - `databricks_schema.bronze`, `.silver`, `.gold` exist (Unity Catalog schemas)
  - `databricks_secret_scope.w3c_etl` exists
  - Expected outputs (bronze_pipeline_id, silver_pipeline_id, workflow_job_id, workflow_job_url) are defined

### 6.3. CI Pipeline

```yaml
# Runs on every push via _reusable-terraform.yml
terraform fmt --check   # HCL formatting
terraform init          # Module download
terraform validate      # Config validation
terraform test          # HCL test suite execution
```

Runs in parallel across both `terraform/part_a` and `terraform/part_b`.

---

## 7. Security Scanning

### 7.1. Static Analysis (CI: `lint` job)

| Tool | Scope | What It Finds |
|------|-------|---------------|
| **bandit** | `airflow/` source code | Hardcoded passwords, SQL injection, shell injection, unsafe `yaml.load`, etc. |

### 7.2. SAST (CI: `codeql` job — weekly)

| Language | Build Mode | Schedule |
|----------|------------|----------|
| Python | `none` (interpreted) | Every push + weekly Monday 06:00 UTC |
| GitHub Actions | `none` | Every push + weekly Monday 06:00 UTC |

Results reported as GitHub code scanning alerts in the Security tab.

### 7.3. Secret Scanning (External)

- **GitGuardian** enabled at the GitHub organization level — automatically scans every push with alert/block configured by org policy.

---

> **CI/CD pipeline details moved to [CI/CD & Pre-commit Hooks Reference](./ci-cd-reference.md).**

## 9. How Recruiters Should Interpret This

| Signal | What It Demonstrates |
|--------|----------------------|
| **305 pytest tests (275 in CI)** | Rigorous automated testing at every pipeline layer — parsing, enrichment, export, E2E |
| **118 dbt data tests** | Production-grade data quality: null checks, referential integrity, business rule validation |
| **9 Terraform HCL assertions + 39 Python terraform tests** | Infrastructure-as-Code validated without cloud credentials — reproducible, CI-friendly |
| **5 linters/type-checkers** | Multi-layered code quality: style, types, security, SQL formatting |
| **CodeQL + bandit + GitGuardian** | Defense-in-depth for security: SAST, static analysis, secret scanning |
| **dbt compile on both PostgreSQL + T-SQL** | Cross-dialect compatibility: develops on Postgres, deploys to Azure SQL |
| **Post-deploy smoke test** | CD pipeline doesn't just deploy — it verifies the system actually works |
| **Codecov integration** | Coverage tracking visible to the team |
| **Marker-based test selection** | Thoughtful test architecture: environment-dependent suites (Docker, dbt, Terraform) are isolated by markers so CI stays fast while local coverage remains comprehensive |

---

## 10. Running Locally

```bash
# Python tests (requires Docker for integration tests)
pytest tests/ -v --tb=short -m "not integration and not dbt_compile"

# Full test suite (with Docker running)
pytest tests/ -v --tb=short

# Lint
ruff check --output-format=github .
ruff format --check .
mypy --ignore-missing-imports tests/
mypy --ignore-missing-imports airflow/spark/databricks/
bandit -r airflow/ -c pyproject.toml

# SQL lint
sqlfluff lint airflow/dbt/w3c/models/ --dialect postgres

# dbt compile
dbt compile --project-dir airflow/dbt/w3c --profiles-dir airflow/dbt

# Terraform tests
cd terraform/part_a && terraform init -backend=false && terraform test
cd terraform/part_b && terraform init -backend=false && terraform test
```

### Docker (Required for Integration Tests)

The full E2E integration test (`test_integration.py`, 18 tests with `@integration` marker) requires a running Docker stack with Airflow + PostgreSQL. Start it with:

```bash
docker compose up -d
pytest tests/ -v --tb=short -m integration
```

---

## 11. Architecture Deep Dive: conftest.py

The `tests/conftest.py` is worth highlighting because it solves a subtle Python packaging challenge common in Airflow projects:

1. **PEP 420 namespace shadowing**: The project's `airflow/` directory has no `__init__.py`. Python 3.3+ treats it as a namespace package. If the project root stays on `sys.path`, `import airflow` resolves to this local directory instead of the installed `apache-airflow` package, causing `ModuleNotFoundError: No module named 'airflow.models'`.

2. **Two-path layout support**: The conftest resolves two possible directory structures — bare metal (`project/airflow/spark/jobs/`) and Docker volume mounts (`project/spark/jobs/`) — making the same test suite work in both environments.

3. **PySpark worker serialization**: UDFs imported from `utils.*` fail to deserialize on PySpark workers because they don't inherit the driver's `sys.path`. The conftest builds a `utils.zip` and ships it via `SparkContext.addPyFile()` — the same pattern used by the production `SparkSubmitOperator`.

This demonstrates deep familiarity with both Airflow's packaging quirks and PySpark's distributed execution model.

---

## 12. Quick Stats Summary

| Metric | Value |
|--------|-------|
| Python unit tests (total) | **305** (183 core + 12 dbt_compile + 12 terraform + 5 dag_integrity + 18 integration) |
| Python unit tests (CI run) | **275** (excludes 18 integration + 12 dbt_compile) |
| dbt data tests | **118** (46 not_null, 18 unique, 20 accepted_values, 10 relationships, 24 expression_is_true) |
| Terraform HCL assertions | **9** (3 Part A plan, 6 Part B resource existence) |
| Terraform Python tests | **39** (14 Part A + 25 Part B) |
| dbt models tested | **16** models + 3 source tables |
| Pre-commit hooks | **15** |
| Linting tools | **5** (ruff, mypy, bandit, SQLFluff, terraform fmt) |
| Security tools | **3** (bandit, CodeQL, GitGuardian) |

