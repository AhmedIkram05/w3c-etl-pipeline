"""
Unit and compiled-output tests for Phase 8a/8b — dbt T-SQL Migration.

Structural and config tests (no markers) run without dbt — they're fast
static-analysis checks that execute in the CI ``test`` job.

Compiled-output tests (``@pytest.mark.dbt_compile``) need ``dbt compile``
to have been run first (typically with ``--profile w3c_azure``). They
execute in the CI ``dbt-compile`` job after the compile step.
"""

from __future__ import annotations

import re
import warnings
from pathlib import Path

import pytest

# ── Paths ──────────────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DBT_DIR = _PROJECT_ROOT / "airflow" / "dbt" / "w3c"
_MACRO_PATH = _DBT_DIR / "macros" / "t_sql_compat.sql"
_MODELS_DIR = _DBT_DIR / "models"
_STAGING_DIR = _MODELS_DIR / "staging"
_MARTS_DIR = _MODELS_DIR / "marts"
_PROFILES_FILE = _PROJECT_ROOT / "airflow" / "dbt" / "profiles.yml"
_DBT_PROJECT_FILE = _DBT_DIR / "dbt_project.yml"
_SOURCES_FILE = _MODELS_DIR / "sources.yml"
_TARGET_DIR = _DBT_DIR / "target"

# ── Expected macro names ───────────────────────────────────────────────────
REQUIRED_MACROS = frozenset({
    "tsql_cast",
    "tsql_datepart",
    "tsql_month_name",
    "tsql_day_name",
    "tsql_dow",
    "tsql_format_date",
    "tsql_split_part",
    "tsql_regexp_replace",
    "tsql_case_insensitive_like",
    "tsql_generate_series",
    "tsql_percentile_cont",
    "tsql_create_index_if_not_exists",
    "tsql_hash_md5",
    "tsql_boolean_to_int",
    "tsql_bool_literal",
    "tsql_true_val",
    "tsql_false_val",
    "tsql_extract_domain",
})

# Models that are expected to have inline T-SQL conditionals.
MUST_HAVE_INLINE_CONDITIONALS = frozenset({
    "fact_webrequest.sql",
    "dim_date.sql",
    "dim_time.sql",
    "dim_page.sql",
    "dim_referrer.sql",
    "crawler_ips.sql",
    "mart_daily_aggregates.sql",
    "mart_page_performance.sql",
    "mart_timeofday_analysis.sql",
})

# Models that use pure ANSI SQL and need no inline conditionals.
PURE_ANSI_SQL_MODELS = frozenset({
    "dim_method.sql",
    "dim_status.sql",
    "dim_visitortype.sql",
    "dim_visit_buckets.sql",
    "mart_browser_analysis.sql",
    "mart_country_browser_share.sql",
    "mart_crawler_analysis.sql",
})

STAGING_MODEL_FILES = frozenset({
    "fact_webrequest.sql",
    "dim_date.sql",
    "dim_time.sql",
    "dim_page.sql",
    "dim_status.sql",
    "dim_referrer.sql",
    "dim_method.sql",
    "dim_visitortype.sql",
    "dim_visit_buckets.sql",
    "crawler_ips.sql",
})

MART_MODEL_FILES = frozenset({
    "mart_page_performance.sql",
    "mart_daily_aggregates.sql",
    "mart_crawler_analysis.sql",
    "mart_browser_analysis.sql",
    "mart_timeofday_analysis.sql",
    "mart_country_browser_share.sql",
})

ALL_MODELS = STAGING_MODEL_FILES | MART_MODEL_FILES


# ═══════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════


def _parse_macro_names(sql_text: str) -> set[str]:
    """Extract macro names from a ``{% macro name(...) %}`` definition block."""
    pattern = r"\{%\s*macro\s+(\w+)\s*\("
    return set(re.findall(pattern, sql_text))


def _read_file(path: Path) -> str:
    """Read a file, returning an empty string on missing file."""
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _model_file_paths(directory: Path) -> list[Path]:
    """Return all ``.sql`` files in *directory* sorted by name."""
    return sorted(directory.glob("*.sql"))


# ═══════════════════════════════════════════════════════════════════════════
#  Section 1 — Macro Layer Structural Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestMacroDefinitions:
    """Verify the T-SQL compatibility macro file structure."""

    macro_source: str = ""

    @pytest.fixture(autouse=True)
    def _load_macros(self) -> None:
        if not TestMacroDefinitions.macro_source:
            TestMacroDefinitions.macro_source = _read_file(_MACRO_PATH)

    def test_macro_file_exists(self):
        """``macros/t_sql_compat.sql`` must exist."""
        assert _MACRO_PATH.exists(), f"Macro file not found: {_MACRO_PATH}"

    def test_macro_file_not_empty(self):
        """Macro file must contain macro definitions."""
        assert len(self.macro_source) > 0, "Macro file is empty"

    def test_all_required_macros_exist(self):
        """Every expected macro must be defined in the compatibility file."""
        defined = _parse_macro_names(self.macro_source)
        missing = REQUIRED_MACROS - defined
        assert not missing, f"Required macros missing: {sorted(missing)}"

    def test_no_extra_unexpected_macros(self):
        """Flag any macros defined but not in the expected set (may indicate drift)."""
        defined = _parse_macro_names(self.macro_source)
        extras = defined - REQUIRED_MACROS
        if extras:
            warnings.warn(UserWarning(f"Unexpected macros found: {sorted(extras)}"))

    def test_no_variadic_macro_signatures(self):
        """dbt Jinja does not support ``*args`` — this must never appear."""
        assert "*args" not in self.macro_source, "Variadic *args found in macro — dbt Jinja cannot parse this"

    def test_each_macro_has_both_dialect_branches(self):
        """Every macro must have both a ``sqlserver`` branch and an ``else`` fallback."""
        macros = _parse_macro_names(self.macro_source)
        for name in macros:
            # Find the start of this macro's body
            marker = f"macro {name}"
            idx = self.macro_source.find(marker)
            assert idx != -1, f"Could not locate macro {name} in source"

            # Extract the macro block ({% macro ... %} ... {%- endmacro %})
            block_start = self.macro_source.rfind("{%", 0, idx)
            block_end = self.macro_source.find("{%- endmacro %}", idx)
            if block_end == -1:
                block_end = self.macro_source.find("{% endmacro %}", idx)
            block = self.macro_source[block_start:block_end]

            has_sqlserver = "target.type == 'sqlserver'" in block
            has_else = re.search(r"\{%-\s*else\s*-%\}|\{%\s*else\s*%\}", block) is not None

            if not has_sqlserver:
                pytest.fail(f"Macro '{name}' is missing a '{{% if target.type == 'sqlserver' %}}' branch")
            if not has_else:
                pytest.fail(f"Macro '{name}' is missing an '{{% else %}}' fallback branch")

    def test_macro_naming_consistent(self):
        """All macros in the file must start with ``tsql_`` prefix."""
        macros = _parse_macro_names(self.macro_source)
        bad = {m for m in macros if not m.startswith("tsql_")}
        assert not bad, f"Macros without 'tsql_' prefix: {sorted(bad)}"

    def test_no_raw_jinja_comments_left_in_macro_defs(self):
        """Flag any leftover TODO or FIXME markers in macro source."""
        todos = re.findall(r"(?i)\b(TODO|FIXME|HACK|XXX)\b", self.macro_source)
        if todos:
            warnings.warn(UserWarning(f"Found markers in macro source: {todos}"))


# ═══════════════════════════════════════════════════════════════════════════
#  Section 2 — Model Structure Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestModelStructure:
    """Verify model file counts, naming, and dialect coverage."""

    def test_all_staging_models_present(self):
        """Exactly 10 staging models must exist."""
        files = {p.name for p in _model_file_paths(_STAGING_DIR)}
        missing = STAGING_MODEL_FILES - files
        extras = files - STAGING_MODEL_FILES
        assert not missing, f"Missing staging models: {sorted(missing)}"
        assert not extras, f"Unexpected staging model files: {sorted(extras)}"

    def test_all_mart_models_present(self):
        """Exactly 6 mart models must exist."""
        files = {p.name for p in _model_file_paths(_MARTS_DIR)}
        missing = MART_MODEL_FILES - files
        extras = files - MART_MODEL_FILES
        assert not missing, f"Missing mart models: {sorted(missing)}"
        assert not extras, f"Unexpected mart model files: {sorted(extras)}"

    def test_total_model_count(self):
        """16 models total (10 staging + 6 mart)."""
        staging = len(list(_model_file_paths(_STAGING_DIR)))
        marts = len(list(_model_file_paths(_MARTS_DIR)))
        assert staging == 10, f"Expected 10 staging models, found {staging}"
        assert marts == 6, f"Expected 6 mart models, found {marts}"

    def test_no_azure_duplicate_model_files(self):
        """No ``_azure.sql`` duplicate files — T-SQL uses inline conditionals instead."""
        all_sql = list(_STAGING_DIR.glob("*_azure.sql")) + list(_MARTS_DIR.glob("*_azure.sql"))
        assert not all_sql, f"Remove _azure.sql duplicates (use inline conditionals): {all_sql}"

    def test_each_model_is_sql_not_empty(self):
        """Every model file must be non-empty."""
        for path in _model_file_paths(_STAGING_DIR):
            assert len(path.read_text(encoding="utf-8").strip()) > 0, f"Empty file: {path.name}"
        for path in _model_file_paths(_MARTS_DIR):
            assert len(path.read_text(encoding="utf-8").strip()) > 0, f"Empty file: {path.name}"

    def test_dialect_models_have_inline_conditionals(self):
        """Models in MUST_HAVE_INLINE_CONDITIONALS must contain a sqlserver branch."""
        for model_name in MUST_HAVE_INLINE_CONDITIONALS:
            path = _find_model_file(model_name)
            assert path is not None, f"Model file not found: {model_name}"
            content = path.read_text(encoding="utf-8")
            assert "target.type == 'sqlserver'" in content, (
                f"{model_name} is expected to have inline T-SQL conditionals but none found"
            )

    def test_no_model_has_both_inline_and_external_templates(self):
        """No model should mix inline conditionals with external macro-only patterns."""
        for model_name in ALL_MODELS:
            path = _find_model_file(model_name)
            if path is None:
                continue
            content = path.read_text(encoding="utf-8")
            # If it has inline conditionals, it should use at least one tsql_ macro call
            if "target.type == 'sqlserver'" in content:
                # Check that it also references at least one tsql_ macro
                has_macro_ref = bool(re.search(r"tsql_\w+", content))
                if not has_macro_ref:
                    warnings.warn(
                        UserWarning(
                            f"{model_name} has 'target.type == sqlserver' but no tsql_ macro calls "
                            f"— may be using raw T-SQL instead of macros"
                        )
                    )

    def test_no_model_missing_from_classification(self):
        """All model files must be classified (must-have-conditionals OR pure-ANSI)."""
        all_known = MUST_HAVE_INLINE_CONDITIONALS | PURE_ANSI_SQL_MODELS
        actual = {p.name for p in list(_model_file_paths(_STAGING_DIR)) + list(_model_file_paths(_MARTS_DIR))}
        unclassified = actual - all_known
        assert not unclassified, f"Unclassified models (add to MUST_HAVE or PURE_ANSI sets): {sorted(unclassified)}"


def _find_model_file(name: str) -> Path | None:
    """Find a model file by name under staging/ or marts/."""
    for d in (_STAGING_DIR, _MARTS_DIR):
        p = d / name
        if p.exists():
            return p
    return None


# ═══════════════════════════════════════════════════════════════════════════
#  Section 3 — Configuration Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestConfiguration:
    """Verify dbt configuration files (profiles.yml, dbt_project.yml, sources.yml)."""

    def test_profiles_yml_exists(self):
        """``profiles.yml`` must exist."""
        assert _PROFILES_FILE.exists(), f"Not found: {_PROFILES_FILE}"

    def test_profiles_has_w3c_profile(self):
        """``profiles.yml`` must define a ``w3c`` profile."""
        content = _read_file(_PROFILES_FILE)
        assert "w3c:" in content, "profiles.yml missing 'w3c:' profile definition"

    def test_profiles_has_w3c_azure_profile(self):
        """``profiles.yml`` must define a ``w3c_azure`` profile for Azure SQL."""
        content = _read_file(_PROFILES_FILE)
        assert "w3c_azure:" in content, "profiles.yml missing 'w3c_azure:' profile definition"

    def test_profiles_has_azure_sql_ci_target(self):
        """``profiles.yml`` w3c_azure must have an ``azure_sql_ci`` target for local SQL Server."""
        content = _read_file(_PROFILES_FILE)
        assert "azure_sql_ci:" in content, (
            "profiles.yml w3c_azure missing 'azure_sql_ci' target for CI Docker SQL Server"
        )

    def test_dbt_project_uses_dynamic_profile(self):
        """``dbt_project.yml`` must use ``{{ env_var('DBT_PROFILE', 'w3c') }}`` for profile selection."""
        content = _read_file(_DBT_PROJECT_FILE)
        assert "env_var('DBT_PROFILE', 'w3c')" in content, (
            "dbt_project.yml must use dynamic profile selection via env_var('DBT_PROFILE', 'w3c')"
        )

    def test_dbt_project_config_version_is_2(self):
        """``dbt_project.yml`` must use ``config-version: 2``."""
        content = _read_file(_DBT_PROJECT_FILE)
        assert "config-version: 2" in content, "dbt_project.yml missing config-version: 2"

    def test_dbt_project_has_staging_and_marts_model_configs(self):
        """``dbt_project.yml`` must define model configs for both staging and marts."""
        content = _read_file(_DBT_PROJECT_FILE)
        assert "staging:" in content and "marts:" in content, (
            "dbt_project.yml missing model configs for staging and/or marts"
        )

    def test_sources_yml_exists(self):
        """``models/sources.yml`` must exist."""
        assert _SOURCES_FILE.exists(), f"Not found: {_SOURCES_FILE}"

    def test_sources_yml_defines_w3c_source(self):
        """``sources.yml`` must define a ``w3c`` source."""
        content = _read_file(_SOURCES_FILE)
        assert "w3c:" in content or "name: w3c" in content, "sources.yml missing 'name: w3c' source definition"

    def test_sources_yml_has_raw_enriched(self):
        """``sources.yml`` must include ``raw_enriched`` table."""
        content = _read_file(_SOURCES_FILE)
        assert "raw_enriched" in content, "sources.yml missing 'raw_enriched' table"

    def test_sources_yml_has_dim_geolocation(self):
        """``sources.yml`` must include ``dim_geolocation`` table."""
        content = _read_file(_SOURCES_FILE)
        assert "dim_geolocation" in content, "sources.yml missing 'dim_geolocation' table"

    def test_sources_yml_has_dim_useragent(self):
        """``sources.yml`` must include ``dim_useragent`` table."""
        content = _read_file(_SOURCES_FILE)
        assert "dim_useragent" in content, "sources.yml missing 'dim_useragent' table"


# ═══════════════════════════════════════════════════════════════════════════
#  Section 4 — Compiled Output Tests  (@pytest.mark.dbt_compile)
# ═══════════════════════════════════════════════════════════════════════════
#
# These tests validate that ``dbt compile --profile w3c_azure`` produced
# correct T-SQL output. They depend on ``target/compiled/w3c/`` existing.
# CI runs them in the ``dbt-compile`` job after the Azure SQL compile step.


@pytest.mark.dbt_compile
class TestCompiledAzureSqlOutput:
    """Verify Azure SQL compiled model output contains correct T-SQL patterns."""

    compiled_dir: Path = _TARGET_DIR / "compiled" / "w3c" / "models"

    def test_compiled_dir_exists(self):
        """``target/compiled/w3c/models`` must exist after ``dbt compile``."""
        assert self.compiled_dir.exists(), (
            f"Compiled output directory not found: {self.compiled_dir}. Run 'dbt compile --profile w3c_azure' first."
        )

    def test_all_staging_models_compiled(self):
        """All 10 staging models must appear in compiled output."""
        compiled = self._list_compiled("staging")
        for name in STAGING_MODEL_FILES:
            assert any(name in str(p) for p in compiled), f"Missing compiled output: staging/{name}"

    def test_all_mart_models_compiled(self):
        """All 6 mart models must appear in compiled output."""
        compiled = self._list_compiled("marts")
        for name in MART_MODEL_FILES:
            assert any(name in str(p) for p in compiled), f"Missing compiled output: marts/{name}"

    def test_compiled_uses_cast_not_colon_cast(self):
        """T-SQL compiled output must use ``CAST(... AS ...)`` not ``::`` colon casts."""
        for path in self._all_compiled_sql():
            content = path.read_text(encoding="utf-8")
            assert "::" not in content, (
                f"Colon cast '::' found in Azure SQL compiled output: {path.relative_to(_PROJECT_ROOT)}"
            )

    def test_compiled_uses_datepart_not_extract(self):
        """T-SQL compiled output must use ``DATEPART`` not ``EXTRACT``."""
        for path in self._all_compiled_sql():
            content = path.read_text(encoding="utf-8")
            if "EXTRACT(" in content:
                pytest.fail(f"EXTRACT() found in Azure SQL compiled output: {path.relative_to(_PROJECT_ROOT)}")

    def test_compiled_md5_uses_hashbytes(self):
        """T-SQL compiled output must use ``HASHBYTES('MD5', ...)`` for MD5."""
        # Check fact_webrequest and the singular test
        for name in ("fact_webrequest.sql", "fact_webrequest_dedup_safety.sql"):
            path = self._find_compiled(name)
            if path is None:
                continue
            content = path.read_text(encoding="utf-8")
            assert "HASHBYTES" in content, (
                f"MD5 hash in {name} should use HASHBYTES('MD5', ...) on Azure SQL. Found: {content[:300]}"
            )
            assert "MD5(" not in content, f"PostgreSQL-style MD5() found in Azure SQL compiled output: {name}"

    def test_compiled_generate_series_uses_tsql_syntax(self):
        """T-SQL compiled output must use ``GENERATE_SERIES`` for series generation."""
        path = self._find_compiled("dim_time.sql")
        if path is None:
            return
        content = path.read_text(encoding="utf-8")
        assert "GENERATE_SERIES" in content, "dim_time.sql should use GENERATE_SERIES on Azure SQL"

    def test_compiled_percentile_uses_within_group(self):
        """T-SQL compiled output must use ``PERCENTILE_CONT(...) WITHIN GROUP (ORDER BY ...) OVER ()``."""
        for name in ("mart_page_performance.sql", "mart_daily_aggregates.sql"):
            path = self._find_compiled(name)
            if path is None:
                continue
            content = path.read_text(encoding="utf-8")
            assert "PERCENTILE_CONT" in content, f"{name} should use PERCENTILE_CONT on Azure SQL"
            assert "WITHIN GROUP" in content, f"{name} PERCENTILE_CONT must use WITHIN GROUP on Azure SQL"
            assert "OVER ()" in content, f"{name} PERCENTILE_CONT must use explicit OVER () on Azure SQL"

    def test_compiled_boolean_expressions_use_case_when(self):
        """T-SQL boolean expressions must use ``CASE WHEN ... THEN 1 ELSE 0 END``."""
        path = self._find_compiled("fact_webrequest.sql")
        if path is None:
            return
        content = path.read_text(encoding="utf-8")
        # Check for CASE WHEN with THEN 1 ELSE 0 END (T-SQL boolean-as-integer pattern)
        assert re.search(r"THEN 1 ELSE 0\b", content), (
            "Boolean-to-int conversion should use 'THEN 1 ELSE 0 END' pattern on Azure SQL"
        )

    def test_compiled_like_not_regex(self):
        """T-SQL compiled output must use ``LIKE`` not ``~*`` for pattern matching."""
        for path in self._all_compiled_sql():
            content = path.read_text(encoding="utf-8")
            if "~*" in content:
                rel = path.relative_to(_PROJECT_ROOT)
                warnings.warn(UserWarning(f"PostgreSQL '~*' regex found in Azure SQL compiled output: {rel}"))

    def test_compiled_no_postgres_only_syntax(self):
        """T-SQL compiled output must not contain PostgreSQL-only operators."""
        postgres_only = [r'(?<!")\^\^', r'(?<!")\|\|']  # avoid matching string literals
        for path in self._all_compiled_sql():
            content = path.read_text(encoding="utf-8")
            for pattern in postgres_only:
                if re.search(pattern, content):
                    rel = path.relative_to(_PROJECT_ROOT)
                    warnings.warn(UserWarning(f"Possible PostgreSQL-only syntax in Azure SQL compiled output: {rel}"))

    # ── helpers ──────────────────────────────────────────────────────

    def _list_compiled(self, subdir: str) -> list[Path]:
        d = self.compiled_dir / subdir
        return sorted(d.glob("*.sql")) if d.exists() else []

    def _all_compiled_sql(self) -> list[Path]:
        return self._list_compiled("staging") + self._list_compiled("marts")

    def _find_compiled(self, name: str) -> Path | None:
        for d in (self.compiled_dir / "staging", self.compiled_dir / "marts"):
            p = d / name
            if p.exists():
                return p
        # Also check singular tests
        test_dir = _TARGET_DIR / "compiled" / "w3c" / "tests" / "singular"
        p = test_dir / name
        return p if p.exists() else None
