"""
Behavioural tests for the correctness-critical ``t_sql_compat.sql`` macros.

``tests/test_dbt_tsql_migration.py`` only checks the *compiled shape* of the
macro output (string assertions). These tests actually EXECUTE the Jinja
macros for both dialects and pin the rule each one encodes.

What is genuinely executed here
-------------------------------
* Every macro is rendered with **Jinja2** for ``target.type == 'sqlserver'``
  and for the PostgreSQL fallback. That executes the macro's own dialect
  branch logic and produces the exact SQL text the model would emit.
* The *rule* behind each SQL branch that we cannot execute without a database
  is evaluated in Python and asserted, so the two dialects are proven to
  encode the same result for the same input:
    - ``tsql_dow``    — day-of-week (Sunday = 0) via ``datetime``.
    - ``tsql_extract_domain`` — a Python port of each branch's SQL, checked
      against the same expected values.

What is only shape-asserted (no live database locally)
------------------------------------------------------
* The generated SQL text is asserted well-formed (balanced parentheses,
  expected functions/clauses present). The SQL is not executed — actual
  query execution is covered by the ``dbt_compile`` CI job and, for real
  data, by the warehouse.
* ``tsql_percentile_cont`` is dialect-identical, so there is no rule to
  evaluate in Python beyond "the two branches render the same valid SQL".
"""

from __future__ import annotations

import datetime
import types
from pathlib import Path
from typing import Any

import pytest

jinja2 = pytest.importorskip("jinja2")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _resolve_macro_path() -> Path:
    """Locate ``t_sql_compat.sql`` in either the bare-metal or Docker layout."""
    candidates = [
        _PROJECT_ROOT / "pipeline" / "dbt" / "w3c" / "macros" / "t_sql_compat.sql",
        _PROJECT_ROOT / "dbt" / "w3c" / "macros" / "t_sql_compat.sql",
    ]
    for path in candidates:
        if path.exists():
            return path
    return candidates[0]


_MACRO_PATH = _resolve_macro_path()

_MODULES: dict[str, Any] = {}


def _macro_module(dialect: str) -> Any:
    """Render the macro file as a Jinja module for a given ``target.type``.

    The ``target`` global is the only dbt runtime value these macros read.
    """
    if dialect not in _MODULES:
        source = _MACRO_PATH.read_text(encoding="utf-8")
        env = jinja2.Environment(undefined=jinja2.StrictUndefined)
        env.globals["target"] = types.SimpleNamespace(type=dialect)
        _MODULES[dialect] = env.from_string(source).make_module()
    return _MODULES[dialect]


DIALECTS = ("sqlserver", "postgres")


def _balanced(sql: str) -> bool:
    """True if parentheses balance, ignoring ``--`` line comments."""
    stripped = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())
    depth = 0
    for ch in stripped:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0


# ═══════════════════════════════════════════════════════════════════════════
#  tsql_percentile_cont
# ═══════════════════════════════════════════════════════════════════════════


class TestPercentileCont:
    """``tsql_percentile_cont`` emits a valid windowed PERCENTILE_CONT."""

    @pytest.mark.parametrize("dialect", DIALECTS)
    def test_renders_valid_percentile(self, dialect):
        sql = _macro_module(dialect).tsql_percentile_cont(0.95, "response_time_ms")
        assert "PERCENTILE_CONT(0.95)" in sql
        assert "WITHIN GROUP" in sql
        assert "ORDER BY response_time_ms" in sql
        assert "OVER ()" in sql
        assert _balanced(sql), f"unbalanced parentheses in: {sql!r}"

    def test_dialects_are_identical(self):
        """The two branches are the same expression, so output must match."""
        assert (
            _macro_module("sqlserver").tsql_percentile_cont(0.5, "x")
            == _macro_module("postgres").tsql_percentile_cont(0.5, "x")
        )


# ═══════════════════════════════════════════════════════════════════════════
#  tsql_extract_domain — rule evaluated in Python, SQL rendered per dialect
# ═══════════════════════════════════════════════════════════════════════════

# Inputs -> expected output of the macro's rule (identical across dialects).
_EXTRACT_CASES = {
    "https://www.example.com/x": "example.com",
    "http://example.com/a/b": "example.com",
    # Host-less / scheme-less inputs fall through to the ELSE unchanged.
    "https://": "https://",
    "http://": "http://",
    "/path/to/page": "/path/to/page",
    # Scheme-less with a leading host: leading www stripped, path kept.
    "www.example.com/path": "example.com/path",
    # Only a LEADING www is stripped ('abcwww.com' keeps its middle www).
    "abcwww.com/page": "abcwww.com/page",
    "Unknown": "unknown",
    "-": "-",
}


def _charindex(needle: str, haystack: str, start_one_based: int = 1) -> int:
    """1-based ``CHARINDEX`` semantics; 0 when not found."""
    found = haystack.find(needle, start_one_based - 1)
    return found + 1 if found != -1 else 0


def _sqlserver_rule(url: str) -> str:
    """Python port of the SQL Server branch of ``tsql_extract_domain``.

    ``LIKE`` / ``CHARINDEX`` are case-insensitive under the default SQL Server
    collation, so the scheme test lowercases only for the prefix check.
    """
    low = url.lower()
    if low.startswith("https://") and _charindex("/", url + "/", 9) > 9:
        end = _charindex("/", url + "/", 9)
        host = url[8:8 + (end - 9)]
    elif low.startswith("http://") and _charindex("/", url + "/", 8) > 8:
        end = _charindex("/", url + "/", 8)
        host = url[7:7 + (end - 8)]
    else:
        host = url
    host = host.lower()
    return host[4:] if host.startswith("www.") else host


def _postgres_rule(url: str) -> str:
    """Python port of the PostgreSQL branch of ``tsql_extract_domain``."""
    import re

    # REGEXP_REPLACE leaves the input unchanged when the pattern does not match.
    extracted = re.sub(r"^https?://([^/]+).*", r"\1", url).lower()
    return re.sub(r"^www\.", "", extracted)


class TestExtractDomain:
    """Both dialect branches encode the same host-extraction rule."""

    def test_rule_matches_expected_for_every_case(self):
        for url, expected in _EXTRACT_CASES.items():
            assert _sqlserver_rule(url) == expected, f"sqlserver rule on {url!r}"
            assert _postgres_rule(url) == expected, f"postgres rule on {url!r}"

    def test_dialect_rules_agree(self):
        for url in _EXTRACT_CASES:
            assert _sqlserver_rule(url) == _postgres_rule(url), f"dialect divergence on {url!r}"

    def test_uppercase_scheme_is_a_known_dialect_difference(self):
        """Uppercase schemes are NOT in the required case set: SQL Server's
        ``LIKE``/``CHARINDEX`` are case-insensitive (default collation) while
        PostgreSQL's ``REGEXP_REPLACE`` is case-sensitive. Pinned so nobody
        assumes the two branches are case-parallel.
        """
        url = "HTTPS://WWW.EXAMPLE.COM/X"
        assert _sqlserver_rule(url) == "example.com"
        assert _postgres_rule(url) == "https://www.example.com/x"

    @pytest.mark.parametrize("dialect", DIALECTS)
    def test_renders_well_formed_sql(self, dialect):
        for url in _EXTRACT_CASES:
            sql = _macro_module(dialect).tsql_extract_domain(repr(url))
            assert _balanced(sql), f"unbalanced parentheses for {url!r}: {sql!r}"
            assert len(sql.strip()) > 0

    def test_sqlserver_renders_tsql_functions(self):
        sql = _macro_module("sqlserver").tsql_extract_domain("col")
        assert "CHARINDEX('/'" in sql
        assert "SUBSTRING(" in sql
        assert sql.count("LIKE 'www.%'") == 1

    def test_postgres_renders_regexp(self):
        sql = _macro_module("postgres").tsql_extract_domain("col")
        assert "REGEXP_REPLACE(" in sql
        assert "'^https?://([^/]+).*'" in sql
        assert "'^www\\.'" in sql


# ═══════════════════════════════════════════════════════════════════════════
#  tsql_dow — rule evaluated in Python
# ═══════════════════════════════════════════════════════════════════════════

# 1900-01-07 is a Sunday — the anchor the SQL Server DATEDIFF uses.
_DOW_EPOCH = datetime.date(1900, 1, 7)


def _sqlserver_dow(date: datetime.date) -> int:
    """Python port of ``DATEDIFF(day, '19000107', d) % 7`` (Sunday = 0)."""
    return (date - _DOW_EPOCH).days % 7


def _postgres_dow(date: datetime.date) -> int:
    """Python port of ``EXTRACT(dow FROM d)`` (Sunday = 0)."""
    return (date.weekday() + 1) % 7


class TestDayOfWeek:
    """``tsql_dow`` produces Sunday = 0 on both dialects."""

    @pytest.mark.parametrize(
        "date, expected",
        [
            (datetime.date(1900, 1, 7), 0),   # Sunday anchor
            (datetime.date(1970, 1, 1), 4),   # Thursday
            (datetime.date(2000, 1, 1), 6),   # Saturday
            (datetime.date(2024, 2, 29), 4),  # Thursday (leap day)
            (datetime.date(2009, 10, 24), 6),  # Saturday
        ],
    )
    def test_rule_is_correct_for_known_dates(self, date, expected):
        assert _sqlserver_dow(date) == expected
        assert _postgres_dow(date) == expected

    def test_dialect_rules_agree(self):
        day = _DOW_EPOCH
        while day < datetime.date(1903, 1, 1):
            assert _sqlserver_dow(day) == _postgres_dow(day), day
            day += datetime.timedelta(days=1)

    def test_sqlserver_renders_datediff_anchor(self):
        sql = _macro_module("sqlserver").tsql_dow("log_date")
        assert "DATEDIFF(day, '19000107', log_date)" in sql
        assert "% 7" in sql
        assert _balanced(sql)

    def test_postgres_renders_extract_dow(self):
        sql = _macro_module("postgres").tsql_dow("log_date")
        assert sql == "EXTRACT(dow FROM log_date)"
