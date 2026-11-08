"""Tests for the shared Azure SQL connection-string helpers."""

import os
import sys

_PLUGINS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pipeline", "plugins")
if _PLUGINS_DIR not in sys.path:
    sys.path.insert(0, _PLUGINS_DIR)

from operators.azure_sql import build_conn_str, odbc_value  # noqa: E402


def test_odbc_value_escapes_closing_brace():
    assert odbc_value("a}b") == "{a}}b}"


def test_odbc_value_keeps_opening_brace():
    assert odbc_value("a{b") == "{a{b}"


def test_odbc_value_neutralises_delimiters():
    assert odbc_value("pass;word=1") == "{pass;word=1}"


def test_build_conn_str_uses_escaped_password():
    conn = build_conn_str("srv", "db", "user", "p}s;w")
    assert "SERVER=srv;" in conn
    assert "DATABASE=db;" in conn
    assert "UID=user;" in conn
    assert "PWD={p}}s;w};" in conn
    assert "Encrypt=yes" in conn
