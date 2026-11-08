"""Shared Azure SQL connection-string helpers.

Both the Airflow DAG (``dags/w3c/spark_ingestion_azure.py``) and the CSV
export operator build the same ODBC string. Operators must not import from
``dags``, so the code lives here and the DAG imports it.
"""

from __future__ import annotations


def odbc_value(value: str) -> str:
    """Quote an ODBC connection-string value so ``;``, ``=`` and braces are safe.

    A braced value is literal except that every ``}`` must be doubled
    (``}}``); ``{`` needs no escaping. The braces also stop a ``;`` or ``=``
    inside the password from terminating the ``PWD=`` pair.
    """
    return "{" + value.replace("}", "}}") + "}"


def build_conn_str(server: str, database: str, username: str, password: str) -> str:
    """Build the Azure SQL ODBC connection string (Driver 18, encrypted)."""
    return (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={odbc_value(password)};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
