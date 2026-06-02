"""
Export Airflow-Managed Dimension Tables.

Reads enriched rows from two upstream sources and builds the dimension
tables that the warehouse exposes to dbt / Power BI:

* ``dim_geolocation``  — sourced from the **Spark Silver Delta table**
  (single source of truth for client-IP geolocation). The silver writer
  (``airflow/spark/jobs/silver_enrichment.py``) populates ``country``,
  ``region``, ``city``, ``postcode``, ``latitude``, ``longitude`` and
  ``isp`` from local MaxMind GeoLite2 databases (City + ASN), giving us
  in-process enrichment with no external network calls.
* ``dim_useragent``    — parsed with the ``user-agents`` library from
  the ``raw_enriched`` Postgres table (still the canonical source for
  raw user-agent strings in the warehouse).

Both tables use ``INSERT … ON CONFLICT DO NOTHING`` so every run is
idempotent and incremental. Default ``-1`` surrogate-key rows are
inserted so fact tables can reference them for unknown / missing values.

This module is designed to be called as an Airflow ``PythonOperator``
callable (it accepts ``**context``).
"""

from __future__ import annotations

import os
import time
from urllib.parse import unquote_plus

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from user_agents import parse as ua_parse

# Default location of the Silver Delta table written by Spark. Can be
# overridden with the ``W3C_SILVER_PATH`` environment variable.
DEFAULT_SILVER_PATH = "/opt/spark/delta/silver"


def get_conn():
    """Create and return a new psycopg2 DB connection.

    Callers must close the connection when finished.
    """
    return psycopg2.connect(
        host=os.environ.get("W3C_DB_HOST", "postgres"),
        port=int(os.environ.get("W3C_DB_PORT") or 5432),
        dbname=os.environ.get("W3C_DB_NAME", "w3c_warehouse"),
        user=os.environ.get("W3C_DB_USER", "airflow"),
        password=os.environ.get("W3C_DB_PASS", "airflow"),
    )


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════


def _parse_user_agent(ua_string: str) -> dict:
    """Parse a user-agent string into structured fields.

    Returns a dict with keys: ``AgentType``, ``BrowserName``,
    ``BrowserVersion``, ``OS``, ``DeviceType``.
    """
    if not ua_string or not ua_string.strip() or ua_string.strip() in ("-", "Unknown"):
        return {
            "AgentType": "Unknown",
            "BrowserName": "Unknown",
            "BrowserVersion": "Unknown",
            "OS": "Unknown",
            "DeviceType": "Unknown",
        }

    normalized = unquote_plus(ua_string)
    ua = ua_parse(normalized)

    agent_type = "Crawler" if getattr(ua, "is_bot", False) else "Browser"
    browser_name = ua.browser.family or "Unknown"
    browser_version = ua.browser.version_string or "Unknown"
    os_name = ua.os.family or "Unknown"

    if getattr(ua, "is_mobile", False):
        device = "Mobile"
    elif getattr(ua, "is_tablet", False):
        device = "Tablet"
    elif getattr(ua, "is_pc", False):
        device = "Desktop"
    elif getattr(ua, "is_bot", False):
        device = "Bot"
    else:
        device = "Other"

    return {
        "AgentType": agent_type,
        "BrowserName": browser_name,
        "BrowserVersion": browser_version,
        "OS": os_name,
        "DeviceType": device,
    }


def _coalesce(value, default):
    """Return *value* unless it is ``None`` or empty/whitespace, else *default*."""
    if value is None:
        return default
    if isinstance(value, str) and value.strip() in ("", "-"):
        return default
    return value


# ══════════════════════════════════════════════════════════════════════════
# DIMENSION BUILDERS
# ══════════════════════════════════════════════════════════════════════════


def _ensure_default_rows(conn):
    """Insert default ``-1`` surrogate-key rows for FK integrity.

    These rows represent "unknown" values so fact tables can always
    reference a valid foreign key even when enrichment data is missing.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dim_geolocation
                (geolocation_sk, ip, country, region, city, postcode, latitude, longitude, isp)
            VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', '-', NULL, NULL, '-')
            ON CONFLICT DO NOTHING
        """)
        cur.execute("""
            INSERT INTO dim_useragent
                (user_agent_sk, user_agent, agent_type, browser_name, browser_version, operating_system, device_type)
            VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown')
            ON CONFLICT DO NOTHING
        """)
    conn.commit()


def _read_silver_geo_dim(silver_path: str) -> pd.DataFrame:
    """Read the Silver Delta table and return distinct IP-level geo rows.

    Uses pandas ``read_parquet``, which auto-discovers the Hive-style
    ``log_date=YYYY-MM-DD`` partitions and returns a single concatenated
    DataFrame.  Returns a DataFrame with one row per distinct
    ``client_ip`` carrying the geo fields needed by ``dim_geolocation``.

    Required columns (from the Silver schema):
        ``client_ip``, ``country``, ``region``, ``city``,
        ``latitude``, ``longitude``, ``isp``.

    The ``postcode`` column is optional — Silver may not yet have been
    re-run after the schema gained it, so we fall back to the ``-``
    default for any row that is missing the column entirely.
    """
    df = pd.read_parquet(silver_path)
    if df.empty:
        return df

    has_postcode = "postcode" in df.columns

    # Defragment: assigning back from a filtered/copied frame avoids
    # SettingWithCopyWarning in the next stage.
    geo = df[["client_ip", "country", "region", "city", "latitude", "longitude", "isp"]].copy()
    geo = geo.dropna(subset=["client_ip"])
    geo = geo[geo["client_ip"].astype(str).str.strip() != ""]
    geo = geo[geo["client_ip"].astype(str).str.strip() != "-"]
    # First non-null value per client_ip wins (silver is a single source
    # of truth, but distinct IP enrichment keeps the row count down).
    geo = geo.groupby("client_ip", as_index=False).first()

    if has_postcode:
        # Bring the postcode column along for the same distinct IP.
        pc = (
            df[["client_ip", "postcode"]]
            .dropna(subset=["client_ip"])
            .groupby("client_ip", as_index=False)["postcode"]
            .first()
        )
        geo = geo.merge(pc, on="client_ip", how="left")
    else:
        # Column hasn't been written yet (silver not re-run). Use None
        # and let the defaults layer below apply "-".
        geo["postcode"] = None

    return geo


def _build_dim_geolocation(conn, silver_path: str | None = None):
    """Populate ``dim_geolocation`` from the Silver Delta table.

    The Silver writer (``silver_enrichment.py``) is the single source of
    truth for geo enrichment — it produces ``country``, ``region``,
    ``city``, ``postcode``, ``latitude``, ``longitude`` and ``isp``
    using local MaxMind GeoLite2 databases.  We extract one row per
    distinct ``client_ip`` and upsert into the dimension.

    Parameters
    ----------
    conn : psycopg2 connection
        Open connection to the warehouse database (read-side cursor).
    silver_path : str or None
        Path to the Silver Delta table.  ``None`` falls back to
        ``$W3C_SILVER_PATH`` or ``DEFAULT_SILVER_PATH``.

    Idempotent — uses ``ON CONFLICT (ip) DO NOTHING``.
    """
    if silver_path is None:
        silver_path = os.environ.get("W3C_SILVER_PATH", DEFAULT_SILVER_PATH)

    print(f"[dim_geolocation] Reading geo dim from Silver Delta: {silver_path}")

    try:
        geo = _read_silver_geo_dim(silver_path)
    except FileNotFoundError as exc:
        print(f"[dim_geolocation] Silver path not found ({exc}); nothing to do.")
        return
    except Exception as exc:
        print(f"[dim_geolocation] Failed to read Silver Delta ({exc}); nothing to do.")
        return

    if geo.empty:
        print("[dim_geolocation] Silver Delta is empty; nothing to do.")
        return

    # Determine which IPs are already in the dim.
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT ip FROM dim_geolocation")
    existing_ips = {row[0] for row in cur.fetchall()}
    cur.close()

    new_rows = geo[~geo["client_ip"].astype(str).isin(existing_ips)]
    if new_rows.empty:
        print(f"[dim_geolocation] All {len(geo)} IPs already in dim_geolocation — nothing to do.")
        return

    print(f"[dim_geolocation] Inserting {len(new_rows)} new IP(s) from Silver …")

    # Build the INSERT payload.  Apply the schema's documented defaults
    # for missing / sentinel values:
    #   - text geo fields (country, region, city) → "Unknown"
    #   - postcode                                → "-"
    #   - isp                                     → "-"
    #   - lat/lon                                 → NULL
    insert_data = []
    for _, row in new_rows.iterrows():
        ip = str(row["client_ip"]).strip()
        insert_data.append((
            ip,
            _coalesce(row.get("country"), "Unknown"),
            _coalesce(row.get("region"), "Unknown"),
            _coalesce(row.get("city"), "Unknown"),
            _coalesce(row.get("postcode"), "-"),
            row.get("latitude") if pd.notna(row.get("latitude")) else None,
            row.get("longitude") if pd.notna(row.get("longitude")) else None,
            _coalesce(row.get("isp"), "-"),
        ))

    if not insert_data:
        return

    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            with get_conn() as write_conn, write_conn.cursor() as write_cur:
                execute_values(
                    write_cur,
                    """
                    INSERT INTO dim_geolocation
                        (ip, country, region, city, postcode, latitude, longitude, isp)
                    VALUES %s
                    ON CONFLICT (ip) DO NOTHING
                    """,
                    insert_data,
                    page_size=1000,
                )
                write_conn.commit()
            print(f"[dim_geolocation] Inserted / confirmed {len(insert_data)} row(s).")
            return
        except psycopg2.OperationalError as e:
            if attempt == attempts:
                raise
            print(f"[dim_geolocation] DB write attempt {attempt} failed: {e} — retrying …")
            time.sleep(2 * attempt)


def _build_dim_useragent(conn):
    """Populate ``dim_useragent`` by parsing distinct user-agent strings.

    Reads ``user_agent`` values from ``raw_enriched``, parses each with
    the ``user-agents`` library, and inserts.  Idempotent — uses
    ``ON CONFLICT DO NOTHING``.
    """
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT user_agent FROM raw_enriched WHERE user_agent IS NOT NULL AND user_agent != '-'")
    rows = cur.fetchall()
    cur.close()

    if not rows:
        print("[dim_useragent] No user-agent rows found in raw_enriched — nothing to do.")
        return

    insert_data = []
    for (ua_raw,) in rows:
        parsed = _parse_user_agent(ua_raw)
        insert_data.append((
            ua_raw[:1000],
            parsed["AgentType"],
            parsed["BrowserName"],
            parsed["BrowserVersion"],
            parsed["OS"],
            parsed["DeviceType"],
        ))

    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            with get_conn() as write_conn, write_conn.cursor() as write_cur:
                execute_values(
                    write_cur,
                    """
                    INSERT INTO dim_useragent
                        (user_agent, agent_type, browser_name, browser_version,
                         operating_system, device_type)
                    VALUES %s
                    ON CONFLICT (user_agent) DO NOTHING
                    """,
                    insert_data,
                    page_size=1000,
                )
                write_conn.commit()
            print(f"[dim_useragent] Inserted / confirmed {len(insert_data)} row(s).")
            return
        except psycopg2.OperationalError as e:
            if attempt == attempts:
                raise
            print(f"[dim_useragent] DB write attempt {attempt} failed: {e} — retrying …")
            time.sleep(2 * attempt)


# ══════════════════════════════════════════════════════════════════════════
# PUBLIC CALLABLE
# ══════════════════════════════════════════════════════════════════════════


def _ensure_dimension_tables(conn):
    """Create dimension tables if they do not exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_geolocation (
                geolocation_sk SERIAL PRIMARY KEY,
                ip           VARCHAR(50) UNIQUE,
                country      VARCHAR(100),
                region       VARCHAR(100),
                city         VARCHAR(100),
                postcode     VARCHAR(20),
                latitude     DOUBLE PRECISION,
                longitude    DOUBLE PRECISION,
                isp          VARCHAR(200)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_useragent (
                user_agent_sk    SERIAL PRIMARY KEY,
                user_agent       TEXT UNIQUE,
                agent_type       VARCHAR(20),
                browser_name     VARCHAR(50),
                browser_version  VARCHAR(50),
                operating_system VARCHAR(50),
                device_type      VARCHAR(20)
            );
        """)
    conn.commit()
    print("[OK] Dimension tables ensured.")


def export_dimensions(**context) -> None:
    """Build Airflow-managed dimension tables.

    0. Creates dimension tables if they don't exist.
    1. Inserts default ``-1`` rows (idempotent).
    2. Builds ``dim_geolocation`` from the Silver Delta table (single
       source of truth for client-IP geolocation).
    3. Builds ``dim_useragent`` from the ``raw_enriched`` Postgres
       table by parsing user-agent strings.

    Accepts ``**context`` for Airflow ``PythonOperator`` compatibility.
    Designed to be idempotent — safe to retry and re-run.
    """
    print("=" * 60)
    print("export_dimensions: Starting Airflow-managed dimension build")
    print("=" * 60)

    conn = get_conn()

    try:
        # Step 0 — ensure dimension tables exist.
        _ensure_dimension_tables(conn)

        # Step 1 — ensure default -1 rows exist.
        _ensure_default_rows(conn)
        print("[OK] Default -1 rows present.")

        # Step 2 — build dim_geolocation from Silver Delta.
        _build_dim_geolocation(conn)

        # Step 3 — build dim_useragent (library parsing).
        _build_dim_useragent(conn)

    finally:
        conn.close()

    print("=" * 60)
    print("export_dimensions: Complete.")
    print("=" * 60)
