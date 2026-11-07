"""
Export Airflow-Managed Dimension Tables.

Reads enriched rows from two upstream sources and builds the dimension
tables that the warehouse exposes to dbt / Power BI:

* ``dim_geolocation``  — sourced from the **Spark Silver Delta table**
  (single source of truth for client-IP geolocation). The silver writer
  (``pipeline/spark/jobs/silver_enrichment.py``) populates ``country``,
  ``region``, ``city``, ``postcode``, ``latitude``, ``longitude`` and
  ``isp`` from local MaxMind GeoLite2 databases (City + ASN), giving us
  in-process enrichment with no external network calls.
* ``dim_useragent``    — parsed with the ``user-agents`` library from
  the ``raw_enriched`` Postgres table (still the canonical source for
  raw user-agent strings in the warehouse).

``dim_useragent`` uses ``INSERT … ON CONFLICT DO NOTHING`` so every run
is idempotent and incremental. ``dim_geolocation`` is SCD Type 2 keyed on
``ip``: a changed MaxMind attribute for an IP expires its current row
(``valid_to`` / ``is_current = 0``) and inserts a fresh current version,
mirroring the Azure path
(``spark_ingestion_azure._build_dim_geolocation``). Default ``-1``
surrogate-key rows are inserted so fact tables can reference them for
unknown / missing values.

This module is designed to be called as an Airflow ``PythonOperator``
callable (it accepts ``**context``).
"""

from __future__ import annotations

import hashlib
import logging
import os
import random
import time
from urllib.parse import unquote_plus

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from user_agents import parse as ua_parse

log = logging.getLogger(__name__)

# Default location of the Silver Delta table written by Spark. Can be
# overridden with the ``W3C_SILVER_PATH`` environment variable.
DEFAULT_SILVER_PATH = "/opt/spark/delta/silver"

# SCD Type 2 write for ``dim_geolocation``, shared by ``_build_dim_geolocation``
# and the Postgres-backed test (``tests/test_dim_geolocation_scd2_db.py``) so the
# SQL asserted against a real server cannot drift from the SQL that runs in
# production. Step 1 expires the current version of every IP whose tracked
# attributes changed; the row-wise ``IS DISTINCT FROM`` treats NULL lat/lon
# correctly. Step 2 inserts a current version for every IP that has none now
# (brand-new plus just-expired) — an unchanged IP matches nothing, so re-runs
# never churn versions.
GEO_SCD2_EXPIRE_SQL = """
    UPDATE dim_geolocation AS d
       SET valid_to = NOW(), is_current = FALSE
      FROM (VALUES %s) AS s(
               ip, country, region, city, postcode,
               latitude, longitude, isp)
     WHERE d.ip = s.ip
       AND d.is_current
       AND (d.country, d.region, d.city, d.postcode,
            d.latitude, d.longitude, d.isp)
           IS DISTINCT FROM
           (s.country::VARCHAR(100), s.region::VARCHAR(100),
            s.city::VARCHAR(100), s.postcode::VARCHAR(20),
            s.latitude::DOUBLE PRECISION,
            s.longitude::DOUBLE PRECISION, s.isp::VARCHAR(200))
    """

GEO_SCD2_INSERT_SQL = """
    INSERT INTO dim_geolocation
        (ip, country, region, city, postcode, latitude, longitude, isp)
    SELECT s.ip::VARCHAR(50), s.country::VARCHAR(100),
           s.region::VARCHAR(100), s.city::VARCHAR(100),
           s.postcode::VARCHAR(20),
           s.latitude::DOUBLE PRECISION,
           s.longitude::DOUBLE PRECISION, s.isp::VARCHAR(200)
      FROM (VALUES %s) AS s(
               ip, country, region, city, postcode,
               latitude, longitude, isp)
     WHERE NOT EXISTS (
         SELECT 1 FROM dim_geolocation d
          WHERE d.ip = s.ip AND d.is_current
     )
    """


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
                (user_agent_sk, ua_hash, user_agent, agent_type, browser_name, browser_version, operating_system, device_type)
            VALUES (-1, MD5('Unknown'), 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown')
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
    # Deterministic per-IP pick: sort by the same key order the dbt geo_lookup
    # ROW_NUMBER uses, then take the first row of each client_ip. Without the
    # sort, groupby.first() follows physical parquet row order, so the chosen
    # geo for an IP varies between runs (and with partition/compaction layout).
    geo = (
        geo
        .sort_values(
            ["client_ip", "country", "region", "city", "latitude", "longitude"],
            na_position="last",
        )
        .groupby("client_ip", as_index=False)
        .first()
    )

    if has_postcode:
        # Bring the postcode column along for the same distinct IP.
        pc = (
            df[["client_ip", "postcode"]]
            .dropna(subset=["client_ip"])
            .sort_values(["client_ip", "postcode"], na_position="last")
            .groupby("client_ip", as_index=False)["postcode"]
            .first()
        )
        geo = geo.merge(pc, on="client_ip", how="left")
    else:
        # Column hasn't been written yet (silver not re-run). Use None
        # and let the defaults layer below apply "-".
        geo["postcode"] = None

    return geo


def _write_geo_versions(conn, insert_data) -> None:
    """Run the two shared SCD2 statements for ``dim_geolocation`` on *conn*.

    Extracted so the operator and the Postgres-backed test execute the exact
    same SQL.  Callers own the transaction (commit / rollback) — this only
    issues the expire then insert statements, in that order.
    """
    with conn.cursor() as cur:
        execute_values(cur, GEO_SCD2_EXPIRE_SQL, insert_data, page_size=1000)
        execute_values(cur, GEO_SCD2_INSERT_SQL, insert_data, page_size=1000)


def _resolve_silver_path(silver_path: str | None) -> str:
    """Return explicit path or fall back to env/default."""
    if silver_path is not None:
        return silver_path
    return os.environ.get("W3C_SILVER_PATH", DEFAULT_SILVER_PATH)


def _build_geo_insert_data(geo) -> list:
    """Apply schema defaults and build the SCD2 insert payload."""
    insert_data = []
    for _, row in geo.iterrows():
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
    return insert_data


def _write_geo_with_retry(insert_data) -> None:
    """Write geo versions with 3-attempt retry on operational errors."""
    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            with get_conn() as write_conn:
                _write_geo_versions(write_conn, insert_data)
                write_conn.commit()
            return
        except psycopg2.OperationalError as e:
            if attempt == attempts:
                raise
            delay = 2 * attempt
            log.warning("[dim_geolocation] DB write attempt %s failed: %s — retrying in %.1fs", attempt, e, delay)
            time.sleep(delay + random.uniform(0, delay / 2))


def _build_dim_geolocation(silver_path: str | None = None):
    """Populate ``dim_geolocation`` from the Silver Delta table (SCD Type 2).

    The Silver writer (``silver_enrichment.py``) is the single source of
    truth for geo enrichment — it produces ``country``, ``region``,
    ``city``, ``postcode``, ``latitude``, ``longitude`` and ``isp``
    using local MaxMind GeoLite2 databases.  We extract one row per
    distinct ``client_ip`` and write it as the current version.

    Mirrors the Azure path's SCD Type 2 semantics
    (``spark_ingestion_azure._build_dim_geolocation``) — the same
    ``valid_from`` / ``valid_to`` / ``is_current`` convention — but keyed
    on ``ip`` (the local natural key) instead of ``geo_hash``.  Every geo
    attribute except ``ip`` is tracked, so a MaxMind change expires the
    current row (``valid_to`` / ``is_current = 0``) and inserts a fresh
    current row; an unchanged IP matches nothing, so re-runs are
    idempotent and never churn versions.

    Parameters
    ----------
    silver_path : str or None
        Path to the Silver Delta table.  ``None`` falls back to
        ``$W3C_SILVER_PATH`` or ``DEFAULT_SILVER_PATH``.

    A read failure of the Silver Delta is fatal: it is logged at ERROR and
    re-raised so a failed read cannot leave stale dimensions behind a green
    task.  An empty (but readable) Silver Delta is still a no-op.
    """
    silver_path = _resolve_silver_path(silver_path)

    log.info(f"[dim_geolocation] Reading geo dim from Silver Delta: {silver_path}")

    try:
        geo = _read_silver_geo_dim(silver_path)
    except FileNotFoundError as exc:
        # A missing Silver path is a genuine read failure, not an empty source:
        # returning here would leave stale dimensions while the task stays green.
        log.error("[dim_geolocation] Silver path not found at %s: %s", silver_path, exc)
        raise
    except Exception as exc:
        log.error("[dim_geolocation] Failed to read Silver Delta at %s: %s", silver_path, exc)
        raise

    # Distinguish "no rows" (a legitimate no-op) from "read blew up" (above).
    if geo.empty:
        log.info("[dim_geolocation] Silver Delta is empty; nothing to do.")
        return

    # Build the source payload.  Apply the schema's documented defaults
    # for missing / sentinel values:
    #   - text geo fields (country, region, city) → "Unknown"
    #   - postcode                                → "-"
    #   - isp                                     → "-"
    #   - lat/lon                                 → NULL
    insert_data = _build_geo_insert_data(geo)

    if not insert_data:
        return

    log.info(f"[dim_geolocation] Versioning {len(insert_data)} IP(s) from Silver …")
    _write_geo_with_retry(insert_data)
    log.info(f"[dim_geolocation] SCD2 write complete for {len(insert_data)} IP(s).")


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
        log.info("[dim_useragent] No user-agent rows found in raw_enriched — nothing to do.")
        return

    insert_data = []
    for (ua_raw,) in rows:
        parsed = _parse_user_agent(ua_raw)
        # Key on md5 of the FULL raw UA (matches dbt's tsql_ua_hash join).
        # The raw string is stored whole, so UAs sharing a long prefix no
        # longer collapse onto a single dim row and inherit wrong attributes.
        ua_hash = hashlib.md5(ua_raw.encode("utf-8"), usedforsecurity=False).hexdigest()
        insert_data.append((
            ua_hash,
            ua_raw,
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
                        (ua_hash, user_agent, agent_type, browser_name, browser_version,
                         operating_system, device_type)
                    VALUES %s
                    ON CONFLICT (ua_hash) DO NOTHING
                    """,
                    insert_data,
                    page_size=1000,
                )
                write_conn.commit()
            log.info(f"[dim_useragent] Inserted / confirmed {len(insert_data)} row(s).")
            return
        except psycopg2.OperationalError as e:
            if attempt == attempts:
                raise
            delay = 2 * attempt
            log.warning("[dim_useragent] DB write attempt %s failed: %s — retrying in %.1fs", attempt, e, delay)
            time.sleep(delay + random.uniform(0, delay / 2))


# ══════════════════════════════════════════════════════════════════════════
# PUBLIC CALLABLE
# ══════════════════════════════════════════════════════════════════════════


def _ensure_dimension_tables(conn):
    """Create dimension tables if they do not exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_geolocation (
                geolocation_sk SERIAL PRIMARY KEY,
                ip           VARCHAR(50),
                country      VARCHAR(100),
                region       VARCHAR(100),
                city         VARCHAR(100),
                postcode     VARCHAR(20),
                latitude     DOUBLE PRECISION,
                longitude    DOUBLE PRECISION,
                isp          VARCHAR(200),
                -- SCD Type 2 validity columns (mirrors the Azure dim_geolocation)
                valid_from   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                valid_to     TIMESTAMPTZ NOT NULL DEFAULT '9999-12-31',
                is_current   BOOLEAN NOT NULL DEFAULT TRUE
            );
        """)
        # Migration for deployments predating SCD2: the old UNIQUE(ip)
        # constraint forbids a second version of an IP, so drop it, add the
        # validity columns (existing rows become the first current version via
        # the defaults) and enforce one current row per IP. Idempotent for both
        # a fresh table (the columns already exist; the constraint never did)
        # and an upgraded one.
        cur.execute(
            "ALTER TABLE dim_geolocation ADD COLUMN IF NOT EXISTS valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW()"
        )
        cur.execute(
            "ALTER TABLE dim_geolocation ADD COLUMN IF NOT EXISTS valid_to TIMESTAMPTZ NOT NULL DEFAULT '9999-12-31'"
        )
        cur.execute("ALTER TABLE dim_geolocation ADD COLUMN IF NOT EXISTS is_current BOOLEAN NOT NULL DEFAULT TRUE")
        cur.execute("ALTER TABLE dim_geolocation DROP CONSTRAINT IF EXISTS dim_geolocation_ip_key")
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_geolocation__current ON dim_geolocation (ip) WHERE is_current"
        )
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_useragent (
                user_agent_sk    SERIAL PRIMARY KEY,
                ua_hash          TEXT CONSTRAINT ux_dim_useragent_ua_hash UNIQUE,
                user_agent       TEXT UNIQUE,
                agent_type       VARCHAR(20),
                browser_name     VARCHAR(50),
                browser_version  VARCHAR(50),
                operating_system VARCHAR(50),
                device_type      VARCHAR(20)
            );
        """)
        # Migration for deployments predating ua_hash: add the column, backfill
        # it (md5 of the stored UA — matches dbt's tsql_ua_hash on Postgres) and
        # enforce uniqueness. The old user_agent uniqueness is kept: an equal
        # string yields an equal hash, so ON CONFLICT (ua_hash) still dedupes.
        cur.execute("ALTER TABLE dim_useragent ADD COLUMN IF NOT EXISTS ua_hash TEXT")
        cur.execute("""
            UPDATE dim_useragent
               SET ua_hash = MD5(user_agent)
             WHERE ua_hash IS NULL AND user_agent IS NOT NULL
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_useragent_ua_hash ON dim_useragent (ua_hash)")
    conn.commit()
    log.info("[OK] Dimension tables ensured.")


def export_dimensions(**context) -> None:
    """Build Airflow-managed dimension tables.

    0. Creates dimension tables if they don't exist.
    1. Inserts default ``-1`` rows (idempotent).
     2. Builds ``dim_geolocation`` from the Silver Delta table (single
        source of truth for client-IP geolocation) as SCD Type 2 — see
        ``_build_dim_geolocation``.
     3. Builds ``dim_useragent`` from the ``raw_enriched`` Postgres
        table by parsing user-agent strings.

    A Silver read failure aborts the task (``_build_dim_geolocation`` logs
    at ERROR and re-raises).

    Accepts ``**context`` for Airflow ``PythonOperator`` compatibility.
    Designed to be idempotent — safe to retry and re-run.
    """
    log.info("export_dimensions: Starting Airflow-managed dimension build")

    conn = get_conn()

    try:
        # Step 0 — ensure dimension tables exist.
        _ensure_dimension_tables(conn)

        # Step 1 — ensure default -1 rows exist.
        _ensure_default_rows(conn)
        log.info("[OK] Default -1 rows present.")

        # Step 2 — build dim_geolocation from Silver Delta.
        _build_dim_geolocation()

        # Step 3 — build dim_useragent (library parsing).
        _build_dim_useragent(conn)

    finally:
        conn.close()

    log.info("export_dimensions: Complete.")
