"""
Export Airflow-Managed Dimension Tables.

Reads enriched rows from the Spark-produced ``raw_enriched`` table in
PostgreSQL and builds two dimension tables that require Python-level
enrichment (external geo-IP API and user-agent parsing):

* ``dim_geolocation``  — resolved via ip-api.com batch API
* ``dim_useragent``    — parsed with the ``user-agents`` library

Both tables use ``INSERT … ON CONFLICT DO NOTHING`` so every run is
idempotent and incremental. Default ``-1`` surrogate-key rows are
inserted so fact tables can reference them for unknown / missing values.

This module is designed to be called as an Airflow ``PythonOperator``
callable (it accepts ``**context``).
"""

from __future__ import annotations

import os
import time
from ipaddress import ip_address
from urllib.parse import unquote_plus

import psycopg2
import requests
from psycopg2.extras import execute_values
from user_agents import parse as ua_parse


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


def _build_dim_geolocation(conn):
    """Populate ``dim_geolocation`` via ip-api.com batch API.

    Reads distinct ``client_ip`` values from ``raw_enriched`` and resolves
    their geographic location + ISP through the ip-api.com batch endpoint.
    Private / link-local / loopback addresses are short-circuited without
    an API call.  Idempotent — uses ``ON CONFLICT DO NOTHING``.
    """
    cur = conn.cursor()

    # Determine which IPs are already resolved.
    cur.execute("SELECT DISTINCT ip FROM dim_geolocation")
    existing_ips = {row[0] for row in cur.fetchall()}

    # Fetch distinct client IPs from the enriched (Spark) table.
    cur.execute("SELECT DISTINCT client_ip FROM raw_enriched WHERE client_ip IS NOT NULL AND client_ip != '-'")
    all_ips = {row[0] for row in cur.fetchall()}

    cur.close()

    to_fetch = list(all_ips - existing_ips)
    if not to_fetch:
        print(f"[dim_geolocation] All {len(all_ips)} IPs already resolved — nothing to do.")
        return

    print(f"[dim_geolocation] Resolving {len(to_fetch)} new IP(s) via ip-api.com …")

    cache: dict[str, dict] = {}
    api_batch: list[str] = []

    for ip in to_fetch:
        try:
            addr = ip_address(ip)
            if addr.is_private or addr.is_link_local or addr.is_loopback:
                cache[ip] = {
                    "Country": "Private",
                    "StateRegion": "Private",
                    "City": "Private Network",
                    "Postcode": "-",
                    "Latitude": None,
                    "Longitude": None,
                    "ISP": "-",
                }
            else:
                api_batch.append(ip)
        except Exception:
            # Malformed IP — treat as unknown.
            cache[ip] = {
                "Country": "Unknown",
                "StateRegion": "Unknown",
                "City": "Unknown",
                "Postcode": "-",
                "Latitude": None,
                "Longitude": None,
                "ISP": "-",
            }

    # ── Batch-resolve public IPs via ip-api.com ────────────────────────
    # Free tier limit: 45 requests/min (each batch of up to 100 IPs counts
    # as one request). We send 100-IP batches and sleep 1.5 s between them.
    if api_batch:
        for i in range(0, len(api_batch), 100):
            batch_ips = api_batch[i : i + 100]
            payload = [
                {
                    "query": ip,
                    "fields": "status,country,regionName,city,zip,lat,lon,isp,query",
                }
                for ip in batch_ips
            ]
            try:
                resp = requests.post("http://ip-api.com/batch", json=payload, timeout=30)
                for entry in resp.json():
                    ip = entry.get("query", "")
                    if entry.get("status") == "success":
                        lat = entry.get("lat")
                        lon = entry.get("lon")
                        cache[ip] = {
                            "Country": entry.get("country", "Unknown"),
                            "StateRegion": entry.get("regionName", "Unknown"),
                            "City": entry.get("city", "Unknown"),
                            "Postcode": str(entry.get("zip", "-")),
                            "Latitude": lat,
                            "Longitude": lon,
                            "ISP": entry.get("isp", "-"),
                        }
                    else:
                        cache[ip] = {
                            "Country": "Unknown",
                            "StateRegion": "Unknown",
                            "City": "Unknown",
                            "Postcode": "-",
                            "Latitude": None,
                            "Longitude": None,
                            "ISP": "-",
                        }
                # Rate-limit: stay well under 45 req / min
                if i + 100 < len(api_batch):
                    time.sleep(1.5)
            except Exception as e:
                print(f"[dim_geolocation] API batch failed ({e}) — marking IPs as Unknown.")
                for ip in batch_ips:
                    if ip not in cache:
                        cache[ip] = {
                            "Country": "Unknown",
                            "StateRegion": "Unknown",
                            "City": "Unknown",
                            "Postcode": "-",
                            "Latitude": None,
                            "Longitude": None,
                            "ISP": "-",
                        }

    # ── Assemble INSERT rows ──────────────────────────────────────────
    insert_data = [
        (
            ip,
            cache[ip]["Country"],
            cache[ip]["StateRegion"],
            cache[ip]["City"],
            cache[ip]["Postcode"],
            cache[ip]["Latitude"],
            cache[ip]["Longitude"],
            cache[ip]["ISP"],
        )
        for ip in to_fetch
        if ip in cache
    ]

    if not insert_data:
        return

    # ── Write with retry ──────────────────────────────────────────────
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
    """Build Airflow-managed dimension tables from ``raw_enriched``.

    0. Creates dimension tables if they don't exist.
    1. Inserts default ``-1`` rows (idempotent).
    2. Resolves ``dim_geolocation`` via ip-api.com batch API.
    3. Parses ``dim_useragent`` via the ``user-agents`` library.

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

        # Step 2 — build dim_geolocation (external API).
        _build_dim_geolocation(conn)

        # Step 3 — build dim_useragent (library parsing).
        _build_dim_useragent(conn)

    finally:
        conn.close()

    print("=" * 60)
    print("export_dimensions: Complete.")
    print("=" * 60)
