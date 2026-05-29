"""
W3C ETL Airflow DAG

This module implements an incremental ETL pipeline for IIS W3C Extended Log
Format files. It provides helpers to parse raw log lines, build dimensional
tables in a PostgreSQL (RDS) warehouse, populate a fact table via SQL joins,
and export the final star-schema tables as CSV files for the user if they require them.
"""

import datetime as dt
import requests
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import time
import psycopg2
from urllib.parse import unquote_plus, urlparse
from psycopg2.extras import execute_values

from ipaddress import ip_address

from user_agents import parse as ua_parse

# ── Folder paths ───────────────────────────────────────────────────────────────
# Paths inside the Airflow worker/container where DAG assets live. These are
# used both by local development and inside the Airflow Docker container.
WorkingDirectory =  "/opt/airflow/dags/w3c"
# Directory where incoming W3C `.log` files should be placed.
LogFiles         =  "/opt/airflow/data/LogFiles/"
# Output directory for star-schema CSV exports.
StarSchema       =  "/opt/airflow/data/Star-Schema/"

# ── PostgreSQL connection settings ─────────────────────────────────────────────
# These are read from environment variables so the same DAG can target local
# Docker Postgres during development or an external AWS RDS instance.
# The current production setup is using the AWS RDS instance.

# Set W3C_USE_RDS=true and provide the W3C_DB_* variables in your .env file to use AWS RDS. 
# Otherwise, the defaults target the local Docker PostgreSQL.
DB_HOST = os.environ.get("W3C_DB_HOST", "postgres")
DB_PORT = int(os.environ.get("W3C_DB_PORT") or 5432)
DB_NAME = os.environ.get("W3C_DB_NAME", "w3c_warehouse")
DB_USER = os.environ.get("W3C_DB_USER", "airflow")
DB_PASS = os.environ.get("W3C_DB_PASS", "airflow")

def get_conn():
    """Create and return a new psycopg2 DB connection using module config.

    Callers should close the connection when finished. Keeping connection
    creation in a single helper centralises configuration and makes retries
    or instrumentation easier in the future.
    """
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )

# ── Create directory structure ─────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def GetFileExtension(uriStem):
    """Return the file extension for a URI stem (page path).

    Examples: '/path/index.html?x=1' -> 'html'. If no extension exists,
    returns the string 'no_extension'.
    """
    uriStem = uriStem.split("?")[0]
    if "." in uriStem:
        return uriStem.rsplit(".", 1)[-1].lower().strip()
    return "no_extension"

def GetPageCategory(extension):
    """Map a file extension to a human-friendly page category.

    Used when building the `dim_page` dimension to provide a coarse
    classification (Static Page, Image, Script, etc.).
    """
    categories = {
        "aspx": "Dynamic Page", "asp": "Dynamic Page",
        "html": "Static Page",  "htm": "Static Page", "shtml": "Static Page",
        "jpg":  "Image",        "jpeg": "Image", "png": "Image",
        "gif":  "Image",        "bmp": "Image",  "ico": "Icon",
        "css":  "Stylesheet",   "js": "Script",
        "txt":  "Text File",    "pdf": "Document",
        "no_extension": "Directory",
    }
    return categories.get(extension, "Other")

def GetStatusInfo(statusCode):
    """Return (category, human_readable) for an HTTP status code.

    Examples: 200 -> ('2xx','Success'), 404 -> ('4xx','Client Error').
    Non-integer input yields ('Other','Unknown').
    """
    try:
        code = int(statusCode)
    except Exception:
        return "Other", "Unknown"
    if 200 <= code < 300:   return "2xx", "Success"
    elif 300 <= code < 400: return "3xx", "Redirect"
    elif 400 <= code < 500: return "4xx", "Client Error"
    elif 500 <= code < 600: return "5xx", "Server Error"
    else:                   return "Other", "Unknown"

def ParseUserAgent(uaString):
    """Parse a user-agent string using the `user-agents` library.

    Returns a dict with keys: `AgentType`, `BrowserName`, `BrowserVersion`,
    `OS`, and `DeviceType`.
    """
    if not uaString or uaString.strip() == "-" or uaString == "Unknown":
        return {"AgentType": "Unknown", "BrowserName": "Unknown",
                "BrowserVersion": "Unknown", "OS": "Unknown", "DeviceType": "Unknown"}

    normalized = unquote_plus(uaString)
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
    return {"AgentType": agent_type, "BrowserName": browser_name,
            "BrowserVersion": browser_version, "OS": os_name, "DeviceType": device}

def ParseReferrer(referrerUrl):
    """Classify a referrer URL and return (domain, traffic_type).

    Special values: '-' or empty -> 'Direct'. Known social/search domains are
    categorised accordingly; otherwise 'Referral' or 'Internal'. Uses
    `urllib.parse.urlparse` for robust domain extraction.
    """
    if not referrerUrl or referrerUrl.strip() == "-":
        return "Direct", "Direct"
    if referrerUrl == "Unknown":
        return "Unknown", "Unknown"

    searchEngines = ["google", "bing", "yahoo", "baidu", "duckduckgo", "yandex", "ask"]
    social = ["facebook", "twitter", "linkedin", "reddit", "t.co", "instagram"]

    try:
        candidate = referrerUrl.strip()
        if "//" not in candidate:
            candidate = "http://" + candidate
        parsed = urlparse(candidate)
        domain = parsed.netloc.split("@")[-1].split(":")[0].lower().replace("www.", "")
        if not domain:
            domain = str(referrerUrl)
    except Exception:
        domain = str(referrerUrl)

    if "darwinsbeagleplants" in domain or "134.36.36.75" in domain:
        refType = "Internal"
    elif any(s in domain for s in searchEngines):
        refType = "Search Engine"
    elif any(s in domain for s in social):
        refType = "Social"
    else:
        refType = "Referral"

    return domain, refType


def _safe_int(val):
    """Convert to non-negative int or return None on failure.

    Used when parsing numeric fields from raw log text where '-' or
    malformed values may appear.
    """
    try:
        v = int(val)
        return v if v >= 0 else None
    except Exception:
        return None

def _safe_date(val):
    """Parse an ISO YYYY-MM-DD date string to a date, or return None.

    Swallows parsing errors and returns None which the DAG treats as NULL.
    """
    try:
        return datetime.strptime(val.strip(), "%Y-%m-%d").date()
    except Exception:
        return None

def _ensure_db_exists():
    """Create the target database if it doesn't already exist natively.
    Skipped for AWS RDS, but ensures the local Docker Postgres setup works immediately.
    """
    # Skip creation for AWS RDS as it is provisioned through the AWS console
    if os.environ.get("W3C_USE_RDS", "false").lower() == "true":
        print("Using AWS RDS: Skipping database creation as it is managed via AWS Console.")
        return

    print(f"Checking if local database '{DB_NAME}' exists...")
    try:
        # Connect to the default 'airflow' database to be able to run CREATE DATABASE
        setup_conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname="airflow",  # Connect to the default DB
            user=DB_USER,
            password=DB_PASS
        )
        setup_conn.autocommit = True
        setup_cur = setup_conn.cursor()
        
        setup_cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
        if not setup_cur.fetchone():
            print(f"Database '{DB_NAME}' not found. Creating it now...")
            setup_cur.execute(f"CREATE DATABASE {DB_NAME} OWNER {DB_USER}")
            print("Database created successfully.")
        else:
            print("Database already exists.")
            
        setup_cur.close()
        setup_conn.close()
    except Exception as e:
        print(f"Warning: Could not check/create local database automatically: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# TASK 1: Create all AWS RDS PostgreSQL tables
# ══════════════════════════════════════════════════════════════════════════════

def CreateDatabaseTables():
    """Create all required tables and insert default '-1' lookup rows.

    This function is idempotent and can be run multiple times safely; it
    uses `CREATE TABLE IF NOT EXISTS` and `ON CONFLICT DO NOTHING` for
    default rows used as unknown lookup keys.
    """
    # Ensure working directories exist before any DB work.
    for folder in [WorkingDirectory, LogFiles, StarSchema]:
        try:
            os.makedirs(folder, exist_ok=True)
        except Exception:
            pass

    # Ensure the local Database is actually created before we get the connection 
    _ensure_db_exists()

    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_logs (
            id           SERIAL PRIMARY KEY,
            log_date     DATE,
            log_time     TIME,
            server_ip    VARCHAR(50),
            method       VARCHAR(10),
            uri_stem     TEXT,
            uri_query    TEXT,
            server_port  INTEGER,
            username     VARCHAR(100),
            client_ip    VARCHAR(50),
            user_agent   TEXT,
            cookie       TEXT,
            referrer     TEXT,
            status       INTEGER,
            sub_status   INTEGER,
            win32_status INTEGER,
            bytes_sent   INTEGER,
            bytes_recv   INTEGER,
            time_taken   INTEGER,
            source_file  VARCHAR(100)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_geolocation (
            geolocation_sk SERIAL PRIMARY KEY,
            ip           VARCHAR(50) UNIQUE,
            country      VARCHAR(100),
            region       VARCHAR(100),
            city         VARCHAR(100),
            postcode     VARCHAR(20),
            latitude     VARCHAR(20),
            longitude    VARCHAR(20),
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

    # --- Standard Default "-1" Rows for Airflow-managed tables ---
    cur.execute("""
        INSERT INTO dim_geolocation (geolocation_sk, ip, country, region, city, postcode, latitude, longitude, isp)
        VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', '-', NULL, NULL, '-') ON CONFLICT DO NOTHING;
    """)
    cur.execute("""
        INSERT INTO dim_useragent (user_agent_sk, user_agent, agent_type, browser_name, browser_version, operating_system, device_type)
        VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown') ON CONFLICT DO NOTHING;
    """)

    # ── Indexes for incremental query performance ──────────────────────────────
    # NOTE: fact_webrequest indexes are managed by dbt (post_hook in staging/fact_webrequest.sql)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_logs_source_file ON raw_logs(source_file);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_logs_client_ip ON raw_logs(client_ip);")

    conn.commit()
    cur.close()
    conn.close()
    print("Database tables and indexes created successfully.")
    print("NOTE: crawler_ips and dim_visitortype are now managed by dbt (dbt_staging schema).")
    print("NOTE: dbt manages all dimensions + fact in dbt_staging schema, and marts in dbt_marts schema.")


# ══════════════════════════════════════════════════════════════════════════════
# TASK 2: Load raw log files into AWS RDS 'raw_logs' table
# ══════════════════════════════════════════════════════════════════════════════

def LoadRawLogsToDatabase():
    """Load NEW .log files from `LogFiles` into the `raw_logs` staging table.

    The loader skips files already present in `raw_logs.source_file` so the
    operation is incremental. Raw lines are parsed with `_ParseRawLogLine`
    and inserted in 5000-row batches using `_InsertRawBatch` for performance.
    """
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("SELECT DISTINCT source_file FROM raw_logs;")
    alreadyLoaded = {row[0] for row in cur.fetchall()}

    allLogFiles = sorted([f for f in os.listdir(LogFiles) if f.endswith(".log")])
    logFiles = [f for f in allLogFiles if f not in alreadyLoaded]

    if not logFiles:
        print("No new .log files to load — warehouse is already up to date.")
        cur.close()
        conn.close()
        return

    print(f"Found {len(logFiles)} new file(s) to load (skipping {len(alreadyLoaded)} already-loaded).")

    totalInserted = 0

    for fileName in logFiles:
        print(f"Loading {fileName}...")
        with open(LogFiles + fileName, "r", errors="replace") as f:
            lines = f.readlines()

        fileFormat = None
        for line in lines:
            line = line.rstrip("\n")
            if line.startswith("#Fields:"):
                fields = line.replace("#Fields:", "").strip().split()
                fileFormat = len(fields)
                break

        if fileFormat is None:
            print(f"WARNING: No #Fields header found in {fileName} — skipping file entirely.")
            continue
        if fileFormat not in (14, 18):
            print(f"WARNING: Unrecognised field count {fileFormat} in {fileName} — skipping file entirely.")
            continue

        batch = []
        for line in lines:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            row = _ParseRawLogLine(line, fileFormat, fileName)
            if row:
                batch.append(row)
            if len(batch) >= 5000:
                _InsertRawBatch(cur, batch)
                totalInserted += len(batch)
                batch = []

        if batch:
            _InsertRawBatch(cur, batch)
            totalInserted += len(batch)

        conn.commit()
        print(f"  Inserted so far: {totalInserted}")

    cur.close()
    conn.close()
    print(f"Raw log loading complete. Total: {totalInserted} rows.")


def _ParseRawLogLine(line, fileFormat, fileName):
    """Parse a single raw W3C log line into a tuple for DB insertion.

    The parser supports two common `.log` field counts (14 and 18). Returns a
    tuple matching the `raw_logs` table column order, or `None` on parse
    failure. Exceptions are swallowed to avoid failing the entire file load.
    """
    try:
        if fileFormat == 14:
            rs = line.rsplit(" ", 4)
            if len(rs) < 5: return None
            ls = rs[0].split(" ", 9)
            if len(ls) < 10: return None
            return (_safe_date(ls[0]), ls[1], ls[2], ls[3], ls[4], ls[5],
                    _safe_int(ls[6]), ls[7], ls[8], ls[9],
                    None, None,
                    _safe_int(rs[1]), _safe_int(rs[2]), _safe_int(rs[3]),
                    None, None, _safe_int(rs[4]), fileName)
        elif fileFormat == 18:
            rs = line.rsplit(" ", 6)
            if len(rs) < 7: return None
            ls = rs[0].split(" ", 11)
            if len(ls) < 12: return None
            referrer = ls[11].strip()
            return (_safe_date(ls[0]), ls[1], ls[2], ls[3], ls[4], ls[5],
                    _safe_int(ls[6]), ls[7], ls[8], ls[9],
                    ls[10], referrer,
                    _safe_int(rs[1]), _safe_int(rs[2]), _safe_int(rs[3]),
                    _safe_int(rs[4]), _safe_int(rs[5]), _safe_int(rs[6]), fileName)
        return None
    except Exception:
        return None

def _InsertRawBatch(cur, batch):
    """Insert a batch of parsed raw log tuples using psycopg2 execute_values.

    Batching improves performance over single-row INSERTs and the page_size
    is tuned to 5000 rows per batch to balance memory and network usage.
    """
    execute_values(cur, """
        INSERT INTO raw_logs (
            log_date, log_time, server_ip, method, uri_stem, uri_query,
            server_port, username, client_ip, user_agent, cookie, referrer,
            status, sub_status, win32_status, bytes_sent, bytes_recv,
            time_taken, source_file
        ) VALUES %s
    """, batch, page_size=5000)


# ══════════════════════════════════════════════════════════════════════════════
# BUILD DIMENSIONS (SELECT DISTINCT FROM RAW)
# ══════════════════════════════════════════════════════════════════════════════
#
# NOTE: DetectCrawlerIPs and dim_visitortype have been migrated to dbt models
# (models/staging/crawler_ips.sql, models/staging/dim_visitortype.sql).
# Only dimensions requiring external APIs or Python libraries remain here.

def makeLocationDimension():
    """Populate `dim_geolocation` by calling an external geo-IP API in batches.

    Private and link-local ranges are short-circuited; public IPs are looked
    up via ip-api.com's batch endpoint (100 ips per request). Results are
    inserted with `ON CONFLICT DO NOTHING` so the function is incremental.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT ip FROM dim_geolocation")
    existing_ips = {row[0] for row in cur.fetchall()}
    
    cur.execute("SELECT DISTINCT client_ip FROM raw_logs WHERE client_ip IS NOT NULL AND client_ip != '-'")
    all_ips = {row[0] for row in cur.fetchall()}

    # Close RDS connection early: geo API calls can take a long time and stale idle DB sessions are
    # more likely to be dropped by network/RDS before the eventual INSERT.
    cur.close()
    conn.close()
    
    toFetch = list(all_ips - existing_ips)
    
    cache = {}
    requests_to_make = []
    for ip in toFetch:
        # Prefer the stdlib `ipaddress` checks for correctness and IPv6
        # support instead of brittle string-prefix matching.
        try:
            addr = ip_address(ip)
            if addr.is_private or addr.is_link_local or addr.is_loopback:
                cache[ip] = {"Country": "Private", "StateRegion": "Private",
                             "City": "Private Network", "Postcode": "-",
                             "Latitude": None, "Longitude": None, "ISP": "-"}
            else:
                requests_to_make.append(ip)
        except Exception:
            # Malformed IP strings are treated as unknown to avoid failing
            # the whole batch; they will be inserted as unknown geo rows.
            cache[ip] = {"Country": "Unknown", "StateRegion": "Unknown",
                         "City": "Unknown", "Postcode": "-",
                         "Latitude": None, "Longitude": None, "ISP": "-"}
            
    if requests_to_make:
        for i in range(0, len(requests_to_make), 100):
            batch   = requests_to_make[i:i+100]
            payload = [{"query": ip, "fields": "status,country,regionName,city,zip,lat,lon,isp,query"}
                       for ip in batch]
            try:
                # ip-api.com batch endpoint — HTTP only on the free tier; HTTPS requires a paid plan.
                resp = requests.post("http://ip-api.com/batch", json=payload, timeout=30)
                for entry in resp.json():
                    ip = entry.get("query", "")
                    if entry.get("status") == "success":
                        lat = entry.get("lat")
                        lon = entry.get("lon")
                        cache[ip] = {"Country": entry.get("country", "Unknown"),
                                     "StateRegion": entry.get("regionName", "Unknown"),
                                     "City": entry.get("city", "Unknown"),
                                     "Postcode": str(entry.get("zip", "-")),
                                     "Latitude": str(lat) if lat is not None else None,
                                     "Longitude": str(lon) if lon is not None else None,
                                     "ISP": entry.get("isp", "-")}
                    else:
                        cache[ip] = {"Country": "Unknown", "StateRegion": "Unknown",
                                     "City": "Unknown", "Postcode": "-",
                                     "Latitude": None, "Longitude": None, "ISP": "-"}
                if i + 100 < len(requests_to_make):
                    time.sleep(1.5)
            except Exception as e:
                for ip in batch:
                    if ip not in cache:
                        cache[ip] = {"Country": "Unknown", "StateRegion": "Unknown",
                                     "City": "Unknown", "Postcode": "-",
                                     "Latitude": None, "Longitude": None, "ISP": "-"}

    insert_data = []
    for ip in toFetch:
        if ip in cache:
            geo = cache[ip]
            insert_data.append((ip, geo["Country"], geo["StateRegion"], geo["City"],
                               geo["Postcode"], geo["Latitude"], geo["Longitude"], geo["ISP"]))

    if not insert_data:
        return

    # Reconnect for write phase and retry transient SSL/network disconnects.
    attempts = 3
    for attempt in range(1, attempts + 1):
        conn = None
        cur = None
        try:
            conn = get_conn()
            cur = conn.cursor()
            execute_values(cur, """
                INSERT INTO dim_geolocation (ip, country, region, city, postcode, latitude, longitude, isp)
                VALUES %s ON CONFLICT (ip) DO NOTHING
            """, insert_data, page_size=1000)

            conn.commit()
            break
        except psycopg2.OperationalError as e:
            if conn:
                conn.rollback()
            if attempt == attempts:
                raise
            # Short backoff then retry with a fresh connection.
            time.sleep(2 * attempt)
            print(f"dim_geolocation insert retry {attempt}/{attempts - 1} after OperationalError: {e}")
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()



MethodDescriptions = {
    "GET": "Retrieve a resource", "POST": "Submit data to the server",
    "HEAD": "Retrieve headers only", "PUT": "Upload a resource",
    "DELETE": "Delete a resource", "OPTIONS": "Describe communication options",
    "PATCH": "Partial resource update",
}



def makeUserAgentDimension():
    """Populate `dim_useragent` by parsing raw user-agent strings.

    The lightweight parser aims to capture the agent type (crawler vs
    browser), browser family and OS for common analysis needs.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT user_agent FROM raw_logs WHERE user_agent IS NOT NULL AND user_agent != '-'")
    rows = cur.fetchall()
    insert_data = []
    for (uaRaw,) in rows:
        parsed = ParseUserAgent(uaRaw)
        insert_data.append((uaRaw[:1000], parsed["AgentType"], parsed["BrowserName"],
                            parsed["BrowserVersion"], parsed["OS"], parsed["DeviceType"]))
                            
    execute_values(cur, """
        INSERT INTO dim_useragent (user_agent, agent_type, browser_name, browser_version, operating_system, device_type)
        VALUES %s ON CONFLICT (user_agent) DO NOTHING
    """, insert_data)
    conn.commit()
    cur.close()
    conn.close()




# ══════════════════════════════════════════════════════════════════════════════
# TASK: Export all tables to CSV
# ══════════════════════════════════════════════════════════════════════════════




# ══════════════════════════════════════════════════════════════════════════════
# TASK: Export all tables to CSV
# ══════════════════════════════════════════════════════════════════════════════

def ExportCSV():
    """
    Exports every table to CSV files in the StarSchema folder
    Inside the container:  /opt/airflow/dags/w3c/StarSchema/
    
    This step is pretty pointless since Power BI is connected to
    the RDS and not the .csv files but its just nice to be able to 
    see the data in a more familiar format.
    """
    export_dir = StarSchema

    tables = [
        # dbt staging layer (dbt_staging schema)
        "dbt_staging.fact_webrequest",
        "dbt_staging.dim_date",
        "dbt_staging.dim_time",
        "dbt_staging.dim_page",
        "dbt_staging.dim_status",
        "dbt_staging.dim_referrer",
        "dbt_staging.dim_method",
        "dbt_staging.dim_visitortype",
        "dbt_staging.dim_visit_buckets",
        "dbt_staging.crawler_ips",
        # dbt mart layer (dbt_marts schema)
        "dbt_marts.mart_page_performance",
        "dbt_marts.mart_daily_aggregates",
        "dbt_marts.mart_crawler_analysis",
        "dbt_marts.mart_browser_analysis",
        "dbt_marts.mart_timeofday_analysis",
        # Airflow-managed (public schema)
        "public.dim_geolocation",
        "public.dim_useragent",
    ]

    conn = get_conn()
    for table in tables:
        path = os.path.join(export_dir, f"{table}.csv")
        with conn.cursor() as cur, open(path, "w") as f:
            cur.copy_expert(f"COPY {table} TO STDOUT WITH CSV HEADER", f)
        print(f"  Exported {table} → {path}")

    conn.close()
    print(f"CSV export complete. {len(tables)} files written to {export_dir}")


# ══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
#
# The DAG orchestrates the ETL pipeline: create tables, load raw logs,
# build dimension tables, build the fact table, and export CSVs. Schedule is
# weekly by default but can be changed to meet your environment needs.
# ═════════════════════════════════════════════════════════════════════════════=

dag = DAG(
    dag_id="Process_W3C_Data",
    schedule="0 17 * * FRI",
    start_date=dt.datetime(2026, 3, 1),
    catchup=False,
)

task_CreateDatabaseTables = PythonOperator(
    task_id="task_CreateDatabaseTables",
    python_callable=CreateDatabaseTables,
    dag=dag,
)
task_LoadRawLogsToDatabase = PythonOperator(
    task_id="task_LoadRawLogsToDatabase",
    python_callable=LoadRawLogsToDatabase,
    dag=dag,
)
# Airflow-managed dimensions (enrichment requiring external APIs or Python libs)
# NOTE: DetectCrawlerIPs and dim_visitortype are now dbt models (staging/).
task_makeLocationDimension = PythonOperator(task_id="task_makeLocationDimension", python_callable=makeLocationDimension, dag=dag)
task_makeUserAgentDimension = PythonOperator(task_id="task_makeUserAgentDimension", python_callable=makeUserAgentDimension, dag=dag)

# Great Expectations: validate raw_logs before dbt transformation
GE_DIR = "/opt/airflow/great_expectations"

task_ge_raw_logs = BashOperator(
    task_id="task_ge_raw_logs",
    bash_command=f"python {GE_DIR}/run_checkpoint.py raw_logs_checkpoint",
    dag=dag,
)

# dbt-managed dimensions + fact table + mart models
#   Staging layer: dim_date, dim_time, dim_page, dim_status, dim_method,
#                  dim_referrer, dim_visit_buckets, fact_webrequest,
#                  crawler_ips, dim_visitortype
#   Mart layer:    mart_page_performance, mart_daily_aggregates, mart_crawler_analysis,
#                  mart_browser_analysis, mart_timeofday_analysis
DBT_PROJECT_DIR = "/opt/airflow/dbt/w3c"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

task_dbt_deps = BashOperator(
    task_id="task_dbt_deps",
    bash_command=f"""
if [ ! -d "{DBT_PROJECT_DIR}/dbt_packages" ]; then
    dbt deps --project-dir {DBT_PROJECT_DIR}
else
    echo "dbt_packages already exists, skipping dbt deps"
fi
""",
    dag=dag,
)

task_dbt_run = BashOperator(
    task_id="task_dbt_run",
    bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)
task_dbt_test = BashOperator(
    task_id="task_dbt_test",
    bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)
task_dbt_docs = BashOperator(
    task_id="task_dbt_docs",
    bash_command=f"dbt docs generate --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)
task_ExportCSV = PythonOperator(
    task_id="task_ExportCSV",
    python_callable=ExportCSV,
    dag=dag,
)

# ── Dependencies ───────────────────────────────────────────────────────────────

# Phase 1 & 2: Table creation → Raw log loading
task_CreateDatabaseTables >> task_LoadRawLogsToDatabase

# Phase 3a: Airflow-managed enrichment tasks (external APIs, Python libs)
task_LoadRawLogsToDatabase >> task_makeLocationDimension
task_LoadRawLogsToDatabase >> task_makeUserAgentDimension

# Phase 3b: Great Expectations validates raw_logs quality before dbt
#           Runs in parallel with enrichment tasks (both read raw_logs, don't modify it)
task_LoadRawLogsToDatabase >> task_ge_raw_logs

# Phase 3c: All enrichment + validation must complete before dbt
airflow_tasks = [
    task_makeLocationDimension,
    task_makeUserAgentDimension,
    task_ge_raw_logs
]
airflow_tasks >> task_dbt_deps

# Phase 3d: Run dbt models (staging → fact → marts)
task_dbt_deps >> task_dbt_run

# Phase 3e: dbt quality checks
task_dbt_run >> task_dbt_test

# Phase 3f: dbt documentation generation
task_dbt_test >> task_dbt_docs

# Phase 4: Export fresh CSVs after every successful pipeline run
task_dbt_docs >> task_ExportCSV
