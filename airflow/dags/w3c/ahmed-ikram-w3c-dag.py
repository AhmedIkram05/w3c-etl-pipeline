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
import time
import psycopg2
from urllib.parse import unquote_plus, urlparse
from psycopg2.extras import execute_values

try:
    import holidays
except ImportError:
    holidays = None
    
from ipaddress import ip_address

from user_agents import parse as ua_parse

# ── Folder paths ───────────────────────────────────────────────────────────────
# Paths inside the Airflow worker/container where DAG assets live. These are
# used both by local development and inside the Airflow Docker container.
WorkingDirectory = "/opt/airflow/dags/w3c"
# Directory where incoming W3C `.log` files should be placed.
LogFiles         = WorkingDirectory + "/LogFiles/"
# Output directory for star-schema CSV exports.
StarSchema           = WorkingDirectory + "/StarSchema/"

# ── PostgreSQL connection settings ─────────────────────────────────────────────
# These are read from environment variables so the same DAG can target local
# Docker Postgres during development or an external AWS RDS instance.
# The current production setup is using the AWS RDS instance.

# Set W3C_USE_RDS=true and provide the W3C_DB_* variables in your .env file to use AWS RDS. 
# Otherwise, the defaults target the local Docker PostgreSQL.
DB_HOST = os.environ.get("W3C_DB_HOST", "postgres")
DB_PORT = int(os.environ.get("W3C_DB_PORT", 5432))
DB_NAME = os.environ.get("W3C_DB_NAME", "w3c-warehouse")
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
        CREATE TABLE IF NOT EXISTS crawler_ips (
            ip VARCHAR(50) PRIMARY KEY
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_webrequest (
            id             SERIAL PRIMARY KEY,
            raw_log_id     INTEGER UNIQUE,
            date_sk        INTEGER,
            time_sk        INTEGER,
            method_sk      INTEGER,
            page_sk        INTEGER,
            geolocation_sk INTEGER,
            user_agent_sk  INTEGER,
            referrer_sk    INTEGER,
            status_sk      INTEGER,
            visitor_sk     INTEGER,
            response_time_ms INTEGER,
            bytes_sent       INTEGER,
            bytes_received   INTEGER,
            request_count    INTEGER DEFAULT 1
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
        CREATE TABLE IF NOT EXISTS dim_date (
            date_sk      INTEGER PRIMARY KEY,
            date         DATE UNIQUE,
            year         INTEGER,
            month        INTEGER,
            month_name   VARCHAR(20),
            day          INTEGER,
            day_name     VARCHAR(20),
            quarter      INTEGER,
            week_of_year INTEGER,
            is_weekend   VARCHAR(3),
            holiday_flag VARCHAR(3)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_time (
            time_sk   INTEGER PRIMARY KEY,
            hour      INTEGER,
            minute    INTEGER,
            am_pm     VARCHAR(2),
            time_band VARCHAR(20),
            shift_id  INTEGER
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_page (
            page_sk        SERIAL PRIMARY KEY,
            page_path      TEXT,
            query_string   TEXT,
            directory      TEXT,
            file_name      TEXT,
            file_extension VARCHAR(20),
            page_category  VARCHAR(50),
            UNIQUE (page_path, query_string)
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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_status (
            status_sk   SERIAL PRIMARY KEY,
            status_code INTEGER,
            sub_status  INTEGER,
            win32_status INTEGER,
            description VARCHAR(100),
            category    VARCHAR(10),
            severity    VARCHAR(20),
            UNIQUE (status_code, sub_status, win32_status)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_referrer (
            referrer_sk     SERIAL PRIMARY KEY,
            referrer_url    TEXT UNIQUE,
            referrer_domain TEXT,
            traffic_source  VARCHAR(30)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_visitortype (
            visitor_sk   INTEGER PRIMARY KEY,
            crawler_flag VARCHAR(3),
            visitor_type VARCHAR(10)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_method (
            method_sk   SERIAL PRIMARY KEY,
            method_name VARCHAR(10) UNIQUE,
            description VARCHAR(100)
        );
    """)

    # --- Standard Default "-1" Rows with Unknown Values used for NULL joins ---
    cur.execute("""
        INSERT INTO dim_date (date_sk, date, year, month, month_name, day, day_name, quarter, week_of_year, is_weekend, holiday_flag)
        VALUES (-1, '1900-01-01', 1900, 1, 'Unknown', 1, 'Unknown', 1, 1, 'No', 'No') ON CONFLICT DO NOTHING;
    """)
    cur.execute("""
        INSERT INTO dim_time (time_sk, hour, minute, am_pm, time_band, shift_id)
        VALUES (-1, -1, -1, 'U', 'Unknown', -1) ON CONFLICT DO NOTHING;
    """)
    cur.execute("""
        INSERT INTO dim_geolocation (geolocation_sk, ip, country, region, city, postcode, latitude, longitude, isp)
        VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', '-', NULL, NULL, '-') ON CONFLICT DO NOTHING;
    """)
    cur.execute("""
        INSERT INTO dim_page (page_sk, page_path, query_string, directory, file_name, file_extension, page_category)
        VALUES (-1, 'Unknown', '-', 'Unknown', 'Unknown', 'Other', 'Other') ON CONFLICT DO NOTHING;
    """)
    cur.execute("""
        INSERT INTO dim_useragent (user_agent_sk, user_agent, agent_type, browser_name, browser_version, operating_system, device_type)
        VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown') ON CONFLICT DO NOTHING;
    """)
    cur.execute("""
        INSERT INTO dim_status (status_sk, status_code, sub_status, win32_status, description, category, severity)
        VALUES (-1, -1, -1, -1, 'Unknown', 'Unknown', 'Unknown') ON CONFLICT DO NOTHING;
    """)
    cur.execute("""
        INSERT INTO dim_referrer (referrer_sk, referrer_url, referrer_domain, traffic_source)
        VALUES (-1, 'Unknown', 'Unknown', 'Unknown') ON CONFLICT DO NOTHING;
    """)
    cur.execute("""
        INSERT INTO dim_method (method_sk, method_name, description)
        VALUES (-1, 'Unknown', 'Unknown') ON CONFLICT DO NOTHING;
    """)
    cur.execute("""
        INSERT INTO dim_visitortype (visitor_sk, crawler_flag, visitor_type)
        VALUES (-1, 'Ukn', 'Unknown'), (1, 'Yes', 'Crawler'), (2, 'No', 'Human') ON CONFLICT DO NOTHING;
    """)

    # ── Indexes for incremental query performance ──────────────────────────────
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_raw_log_id ON fact_webrequest(raw_log_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_logs_source_file ON raw_logs(source_file);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_logs_client_ip ON raw_logs(client_ip);")

    conn.commit()
    cur.close()
    conn.close()
    print("All database tables and indexes created successfully with default -1 logical lookup keys.")


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

def DetectCrawlerIPs():
    """Populate `crawler_ips` by finding clients that requested robots.txt.

    This simple heuristic adds any `client_ip` that requested a `robots.txt`
    path into `crawler_ips` so the visitor dimension can mark crawlers / bots.
    """
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO crawler_ips (ip)
        SELECT DISTINCT client_ip
        FROM raw_logs
        WHERE (uri_stem ILIKE '%/robots.txt' OR uri_stem ILIKE 'robots.txt')
        AND client_ip IS NOT NULL
        ON CONFLICT (ip) DO NOTHING;
    """)
    conn.commit()
    cur.close()
    conn.close()

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

def makeDateDimension():
    """Build `dim_date` from distinct `log_date` values in `raw_logs`.

    Uses the `holidays` package to mark UK holidays. The
    deterministic surrogate key is `YYYYMMDD` as an integer.
    """
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT DISTINCT log_date FROM raw_logs WHERE log_date IS NOT NULL")
    rows = cur.fetchall()
    
    if holidays:
        # Build holiday years from actual data so all years in the dataset are covered.
        years = sorted({d_val.year for (d_val,) in rows})
        uk_holidays = holidays.UK(years=years)
    else:
        uk_holidays = {}
        
    insert_data = []
    for (d_val,) in rows:
        date_sk   = int(d_val.strftime("%Y%m%d"))
        weekday   = d_val.strftime("%A")
        quarter   = (d_val.month - 1) // 3 + 1
        weekNum   = d_val.isocalendar()[1]
        isWeekend = "Yes" if d_val.weekday() >= 5 else "No"
        isHoliday = "Yes" if d_val in uk_holidays else "No"
        
        insert_data.append((
            date_sk, d_val, d_val.year, d_val.month, d_val.strftime('%B'),
            d_val.day, weekday, quarter, weekNum, isWeekend, isHoliday
        ))
        
    execute_values(cur, """
        INSERT INTO dim_date (date_sk, date, year, month, month_name, day, day_name, quarter, week_of_year, is_weekend, holiday_flag)
        VALUES %s ON CONFLICT (date_sk) DO NOTHING
    """, insert_data)
    conn.commit()
    cur.close()
    conn.close()

def makeTimeDimension():
    """Populate `dim_time` with deterministic keys for every minute of day.

    `time_sk` uses HHMM integer format (e.g. 1430). Time bands and shift ids
    are coarse buckets useful for aggregation in reports.
    """
    conn = get_conn()
    cur  = conn.cursor()
    
    insert_data = []
    for h in range(24):
        for m in range(60):
            time_sk = h * 100 + m
            ampm = "AM" if h < 12 else "PM"
            if h < 6:        timeBand, shiftID = "Early Morning", 1
            elif h < 12:     timeBand, shiftID = "Morning", 2
            elif h < 18:     timeBand, shiftID = "Afternoon", 3
            else:            timeBand, shiftID = "Evening", 4
            insert_data.append((time_sk, h, m, ampm, timeBand, shiftID))
            
    execute_values(cur, """
        INSERT INTO dim_time (time_sk, hour, minute, am_pm, time_band, shift_id)
        VALUES %s ON CONFLICT (time_sk) DO NOTHING
    """, insert_data)
    conn.commit()
    cur.close()
    conn.close()

def makeMethodDimension():
    """Build `dim_method` from distinct HTTP methods observed in raw logs.

    Results are inserted with `ON CONFLICT DO NOTHING` to remain
    incremental-safe.
    """
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT DISTINCT method FROM raw_logs WHERE method IS NOT NULL")
    rows = cur.fetchall()
    insert_data = []
    for (m,) in rows:
        m = m.strip()
        if not m: continue
        description = MethodDescriptions.get(m.upper(), "Other HTTP Method")
        insert_data.append((m.upper(), description))
        
    execute_values(cur, """
        INSERT INTO dim_method (method_name, description)
        VALUES %s ON CONFLICT (method_name) DO NOTHING
    """, insert_data)
    conn.commit()
    cur.close()
    conn.close()

MethodDescriptions = {
    "GET": "Retrieve a resource", "POST": "Submit data to the server",
    "HEAD": "Retrieve headers only", "PUT": "Upload a resource",
    "DELETE": "Delete a resource", "OPTIONS": "Describe communication options",
    "PATCH": "Partial resource update",
}

def makePageDimension():
    """Create `dim_page` containing unique page paths and categories.

    Stores a normalised page path, query string and derived metadata such as
    directory and file extension to aid slicers in Power BI.
    """
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT DISTINCT COALESCE(uri_stem, 'Unknown'), COALESCE(uri_query, '-') FROM raw_logs")
    rows = cur.fetchall()
    insert_data = []
    for uri_stem, uri_query in rows:
        extension = GetFileExtension(uri_stem)
        category  = GetPageCategory(extension)
        parts     = uri_stem.rsplit("/", 1)
        directory = parts[0] if parts[0] else "/"
        fileName  = parts[-1] if len(parts) > 1 else uri_stem
        insert_data.append((uri_stem[:1000], uri_query[:1000], directory[:500], fileName[:200], extension, category))
        
    execute_values(cur, """
        INSERT INTO dim_page (page_path, query_string, directory, file_name, file_extension, page_category)
        VALUES %s ON CONFLICT (page_path, query_string) DO NOTHING
    """, insert_data)
    conn.commit()
    cur.close()
    conn.close()

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

def makeStatusDimension():
    """Create `dim_status` from unique status triples found in `raw_logs`.

    Maps to human-friendly categories and descriptions for common HTTP
    response codes.
    """
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT DISTINCT COALESCE(status, 0), COALESCE(sub_status, 0), COALESCE(win32_status, 0) FROM raw_logs")
    rows = cur.fetchall()
    insert_data = []
    
    StatusDescriptions = {
        "200": "OK", "301": "Moved Permanently", "302": "Found (Redirect)",
        "304": "Not Modified", "400": "Bad Request", "401": "Unauthorized",
        "403": "Forbidden", "404": "Not Found", "405": "Method Not Allowed",
        "500": "Internal Server Error", "503": "Service Unavailable",
    }
    
    for st, sub_st, win32_st in rows:
        category, severity = GetStatusInfo(str(st))
        description = StatusDescriptions.get(str(st), f"HTTP {st}")
        insert_data.append((st, sub_st, win32_st, description, category, severity))
        
    execute_values(cur, """
        INSERT INTO dim_status (status_code, sub_status, win32_status, description, category, severity)
        VALUES %s ON CONFLICT (status_code, sub_status, win32_status) DO NOTHING
    """, insert_data)
    conn.commit()
    cur.close()
    conn.close()

def makeReferrerDimension():
    """Populate `dim_referrer` with parsed referrer domains and traffic types.

    Unknown or empty referrers are normalised to '-' which is treated as
    'Direct' traffic by `ParseReferrer`.
    """
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT DISTINCT COALESCE(NULLIF(referrer, ''), '-') FROM raw_logs")
    rows = cur.fetchall()
    insert_data = []
    for (referrerUrl,) in rows:
        domain, refType = ParseReferrer(referrerUrl)
        insert_data.append((referrerUrl[:1000], domain[:200], refType))
        
    execute_values(cur, """
        INSERT INTO dim_referrer (referrer_url, referrer_domain, traffic_source)
        VALUES %s ON CONFLICT (referrer_url) DO NOTHING
    """, insert_data)
    conn.commit()
    cur.close()
    conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# ETL: Build Fact Table via SQL JOINs
# ══════════════════════════════════════════════════════════════════════════════

def BuildFactTable():
    """Build the `fact_webrequest` table by joining dimensions to raw_logs.

    The function first checks whether there are unprocessed rows in
    `raw_logs` and skips the heavy join if none exist. Inserts use
    `NOT EXISTS` checks to ensure incremental, idempotent behavior.
    """
    conn = get_conn()
    cur  = conn.cursor()

    # Check if there are any unprocessed raw_logs rows before running the heavy query
    cur.execute("""
        SELECT COUNT(*) FROM raw_logs l
        WHERE NOT EXISTS (SELECT 1 FROM fact_webrequest f WHERE f.raw_log_id = l.id);
    """)
    new_rows = cur.fetchone()[0]
    if new_rows == 0:
        print("No new raw_logs to process — fact table is already up to date.")
        cur.close()
        conn.close()
        return
    
    print(f"Executing incremental Fact Table ETL join for {new_rows} new rows...")
    insert_query = """
        INSERT INTO fact_webrequest (
            raw_log_id,
            date_sk, time_sk, method_sk, page_sk, geolocation_sk,
            user_agent_sk, referrer_sk, status_sk, visitor_sk,
            response_time_ms, bytes_sent, bytes_received, request_count
        )
        SELECT 
            l.id,
            COALESCE(d.date_sk, -1),
            COALESCE(t.time_sk, -1),
            COALESCE(m.method_sk, -1),
            COALESCE(p.page_sk, -1),
            COALESCE(g.geolocation_sk, -1),
            COALESCE(u.user_agent_sk, -1),
            COALESCE(r.referrer_sk, -1),
            COALESCE(s.status_sk, -1),
            CASE WHEN c.ip IS NOT NULL THEN 1 ELSE 2 END AS visitor_sk,
            l.time_taken,
            l.bytes_sent,
            l.bytes_recv,
            1 AS request_count
        FROM raw_logs l
        LEFT JOIN dim_date d ON l.log_date = d.date
        LEFT JOIN dim_time t ON EXTRACT(HOUR FROM l.log_time) = t.hour AND EXTRACT(MINUTE FROM l.log_time) = t.minute
        LEFT JOIN dim_method m ON UPPER(l.method) = UPPER(m.method_name)
        LEFT JOIN dim_page p ON COALESCE(l.uri_stem, 'Unknown') = p.page_path AND COALESCE(l.uri_query, '-') = p.query_string
        LEFT JOIN dim_geolocation g ON l.client_ip = g.ip
        LEFT JOIN dim_useragent u ON l.user_agent = u.user_agent
        LEFT JOIN dim_referrer r ON COALESCE(NULLIF(l.referrer, ''), '-') = r.referrer_url
        LEFT JOIN dim_status s ON COALESCE(l.status, 0) = s.status_code 
                              AND COALESCE(l.sub_status, 0) = s.sub_status 
                              AND COALESCE(l.win32_status, 0) = s.win32_status
        LEFT JOIN crawler_ips c ON l.client_ip = c.ip
        WHERE NOT EXISTS (SELECT 1 FROM fact_webrequest f WHERE f.raw_log_id = l.id);
    """
    cur.execute(insert_query)
    conn.commit()
    
    cur.execute("SELECT COUNT(*) FROM fact_webrequest")
    count = cur.fetchone()[0]
    print(f"Fact table ETL generation complete. Total rows in fact table: {count}.")
    
    cur.close()
    conn.close()


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
        "fact_webrequest",
        "dim_date",
        "dim_time",
        "dim_page",
        "dim_geolocation",
        "dim_useragent",
        "dim_status",
        "dim_referrer",
        "dim_method",
        "dim_visitortype",
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
task_DetectCrawlerIPs = PythonOperator(
    task_id="task_DetectCrawlerIPs",
    python_callable=DetectCrawlerIPs,
    dag=dag,
)

# Dimensions
task_makeLocationDimension = PythonOperator(task_id="task_makeLocationDimension", python_callable=makeLocationDimension, dag=dag)
task_makeDateDimension = PythonOperator(task_id="task_makeDateDimension", python_callable=makeDateDimension, dag=dag)
task_makeTimeDimension = PythonOperator(task_id="task_makeTimeDimension", python_callable=makeTimeDimension, dag=dag)
task_makeUserAgentDimension = PythonOperator(task_id="task_makeUserAgentDimension", python_callable=makeUserAgentDimension, dag=dag)
task_makeStatusDimension = PythonOperator(task_id="task_makeStatusDimension", python_callable=makeStatusDimension, dag=dag)
task_makePageDimension = PythonOperator(task_id="task_makePageDimension", python_callable=makePageDimension, dag=dag)
task_makeReferrerDimension = PythonOperator(task_id="task_makeReferrerDimension", python_callable=makeReferrerDimension, dag=dag)
task_makeMethodDimension = PythonOperator(task_id="task_makeMethodDimension", python_callable=makeMethodDimension, dag=dag)

task_BuildFactTable = PythonOperator(
    task_id="task_BuildFactTable",
    python_callable=BuildFactTable,
    dag=dag,
)
task_ExportCSV = PythonOperator(
    task_id="task_ExportCSV",
    python_callable=ExportCSV,
    dag=dag,
)

# ── Dependencies ───────────────────────────────────────────────────────────────

task_CreateDatabaseTables >> task_LoadRawLogsToDatabase

# Phase 2: Dimensions read from raw_logs
task_LoadRawLogsToDatabase >> task_DetectCrawlerIPs
task_LoadRawLogsToDatabase >> task_makeDateDimension
task_LoadRawLogsToDatabase >> task_makeTimeDimension
task_LoadRawLogsToDatabase >> task_makeLocationDimension
task_LoadRawLogsToDatabase >> task_makeUserAgentDimension
task_LoadRawLogsToDatabase >> task_makeStatusDimension
task_LoadRawLogsToDatabase >> task_makePageDimension
task_LoadRawLogsToDatabase >> task_makeReferrerDimension
task_LoadRawLogsToDatabase >> task_makeMethodDimension

# Phase 3: Fact table ETL logic executes only after ALL dimensions are complete
[
    task_DetectCrawlerIPs,
    task_makeDateDimension,
    task_makeTimeDimension,
    task_makeLocationDimension,
    task_makeUserAgentDimension,
    task_makeStatusDimension,
    task_makePageDimension,
    task_makeReferrerDimension,
    task_makeMethodDimension
] >> task_BuildFactTable

# Phase 4: Export fresh CSVs after every successful pipeline run
task_BuildFactTable >> task_ExportCSV