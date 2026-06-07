import dlt
import geoip2.database
import geoip2.errors
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType, FloatType, StringType, StructField, StructType

# ─── Lazy Singleton GeoIP Readers ─────────────────────────────────────
#
# Uses the lazy-singleton pattern from shared geoip.py to avoid both
# PicklingError (CRIT-04) and 7x redundant lookups (CRIT-05).
#
# HOW IT WORKS
#   geoip2.database.Reader is NOT serializable.  If a UDF closures
#   over a Reader instance, PySpark raises PicklingError when shipping
#   the UDF to executors.
#
#   Instead, UDFs call module-level helper functions (_geo_lookup,
#   _asn_lookup) which lazily initialise a singleton reader on first
#   call.  Each executor imports the module independently, so each
#   gets its own reader instance — no serialisation needed.
#
#   - UDFs reference helper FUNCTIONS (serialisable by name)
#   - Helpers reference global readers (re-initialised per executor)
#   - First UDF call per executor triggers lazy initialisation
# -----------------------------------------------------------------------

# Default DBFS paths (accessible from all workers)
_GEO_CITY_DB_PATH = "/dbfs/mnt/w3c-data/GeoLite2-City.mmdb"
_GEO_ASN_DB_PATH = "/dbfs/mnt/w3c-data/GeoLite2-ASN.mmdb"

# Lazy singleton holders
_geo_reader = None
_asn_reader = None
_geo_init_attempted = False
_asn_init_attempted = False


def _ensure_geo_reader():
    """Lazy initialisation of GeoIP city reader (once per executor)."""
    global _geo_reader, _geo_init_attempted
    if _geo_reader is None and not _geo_init_attempted:
        _geo_init_attempted = True
        try:
            _geo_reader = geoip2.database.Reader(_GEO_CITY_DB_PATH)
        except Exception as e:
            print(f"WARNING: GeoLite2-City database could not be loaded: {e}. "
                  f"Geo fields will default to Unknown.")
            _geo_reader = None


def _ensure_asn_reader():
    """Lazy initialisation of GeoIP ASN reader (once per executor)."""
    global _asn_reader, _asn_init_attempted
    if _asn_reader is None and not _asn_init_attempted:
        _asn_init_attempted = True
        try:
            _asn_reader = geoip2.database.Reader(_GEO_ASN_DB_PATH)
        except Exception as e:
            print(f"WARNING: GeoLite2-ASN database could not be loaded: {e}. "
                  f"ISP field will default to Unknown.")
            _asn_reader = None


def _is_usable_ip(ip: str) -> bool:
    """Return True if *ip* looks like a non-private, non-loopback IP."""
    if not ip or ip.strip() in ("-", "Unknown", ""):
        return False
    try:
        from ipaddress import ip_address
        addr = ip_address(ip.strip())
        if addr.is_private or addr.is_link_local or addr.is_loopback:
            return False
        return True
    except ValueError:
        return False


def _geo_lookup(ip: str) -> dict | None:
    """Single consolidated GeoIP city lookup returning all fields.

    Replaces the old pattern of 7 independent calls to
    ``geo_reader.city(ip)`` per row (CRIT-05).
    """
    if not _is_usable_ip(ip):
        return None
    _ensure_geo_reader()
    if _geo_reader is None:
        return None
    try:
        response = _geo_reader.city(ip.strip())
        return {
            "country": response.country.name or "Unknown",
            "region": response.subdivisions.most_specific.name or "Unknown",
            "city": response.city.name or "Unknown",
            "latitude": float(response.location.latitude) if response.location.latitude else None,
            "longitude": float(response.location.longitude) if response.location.longitude else None,
            "postcode": response.postal.code or "Unknown",
        }
    except geoip2.errors.AddressNotFoundError:
        return None
    except Exception:
        return None


def _asn_lookup(ip: str) -> str | None:
    """Single ASN lookup returning ISP/organisation."""
    if not _is_usable_ip(ip):
        return None
    _ensure_asn_reader()
    if _asn_reader is None:
        return None
    try:
        response = _asn_reader.asn(ip.strip())
        return response.autonomous_system_organization or "Unknown"
    except geoip2.errors.AddressNotFoundError:
        return None
    except Exception:
        return None


# ─── Consolidated GeoIP UDFs ─────────────────────────────────────────
# Single struct UDF for city-db fields (6 fields from 1 DB call),
# plus a separate scalar UDF for ISP (ASN DB).

_GEO_STRUCT_SCHEMA = StructType([
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("city", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("postcode", StringType(), True),
])


@udf(_GEO_STRUCT_SCHEMA)
def get_geo_fields(ip: str) -> dict | None:
    """Consolidated GeoIP UDF: one MaxMind call returns 6 fields.

    Fixes CRIT-05 (7 separate GeoIP lookups per row) by replacing
    7 scalar UDFs with a single struct-returning UDF.
    """
    return _geo_lookup(ip)


@udf(StringType())
def get_isp(ip: str) -> str:
    """ISP lookup from ASN database."""
    result = _asn_lookup(ip)
    return result or "Unknown"


# ─── Plain Python Function (NOT a UDF) ───

def _extract_domain(url):
    """Extract domain from URL (plain Python, not UDF)."""
    if not url or url == "-":
        return "Direct"
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        return "Unknown"


# ─── Computed Field UDFs ───

@udf(StringType())
def get_page_category(uri_stem):
    """Categorize page by URI pattern."""
    uri_stem_lower = uri_stem.lower() if uri_stem else ""

    if any(ext in uri_stem_lower for ext in [".css", ".js", ".png", ".jpg", ".gif", ".ico"]):
        return "Static Asset"
    elif "/api/" in uri_stem_lower:
        return "API"
    elif "/admin/" in uri_stem_lower:
        return "Admin"
    elif uri_stem_lower.endswith("/"):
        return "Homepage"
    else:
        return "Content"


@udf(StringType())
def get_referrer_domain(referrer):
    """Extract domain from referrer using plain Python function."""
    return _extract_domain(referrer)


@udf(StringType())
def get_traffic_type(referrer_domain):
    """Classify traffic type based on referrer domain."""
    if referrer_domain == "Direct":
        return "Direct"
    elif any(search in referrer_domain.lower() for search in ["google", "bing", "yahoo", "duckduckgo"]):
        return "Search"
    elif any(social in referrer_domain.lower() for social in ["facebook", "twitter", "linkedin", "reddit"]):
        return "Social"
    else:
        return "Referral"


@udf(BooleanType())
def get_is_crawler(user_agent):
    """Detect if user agent is a crawler."""
    if not user_agent or user_agent == "-":
        return False
    ua_lower = user_agent.lower()
    crawler_keywords = ["bot", "crawler", "spider", "scraper", "curl", "wget", "python-requests"]
    return any(keyword in ua_lower for keyword in crawler_keywords)


@udf(StringType())
def get_size_band(bytes_sent):
    """Categorize response size into bands."""
    if not bytes_sent or bytes_sent < 0:
        return "Unknown"
    elif bytes_sent < 1024:
        return "< 1KB"
    elif bytes_sent < 10240:
        return "1KB-10KB"
    elif bytes_sent < 102400:
        return "10KB-100KB"
    elif bytes_sent < 1048576:
        return "100KB-1MB"
    else:
        return "> 1MB"


# ─── Silver DLT Pipeline ───

@dlt.table(
    name="silver_enriched_logs",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
@dlt.expect_or_drop("valid_country", "country IS NOT NULL")
@dlt.expect_or_drop("valid_traffic_type", "traffic_type IN ('Direct', 'Search', 'Social', 'Referral')")
@dlt.expect_or_drop("valid_page_category", "page_category IS NOT NULL")
def silver_enriched_logs():
    """Silver DLT pipeline with GeoIP enrichment and computed fields.

    - CRIT-04 fix: GeoIP readers use lazy singleton pattern (no PicklingError)
    - CRIT-05 fix: consolidated get_geo_fields struct UDF (1 lookup → 6 fields)
    - CRIT-06 fix: left_anti join on source_file for idempotent re-runs
    """
    # Read from Bronze
    bronze_df = dlt.read("bronze_raw_logs")

    # ── Deduplication against existing Silver data (CRIT-06) ──
    # Prevent duplicate rows on pipeline re-runs by filtering out
    # source_file values already processed in the Silver table.
    try:
        existing_silver = dlt.read("silver_enriched_logs")
        bronze_df = bronze_df.join(
            existing_silver.select("source_file").distinct(),
            on="source_file",
            how="left_anti",
        )
    except Exception:
        # First run — Silver table does not exist yet, process all data
        pass

    # ── GeoIP enrichment (consolidated: 1 UDF → 6 columns) ──
    # Apply consolidated struct UDF, then extract individual fields
    silver_df = bronze_df.withColumn("geo", get_geo_fields(col("client_ip")))

    geo_fields = ["country", "region", "city", "latitude", "longitude", "postcode"]
    for field in geo_fields:
        silver_df = silver_df.withColumn(field, col(f"geo.{field}"))

    silver_df = silver_df.drop("geo")

    # ISP comes from the ASN DB (separate lookup)
    silver_df = silver_df.withColumn("isp", get_isp(col("client_ip")))

    # ── Computed fields ──
    silver_df = silver_df \
        .withColumn("page_category", get_page_category(col("uri_stem"))) \
        .withColumn("referrer_domain", get_referrer_domain(col("referrer"))) \
        .withColumn("traffic_type", get_traffic_type(col("referrer_domain"))) \
        .withColumn("is_crawler", get_is_crawler(col("user_agent"))) \
        .withColumn("size_band", get_size_band(col("bytes_sent")))

    # Note: UA columns (agent_type, browser_name, browser_version, os, device_type)
    # are intentionally excluded from Silver. They are parsed at the dimension
    # level by export_dimensions_azure (which reads user_agent string from
    # dbo.raw_enriched). See IMP-08 investigation: downstream dbt models join
    # dim_useragent, not Silver directly — no correctness impact.

    # Select final Silver columns (25 core + 6 geo = 31 total)
    silver_df = silver_df.select(
        "log_date", "log_time", "server_ip", "method", "uri_stem",
        "uri_query", "client_ip", "user_agent", "cookie", "referrer",
        "status", "sub_status", "win32_status", "bytes_sent", "bytes_recv",
        "server_port", "username", "time_taken", "source_file",
        "postcode", "page_category", "referrer_domain", "traffic_type",
        "is_crawler", "size_band",
        "country", "region", "city", "latitude", "longitude", "isp",
    )

    return silver_df
