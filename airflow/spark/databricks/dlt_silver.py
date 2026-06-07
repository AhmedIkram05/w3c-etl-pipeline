import dlt
from pyspark.sql.functions import udf, col, lit, when, lower, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
import geoip2.database
import geoip2.errors


# ─── Lazy Reader Factory Pattern (called at driver level, NOT inside UDF) ───

def _make_geo_reader():
    """Create GeoIP2 database reader (called at driver level) with error handling."""
    try:
        geo_db_path = spark.conf.get("geoip.city_db_path", "/Volumes/w3c_etl_databricks/bronze/w3c_data/GeoLite2-City.mmdb")
        return geoip2.database.Reader(geo_db_path)
    except Exception as e:
        print(f"WARNING: GeoLite2-City database could not be loaded: {e}. Falling back to Unknown.")
        return None


def _make_asn_reader():
    """Create ASN database reader (called at driver level) with error handling."""
    try:
        asn_db_path = spark.conf.get("geoip.asn_db_path", "/Volumes/w3c_etl_databricks/bronze/w3c_data/GeoLite2-ASN.mmdb")
        return geoip2.database.Reader(asn_db_path)
    except Exception as e:
        print(f"WARNING: GeoLite2-ASN database could not be loaded: {e}. Falling back to Unknown.")
        return None


# Initialize readers at driver level
geo_reader = _make_geo_reader()
asn_reader = _make_asn_reader()


# ─── GeoIP UDFs ───

@udf(StringType())
def get_country(ip):
    """Get country from IP address."""
    try:
        response = geo_reader.city(ip)
        return response.country.name or "Unknown"
    except:
        return "Unknown"


@udf(StringType())
def get_region(ip):
    """Get region from IP address."""
    try:
        response = geo_reader.city(ip)
        return response.subdivisions.most_specific.name or "Unknown"
    except:
        return "Unknown"


@udf(StringType())
def get_city(ip):
    """Get city from IP address."""
    try:
        response = geo_reader.city(ip)
        return response.city.name or "Unknown"
    except:
        return "Unknown"


@udf(FloatType())
def get_latitude(ip):
    """Get latitude from IP address."""
    try:
        response = geo_reader.city(ip)
        return response.location.latitude
    except:
        return None


@udf(FloatType())
def get_longitude(ip):
    """Get longitude from IP address."""
    try:
        response = geo_reader.city(ip)
        return response.location.longitude
    except:
        return None


@udf(StringType())
def get_postcode(ip):
    """Get postcode from IP address."""
    try:
        response = geo_reader.city(ip)
        return response.postal.code or "Unknown"
    except:
        return "Unknown"


@udf(StringType())
def get_isp(ip):
    """Get ISP from IP address."""
    try:
        response = asn_reader.asn(ip)
        return response.autonomous_system_organization or "Unknown"
    except:
        return "Unknown"


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
    except:
        return "Unknown"


# ─── Computed Field UDFs ───

@udf(StringType())
def get_page_category(uri_stem):
    """Categorize page by URI pattern."""
    uri_stem_lower = lower(uri_stem) if uri_stem else ""
    
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
    ua_lower = lower(user_agent)
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
    """Silver DLT pipeline with GeoIP enrichment and computed fields."""
    # Read from Bronze
    bronze_df = dlt.read("bronze_raw_logs")
    
    # Apply GeoIP enrichment
    silver_df = bronze_df \
        .withColumn("country", get_country(col("client_ip"))) \
        .withColumn("region", get_region(col("client_ip"))) \
        .withColumn("city", get_city(col("client_ip"))) \
        .withColumn("latitude", get_latitude(col("client_ip"))) \
        .withColumn("longitude", get_longitude(col("client_ip"))) \
        .withColumn("postcode", get_postcode(col("client_ip"))) \
        .withColumn("isp", get_isp(col("client_ip")))
    
    # Apply computed fields
    silver_df = silver_df \
        .withColumn("page_category", get_page_category(col("uri_stem"))) \
        .withColumn("referrer_domain", get_referrer_domain(col("referrer"))) \
        .withColumn("traffic_type", get_traffic_type(col("referrer_domain"))) \
        .withColumn("is_crawler", get_is_crawler(col("user_agent"))) \
        .withColumn("size_band", get_size_band(col("bytes_sent")))
    
    # Note: UA columns (agent_type, browser_name, browser_version, os, device_type) are excluded
    # They are computed but not materialized in Silver DDL to reduce storage
    
    # Select final Silver columns (25 core + 6 geo = 31 total)
    silver_df = silver_df.select(
        "log_date", "log_time", "server_ip", "method", "uri_stem",
        "uri_query", "client_ip", "user_agent", "cookie", "referrer",
        "status", "sub_status", "win32_status", "bytes_sent", "bytes_recv",
        "server_port", "username", "time_taken", "source_file",
        "postcode", "page_category", "referrer_domain", "traffic_type",
        "is_crawler", "size_band",
        "country", "region", "city", "latitude", "longitude", "isp"
    )
    
    return silver_df