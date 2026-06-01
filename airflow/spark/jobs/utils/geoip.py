"""
Geo-IP enrichment PySpark UDFs.

Uses the local GeoLite2 City DB (``geoip2``) — fast, offline, zero network cost.
IPs not found in the database resolve to "Unknown".

The GeoLite2 database file (``GeoLite2-City.mmdb``) should be mounted at
``/opt/spark/data/GeoLite2-City.mmdb``. Obtain it from
https://dev.maxmind.com/geoip/geolite2-free-geolocation-data
(free registration required).

Usage
-----
The reader is a module-level singleton initialized with the default path at
import time.  Call ``init_reader(path)`` to override the database path before
any UDFs are invoked::

    from utils.geoip import init_reader, geoip_country

    init_reader("/custom/path/GeoLite2-City.mmdb")
    df = df.withColumn("country", geoip_country("client_ip"))

If the database file cannot be found or opened, all lookups return "Unknown".
"""

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# ── Lazy-loading GeoLite2 reader singleton ────────────────────────────
# A single reader instance is shared by all executor tasks.
# Initialised with the default path; call init_reader() to override.
_reader = None
_reader_error = None
_reader_path = "/opt/spark/data/GeoLite2-City.mmdb"


def init_reader(db_path: str | None = None):
    """Initialise or re-initialise the GeoLite2 reader with *db_path*.

    Parameters
    ----------
    db_path : str or None
        Path to ``GeoLite2-City.mmdb``.  ``None`` (default) keeps the
        current path (or the hard-coded default on first call).
    """
    global _reader, _reader_error, _reader_path

    if db_path is not None:
        _reader_path = db_path

    try:
        import geoip2.database
        import geoip2.errors

        _reader = geoip2.database.Reader(_reader_path)
        _reader_error = None
    except FileNotFoundError:
        _reader = None
        _reader_error = f"GeoLite2-City.mmdb not found at {_reader_path}"
    except Exception as exc:
        _reader = None
        _reader_error = f"Failed to open GeoLite2 database: {exc}"


# Initialise with the default path (can be overridden later).
init_reader()


def _is_usable_ip(ip: str) -> bool:
    """Return True if *ip* looks like a non-private, non-loopback IP.

    Short-circuits private / link-local / loopback ranges without hitting
    the geo-IP database.
    """
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


def _lookup(ip: str) -> dict | None:
    """Look up *ip* in the local GeoLite2 DB.

    Returns a dict with keys ``country``, ``region``, ``city``, ``latitude``,
    ``longitude``, ``isp``, or ``None`` on failure.
    """
    if not _is_usable_ip(ip):
        return None
    if _reader is None or _reader_error is not None:
        return None
    try:
        import geoip2.errors

        response = _reader.city(ip.strip())
        return {
            "country": response.country.name or "Unknown",
            "region": response.subdivisions.most_specific.name or "Unknown",
            "city": response.city.name or "Unknown",
            "latitude": str(response.location.latitude) if response.location.latitude else None,
            "longitude": str(response.location.longitude) if response.location.longitude else None,
            "isp": "Unknown",  # GeoLite2 does not provide ISP data
        }
    except geoip2.errors.AddressNotFoundError:
        return None
    except Exception:
        return None


# ── Row-level UDFs ───────────────────────────────────────────────────
# Each UDF handles a single IP -> field mapping. This keeps the interface
# simple and allows Spark to push down predicate/column pruning.


@udf(returnType=StringType())
def geoip_country(ip: str) -> str:
    result = _lookup(ip)
    if result is None:
        return "Unknown"
    return result["country"]


@udf(returnType=StringType())
def geoip_region(ip: str) -> str:
    result = _lookup(ip)
    if result is None:
        return "Unknown"
    return result["region"]


@udf(returnType=StringType())
def geoip_city(ip: str) -> str:
    result = _lookup(ip)
    if result is None:
        return "Unknown"
    return result["city"]


@udf(returnType=StringType())
def geoip_latitude(ip: str) -> str:
    result = _lookup(ip)
    if result is None:
        return None
    return result["latitude"]


@udf(returnType=StringType())
def geoip_longitude(ip: str) -> str:
    result = _lookup(ip)
    if result is None:
        return None
    return result["longitude"]


@udf(returnType=StringType())
def geoip_isp(ip: str) -> str:
    result = _lookup(ip)
    if result is None:
        return "Unknown"
    return result["isp"]
