"""
Geo-IP enrichment PySpark UDFs.

Uses the local MaxMind GeoLite2 databases (``geoip2``) — fast, offline,
zero network cost.  Two databases are supported:

* **City DB** (``GeoLite2-City.mmdb``) — country, region, city,
  latitude, longitude, and postal code.
* **ASN DB** (``GeoLite2-ASN.mmdb``) — autonomous-system organisation
  (used to populate the ``isp`` field).

IPs not found in the database resolve to "Unknown" (or the documented
default for the specific field).  If a database file is missing or
cannot be opened, the corresponding lookups fall back to "Unknown"
without raising.

The GeoLite2 database files should be mounted at
``/opt/spark/data/GeoLite2-City.mmdb`` and
``/opt/spark/data/GeoLite2-ASN.mmdb``.  Obtain them from
https://dev.maxmind.com/geoip/geolite2-free-geolocation-data
(free registration required).

Usage
-----
The readers are module-level singletons initialised with the default
paths at import time.  Call ``init_reader(path)`` / ``init_asn_reader(path)``
to override the database path before any UDFs are invoked::

    from utils.geoip import init_reader, init_asn_reader, geoip_country

    init_reader("/custom/path/GeoLite2-City.mmdb")
    init_asn_reader("/custom/path/GeoLite2-ASN.mmdb")
    df = df.withColumn("country", geoip_country("client_ip"))

If either database file cannot be found or opened, all lookups against
that database return the field's documented default ("Unknown" or
``None`` for coordinates).
"""

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# ── Lazy-loading GeoLite2 City reader singleton ───────────────────────
# A single reader instance is shared by all executor tasks.
# Initialised with the default path; call init_reader() to override.
_reader = None
_reader_error = None
_reader_path = "/opt/spark/data/GeoLite2-City.mmdb"

# ── Lazy-loading GeoLite2 ASN reader singleton ────────────────────────
# The ASN DB supplies the autonomous-system / organisation name that we
# surface as the ``isp`` field.  City DB alone does not provide ISP.
_asn_reader = None
_asn_reader_error = None
_asn_reader_path = "/opt/spark/data/GeoLite2-ASN.mmdb"


def init_reader(db_path: str | None = None):
    """Initialise or re-initialise the GeoLite2 **City** reader.

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
        _reader_error = f"Failed to open GeoLite2-City database: {exc}"


def init_asn_reader(db_path: str | None = None):
    """Initialise or re-initialise the GeoLite2 **ASN** reader.

    Parameters
    ----------
    db_path : str or None
        Path to ``GeoLite2-ASN.mmdb``.  ``None`` (default) keeps the
        current path (or the hard-coded default on first call).

    If the database file is missing or unreadable, ``_asn_reader`` is
    left as ``None`` and ``geoip_isp`` lookups will fall back to
    ``"Unknown"`` (preserving backward compatibility).
    """
    global _asn_reader, _asn_reader_error, _asn_reader_path

    if db_path is not None:
        _asn_reader_path = db_path

    try:
        import geoip2.database
        import geoip2.errors

        _asn_reader = geoip2.database.Reader(_asn_reader_path)
        _asn_reader_error = None
    except FileNotFoundError:
        _asn_reader = None
        _asn_reader_error = f"GeoLite2-ASN.mmdb not found at {_asn_reader_path}"
    except Exception as exc:
        _asn_reader = None
        _asn_reader_error = f"Failed to open GeoLite2-ASN database: {exc}"


# Initialise with the default paths (can be overridden later).
init_reader()
init_asn_reader()


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
    """Look up *ip* in the local GeoLite2 **City** DB.

    Returns a dict with keys ``country``, ``region``, ``city``,
    ``latitude``, ``longitude``, ``postcode``, or ``None`` on failure.
    The ``isp`` key is intentionally left out — ISP data is sourced
    from the ASN DB (see ``_lookup_asn``).
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
            "postcode": response.postal.code or None,
        }
    except geoip2.errors.AddressNotFoundError:
        return None
    except Exception:
        return None


def _lookup_asn(ip: str) -> str | None:
    """Look up *ip* in the local GeoLite2 **ASN** DB.

    Returns the organisation name (e.g. ``"Google LLC"``) or ``None``
    if the IP is not in the DB, the ASN reader was not initialised,
    or any other error occurred.  Callers should map ``None`` to
    ``"Unknown"``.
    """
    if not _is_usable_ip(ip):
        return None
    if _asn_reader is None or _asn_reader_error is not None:
        return None
    try:
        import geoip2.errors

        response = _asn_reader.asn(ip.strip())
        return response.autonomous_system_organization or None
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
def geoip_postcode(ip: str) -> str:
    """Return the postal / ZIP code for *ip* from the City DB.

    Returns ``None`` (Spark NULL) when the field is missing so that
    downstream code can apply its own default (the dim_geolocation
    schema uses ``"-"``).  IPs that fail the City lookup also return
    ``None``.
    """
    result = _lookup(ip)
    if result is None:
        return None
    return result["postcode"]


@udf(returnType=StringType())
def geoip_isp(ip: str) -> str:
    """Return the ISP / organisation for *ip* from the ASN DB.

    Falls back to ``"Unknown"`` if the ASN DB is not initialised, the
    IP is not in the DB, or the field is missing — preserving the
    previous behaviour of ``geoip_isp``.
    """
    result = _lookup_asn(ip)
    if result is None or not result:
        return "Unknown"
    return result
