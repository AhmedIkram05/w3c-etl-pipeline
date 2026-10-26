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
The UDF factories bake the database path into the UDF closure, so a
caller-supplied path override travels with the UDF to every executor::

    from utils.geoip import asn_lookup, city_field, city_lookup

    city_udf = city_lookup("/custom/path/GeoLite2-City.mmdb")
    isp_udf = asn_lookup("/custom/path/GeoLite2-ASN.mmdb")
    df = df.withColumn("geo", city_udf("client_ip"))
    df = df.withColumn("country", city_field("country", "/custom/path/GeoLite2-City.mmdb")("client_ip"))
    df = df.withColumn("isp", isp_udf("client_ip"))

Passing ``None`` keeps the default paths shown above.  The path MUST be
transported this way: executors re-import this module, so any driver-side
module state (e.g. a global ``init_reader(path)``) never reaches them and
the override would be silently replaced by the default path.

If either database file cannot be found or opened, all lookups against
that database return the field's documented default ("Unknown" or
``None`` for coordinates).
"""

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructField, StructType

# ── Default database paths (documented deployment locations) ──────────
_CITY_DB_DEFAULT_PATH = "/opt/spark/data/GeoLite2-City.mmdb"
_ASN_DB_DEFAULT_PATH = "/opt/spark/data/GeoLite2-ASN.mmdb"

_GEO_FIELDS = ("country", "region", "city", "latitude", "longitude", "postcode")
# Fields that return NULL (not "Unknown") when the lookup fails.
_NULL_DEFAULT_FIELDS = ("latitude", "longitude", "postcode")


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


class _GeoReader:
    """Self-contained GeoIP lookup bound to one database path.

    Instances are created by the UDF factories and travel inside the UDF
    closure: cloudpickle serialises the instance's ``db_path`` by value, so
    the caller's path override reaches executors. A driver-side module
    global cannot do this — executors re-import this module and would
    silently fall back to the default path.

    The ``geoip2.database.Reader`` is opened lazily on first use per
    process (it is not picklable, so it must never exist at serialisation
    time). A failed open is remembered so it is not retried per row.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._reader = None
        self._error: str | None = None
        self._opened = False

    def __getstate__(self) -> dict:
        # Only the path survives pickling — each executor reopens the DB.
        return {"db_path": self.db_path}

    def __setstate__(self, state: dict) -> None:
        self.db_path = state["db_path"]
        self._reader = None
        self._error = None
        self._opened = False

    def _ensure_ready(self) -> None:
        if self._opened:
            return
        self._opened = True
        try:
            import geoip2.database

            self._reader = geoip2.database.Reader(self.db_path)
        except FileNotFoundError:
            self._reader = None
            self._error = f"GeoLite2 database not found at {self.db_path}"
        except Exception as exc:
            self._reader = None
            self._error = f"Failed to open GeoLite2 database: {exc}"

    def city(self, ip: str) -> dict | None:
        """Look up *ip* in the City DB.

        Returns a dict with keys ``country``, ``region``, ``city``,
        ``latitude``, ``longitude``, ``postcode``, or ``None`` when the IP
        is unusable, absent from the DB, or the DB is unavailable.
        """
        if not _is_usable_ip(ip):
            return None
        self._ensure_ready()
        if self._reader is None or self._error is not None:
            return None
        try:
            import geoip2.errors

            response = self._reader.city(ip.strip())
            return {
                "country": response.country.name or "Unknown",
                "region": response.subdivisions.most_specific.name or "Unknown",
                "city": response.city.name or "Unknown",
                "latitude": str(response.location.latitude) if response.location.latitude is not None else None,
                "longitude": str(response.location.longitude) if response.location.longitude is not None else None,
                "postcode": response.postal.code or None,
            }
        except geoip2.errors.AddressNotFoundError:
            return None
        except Exception:
            return None

    def asn(self, ip: str) -> str | None:
        """Look up *ip* in the ASN DB; returns the organisation or ``None``."""
        if not _is_usable_ip(ip):
            return None
        self._ensure_ready()
        if self._reader is None or self._error is not None:
            return None
        try:
            import geoip2.errors

            response = self._reader.asn(ip.strip())
            return response.autonomous_system_organization or None
        except geoip2.errors.AddressNotFoundError:
            return None
        except Exception:
            return None


# ── UDF factories ────────────────────────────────────────────────────
# Each factory returns a UDF whose closure carries the configured DB path,
# so path overrides survive the trip to executors (see module docstring).


def city_lookup(db_path: str | None = None):
    """Consolidated City-DB UDF factory: one call returns all six fields.

    Field values: a failed or unusable lookup yields ``"Unknown"`` for
    country/region/city and ``None`` for latitude/longitude/postcode.
    Callers unpack fields from the struct (``col("geo.country")``) so a row
    triggers one mmdb lookup instead of six.
    """
    reader = _GeoReader(db_path or _CITY_DB_DEFAULT_PATH)

    @udf(
        returnType=StructType([
            StructField("country", StringType(), True),
            StructField("region", StringType(), True),
            StructField("city", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("postcode", StringType(), True),
        ])
    )
    def get_geo_fields(ip: str) -> dict:
        result = reader.city(ip)
        if result is None:
            return {
                "country": "Unknown",
                "region": "Unknown",
                "city": "Unknown",
                "latitude": None,
                "longitude": None,
                "postcode": None,
            }
        return result

    return get_geo_fields


def city_field(field: str, db_path: str | None = None):
    """Scalar City-DB UDF factory for one of the six geo fields.

    Failure semantics match the historical scalar ``geoip_*`` UDFs: text
    fields return ``"Unknown"``, coordinates and postcode return ``None``.
    """
    if field not in _GEO_FIELDS:
        raise ValueError(f"Unknown GeoIP field: {field!r}")
    reader = _GeoReader(db_path or _CITY_DB_DEFAULT_PATH)
    null_on_failure = field in _NULL_DEFAULT_FIELDS

    @udf(returnType=StringType())
    def geoip_scalar(ip: str):
        result = reader.city(ip)
        if result is None:
            return None if null_on_failure else "Unknown"
        return result[field]

    geoip_scalar.__name__ = f"geoip_{field}"
    return geoip_scalar


def asn_lookup(db_path: str | None = None):
    """ASN-DB UDF factory: returns the ISP / organisation for *ip*.

    Falls back to ``"Unknown"`` if the ASN DB is unavailable, the IP is
    not in the DB, or the field is missing.
    """
    reader = _GeoReader(db_path or _ASN_DB_DEFAULT_PATH)

    @udf(returnType=StringType())
    def geoip_isp(ip: str) -> str:
        result = reader.asn(ip)
        return result if result else "Unknown"

    return geoip_isp
