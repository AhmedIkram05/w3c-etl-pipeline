"""
Computed-field UDFs for the Silver enrichment layer.

These PySpark UDFs replicate the business logic from the Airflow DAG
(``w3c-dag.py`` helper functions) and the dbt models, covering:

- Page category classification from URI stem extensions
- Status code categorisation (2xx / 3xx / 4xx / 5xx)
- Referrer domain extraction and traffic-source classification
- Crawler detection (IP-based — IPs that requested ``robots.txt``)
- Size band bucketing (total bytes sent + received)
- Time band labelling (Early Morning / Morning / Afternoon / Evening)
"""

from urllib.parse import urlparse

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# ══════════════════════════════════════════════════════════════════════
#  PAGE CATEGORY
# ══════════════════════════════════════════════════════════════════════

_EXTENSION_CATEGORIES = {
    "aspx": "Dynamic Page",
    "asp": "Dynamic Page",
    "html": "Static Page",
    "htm": "Static Page",
    "shtml": "Static Page",
    "jpg": "Image",
    "jpeg": "Image",
    "png": "Image",
    "gif": "Image",
    "bmp": "Image",
    "ico": "Icon",
    "css": "Stylesheet",
    "js": "Script",
    "txt": "Text File",
    "pdf": "Document",
    "no_extension": "Directory",
}


@udf(returnType=StringType())
def page_category(uri_stem):
    """Classify a URI stem into a human-friendly page category.

    Extracts the file extension from *uri_stem*, then maps it to a
    category (Static Page, Image, Script, etc.).  URI stems without
    an extension are classified as "Directory".
    """
    # NOTE: Python type hints intentionally omitted on UDF signatures
    # to suppress PySpark 4.x "Cannot infer the eval type" warnings;
    # the returnType=StringType() decorator arg carries the schema.
    if not uri_stem or uri_stem.strip() in ("-", "Unknown", ""):
        return "Unknown"
    stem = uri_stem.split("?")[0]
    if "." in stem:
        ext = stem.rsplit(".", 1)[-1].lower().strip()
        return _EXTENSION_CATEGORIES.get(ext, "Other")
    return "Directory"


# ══════════════════════════════════════════════════════════════════════
#  REFERRER PARSING
# ══════════════════════════════════════════════════════════════════════

_SEARCH_ENGINES = ["google", "bing", "yahoo", "baidu", "duckduckgo", "yandex", "ask"]
_SOCIAL_DOMAINS = ["facebook", "twitter", "linkedin", "reddit", "t.co", "instagram"]
_INTERNAL_DOMAINS = ["darwinsbeagleplants", "134.36.36.75"]


@udf(returnType=StringType())
def referrer_domain(referrer_url):
    """Extract the domain from a referrer URL.

    Returns ``'Direct'`` for empty/dash referrers, ``'Unknown'`` for the
    literal string ``'Unknown'``, or the parsed domain otherwise.
    """
    if not referrer_url or referrer_url.strip() == "-":
        return "Direct"
    if referrer_url == "Unknown":
        return "Unknown"
    try:
        candidate = referrer_url.strip()
        if "//" not in candidate:
            candidate = "http://" + candidate
        parsed = urlparse(candidate)
        domain = parsed.netloc.split("@")[-1].split(":")[0].lower().replace("www.", "")
        return domain or referrer_url
    except Exception:
        return str(referrer_url)


def _extract_domain(referrer_url: str) -> str:
    """Extract domain from a referrer URL (plain function, not a UDF).

    Used by ``traffic_type`` which cannot call UDFs from inside a UDF body
    (no SparkContext available on executor workers).
    """
    if not referrer_url or referrer_url.strip() == "-":
        return "Direct"
    if referrer_url == "Unknown":
        return "Unknown"
    try:
        candidate = referrer_url.strip()
        if "//" not in candidate:
            candidate = "http://" + candidate
        parsed = urlparse(candidate)
        domain = parsed.netloc.split("@")[-1].split(":")[0].lower().replace("www.", "")
        return domain or referrer_url
    except Exception:
        return str(referrer_url)


@udf(returnType=StringType())
def traffic_type(referrer_url):
    """Classify a referrer into a traffic-source category.

    Categories: ``Direct``, ``Internal``, ``Search Engine``,
    ``Social``, ``Referral``, ``Unknown``.
    """
    if not referrer_url or referrer_url.strip() == "-":
        return "Direct"
    if referrer_url == "Unknown":
        return "Unknown"

    domain = _extract_domain(referrer_url)

    if any(s in domain for s in _INTERNAL_DOMAINS):
        return "Internal"
    if any(s in domain for s in _SEARCH_ENGINES):
        return "Search Engine"
    if any(s in domain for s in _SOCIAL_DOMAINS):
        return "Social"
    return "Referral"


# ══════════════════════════════════════════════════════════════════════
#  CRAWLER DETECTION (IP-based)
# ══════════════════════════════════════════════════════════════════════

# NOTE: The crawler IP list is populated at runtime by the Silver job
# from the Bronze Delta table.  The UDF below checks membership against
# a broadcast set; it is re-created each run with a fresh IP list.


def make_crawler_udf(crawler_ips: set) -> callable:
    """Create a ``is_crawler`` UDF closed over the current crawler IP set.

    Parameters
    ----------
    crawler_ips : set of str
        IP addresses that have requested ``robots.txt``.

    Returns
    -------
    PySpark UDF (StringType -> StringType)
    """
    # Copy the set so the closure is not affected by later mutations.
    ips = frozenset(crawler_ips)

    @udf(returnType=StringType())
    def is_crawler_impl(ip):
        if not ip or ip.strip() in ("-", "Unknown", ""):
            return "Unknown"
        return "true" if ip.strip() in ips else "false"

    return is_crawler_impl


# ══════════════════════════════════════════════════════════════════════
#  SIZE BAND
# ══════════════════════════════════════════════════════════════════════

_SIZE_BANDS = [
    (1, "Zero"),  # total bytes == 0
    (1024, "Tiny"),  # 1 - 1023
    (10240, "Small"),  # 1024 - 10239
    (102400, "Medium"),  # 10240 - 102399
    (1048576, "Large"),  # 102400 - 1048575
]


@udf(returnType=StringType())
def size_band(bytes_sent, bytes_recv):
    """Bucket total bytes (sent + received) into a human-readable band.

    Thresholds (total bytes):
        = 0      → ``Zero``
        1-1023   → ``Tiny``
        1024-10239    → ``Small``
        10240-102399  → ``Medium``
        102400-1048575 → ``Large``
        >= 1048576    → ``Huge``
    """
    total = (bytes_sent or 0) + (bytes_recv or 0)
    for threshold, label in _SIZE_BANDS:
        if total < threshold:
            return label
    return "Huge"
