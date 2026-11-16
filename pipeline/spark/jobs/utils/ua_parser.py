"""
User-Agent string parsing PySpark UDFs.

Uses the ``user-agents`` library (``ua_parse``) to extract browser name,
browser version, operating system, device type, and crawler status from raw
user-agent strings. Implemented as **pandas UDFs** (vectorised) for better
performance over row-at-a-time UDFs.
"""

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, StructField, StructType
from user_agents import parse as ua_parse


def _safe_parse(ua_string: str) -> dict:
    """Parse a single UA string into a result dict.

    Returns a dict with keys matching the UDF return types:
    ``agent_type``, ``browser_name``, ``browser_version``,
    ``operating_system``, ``device_type``.
    """
    # Pandas passes NULL as NaN (a float) into a pandas_udf, so guard the
    # type before calling str methods. `not ua_string` does not catch NaN.
    if not isinstance(ua_string, str) or not ua_string.strip() or ua_string.strip() in ("-", "Unknown", ""):
        return {
            "agent_type": "Unknown",
            "browser_name": "Unknown",
            "browser_version": "Unknown",
            "operating_system": "Unknown",
            "device_type": "Unknown",
        }

    from urllib.parse import unquote_plus

    normalized = unquote_plus(ua_string.strip())
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
        "agent_type": agent_type,
        "browser_name": browser_name,
        "browser_version": browser_version,
        "operating_system": os_name,
        "device_type": device,
    }


# ── Consolidated struct UDF ──────────────────────────────────────────
# One ua_parse call returns all five fields. The per-field scalar UDFs were
# removed once silver_backfill migrated to enrich_distinct: no pipeline path
# called them any more, and keeping them invited a second implementation.

_UA_STRUCT_SCHEMA = StructType([
    StructField("agent_type", StringType(), True),
    StructField("browser_name", StringType(), True),
    StructField("browser_version", StringType(), True),
    StructField("operating_system", StringType(), True),
    StructField("device_type", StringType(), True),
])


@pandas_udf(returnType=_UA_STRUCT_SCHEMA)
def parse_ua_fields(ua_series: pd.Series) -> pd.DataFrame:
    """Consolidated UA parsing UDF: one ``ua_parse`` call per row.

    Replaces five scalar ``parse_*`` UDFs that each re-parsed the same
    string; callers unpack fields from the struct. Output values match
    the scalar UDFs exactly.
    """
    parsed = ua_series.apply(_safe_parse)
    return pd.DataFrame(parsed.tolist(), index=ua_series.index, columns=_UA_STRUCT_SCHEMA.fieldNames())
