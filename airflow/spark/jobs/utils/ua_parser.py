"""
User-Agent string parsing PySpark UDFs.

Uses the ``user-agents`` library (``ua_parse``) to extract browser name,
browser version, operating system, device type, and crawler status from raw
user-agent strings. Implemented as **pandas UDFs** (vectorised) for better
performance over row-at-a-time UDFs.
"""

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
from user_agents import parse as ua_parse


def _safe_parse(ua_string: str) -> dict:
    """Parse a single UA string into a result dict.

    Returns a dict with keys matching the UDF return types:
    ``agent_type``, ``browser_name``, ``browser_version``,
    ``operating_system``, ``device_type``.
    """
    if not ua_string or ua_string.strip() in ("-", "Unknown", ""):
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


# ── Pandas UDFs — each returns a single column ───────────────────────
# Using pandas_udf with PyArrow for vectorised execution.


@pandas_udf(returnType=StringType())
def parse_agent_type(ua_series: pd.Series) -> pd.Series:
    return ua_series.apply(lambda ua: _safe_parse(ua)["agent_type"])


@pandas_udf(returnType=StringType())
def parse_browser_name(ua_series: pd.Series) -> pd.Series:
    return ua_series.apply(lambda ua: _safe_parse(ua)["browser_name"])


@pandas_udf(returnType=StringType())
def parse_browser_version(ua_series: pd.Series) -> pd.Series:
    return ua_series.apply(lambda ua: _safe_parse(ua)["browser_version"])


@pandas_udf(returnType=StringType())
def parse_operating_system(ua_series: pd.Series) -> pd.Series:
    return ua_series.apply(lambda ua: _safe_parse(ua)["operating_system"])


@pandas_udf(returnType=StringType())
def parse_device_type(ua_series: pd.Series) -> pd.Series:
    return ua_series.apply(lambda ua: _safe_parse(ua)["device_type"])
