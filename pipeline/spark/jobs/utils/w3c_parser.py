"""
W3C Extended Log Format parser — PySpark port.

Replicates the parsing logic from the Airflow DAG's `_ParseRawLogLine`
function, supporting both 14-field and 18-field IIS W3C formats.

Field counts
------------
14 fields (2009–mid 2010):
  date time s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username
  c-ip cs(User-Agent) sc-status sc-substatus sc-win32-status time-taken

18 fields (mid 2010–2011):
  date time s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username
  c-ip cs(User-Agent) cs(Cookie) cs(Referer)
  sc-status sc-substatus sc-win32-status sc-bytes cs-bytes time-taken
"""

from datetime import datetime


def safe_int(val: str):
    """Convert to non-negative int or return None.

    Handles '-' placeholders and malformed values gracefully.
    """
    try:
        v = int(val)
        return v if v >= 0 else None
    except (ValueError, TypeError):
        return None


def safe_date(val: str):
    """Parse an ISO ``YYYY-MM-DD`` date string, or return None."""
    try:
        return datetime.strptime(val.strip(), "%Y-%m-%d").date()
    except (ValueError, TypeError, AttributeError):
        return None


def parse_log_line(line: str, file_format: int, source_file: str):
    """Parse a single W3C log line into a dict matching ``bronze_schema``.

    Parameters
    ----------
    line : str
        A single non-comment line from a ``.log`` file.
    file_format : int
        Number of fields (14 or 18) detected from the ``#Fields:`` header.
    source_file : str
        Filename (e.g. ``u_ex091024.log``) — stored as a dimension attribute.

    Returns
    -------
    dict or None
        Column-value mapping, or ``None`` if the line could not be parsed.
    """
    try:
        if file_format == 14:
            # Last 4 tokens are fixed-width numeric fields: status sub_status win32_status time_taken
            rs = line.rsplit(" ", 4)
            if len(rs) < 5:
                return None
            # First 10 tokens are the variable-width prefix
            ls = rs[0].split(" ", 9)
            if len(ls) < 10:
                return None

            return {
                "log_date": safe_date(ls[0]),
                "log_time": ls[1],
                "server_ip": ls[2],
                "method": ls[3],
                "uri_stem": ls[4],
                "uri_query": ls[5],
                "server_port": safe_int(ls[6]),
                "username": ls[7],
                "client_ip": ls[8],
                "user_agent": ls[9],
                "cookie": None,
                "referrer": None,
                "status": safe_int(rs[1]),
                "sub_status": safe_int(rs[2]),
                "win32_status": safe_int(rs[3]),
                "bytes_sent": None,
                "bytes_recv": None,
                "time_taken": safe_int(rs[4]),
                "source_file": source_file,
            }

        elif file_format == 18:
            # Last 6 tokens: status sub_status win32_status bytes_sent bytes_recv time_taken
            rs = line.rsplit(" ", 6)
            if len(rs) < 7:
                return None
            # First 12 tokens include cookie and referrer
            ls = rs[0].split(" ", 11)
            if len(ls) < 12:
                return None

            referrer = ls[11].strip()
            return {
                "log_date": safe_date(ls[0]),
                "log_time": ls[1],
                "server_ip": ls[2],
                "method": ls[3],
                "uri_stem": ls[4],
                "uri_query": ls[5],
                "server_port": safe_int(ls[6]),
                "username": ls[7],
                "client_ip": ls[8],
                "user_agent": ls[9],
                "cookie": ls[10],
                "referrer": referrer,
                "status": safe_int(rs[1]),
                "sub_status": safe_int(rs[2]),
                "win32_status": safe_int(rs[3]),
                "bytes_sent": safe_int(rs[4]),
                "bytes_recv": safe_int(rs[5]),
                "time_taken": safe_int(rs[6]),
                "source_file": source_file,
            }

        return None

    except Exception:
        return None
