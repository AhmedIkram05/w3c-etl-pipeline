"""
Unit tests for the W3C Extended Log Format parser.

Tests cover both 14-field and 18-field IIS formats using real log
lines from the project dataset.
"""

from datetime import date

# Flexible import: works from host project root and inside Spark container
try:
    from airflow.spark.jobs.utils.w3c_parser import parse_log_line, safe_date, safe_int
except ImportError:
    from utils.w3c_parser import parse_log_line, safe_date, safe_int  # type: ignore[import-untyped]


# ══════════════════════════════════════════════════════════════════════
#  safe_int
# ══════════════════════════════════════════════════════════════════════


class TestSafeInt:
    def test_positive_int(self):
        assert safe_int("42") == 42

    def test_zero(self):
        assert safe_int("0") == 0

    def test_negative_becomes_none(self):
        assert safe_int("-1") is None

    def test_dash_placeholder(self):
        assert safe_int("-") is None

    def test_empty_string(self):
        assert safe_int("") is None

    def test_whitespace_string(self):
        assert safe_int("  ") is None

    def test_non_numeric(self):
        assert safe_int("abc") is None

    def test_none_input(self):
        assert safe_int(None) is None


# ══════════════════════════════════════════════════════════════════════
#  safe_date
# ══════════════════════════════════════════════════════════════════════


class TestSafeDate:
    def test_valid_iso_date(self):
        assert safe_date("2009-10-24") == date(2009, 10, 24)

    def test_dash_placeholder(self):
        assert safe_date("-") is None

    def test_empty_string(self):
        assert safe_date("") is None

    def test_invalid_format(self):
        assert safe_date("10/24/2009") is None

    def test_none_input(self):
        assert safe_date(None) is None


# ══════════════════════════════════════════════════════════════════════
#  parse_log_line — 14-field format
# ══════════════════════════════════════════════════════════════════════

_REAL_14_LINE_1 = (
    "2009-10-24 01:40:40 134.36.36.75 HEAD / - 80 - 205.178.184.153 "
    "Mozilla/5.0+(Windows;+U;+Windows+NT+5.1;+en-US;+rv:1.8.1.12)+Gecko/20080201+Firefox/2.0.0.12 "
    "200 0 0 186"
)

_REAL_14_LINE_2 = (
    "2009-10-24 08:34:52 134.36.36.75 GET /abc.php auth=45V456b09m&strPassword=PQWHUP_DDZEQ&nLoginId=43 "
    "80 - 91.212.127.100 "
    "Mozilla/5.0+(Windows;+U;+Windows+NT+5.0;+en-US;+rv:1.8.1.12)+Gecko/20080201+Firefox/2.0.0.12 "
    "404 0 64 92"
)

_REAL_14_ROBOTS = (
    "2009-10-24 01:41:17 134.36.36.75 GET /robots.txt - 80 - 205.178.184.153 "
    "Mozilla/4.0+(compatible;+MSIE+7.0;+Windows+NT+5.1;+.NET+CLR+1.1.4322;+.NET+CLR+2.0.50727) "
    "404 0 2 112"
)


class TestParse14Field:
    def test_standard_get(self):
        result = parse_log_line(_REAL_14_LINE_1, 14, "u_ex091024.log")
        assert result is not None

        assert result["log_date"] == date(2009, 10, 24)
        assert result["log_time"] == "01:40:40"
        assert result["server_ip"] == "134.36.36.75"
        assert result["method"] == "HEAD"
        assert result["uri_stem"] == "/"
        assert result["uri_query"] == "-"
        assert result["server_port"] == 80
        assert result["username"] == "-"
        assert result["client_ip"] == "205.178.184.153"
        assert "Firefox" in result["user_agent"]
        assert result["status"] == 200
        assert result["sub_status"] == 0
        assert result["win32_status"] == 0
        assert result["time_taken"] == 186
        assert result["source_file"] == "u_ex091024.log"

        # 14-field: optional fields default to None
        assert result["cookie"] is None
        assert result["referrer"] is None
        assert result["bytes_sent"] is None
        assert result["bytes_recv"] is None

    def test_with_query_string(self):
        result = parse_log_line(_REAL_14_LINE_2, 14, "u_ex091024.log")
        assert result is not None
        assert result["method"] == "GET"
        assert result["uri_stem"] == "/abc.php"
        assert "auth=" in result["uri_query"]
        assert result["status"] == 404
        assert result["sub_status"] == 0
        assert result["win32_status"] == 64
        assert result["time_taken"] == 92

    def test_robots_txt(self):
        result = parse_log_line(_REAL_14_ROBOTS, 14, "u_ex091024.log")
        assert result is not None
        assert result["uri_stem"] == "/robots.txt"
        assert result["status"] == 404
        assert result["win32_status"] == 2
        assert result["time_taken"] == 112

    def test_short_line_returns_none(self):
        assert parse_log_line("too short", 14, "test.log") is None

    def test_empty_line_returns_none(self):
        assert parse_log_line("", 14, "test.log") is None


# ══════════════════════════════════════════════════════════════════════
#  parse_log_line — 18-field format
# ══════════════════════════════════════════════════════════════════════

_REAL_18_LINE_1 = (
    "2010-07-18 00:07:19 134.36.36.75 GET /Darwin/MyAccount.aspx "
    "p=25&pid=436&prcid=62&ppid=1505 80 - 216.129.119.45 "
    "Mozilla/5.0+(Twiceler-0.9+http://www.cuil.com/twiceler/robot.html) "
    "- - 200 0 0 6635 252 394"
)

_REAL_18_LINE_WITH_REFERRER = (
    "2010-07-18 00:50:44 134.36.36.75 GET /Darwin/style.css - 80 - 67.195.113.235 "
    "Mozilla/5.0+(compatible;+Yahoo!+Slurp/3.0;+http://help.yahoo.com/help/us/ysearch/slurp) "
    "- http://www.darwinsbeagleplants.org/Darwin/Plant.aspx?p=25&ix=7&pid=5&prcid=26&ppid=1502 "
    "304 0 0 229 485 179"
)

_REAL_18_GOOGLEBOT = (
    "2010-07-18 00:18:42 134.36.36.75 GET /Darwin/Image.aspx "
    "p=25&ix=3055&pid=308&prcid=0&ppid=1505&pup=true 80 - 66.249.66.87 "
    "Mozilla/5.0+(compatible;+Googlebot/2.1;++http://www.google.com/bot.html) "
    "- - 200 0 0 4275 356 95"
)


class TestParse18Field:
    def test_standard_get(self):
        result = parse_log_line(_REAL_18_LINE_1, 18, "u_ex100718.log")
        assert result is not None

        assert result["log_date"] == date(2010, 7, 18)
        assert result["log_time"] == "00:07:19"
        assert result["server_ip"] == "134.36.36.75"
        assert result["method"] == "GET"
        assert result["uri_stem"] == "/Darwin/MyAccount.aspx"
        assert result["uri_query"] == "p=25&pid=436&prcid=62&ppid=1505"
        assert result["server_port"] == 80
        assert result["username"] == "-"
        assert result["client_ip"] == "216.129.119.45"
        assert "Twiceler" in result["user_agent"]
        assert result["cookie"] == "-"
        assert result["referrer"] == "-"
        assert result["status"] == 200
        assert result["sub_status"] == 0
        assert result["win32_status"] == 0
        assert result["bytes_sent"] == 6635
        assert result["bytes_recv"] == 252
        assert result["time_taken"] == 394
        assert result["source_file"] == "u_ex100718.log"

    def test_with_referrer(self):
        result = parse_log_line(_REAL_18_LINE_WITH_REFERRER, 18, "u_ex100718.log")
        assert result is not None
        assert (
            result["referrer"]
            == "http://www.darwinsbeagleplants.org/Darwin/Plant.aspx?p=25&ix=7&pid=5&prcid=26&ppid=1502"
        )
        assert result["status"] == 304
        assert result["bytes_sent"] == 229
        assert result["bytes_recv"] == 485
        assert result["time_taken"] == 179

    def test_googlebot(self):
        result = parse_log_line(_REAL_18_GOOGLEBOT, 18, "u_ex100718.log")
        assert result is not None
        assert result["client_ip"] == "66.249.66.87"
        assert "Googlebot" in result["user_agent"]
        assert result["uri_stem"] == "/Darwin/Image.aspx"
        assert result["bytes_sent"] == 4275
        assert result["time_taken"] == 95

    def test_short_line_returns_none(self):
        assert parse_log_line("too short", 18, "test.log") is None

    def test_empty_line_returns_none(self):
        assert parse_log_line("", 18, "test.log") is None


# ══════════════════════════════════════════════════════════════════════
#  parse_log_line — edge cases
# ══════════════════════════════════════════════════════════════════════


class TestParseEdgeCases:
    def test_unknown_format(self):
        """A format value that is neither 14 nor 18 returns None."""
        line = "2009-10-24 01:40:40 134.36.36.75 GET / - 80 - 1.2.3.4 UA 200 0 0 100"
        assert parse_log_line(line, 99, "test.log") is None

    def test_malformed_within_line_returns_best_effort(self):
        """The parser returns None for truly unparseable lines (try/except)."""
        # Line with %-encoded characters that might trip up parsing
        result = parse_log_line("2009-10-24 00:00:00 1.2.3.4 GET /test", 18, "test.log")
        # Not enough tokens for 18-field => should return None
        assert result is None

    def test_negative_time_taken(self):
        """Negative time_taken should be returned as None."""
        line = "2009-10-24 01:40:40 134.36.36.75 GET / - 80 - 1.2.3.4 TestBot 200 0 0 -1"
        result = parse_log_line(line, 14, "test.log")
        assert result is not None
        assert result["time_taken"] is None  # negative → safe_int returns None

    def test_dash_bytes_18_field(self):
        """Dashes in byte fields become None."""
        line = "2010-07-18 00:00:00 1.2.3.4 GET / - 80 - 1.2.3.4 UA - - 200 0 0 - - 100"
        result = parse_log_line(line, 18, "test.log")
        assert result is not None
        assert result["bytes_sent"] is None  # dash → safe_int returns None
        assert result["bytes_recv"] is None
        assert result["time_taken"] == 100
