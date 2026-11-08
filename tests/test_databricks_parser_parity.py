"""Cross-path parity for the W3C parser and the bronze contract.

The same log file must land as the same rows whichever path ingests it:

* local Spark  — ``spark/jobs/bronze_ingestion.py`` + ``utils/w3c_parser.py``
* Databricks   — ``spark/databricks/01_bronze_ingestion.py``
* DLT          — ``spark/databricks/dlt_bronze.py``

The Databricks copies cannot import ``utils/`` (terraform provisions them as
single files, with no ``utils`` bundle), so they are duplicated deliberately.
That makes drift a silent data problem rather than an import error: a fix
applied to one copy changes what the same file contains depending on which
path ran. These tests pin the agreement the comments in those files promise.

Field *order* is not asserted. ``01_bronze_ingestion`` declares the schema in
a different order, which is safe because rows are built as dicts and Spark
maps ``createDataFrame(dicts, schema)`` by name; only the name/type contract
matters, and that is asserted exactly.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")
from pyspark.sql.types import DateType, IntegerType, LongType, StringType

_ROOT = Path(__file__).resolve().parent.parent

# dlt is only importable inside Databricks; the module needs the decorators
# to be identity functions at import time.
_dlt_stub = MagicMock()
_dlt_stub.table.return_value = lambda f: f
_dlt_stub.streaming_table.return_value = lambda f: f
_dlt_stub.expect_or_drop.return_value = lambda f: f
sys.modules.setdefault("dlt", _dlt_stub)

for _path in (
    _ROOT / "pipeline" / "spark" / "jobs",
    _ROOT / "pipeline" / "spark" / "databricks",
):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import bronze_ingestion as local_bronze  # noqa: E402
from utils import w3c_parser as local  # noqa: E402
from utils.schemas import bronze_schema as local_schema  # noqa: E402

_MODULES = {
    "01": importlib.import_module("01_bronze_ingestion"),
    "dlt": importlib.import_module("dlt_bronze"),
}

# One representative line per shape the three implementations claim to handle.
LINES = {
    "14_fields": ("2009-10-24 00:00:00 10.0.0.1 GET /index.html - 80 - 1.2.3.4 Mozilla/5.0 200 0 0 15", 14),
    "18_fields": (
        "2010-06-01 12:00:00 10.0.0.1 GET /a.aspx q=1 80 - 1.2.3.4 Mozilla/5.0 sid=abc - 200 0 0 512 256 42",
        18,
    ),
    "ua_with_spaces": (
        "2010-06-01 12:00:00 10.0.0.1 POST /b 80 - 5.6.7.8 Mozilla/5.0 (Windows NT 6.1; WOW64) "
        "sid=x http://ref.example/x 404 2 5 100 50 9",
        18,
    ),
    "dash_placeholders": ("2010-06-01 12:00:00 10.0.0.1 GET / - 80 - 1.2.3.4 - - - 200 0 0 - - 5", 18),
    "truncated": ("2009-10-24 00:00:00 10.0.0.1 GET", 14),
    "empty": ("", 14),
}


def _dlt_parse(line: str, file_format: int, source_file: str):
    return _MODULES["dlt"]._parse_log_line(line, file_format, source_file)


@pytest.mark.parametrize("case", sorted(LINES))
def test_parse_log_line_identical_across_paths(case: str) -> None:
    """Every path parses a line to the same dict, including failure cases."""
    line, file_format = LINES[case]
    results = {
        "local": local.parse_log_line(line, file_format, "u_ex.log"),
        "01": _MODULES["01"].parse_log_line(line, file_format, "u_ex.log"),
        "dlt": _dlt_parse(line, file_format, "u_ex.log"),
    }
    first = results["local"]
    for path, result in results.items():
        assert result == first, f"{case}: {path} parsed {result!r}, local parsed {first!r}"


@pytest.mark.parametrize("value", ["1", "0", "-1", "abc", "", " 7 ", "1.5", "-", None])
def test_safe_int_identical_across_paths(value) -> None:
    assert local.safe_int(value) == _MODULES["01"].safe_int(value) == _MODULES["dlt"].safe_int(value)


@pytest.mark.parametrize("value", ["2010-06-01", "2010-13-01", "", "-", " 2010-06-01 ", None])
def test_safe_date_identical_across_paths(value) -> None:
    assert local.safe_date(value) == _MODULES["01"].safe_date(value) == _MODULES["dlt"].safe_date(value)


def _name_types(schema) -> dict[str, type]:
    return {field.name: field.dataType for field in schema.fields}


def test_bronze_schema_contract_identical_across_paths() -> None:
    """name -> type must match exactly; a drift here changes stored rows."""
    contracts = {
        "local": _name_types(local_schema),
        "01": _name_types(_MODULES["01"].BRONZE_SCHEMA),
        "dlt": _name_types(_MODULES["dlt"]._PARSED_ROW_SCHEMA),
    }
    local_contract = contracts["local"]
    assert len(local_contract) == 19
    for path, contract in contracts.items():
        assert contract == local_contract, f"{path} bronze schema drifted: {contract}"


def test_bronze_schema_type_choices_are_stable() -> None:
    """Pin the types whose widening/narrowing would silently change stored rows."""
    contract = _name_types(local_schema)
    assert contract["log_date"] == DateType()
    assert contract["server_port"] == IntegerType()
    assert contract["status"] == IntegerType()
    assert contract["bytes_sent"] == LongType()
    assert contract["bytes_recv"] == LongType()
    assert contract["time_taken"] == LongType()
    assert contract["source_file"] == StringType()


@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        (
            "date time s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip "
            "cs(User-Agent) sc-status sc-substatus sc-win32-status time-taken",
            14,
        ),
        (
            "date time s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs(User-Agent) "
            "cs(Cookie) cs(Referer) sc-status sc-substatus sc-win32-status sc-bytes cs-bytes time-taken",
            18,
        ),
    ],
)
def test_detect_format_identical_across_paths(tmp_path, fields: str, expected: int) -> None:
    log_file = tmp_path / "u_ex100601.log"
    log_file.write_text(f"#Software: IIS\n#Fields: {fields}\n")

    assert local_bronze.detect_format(str(log_file)) == expected
    assert _MODULES["01"].detect_format(str(log_file)) == expected
    assert _MODULES["dlt"]._detect_format_from_lines(log_file.read_text().splitlines()) == expected


def test_detect_format_unknown_header_rejected_everywhere(tmp_path) -> None:
    """A header whose field count is neither 14 nor 18 is skipped on all paths."""
    log_file = tmp_path / "weird_header.log"
    log_file.write_text("#Software: IIS\n#Fields: a b c d e\n")

    assert local_bronze.detect_format(str(log_file)) == 5  # caller maps non-14/18 to skip
    assert _MODULES["01"].detect_format(str(log_file)) == 5
    assert _MODULES["dlt"]._detect_format_from_lines(log_file.read_text().splitlines()) is None


def test_detect_format_missing_header_diverges_by_design(tmp_path) -> None:
    """The one deliberate divergence: no ``#Fields:`` header at all.

    Local and ``01`` return ``None`` and skip the file. DLT returns 18 as a
    documented default (``_detect_format_from_lines`` docstring), because the
    streaming path sees IIS files that are expected to be 18-field. Pinned
    here so it stays a decision rather than becoming an accident; changing it
    alters what a live DLT pipeline ingests.
    """
    log_file = tmp_path / "no_header.log"
    log_file.write_text("#Software: IIS\n")

    assert local_bronze.detect_format(str(log_file)) is None
    assert _MODULES["01"].detect_format(str(log_file)) is None
    assert _MODULES["dlt"]._detect_format_from_lines(log_file.read_text().splitlines()) == 18


def _module_path(module) -> Path:
    assert module.__file__ is not None, f"{module!r} has no source file"
    return Path(module.__file__).resolve()


def test_parity_modules_are_loaded_from_the_expected_files() -> None:
    """Guard against the import shims silently resolving a different module."""
    assert (
        _module_path(_MODULES["01"])
        == (_ROOT / "pipeline" / "spark" / "databricks" / "01_bronze_ingestion.py").resolve()
    )
    assert _module_path(_MODULES["dlt"]) == (_ROOT / "pipeline" / "spark" / "databricks" / "dlt_bronze.py").resolve()
    assert os.path.basename(_module_path(local)) == "w3c_parser.py"
