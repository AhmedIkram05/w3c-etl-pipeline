"""Tests for the canonical shared Spark session helper."""

import os
import sys

_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
_JOBS_DIR = os.path.join(_TEST_DIR, "..", "pipeline", "spark", "jobs")
if _JOBS_DIR not in sys.path:
    sys.path.insert(0, _JOBS_DIR)

from unittest.mock import MagicMock, patch  # noqa: E402

from utils.session import create_spark_session, get_loaded_files  # noqa: E402


def test_create_spark_session_uses_canonical_config():
    builder = MagicMock()
    mock_session = MagicMock()
    builder.appName.return_value = builder
    builder.config.return_value = builder
    builder.getOrCreate.return_value = mock_session
    with patch("utils.session.SparkSession") as mock_spark:
        mock_spark.builder = builder
        result = create_spark_session("TestApp")
    assert result is mock_session
    builder.appName.assert_called_once_with("TestApp")
    configs = {c.args[0]: c.args[1] for c in builder.config.call_args_list}
    assert configs["spark.ui.enabled"] == "false"
    assert configs["spark.sql.ansi.enabled"] == "true"
    assert "DeltaSparkSessionExtension" in configs["spark.sql.extensions"]


def test_get_loaded_files_returns_source_set():
    spark = MagicMock()
    df = MagicMock()
    spark.read.format.return_value.load.return_value = df
    df.select.return_value.distinct.return_value.collect.return_value = [
        type("R", (), {"source_file": "a.log"})(),
        type("R", (), {"source_file": "b.log"})(),
    ]
    assert get_loaded_files(spark, "/tmp/bronze") == {"a.log", "b.log"}


def test_get_loaded_files_missing_path_returns_empty():
    spark = MagicMock()
    spark.read.format.return_value.load.side_effect = RuntimeError("missing")
    assert get_loaded_files(spark, "/tmp/nope") == set()
