"""
Tests for the GeoIP UDF factories in ``utils/geoip.py`` (#14).

The historical bug: ``init_reader(custom_path)`` ran driver-side, but UDFs
are pickled by module reference — each executor re-imported ``utils.geoip``
and re-opened the hard-coded default DB path, silently ignoring any
``--geolite2-db`` override unless it equalled the default.

The fix: UDF factories (``city_lookup`` / ``city_field`` / ``asn_lookup``)
bake the configured path into the UDF closure, and ``_GeoReader`` instances
transport only ``db_path`` through pickling (the ``geoip2.database.Reader``
is reopened lazily per executor process).
"""

from __future__ import annotations

import pickle

import pytest

pytest.importorskip("pyspark")

from utils.geoip import (  # noqa: E402
    _ASN_DB_DEFAULT_PATH,
    _CITY_DB_DEFAULT_PATH,
    _GeoReader,
    asn_lookup,
    city_field,
    city_lookup,
)


class TestGeoReaderPathTransport:
    """``_GeoReader`` pickles only the DB path — the override survives."""

    def test_getstate_carries_only_db_path(self, tmp_path):
        reader = _GeoReader(str(tmp_path / "city.mmdb"))
        assert reader.__getstate__() == {"db_path": str(tmp_path / "city.mmdb")}

    def test_pickle_roundtrip_preserves_path_drops_reader(self, tmp_path):
        reader = _GeoReader(str(tmp_path / "city.mmdb"))
        clone = pickle.loads(pickle.dumps(reader))
        assert clone.db_path == reader.db_path
        # The reader must be reopened lazily on the executor, not pickled.
        assert clone._opened is False
        assert clone._reader is None

    def test_missing_db_falls_back_without_raising(self, tmp_path):
        reader = _GeoReader(str(tmp_path / "does-not-exist.mmdb"))
        assert reader.city("8.8.8.8") is None
        assert reader.asn("8.8.8.8") is None
        # Failed open is remembered (no per-row retry storm).
        assert reader._opened is True
        assert "not found" in (reader._error or "")


class TestUdfFactoriesCarryPath:
    """Factory-created UDFs bind the configured path inside their closure."""

    @staticmethod
    def _closure_reader(udf_obj):
        """Extract the ``_GeoReader`` bound inside a factory-produced UDF.

        PySpark 4 returns a dispatch wrapper function whose closure holds
        the ``UserDefinedFunction``; the user function lives in ``udf.func``.
        """
        udf_impl = udf_obj
        if not hasattr(udf_impl, "func"):
            wrapped = [c.cell_contents for c in (udf_impl.__closure__ or []) if hasattr(c.cell_contents, "func")]
            assert len(wrapped) == 1, "expected one UserDefinedFunction in the wrapper closure"
            udf_impl = wrapped[0]
        func = udf_impl.func
        readers = [c.cell_contents for c in (func.__closure__ or []) if isinstance(c.cell_contents, _GeoReader)]
        assert len(readers) == 1, "UDF closure must hold exactly one _GeoReader"
        return readers[0]

    def test_city_lookup_override_reaches_closure(self):
        udf_obj = city_lookup("/custom/GeoLite2-City.mmdb")
        assert self._closure_reader(udf_obj).db_path == "/custom/GeoLite2-City.mmdb"

    def test_city_lookup_none_uses_default(self):
        udf_obj = city_lookup(None)
        assert self._closure_reader(udf_obj).db_path == _CITY_DB_DEFAULT_PATH

    def test_city_field_override_reaches_closure(self):
        udf_obj = city_field("country", "/custom/GeoLite2-City.mmdb")
        assert self._closure_reader(udf_obj).db_path == "/custom/GeoLite2-City.mmdb"

    def test_asn_lookup_none_uses_default(self):
        udf_obj = asn_lookup(None)
        assert self._closure_reader(udf_obj).db_path == _ASN_DB_DEFAULT_PATH

    def test_city_field_rejects_unknown_field(self):
        with pytest.raises(ValueError, match="Unknown GeoIP field"):
            city_field("not_a_field")


class TestCallSitesUseFactories:
    """The silver jobs must not rely on driver-side module init anymore."""

    def test_silver_jobs_no_driver_side_init(self):
        import pathlib

        jobs_dir = pathlib.Path(__file__).resolve().parent.parent / "pipeline" / "spark" / "jobs"
        if not jobs_dir.is_dir():
            jobs_dir = pathlib.Path(__file__).resolve().parent.parent / "spark" / "jobs"
        for job in ("silver_enrichment.py", "silver_backfill.py"):
            source = (jobs_dir / job).read_text(encoding="utf-8")
            assert "init_reader" not in source, f"{job} still uses driver-side init_reader (#14 regression)"
            assert "city_lookup" in source or "city_field" in source, f"{job} must use the path-carrying factories"
