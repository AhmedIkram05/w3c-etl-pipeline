"""Postgres-backed proof of the local ``dim_geolocation`` SCD Type 2 write.

``export_dimensions`` builds ``dim_geolocation`` from the Silver Delta table as
SCD Type 2 (``valid_from`` / ``valid_to`` / ``is_current``, partial unique index
``ux_dim_geolocation__current ON (ip) WHERE is_current``).  Every other test of
that write in this repo mocks ``execute_values`` and asserts on the SQL *text*
(``tests/test_export_dimensions.py``) — none of it has ever run on a server.

This module closes that gap: it executes the real statements against a real
Postgres.  The SQL and DDL are imported from the operator module
(``_ensure_dimension_tables`` and ``_write_geo_versions``, which uses the
module-level ``GEO_SCD2_EXPIRE_SQL`` / ``GEO_SCD2_INSERT_SQL`` constants), so it
cannot drift from the implementation.

Only a real server proves: the ``VALUES %s`` casts and the two-step expire/insert
actually type-check; ``IS DISTINCT FROM`` detects a change to any tracked column
(including NULL lat/lon); the ``DEFAULT`` sentinel ``valid_to`` / ``is_current``
are applied; the partial unique index rejects a second current row; and the
``is_current`` reader predicate returns exactly one row per IP.

Isolation and cleanup
---------------------
Each test opens a throwaway schema, points ``search_path`` at it, and runs the
operator's ensure function there, so unqualified ``dim_geolocation`` resolves to
the throwaway table and can never touch a real one.  The schema is dropped in a
``finally``.  The connection is rolled back wherever a transaction can be
(notably after the index-rejection check); the schema drop is the cleanup because
the ensure function commits its DDL internally.

Connection target / CI behaviour
--------------------------------
The DSN is resolved from ``W3C_TEST_DATABASE_URL`` (set by
``.github/workflows/_reusable-test.yml``), then ``DATABASE_URL``, then the
discrete ``W3C_DB_*`` variables the operator itself reads, then a default that
matches the CI Postgres service (``postgresql://postgres:test@localhost:5432/test``).
When no server is reachable the test **skips** with the reason — unless the
``CI`` environment variable is set, in which case it **fails** rather than
passing silently, because CI is expected to provide a server.

Not marked ``integration`` on purpose: that marker is deselected by the CI test
job (``_reusable-test.yml``), which would make this test useless there.
"""

from __future__ import annotations

import os
import uuid

import psycopg2
import pytest
from plugins.operators.export_dimensions import (
    _ensure_dimension_tables,
    _write_geo_versions,
)


def _connect():
    """Open a connection to the configured test database."""
    dsn = os.environ.get("W3C_TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn)
    return psycopg2.connect(
        host=os.environ.get("W3C_DB_HOST", "localhost"),
        port=int(os.environ.get("W3C_DB_PORT") or 5432),
        dbname=os.environ.get("W3C_DB_NAME", "test"),
        user=os.environ.get("W3C_DB_USER", "postgres"),
        password=os.environ.get("W3C_DB_PASS", "test"),
    )


def _target_label() -> str:
    """Human-readable target for skip/failure messages — never the password."""
    dsn = os.environ.get("W3C_TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if dsn:
        return dsn.split("@")[-1]  # host+db only
    return (
        f"{os.environ.get('W3C_DB_HOST', 'localhost')}:"
        f"{os.environ.get('W3C_DB_PORT') or 5432}/"
        f"{os.environ.get('W3C_DB_NAME', 'test')}"
    )


@pytest.fixture
def geo_db():
    """A Postgres connection with a throwaway schema holding ``dim_geolocation``.

    The tables are created by the operator's own ``_ensure_dimension_tables``
    bound to the schema via ``search_path``.  Every test gets a fresh schema,
    so runs are repeatable and cannot corrupt anything else.
    """
    try:
        conn = _connect()
    except psycopg2.OperationalError as exc:
        message = f"Postgres unreachable at {_target_label()}: {exc}".strip()
        if os.environ.get("CI"):
            pytest.fail(
                f"{message}. CI sets W3C_TEST_DATABASE_URL; this DB test must not skip in CI.",
                pytrace=False,
            )
        pytest.skip(f"{message}. Set W3C_TEST_DATABASE_URL to run this test.")
        return  # unreachable; keeps the type checker happy

    conn.autocommit = False
    schema = "test_scd2_" + uuid.uuid4().hex[:12]
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'SET search_path TO "{schema}"')
        conn.commit()
        _ensure_dimension_tables(conn)  # the DDL under test, first run
        yield conn
    finally:
        try:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            conn.commit()
        finally:
            conn.close()


def _version(
    ip,
    country="US",
    region="California",
    city="Mountain View",
    postcode="94043",
    latitude=37.386,
    longitude=-122.084,
    isp="Google",
):
    """One ``dim_geolocation`` source row, in the operator's insert-tuple order."""
    return (ip, country, region, city, postcode, latitude, longitude, isp)


def _row_count(conn, ip) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM dim_geolocation WHERE ip = %s", (ip,))
        return cur.fetchone()[0]


def _current_rows(conn, ip):
    """Rows the production reader predicate (``... AND is_current``) selects."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT geolocation_sk, is_current, valid_from, valid_to, "
            "country, region, city, postcode, latitude, longitude, isp "
            "FROM dim_geolocation WHERE ip = %s AND is_current",
            (ip,),
        )
        return cur.fetchall()


# ═══════════════════════════════════════════════════════════════════════════
#  (a) DDL is idempotent
# ═══════════════════════════════════════════════════════════════════════════


def test_ensure_dimension_tables_is_idempotent(geo_db):
    """Running the ensure DDL twice succeeds and leaves the SCD2 shape intact."""
    _ensure_dimension_tables(geo_db)  # fixture already ran it once

    with geo_db.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = 'dim_geolocation' "
            "AND column_name IN ('valid_from', 'valid_to', 'is_current')"
        )
        assert cur.fetchone()[0] == 3
        cur.execute(
            "SELECT count(*) FROM pg_indexes "
            "WHERE schemaname = current_schema() AND tablename = 'dim_geolocation' "
            "AND indexname = 'ux_dim_geolocation__current'"
        )
        assert cur.fetchone()[0] == 1


# ═══════════════════════════════════════════════════════════════════════════
#  (b) First insert creates one current row
# ═══════════════════════════════════════════════════════════════════════════


def test_first_insert_creates_one_current_row(geo_db):
    """A brand-new IP gets exactly one row, current, open-ended from NOW()."""
    ip = "1.1.1.1"
    _write_geo_versions(geo_db, [_version(ip)])
    geo_db.commit()

    assert _row_count(geo_db, ip) == 1
    rows = _current_rows(geo_db, ip)
    assert len(rows) == 1
    _sk, is_current, valid_from, valid_to, country, _region, city, postcode, *_ = rows[0]
    assert is_current is True
    assert valid_from is not None
    assert valid_to.year == 9999  # still the '9999-12-31' sentinel
    assert (country, city, postcode) == ("US", "Mountain View", "94043")


# ═══════════════════════════════════════════════════════════════════════════
#  (c) Unchanged attributes create no new version
# ═══════════════════════════════════════════════════════════════════════════


def test_unchanged_attributes_do_not_create_a_new_version(geo_db):
    """An idempotent re-run expires nothing and inserts nothing."""
    ip = "2.2.2.2"
    _write_geo_versions(geo_db, [_version(ip)])
    geo_db.commit()
    sk_before = _current_rows(geo_db, ip)[0][0]

    _write_geo_versions(geo_db, [_version(ip)])  # byte-identical attributes
    geo_db.commit()

    assert _row_count(geo_db, ip) == 1  # no new version
    rows = _current_rows(geo_db, ip)
    assert len(rows) == 1
    assert rows[0][0] == sk_before  # same physical row, untouched


# ═══════════════════════════════════════════════════════════════════════════
#  (d) Changed attributes expire the old row and insert a new current one
# ═══════════════════════════════════════════════════════════════════════════


def test_changed_attributes_expire_old_and_insert_new_current(geo_db):
    """A MaxMind drift closes the current version and opens a fresh one."""
    ip = "3.3.3.3"
    _write_geo_versions(geo_db, [_version(ip)])
    geo_db.commit()
    old_sk = _current_rows(geo_db, ip)[0][0]

    _write_geo_versions(geo_db, [_version(ip, city="Los Angeles", latitude=34.05, longitude=-118.24)])
    geo_db.commit()

    assert _row_count(geo_db, ip) == 2  # history retained
    with geo_db.cursor() as cur:
        cur.execute(
            "SELECT is_current, valid_to, city FROM dim_geolocation WHERE geolocation_sk = %s",
            (old_sk,),
        )
        is_current, valid_to, _city = cur.fetchone()
    assert is_current is False
    assert valid_to.year != 9999  # sentinel replaced with the expiry timestamp

    current = _current_rows(geo_db, ip)
    assert len(current) == 1
    assert current[0][0] != old_sk  # a new physical row
    assert current[0][6] == "Los Angeles"  # the changed attribute


# ═══════════════════════════════════════════════════════════════════════════
#  (e) The partial unique index rejects a second current row for one IP
# ═══════════════════════════════════════════════════════════════════════════


def test_second_current_row_for_same_ip_is_rejected(geo_db):
    """A direct duplicate current insert must fail on the partial unique index."""
    ip = "4.4.4.4"
    _write_geo_versions(geo_db, [_version(ip)])
    geo_db.commit()

    with pytest.raises(psycopg2.errors.UniqueViolation):
        with geo_db.cursor() as cur:
            cur.execute(
                "INSERT INTO dim_geolocation (ip, country, is_current) VALUES (%s, %s, TRUE)",
                (ip, "CA"),
            )
    geo_db.rollback()  # clear the aborted transaction

    assert len(_current_rows(geo_db, ip)) == 1


# ═══════════════════════════════════════════════════════════════════════════
#  (f) The reader predicate selects exactly one row per IP
# ═══════════════════════════════════════════════════════════════════════════


def test_reader_predicate_selects_exactly_one_row_per_ip(geo_db):
    """``WHERE is_current`` returns one row per IP after a change + a no-op."""
    _write_geo_versions(geo_db, [_version("5.5.5.5"), _version("6.6.6.6", city="Austin")])
    geo_db.commit()
    # One IP drifts, the other is unchanged.
    _write_geo_versions(geo_db, [_version("5.5.5.5", city="Dallas"), _version("6.6.6.6", city="Austin")])
    geo_db.commit()

    with geo_db.cursor() as cur:
        cur.execute("SELECT ip, count(*) FROM dim_geolocation WHERE is_current GROUP BY ip")
        current_per_ip = dict(cur.fetchall())
        cur.execute("SELECT count(*) FROM dim_geolocation WHERE is_current")
        total_current = cur.fetchone()[0]

    assert current_per_ip == {"5.5.5.5": 1, "6.6.6.6": 1}
    assert total_current == 2
    assert _row_count(geo_db, "5.5.5.5") == 2  # changed IP keeps history
    assert _row_count(geo_db, "6.6.6.6") == 1  # unchanged IP does not churn
