"""Test DB bootstrap.

Priority: ANALYTICS_TEST_DATABASE_URL env var; otherwise an embedded
PostgreSQL via the `pgserver` package (pip install pgserver). If neither is
available, DSN is None and DB-dependent test modules skip themselves.
"""
import os
import pathlib
import sys

REPO = pathlib.Path(__file__).resolve().parents[1]
os.environ.setdefault("ANALYTICS_MIGRATIONS_DIR", str(REPO / "migrations"))
for p in (str(REPO), str(REPO / "ingester")):
    if p not in sys.path:
        sys.path.insert(0, p)

_dsn = None
_tried = False


def dsn():
    """Returns a DSN to an `analytics` database with migrations applied,
    or None if no database can be provided."""
    global _dsn, _tried
    if _tried:
        return _dsn
    _tried = True

    import psycopg
    from psycopg.conninfo import make_conninfo

    env = os.environ.get("ANALYTICS_TEST_DATABASE_URL")
    if env:
        base = env
    else:
        try:
            import pgserver
        except ImportError:
            return None
        srv = pgserver.get_server("/tmp/analytics-pgtest")
        base = srv.get_uri()

    target = make_conninfo(base, dbname="analytics")
    try:
        psycopg.connect(target).close()
    except psycopg.OperationalError:
        with psycopg.connect(base, autocommit=True) as c:
            c.execute("CREATE DATABASE analytics")

    from app.db import migrate
    migrate(target)
    _dsn = target
    return _dsn
