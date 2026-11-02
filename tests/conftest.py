import sys
import pathlib

REPO = pathlib.Path(__file__).resolve().parents[1]
for p in (str(REPO), str(REPO / "ingester")):
    if p not in sys.path:
        sys.path.insert(0, p)

import pytest  # noqa: E402

import tests._db as _db  # noqa: E402


@pytest.fixture(scope="session")
def dsn():
    d = _db.dsn()
    if d is None:
        pytest.skip("no test database (set ANALYTICS_TEST_DATABASE_URL or pip install pgserver)")
    return d


@pytest.fixture()
def conn(dsn):
    import psycopg
    c = psycopg.connect(dsn, autocommit=True)
    with c.cursor() as cur:
        cur.execute("TRUNCATE events, sessions, rollup_1m, quarantine, "
                    "gap_histogram, worker_state")
    yield c
    c.close()


@pytest.fixture()
def cfg(dsn):
    from app.config import Config
    return Config(
        database_url=dsn, auth_token=None, auth_disabled=False,
        session_gap_s=120.0, rebuild_window_s=900.0,
        raw_retention_days=14, quarantine_retention_days=7,
        rollup_bucket_cap=720, max_events_age_s=3600.0, worker_tick_s=60.0,
        public_port=8000, internal_port=9000, log_level="warning",
        max_events_per_batch=5000, worker_statement_timeout_ms=60000,
        ddl_lock_timeout_ms=3000,
        known_event_types=frozenset({"manifest_request"}),
        sessions_retention_days=90,
    )
