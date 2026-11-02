"""Round-3/4 worker hardening, pinned against a real Postgres.

These are the review's "wedge" shapes: a single malformed row that used to
make every tick fail (and, with a watermark that only advanced on success,
fail FOREVER), plus the persistence/isolation guarantees that keep the tick
loop healthy.
"""
import datetime as dt
import time

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app import metrics
from app.db import make_pool
from app.partitions import _ensure_one
from tests.worker.conftest import insert_event, one, rebuild, rollup_touched
from worker import gap_histogram, main as worker_main


def _bucket(ts):
    return int(ts // 60) * 60


def _poison(cur, ts, ip):
    """Bypass the API: pre-fix data or direct loads could already hold rows
    like this (migration 002 scrubs them once, but new ones could arrive via
    any non-app path)."""
    cur.execute(
        """INSERT INTO events
           (ts, event_type, schema_version, client_ip, provider, channel_id,
            content_type, http_status, delivery, user_agent, country, raw)
           VALUES (%s, 'manifest_request', 1, %s, 'p1', 'ch-poison',
                   'channel', 200, 'proxied', 'ua', 'DE', %s)""",
        (ts, ip, Jsonb({"note": "poison row inserted directly"})),
    )


def test_poison_rows_do_not_wedge_the_tick(conn, cfg):
    """NaN / Infinity / 1e308 in events must not raise in the sessionizer,
    the rollup, or the gap histogram — the old code died in _bucket() and
    to_timestamp() on every tick, forever."""
    now = time.time()
    # pin good traffic inside ONE bucket, away from the minute boundary
    # (a straddling batch makes the per-bucket assertion flaky)
    good_ts = _bucket(now - 100) + 5
    with conn.cursor() as cur:
        _poison(cur, float("nan"), "203.0.113.60")
        _poison(cur, float("inf"), "203.0.113.61")
        _poison(cur, 1e308, "203.0.113.62")
        for i in range(3):  # good traffic mixed in
            insert_event(cur, good_ts + i, ip="203.0.113.63")

    rewritten, deleted = rebuild(conn, cfg, now - 3600)
    rollup_touched(conn, cfg, rewritten, deleted, now - 3600)
    gap_histogram.update(conn.cursor(), cfg)  # must not raise either

    # good traffic sessionized and rolled up...
    assert one(conn, "SELECT count(*) FROM sessions "
               "WHERE client_ip = '203.0.113.63'")[0] == 1
    row = one(conn, "SELECT events FROM rollup_1m WHERE bucket = to_timestamp(%s)",
              (_bucket(good_ts),))
    assert row[0] >= 3
    # ...poison rows produce no sessions and no NaN durations
    # (NaN != NaN is the portable NaN test in SQL)
    assert one(conn, """
        SELECT count(*) FROM sessions
        WHERE client_ip IN ('203.0.113.60','203.0.113.61','203.0.113.62')
           OR duration_s != duration_s""")[0] == 0


def test_data_level_poison_bucket_is_isolated_not_fatal(conn, cfg):
    """A bucket whose INSERT fails at the DATA level (DataError/IntegrityError
    — here a trigger raising SQLSTATE 22003) is skipped and counted, not
    allowed to abort the tick."""
    now = time.time()
    good_ts = _bucket(now - 100) + 5
    # NOTE: the bucket value is INLINED, not a %s bind: inside a dollar-quoted
    # CREATE FUNCTION body, server-side binding leaves "$1" as literal text,
    # and the trigger would fire UndefinedParameter — a ProgrammingError —
    # instead of the intended data-level error. The round-3 version of this
    # test was green for exactly that wrong reason (the old broad
    # `except psycopg.Error` classified it as bucket poison — blocker 5 in
    # the flesh). It is a test-computed int, so inlining is safe.
    b = int(_bucket(good_ts))  # the MINUTE bucket, not good_ts itself
    with conn.cursor() as cur:
        for i in range(3):
            insert_event(cur, good_ts + i, ip="203.0.113.66")
        # landmine on the bucket the good events fall into — a DATA-level
        # error (22003 numeric_value_out_of_range -> psycopg DataError)
        cur.execute(f"""
            CREATE OR REPLACE FUNCTION _rollup_landmine() RETURNS trigger AS $$
            BEGIN
              IF NEW.bucket = to_timestamp({b}) THEN
                RAISE EXCEPTION 'landmine' USING ERRCODE = '22003';
              END IF;
              RETURN NEW;
            END $$ LANGUAGE plpgsql""")
        cur.execute("DROP TRIGGER IF EXISTS rollup_landmine ON rollup_1m")
        cur.execute("CREATE TRIGGER rollup_landmine BEFORE INSERT ON rollup_1m "
                    "FOR EACH ROW EXECUTE FUNCTION _rollup_landmine()")

    before = metrics.ROLLUP_BUCKET_ERRORS._value.get()
    try:
        rewritten, deleted = rebuild(conn, cfg, now - 3600)
        # despite the landmine: no exception, sessions still committed by the
        # caller (same transaction), only the bucket is lost
        rollup_touched(conn, cfg, rewritten, deleted, now - 3600)
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP TRIGGER IF EXISTS rollup_landmine ON rollup_1m")
            cur.execute("DROP FUNCTION IF EXISTS _rollup_landmine()")

    assert metrics.ROLLUP_BUCKET_ERRORS._value.get() > before
    assert one(conn, "SELECT count(*) FROM sessions WHERE client_ip = '203.0.113.66'")[0] == 1


def test_environmental_bucket_error_rolls_back_the_bucket(conn, cfg):
    """Round-4 blocker 5: a NON-data error on a bucket (a trigger bug, a
    timeout, a dead connection — here a plain plpgsql RAISE, P0001) must NOT
    be skipped-with-anchor-advancing: it propagates so the whole core tick
    rolls back and the anchor stays. The old broad `except psycopg.Error`
    skipped the bucket, kept its stale data, and counted the tick "ok"."""
    now = time.time()
    good_ts = _bucket(now - 100) + 5
    b = int(_bucket(good_ts))  # inlined; see note in the data-level landmine test
    with conn.cursor() as cur:
        for i in range(3):
            insert_event(cur, good_ts + i, ip="203.0.113.67")
        cur.execute(f"""
            CREATE OR REPLACE FUNCTION _rollup_landmine() RETURNS trigger AS $$
            BEGIN
              IF NEW.bucket = to_timestamp({b}) THEN
                RAISE EXCEPTION 'trigger bug (not data)';
              END IF;
              RETURN NEW;
            END $$ LANGUAGE plpgsql""")
        cur.execute("DROP TRIGGER IF EXISTS rollup_landmine ON rollup_1m")
        cur.execute("CREATE TRIGGER rollup_landmine BEFORE INSERT ON rollup_1m "
                    "FOR EACH ROW EXECUTE FUNCTION _rollup_landmine()")

    before = metrics.ROLLUP_BUCKET_ERRORS._value.get()
    try:
        rewritten, deleted = rebuild(conn, cfg, now - 3600)
        with pytest.raises(psycopg.Error) as excinfo:
            rollup_touched(conn, cfg, rewritten, deleted, now - 3600)
        # P0001 raise_exception -> ProgrammingError, NOT DataError/IntegrityError
        assert not isinstance(excinfo.value, (psycopg.DataError, psycopg.IntegrityError))
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP TRIGGER IF EXISTS rollup_landmine ON rollup_1m")
            cur.execute("DROP FUNCTION IF EXISTS _rollup_landmine()")

    # not skipped-and-counted: the tick failed instead
    assert metrics.ROLLUP_BUCKET_ERRORS._value.get() == before
    # the half-done bucket block was rolled back atomically (DELETE undone)
    assert one(conn, "SELECT count(*) FROM rollup_1m "
                     "WHERE bucket = to_timestamp(%s)",
                     (_bucket(good_ts),))[0] in (0, None)


def test_anchors_persist_across_restart(dsn, cfg, conn, monkeypatch):
    """last_run_start lives in worker_state now: a restart resumes from the
    last successful tick instead of rewinding a full hour and bursting, and a
    failed core phase never advances its anchor (the poison window cannot
    grow)."""
    pool = make_pool(dsn, statement_timeout_ms=10_000, name="anchor-test")
    try:
        # no anchor yet -> boot default rewinds one hour
        boot = worker_main._load_anchor(pool, worker_main.CORE_KEY,
                                         default=time.time() - 3600.0)
        assert boot <= time.time() - 3599

        now = time.time()
        with conn.cursor() as cur:
            insert_event(cur, now - 50, ip="203.0.113.64")
        ok, anchor = worker_main._phase_core(pool, cfg, boot)
        assert ok
        assert anchor >= boot

        # "restart": reload from DB, must resume (not rewind)
        resumed = worker_main._load_anchor(pool, worker_main.CORE_KEY,
                                            default=time.time() - 3600.0)
        assert resumed == pytest.approx(anchor, abs=5)

        # a failed core phase does NOT advance the anchor
        def boom(cur, cfg, since):
            raise RuntimeError("simulated sessionizer failure")

        monkeypatch.setattr(worker_main.sessionizer, "rebuild_dirty", boom)
        ok3, anchor3 = worker_main._phase_core(pool, cfg, resumed)
        assert not ok3 and anchor3 == resumed
    finally:
        pool.close()


def test_gap_histogram_failure_is_isolated_and_backs_off(dsn, cfg, monkeypatch):
    """A gap-histogram crash must not propagate to the tick loop, must not
    advance its anchor (retry, not next-day), and — round-4 should-fix — must
    not retry on the VERY next tick either: a failing 60 s day-scan burning
    its budget every minute stretched the tick cadence to ~2 min."""
    pool = make_pool(dsn, statement_timeout_ms=10_000, name="gap-test")
    calls = []

    def boom(cur, cfg):
        calls.append(1)
        raise RuntimeError("simulated day-scan failure")

    monkeypatch.setattr(gap_histogram, "update", boom)
    try:
        last, retry_at = worker_main._phase_gap(pool, cfg, last_run=0.0, retry_at=0.0)
        assert last == 0.0, "failed gap run must not advance the anchor"
        assert retry_at > time.time(), "failed gap run must back off"

        # while backing off: due by cadence, but not retried until retry_at
        last2, retry_at2 = worker_main._phase_gap(pool, cfg, last_run=last,
                                                  retry_at=retry_at)
        assert (last2, retry_at2) == (last, retry_at)
        assert len(calls) == 1, "backoff must actually skip the retry"
    finally:
        pool.close()


def test_ddl_phase_failure_is_isolated(dsn, cfg, monkeypatch):
    """A DDL-phase crash must not kill the tick loop either. The success case
    is asserted too — a phase that silently fails every tick (e.g. the SET
    LOCAL bind-parameter bug caught in review round-3) is just as broken."""
    pool = make_pool(dsn, statement_timeout_ms=10_000, name="ddl-test")
    try:
        assert worker_main._phase_ddl(pool, cfg) is True

        def boom(cur):
            raise RuntimeError("simulated ddl failure")

        monkeypatch.setattr(worker_main, "ensure_ahead", boom)
        assert worker_main._phase_ddl(pool, cfg) is False  # swallowed, not raised
    finally:
        pool.close()


def test_anchor_clamped_after_long_outage(dsn, cfg, conn):
    """Round-4 blocker 3: a persisted anchor older than max_events_age_s is
    clamped, so the catch-up tick processes a BOUNDED window and terminates —
    the old code retried the whole backlog in one transaction, hit the 60 s
    timeout, rolled back, and retried the same unbounded window forever.
    Events received during the outage (outside the clamp) are abandoned and
    counted in analytics_worker_backlog_abandoned_seconds."""
    pool = make_pool(dsn, statement_timeout_ms=10_000, name="clamp-test")
    try:
        ancient = time.time() - 10 * 86400
        with conn.cursor() as cur:
            # received during the outage: outside the clamp, abandoned
            insert_event(cur, time.time() - 50, ip="203.0.113.70", received_at=ancient)
            # received now: inside the clamp, must be sessionized
            insert_event(cur, time.time() - 50, ip="203.0.113.71")

        before = metrics.WORKER_BACKLOG_ABANDONED._value.get()
        ok, anchor = worker_main._phase_core(pool, cfg, ancient)
        assert ok, "the clamped window must actually finish"

        # the anchor is the tick's pre-discovery start (DB clock), not ancient
        assert anchor > time.time() - 300
        # the clamped window was bounded, and the abandonment was counted
        assert metrics.WORKER_BACKLOG_ABANDONED._value.get() - before \
            >= 9 * 86400 - 3600

        # fresh event processed; outage-era event abandoned (documented
        # tradeoff: rollup drops buckets older than max_events_age_s anyway)
        assert one(conn, "SELECT count(*) FROM sessions "
                         "WHERE client_ip = '203.0.113.71'")[0] == 1
        assert one(conn, "SELECT count(*) FROM sessions "
                         "WHERE client_ip = '203.0.113.70'")[0] == 0

        # and the SECOND tick on the now-fresh anchor processes normally
        ok2, anchor2 = worker_main._phase_core(pool, cfg, anchor)
        assert ok2 and anchor2 >= anchor
    finally:
        pool.close()


def test_default_partition_rescue(conn, cfg):
    """Worker down > 2 days: events land in events_default; ensure_ahead
    rescues them into the proper partition when it catches up (the plain
    CREATE would fail with a partition-constraint violation). Autocommit
    mode — the way the round-3 test exercised it."""
    now = time.time()
    future_day = dt.date.today() + dt.timedelta(days=10)
    name = f"events_{future_day.strftime('%Y%m%d')}"

    with conn.cursor() as cur:
        # an event arrives beyond every existing partition -> lands in DEFAULT
        cur.execute(
            """INSERT INTO events
               (received_at, ts, event_type, schema_version, client_ip, provider,
                channel_id, content_type, http_status, delivery, user_agent, country, raw)
               VALUES (now() + interval '10 days', %s, 'manifest_request', 1,
                       '203.0.113.65', 'p1', 'ch1', 'channel', 200, 'proxied',
                       'ua', 'DE', %s)""",
            (now, Jsonb({"rescued": True})),
        )
        assert one(conn, "SELECT count(*) FROM events_default")[0] == 1

        _ensure_one(cur, name, future_day, future_day + dt.timedelta(days=1))

        assert one(conn, "SELECT tableoid::regclass FROM events "
                         "WHERE client_ip = '203.0.113.65'")[0] == name
        assert one(conn, "SELECT count(*) FROM events_default")[0] == 0
        cur.execute(f"DROP TABLE IF EXISTS {name}")


def test_default_partition_rescue_inside_worker_transaction(dsn, cfg, conn):
    """Round-4 blocker 4: the rescue path as the REAL worker runs it — inside
    the DDL transaction on a pooled (non-autocommit) connection. The bare
    version aborted the transaction with CheckViolation and then died with
    InFailedSqlTransaction at the next rescue statement; it only ever worked
    in autocommit, which is exactly how the round-3 test above ran it."""
    now = time.time()
    future_day = dt.date.today() + dt.timedelta(days=11)
    name = f"events_{future_day.strftime('%Y%m%d')}"

    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO events
               (received_at, ts, event_type, schema_version, client_ip, provider,
                channel_id, content_type, http_status, delivery, user_agent, country, raw)
               VALUES (now() + interval '11 days', %s, 'manifest_request', 1,
                       '203.0.113.68', 'p1', 'ch1', 'channel', 200, 'proxied',
                       'ua', 'DE', %s)""",
            (now, Jsonb({"rescued": "inside worker tx"})),
        )
        assert one(conn, "SELECT count(*) FROM events_default")[0] == 1

    # pool connections are NOT autocommit: a statement error leaves the
    # transaction aborted until a savepoint/rollback resets it — the exact
    # condition the round-3 rescue never ran under.
    pool = make_pool(dsn, statement_timeout_ms=10_000, name="rescue-tx-test")
    try:
        with pool.connection() as wconn, wconn.cursor() as wcur:
            wcur.execute("SET LOCAL lock_timeout = 3000")  # mirror _phase_ddl
            _ensure_one(wcur, name, future_day, future_day + dt.timedelta(days=1))
            # the transaction must still be alive and usable afterwards
            wcur.execute("SELECT count(*) FROM events_default")
            assert wcur.fetchone()[0] == 0
        # leaving the with-block commits the DDL transaction
    finally:
        pool.close()

    assert one(conn, "SELECT tableoid::regclass FROM events "
                     "WHERE client_ip = '203.0.113.68'")[0] == name
    assert one(conn, "SELECT count(*) FROM events_default")[0] == 0
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {name}")


def test_sessions_retention_prunes_personal_data(conn, cfg):
    """Round-4 privacy item: sessions carry client_ip (personal data, like
    events.raw) — they expire after ANALYTICS_SESSIONS_RETENTION_DAYS, while
    the personal-data-free aggregates (rollup_1m, gap_histogram) stay."""
    now = time.time()
    with conn.cursor() as cur:
        insert_event(cur, now - 50, ip="198.51.100.60")
        rebuild(conn, cfg, now - 3600)
        assert one(conn, "SELECT count(*) FROM sessions")[0] == 1
        # rollup rows for the fresh bucket exist
        rollup_touched(conn, cfg, [], [], now - 3600)
        assert one(conn, "SELECT count(*) FROM rollup_1m")[0] >= 1
        # age the session beyond retention (cfg.sessions_retention_days=90)
        from app.partitions import prune
        cur.execute("UPDATE sessions SET started_at = %s, ended_at = %s",
                    (now - 91 * 86400, now - 91 * 86400 + 60))
        prune(cur, cfg)
    assert one(conn, "SELECT count(*) FROM sessions")[0] == 0
    # the aggregate survives the per-session expiry
    assert one(conn, "SELECT count(*) FROM rollup_1m")[0] >= 1
