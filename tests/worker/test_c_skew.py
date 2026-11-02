"""Scenario C: steady backend clock skew (ts 20 min in the past, received now).

The round-2 bug: a fixed 15-min re-roll window made skewed traffic invisible
in ALL rollup panels, and the late-events counter never fired because those
events advanced their watermarks. Fixed by the data-driven re-roll: buckets
come from events received since the last tick, bounded by
ANALYTICS_MAX_EVENTS_AGE_S with overflow counted in rollup_dropped_late_total.
"""
import time

from app import metrics
from tests.worker.conftest import insert_event, one, rebuild, rollup_touched


def _bucket(ts):
    return int(ts // 60) * 60


def test_skewed_events_land_in_their_ts_buckets(conn, cfg):
    now = time.time()
    # pin the 5 events inside one bucket (a straddling minute boundary makes
    # the per-bucket count assertion flaky)
    skewed = _bucket(now - 1200) + 5
    with conn.cursor() as cur:
        # 5 redirect events with ts 20 min ago, received just now.
        for i in range(5):
            insert_event(cur, skewed + i, ip="198.51.100.9", delivery="redirect",
                         status=200)

    rewritten, deleted = rebuild(conn, cfg, now - 3600)
    rollup_touched(conn, cfg, rewritten, deleted, now - 3600)

    row = one(conn, """
        SELECT events, views, sessions, errors FROM rollup_1m
        WHERE bucket = to_timestamp(%s) AND delivery = 'redirect'
    """, (_bucket(skewed),))
    assert row is not None, "skewed traffic must appear in its ts bucket"
    events, views, sessions, errors = row
    assert events == 5
    assert views == 5          # redirect: views = events
    assert sessions == 1       # one session started in that bucket
    assert errors == 0


def test_proxied_views_come_from_sessions(conn, cfg):
    now = time.time()
    with conn.cursor() as cur:
        insert_event(cur, now - 1200, ip="198.51.100.10", delivery="proxied")

    rewritten, deleted = rebuild(conn, cfg, now - 3600)
    rollup_touched(conn, cfg, rewritten, deleted, now - 3600)

    row = one(conn, """
        SELECT events, views, sessions FROM rollup_1m
        WHERE bucket = to_timestamp(%s) AND delivery = 'proxied'
    """, (_bucket(now - 1200),))
    assert row == (1, 1, 1)  # one event, one session -> one view


def test_too_old_ts_dropped_and_counted(conn, cfg):
    before = metrics.ROLLUP_DROPPED_LATE._value.get()
    now = time.time()
    with conn.cursor() as cur:
        insert_event(cur, now - 7200, ip="198.51.100.11", delivery="redirect")
        insert_event(cur, now - 1200, ip="198.51.100.12", delivery="redirect")

    rewritten, deleted = rebuild(conn, cfg, now - 3600)
    rollup_touched(conn, cfg, rewritten, deleted, now - 3600)

    stale = one(conn, "SELECT count(*) FROM rollup_1m "
                      "WHERE bucket = to_timestamp(%s)", (_bucket(now - 7200),))[0]
    assert stale == 0, "events older than ANALYTICS_MAX_EVENTS_AGE_S are dropped from rollup"
    assert metrics.ROLLUP_DROPPED_LATE._value.get() > before


def test_rollup_is_idempotent(conn, cfg):
    now = time.time()
    skewed = _bucket(now - 1200) + 5
    with conn.cursor() as cur:
        for i in range(3):
            insert_event(cur, skewed + i, ip="198.51.100.13", delivery="redirect")

    rewritten, deleted = rebuild(conn, cfg, now - 3600)
    for _ in range(3):  # re-roll the same window repeatedly
        rollup_touched(conn, cfg, rewritten, deleted, now - 3600)

    total = one(conn, "SELECT count(*), sum(events) FROM rollup_1m")[0]
    rows, events = one(conn, "SELECT count(*), sum(events) FROM rollup_1m")
    assert rows == 1
    assert events == 3


def test_moved_session_fixes_old_bucket(conn, cfg):
    """A late earlier event moves started_at — the bucket it LEFT must also be
    recomputed (rewritten + deleted starts), not just the new one."""
    now = time.time()
    with conn.cursor() as cur:
        insert_event(cur, now - 130, ip="198.51.100.14", delivery="proxied")

    rewritten, deleted = rebuild(conn, cfg, now - 3600)
    rollup_touched(conn, cfg, rewritten, deleted, now - 3600)
    b1_sessions = one(conn, "SELECT sessions FROM rollup_1m WHERE bucket = to_timestamp(%s)",
                      (_bucket(now - 130),))[0]
    assert b1_sessions == 1

    # Late event arrives, 70 s earlier: same run (gap < 120), session moves back
    # across the minute boundary if now-130 and now-200 straddle one.
    with conn.cursor() as cur:
        insert_event(cur, now - 200, ip="198.51.100.14", delivery="proxied")
    rewritten, deleted = rebuild(conn, cfg, now - 30)
    rollup_touched(conn, cfg, rewritten, deleted, now - 30)

    # Session now starts at now-200; the bucket of now-200 must show it...
    assert one(conn, "SELECT sessions FROM rollup_1m WHERE bucket = to_timestamp(%s)",
               (_bucket(now - 200),))[0] == 1
    # ... and the total across both buckets is exactly one session (delete+insert
    # recomputed both, no stale duplicate left in the old bucket).
    total = one(conn, "SELECT coalesce(sum(sessions),0) FROM rollup_1m")[0]
    assert total == 1
