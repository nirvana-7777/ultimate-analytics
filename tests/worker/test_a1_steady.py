"""Scenario A1: steady proxied viewer, 2 hours.

The round-2 bug: an actively watching viewer (watermark ≈ now) was never
selected by "watermark < now - GAP - RW", so its session was built once and
never extended. Fixed by received_at discovery + overlap delete. Also pins
idempotency: a second rebuild must not duplicate the session.
"""
import time

from tests.worker.conftest import one, rebuild


def test_steady_viewer_single_session(conn, cfg):
    now = time.time()
    n = 120
    with conn.cursor() as cur:
        for i in range(n):
            # 60 s apart, ending 60 s ago (still within the gap -> open)
            insert_ev(cur, now - (n - i) * 60)

    rebuild(conn, cfg, now - 7200)

    row = one(conn, "SELECT count(*), min(started_at), max(ended_at), "
                    "sum(event_count), bool_and(closed) FROM sessions")
    count, started, ended, events, all_closed = row
    assert count == 1, f"steady viewer must produce exactly 1 session, got {count}"
    assert abs(started - (now - n * 60)) < 1
    assert abs(ended - (now - 60)) < 1
    assert events == n
    assert all_closed is False  # last event 60 s ago < GAP(120): still open

    dur = one(conn, "SELECT duration_s FROM sessions")[0]
    assert abs(dur - (n - 1) * 60) < 1


def test_rebuild_is_idempotent(conn, cfg):
    now = time.time()
    with conn.cursor() as cur:
        for i in range(120):
            insert_ev(cur, now - (120 - i) * 60)

    rebuild(conn, cfg, now - 7200)
    rebuild(conn, cfg, now - 60)  # second tick sees a subset as "new"
    rebuild(conn, cfg, now - 30)  # and a third, overlapping

    row = one(conn, "SELECT count(*), sum(event_count) FROM sessions")
    assert row[0] == 1, "repeated rebuilds must not duplicate or split sessions"
    assert row[1] == 120


def insert_ev(cur, ts):
    from tests.worker.conftest import insert_event
    insert_event(cur, ts)
