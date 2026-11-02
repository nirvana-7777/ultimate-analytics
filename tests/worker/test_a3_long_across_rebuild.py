"""Scenario A3: 3-hour continuous session crossing the rebuild window.

Two parts:
 1. FIRST build with history older than REBUILD_WINDOW — the anchor must
    rewind to the IP's oldest event (the first-build hole: anchor pulled only
    by deleted rows, which don't exist yet).
 2. Subsequent ticks with only-new-events dirtiness must not fracture the
    long session into ~15-min chained rows (overlap delete + anchor pull).
"""
import time

from tests.worker.conftest import insert_event, one, rebuild


def test_first_build_covers_full_history(conn, cfg):
    now = time.time()
    with conn.cursor() as cur:
        for i in range(180):  # 3h, 60 s apart, ending 60 s ago
            insert_event(cur, now - 10800 + i * 60)

    rebuild(conn, cfg, now - 60)

    count, started, ended, events = one(
        conn, "SELECT count(*), min(started_at), max(ended_at), sum(event_count) FROM sessions")
    assert count == 1
    assert abs(started - (now - 10800)) < 1, \
        "first build must anchor at the IP's oldest event, not now - REBUILD_WINDOW"
    assert abs(ended - (now - 60)) < 1
    assert events == 180
    dur = one(conn, "SELECT duration_s FROM sessions")[0]
    assert abs(dur - 10740) < 1


def test_second_tick_does_not_fracture(conn, cfg):
    now = time.time()
    with conn.cursor() as cur:
        for i in range(180):
            insert_event(cur, now - 10800 + i * 60)

    rebuild(conn, cfg, now - 60)

    # New events arrive; the next tick's discovery only sees these.
    with conn.cursor() as cur:
        for i in range(5):
            insert_event(cur, now - 55 + i * 10)  # 10 s apart, same run

    rebuild(conn, cfg, now - 30)

    count, started, ended, events = one(
        conn, "SELECT count(*), min(started_at), max(ended_at), sum(event_count) FROM sessions")
    assert count == 1, "long session must survive the rebuild window as ONE row"
    assert abs(started - (now - 10800)) < 1
    assert abs(ended - (now - 15)) < 1
    assert events == 185
