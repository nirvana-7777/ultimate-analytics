"""Scenario B: the departing viewer, precisely.

The B fix is that close_stale is a global statement, independent of discovery.
This test removes the rebuild entirely after the first pass: no new events,
IP never rediscovered — the session must still close.
"""
import time
from dataclasses import replace

from tests.worker.conftest import close_stale, insert_event, one, rebuild


def test_close_independent_of_discovery(conn, cfg):
    cfg = replace(cfg, session_gap_s=300.0)
    now = time.time()
    with conn.cursor() as cur:
        for i in range(5):
            # received an hour ago: a later tick sees NO new events for this IP
            insert_event(cur, now - 900 + i * 60, received_at=now - 3600)

    rebuild(conn, cfg, now - 3600)

    # NO new events -> IP is not dirty -> rebuild_dirty must be a no-op ...
    rewritten, deleted = rebuild(conn, cfg, now - 30)
    assert rewritten == [] and deleted == []
    assert one(conn, "SELECT count(*) FROM sessions")[0] == 1

    # ... and the global sweep closes it anyway.
    assert close_stale(conn, cfg) >= 1
    assert one(conn, "SELECT closed FROM sessions")[0] is True
