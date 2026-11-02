"""Scenario A2: viewer stops watching.

The session must be closed by the GLOBAL close sweep once ended_at is older
than SESSION_GAP_S — not by the per-IP rebuild (which never runs for an
inactive IP). Uses a smaller gap so "time passing" is simulated without
sleeping.
"""
import time
from dataclasses import replace

from tests.worker.conftest import close_stale, one, rebuild


def test_departure_closes_session(conn, cfg):
    cfg = replace(cfg, session_gap_s=300.0)
    now = time.time()
    with conn.cursor() as cur:
        # 60 s apart, LAST event 10 minutes ago -> older than gap(300)
        for i in range(11):
            insert_ev(cur, now - 1200 + i * 60)

    rebuild(conn, cfg, now - 3600)
    assert one(conn, "SELECT closed FROM sessions")[0] is False  # not yet: rebuild never closes

    closed = close_stale(conn, cfg)
    assert closed >= 1
    assert one(conn, "SELECT closed FROM sessions")[0] is True
    assert one(conn, "SELECT count(*) FROM sessions")[0] == 1


def test_recent_session_stays_open(conn, cfg):
    now = time.time()
    with conn.cursor() as cur:
        for i in range(5):
            insert_ev(cur, now - 240 + i * 60)  # last event 60 s ago

    rebuild(conn, cfg, now - 3600)
    close_stale(conn, cfg)
    assert one(conn, "SELECT closed FROM sessions")[0] is False


def insert_ev(cur, ts):
    from tests.worker.conftest import insert_event
    insert_event(cur, ts)
