"""Split rules and dominant-delivery semantics.

  * session key is (client_ip, content_type, channel_id) — a content_type
    change splits even on the same channel_id
  * gap > SESSION_GAP_S splits (paused viewer / overnight-open channel)
  * dominant delivery is the MOST FREQUENT mode, ties broken
    error > proxied > redirect (round-2 correction: presence-priority would
    label a 2h proxied session with one error event as 'error')
"""
import time

from tests.worker.conftest import insert_event, one, rebuild


def test_channel_change_splits(conn, cfg):
    now = time.time()
    with conn.cursor() as cur:
        for i in range(3):
            insert_event(cur, now - 300 + i * 60, channel="ch1")
        for i in range(2):
            insert_event(cur, now - 60 + i * 10, channel="ch2")
    rebuild(conn, cfg, now - 3600)
    assert one(conn, "SELECT count(*) FROM sessions")[0] == 2


def test_gap_splits(conn, cfg):
    now = time.time()
    with conn.cursor() as cur:
        insert_event(cur, now - 600)
        insert_event(cur, now - 540)
        insert_event(cur, now - 419)  # 121 s after previous (> 120 gap) -> new session
    rebuild(conn, cfg, now - 3600)
    assert one(conn, "SELECT count(*) FROM sessions")[0] == 2


def test_gap_boundary_does_not_split(conn, cfg):
    now = time.time()
    with conn.cursor() as cur:
        insert_event(cur, now - 600)
        insert_event(cur, now - 480)  # exactly 120 s -> same session (rule is > GAP)
    rebuild(conn, cfg, now - 3600)
    assert one(conn, "SELECT count(*) FROM sessions")[0] == 1


def test_content_type_change_splits(conn, cfg):
    now = time.time()
    with conn.cursor() as cur:
        insert_event(cur, now - 300, channel="ch1", ct="channel")
        insert_event(cur, now - 240, channel="ch1", ct="vod")  # same channel, new content_type
    rebuild(conn, cfg, now - 3600)
    assert one(conn, "SELECT count(*) FROM sessions")[0] == 2


def test_dominant_delivery_by_count_then_priority(conn, cfg):
    now = time.time()
    with conn.cursor() as cur:
        # 5 proxied + 1 error -> majority proxied (presence-priority would say error)
        for i in range(5):
            insert_event(cur, now - 600 + i * 60, ip="198.51.100.20", delivery="proxied")
        insert_event(cur, now - 270, ip="198.51.100.20", delivery="error", status=503)
        # tie 1 redirect vs 1 error, different IP -> error wins
        insert_event(cur, now - 600, ip="198.51.100.21", delivery="redirect")
        insert_event(cur, now - 540, ip="198.51.100.21", delivery="error", status=500)

    rebuild(conn, cfg, now - 3600)

    dom = one(conn, "SELECT delivery FROM sessions WHERE client_ip = '198.51.100.20'")[0]
    assert dom == "proxied", "most-frequent delivery, not presence priority"
    dom2 = one(conn, "SELECT delivery FROM sessions WHERE client_ip = '198.51.100.21'")[0]
    assert dom2 == "error", "tie broken error > proxied > redirect"


def test_country_and_provider_from_first_event(conn, cfg):
    now = time.time()
    with conn.cursor() as cur:
        insert_event(cur, now - 600, ip="198.51.100.22", provider="edge-a", country="DE")
        insert_event(cur, now - 540, ip="198.51.100.22", provider="edge-b", country="FR")
    rebuild(conn, cfg, now - 3600)
    provider, country = one(conn, "SELECT provider, country FROM sessions")
    assert (provider, country) == ("edge-a", "DE")


def test_two_views_behind_nat_do_not_fragment(conn, cfg):
    """Round-3 review: 'Two TVs alternating channels [behind one NAT]
    fragment into one-event sessions.' The per-key grouping keeps each
    channel's stream continuous: events of the same key are compared only
    against each other, not against the interleaved other key."""
    now = time.time()
    with conn.cursor() as cur:
        # two viewers behind one IP, alternating heartbeats every 30 s
        for i in range(10):
            insert_event(cur, now - 300 + i * 30, channel="ch1", ip="198.51.100.30")
            insert_event(cur, now - 285 + i * 30, channel="ch2", ip="198.51.100.30")
    rebuild(conn, cfg, now - 3600)
    count, events, min_dur = one(conn, """
        SELECT count(*), sum(event_count), min(duration_s) FROM sessions
        WHERE client_ip = '198.51.100.30'""")
    assert count == 2, "one continuous session per channel, not 20 fragments"
    assert events == 20
    assert min_dur >= 250, "each session spans the whole 5-minute window"
