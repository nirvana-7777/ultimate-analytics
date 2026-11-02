"""Contract tests against the real ingester app (FastAPI TestClient + real
Postgres). This is the executable spec for the §4/§8 reconciliation:

  * happy path asserts the TYPED COLUMNS in the DB, not just 202 — a backend
    that renames a field still passes a 202-only test and corrupts the rollup
  * unknown event_type -> events row (raw populated), NOT quarantine
  * missing channel_id -> events row with NULL, NOT quarantine
  * bad schema_version / bad types -> quarantine
  * envelope problems -> 400/401/413 as designed
"""
import json
import os
import sys
import time

import pytest

REPO = None  # set below
import pathlib  # noqa: E402

REPO = pathlib.Path(__file__).resolve().parents[2]
for p in (str(REPO), str(REPO / "ingester")):
    if p not in sys.path:
        sys.path.insert(0, p)

import tests._db as _db  # noqa: E402

_DSN = _db.dsn()

pytestmark = pytest.mark.skipif(_DSN is None,
                                reason="no test database")

if _DSN is not None:
    os.environ["ANALYTICS_DATABASE_URL"] = _DSN
    os.environ["ANALYTICS_AUTH_TOKEN"] = "test-token"
    os.environ["ANALYTICS_LOG_LEVEL"] = "warning"

    import app.main as app_main  # noqa: E402  (import AFTER env)
    from fastapi.testclient import TestClient  # noqa: E402

    client = TestClient(app_main.public_app)
    conn_factory = app_main.pool.connection


def _post(payload, token="test-token"):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return client.post("/ingest", json=payload, headers=headers)


def _fetch_event(where="ORDER BY id DESC LIMIT 1"):
    with conn_factory() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT ts, event_type, schema_version, client_ip, provider, "
                    f"channel_id, content_type, http_status, delivery, country, raw "
                    f"FROM events {where}")
        return cur.fetchone()


def _fetch_ua(where="ORDER BY id DESC LIMIT 1"):
    with conn_factory() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT user_agent, raw FROM events {where}")
        return cur.fetchone()


def _quarantine_count(reason=None):
    with conn_factory() as conn, conn.cursor() as cur:
        if reason:
            cur.execute("SELECT count(*) FROM quarantine WHERE reason = %s", (reason,))
        else:
            cur.execute("SELECT count(*) FROM quarantine")
        return cur.fetchone()[0]


@pytest.fixture(autouse=True)
def _clean():
    with conn_factory() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE events, quarantine, worker_state")
    yield


def _event(**over):
    e = {"schema_version": 1, "ts": time.time(), "event_type": "manifest_request",
         "client_ip": "203.0.113.7", "provider": "edge-a", "channel_id": "ch-contract",
         "content_type": "channel", "http_status": 200, "delivery": "redirect",
         "country": "DE"}
    e.update(over)
    return e


def test_no_auth_401():
    r = client.post("/ingest", json={"events": []})
    assert r.status_code == 401


def test_wrong_token_401():
    r = _post({"events": []}, token="wrong")
    assert r.status_code == 401


def test_malformed_envelope_400():
    assert _post({"nope": 1}).status_code == 400
    assert _post({"events": "x"}).status_code == 400
    r = client.post("/ingest", content=b"not json",
                    headers={"Authorization": "Bearer test-token"})
    assert r.status_code == 400


def test_happy_path_202_and_typed_columns():
    """The adversarial assertion: field-level, not just 202."""
    ev = _event(provider="edge-b", channel_id="ch-xyz", content_type="vod",
                http_status=200, delivery="proxied", country="FR",
                is_catchup=True, drm_variant="widevine")
    r = _post({"events": [ev]})
    assert r.status_code == 202
    row = _fetch_event()
    assert row is not None
    (ts, et, sv, ip, provider, ch, ct, hs, dv, ctry, raw) = row
    assert et == "manifest_request"
    assert sv == 1
    assert ip == "203.0.113.7"
    assert provider == "edge-b", "provider must survive into the typed column"
    assert ch == "ch-xyz"
    assert ct == "vod"
    assert hs == 200
    assert dv == "proxied"
    assert ctry == "FR"
    assert raw["channel_id"] == "ch-xyz"
    assert _quarantine_count() == 0


def test_unknown_event_type_lands_in_events_not_quarantine():
    r = _post({"events": [_event(event_type="proxy_thing", channel_id=None)]})
    assert r.status_code == 202
    row = _fetch_event()
    assert row[1] == "proxy_thing"
    assert row[5] is None            # channel_id NULL
    assert row[10] is not None       # raw populated
    assert _quarantine_count() == 0


def test_missing_channel_id_stays_in_events():
    ev = _event()
    del ev["channel_id"]
    ev["http_status"] = 503
    ev["delivery"] = "error"
    assert _post({"events": [ev]}).status_code == 202
    row = _fetch_event()
    assert row[5] is None
    assert row[7] == 503
    assert _quarantine_count() == 0, "error events without channel must stay queryable"


def test_bad_schema_version_quarantined():
    assert _post({"events": [_event(schema_version=2)]}).status_code == 202
    assert _quarantine_count("unsupported_schema_version") == 1
    assert _fetch_event() is None


def test_bad_field_type_quarantined_not_503():
    """One bad boolean must not take the batch down with it (round-3 fix)."""
    ev_ok = _event(provider="edge-c")
    ev_bad = _event(is_catchup="yes", client_ip="203.0.113.8")
    r = _post({"events": [ev_bad, ev_ok]})
    assert r.status_code == 202
    assert _quarantine_count("invalid_is_catchup") == 1
    row = _fetch_event()
    assert row[4] == "edge-c", "the valid event in the same batch must be stored"


def test_non_dict_event_quarantined():
    assert _post({"events": ["oops", _event()]}).status_code == 202
    assert _quarantine_count("event_not_object") == 1


def test_body_cap_413():
    big = "x" * (1_048_576 + 100)
    r = client.post("/ingest", json={"events": [], "pad": big},
                    headers={"Authorization": "Bearer test-token"})
    assert r.status_code == 413


def test_empty_batch_202():
    assert _post({"events": []}).status_code == 202


# ---------------------------------------------------------------- round-3 --
# Poison inputs: the review's blockers 1/3/5, asserted end-to-end against the
# real app + real Postgres. Every one of these used to be either a 503 (=
# the backend drops the whole batch, at-most-once) or a worker wedge.


def test_nan_ts_quarantined_batch_survives():
    """json.loads accepts NaN; `nan <= 0` is False; the old check waved it
    through and int(nan // 60) later wedged the worker tick forever."""
    ev_ok = _event(client_ip="203.0.113.41")
    ev_nan = _event(client_ip="203.0.113.42")
    ev_nan["ts"] = float("nan")   # httpx/json serializes as `NaN` literal
    ev_inf = _event(client_ip="203.0.113.43")
    ev_inf["ts"] = float("-Infinity")
    r = _post({"events": [ev_nan, ev_ok, ev_inf]})
    assert r.status_code == 202
    assert _quarantine_count("invalid_ts") == 2
    assert _fetch_event("WHERE client_ip = '203.0.113.41'") is not None
    assert _fetch_event("WHERE client_ip = '203.0.113.42'") is None


def test_huge_and_future_ts_quarantined():
    now = time.time()
    ev = _event(client_ip="203.0.113.44")
    ev["ts"] = 1e308                     # to_timestamp(1e308) errors in PG
    ev2 = _event(client_ip="203.0.113.45")
    ev2["ts"] = now + 3 * 86400          # > 1 day ahead = poison, not skew
    assert _post({"events": [ev, ev2]}).status_code == 202
    assert _quarantine_count("ts_in_future") == 2
    assert _fetch_event() is None


def test_nul_bytes_sanitized_not_dropped():
    """NUL is rejected by PG text and by jsonb (\\u0000 escape): the old code
    503'd the WHOLE batch. user_agent is client-controlled => reachable."""
    ev = _event(user_agent="bad\x00agent/1", client_ip="203.0.113.46")
    ev["weird_field"] = "key\x00value"
    r = _post({"events": [ev, _event(client_ip="203.0.113.47")]})
    assert r.status_code == 202
    assert _quarantine_count() == 0
    ua, raw = _fetch_ua("WHERE client_ip = '203.0.113.46'")
    assert "\x00" not in ua and "\ufffd" in ua
    assert "\ufffd" in raw["weird_field"] and "\x00" not in raw["weird_field"]
    assert _fetch_event("WHERE client_ip = '203.0.113.47'") is not None


def test_lone_surrogate_sanitized_not_dropped():
    # A lone UTF-16 surrogate cannot be UTF-8 encoded; psycopg raised at
    # executemany time -> 503 -> dropped batch.
    ev = _event(user_agent="sur\ud800rogate", client_ip="203.0.113.48")
    assert _post({"events": [ev]}).status_code == 202
    assert _quarantine_count() == 0
    ua = _fetch_ua("WHERE client_ip = '203.0.113.48'")[0]
    assert "\ufffd" in ua


def test_nan_in_raw_json_stringified():
    # jsonb rejects the NaN token; the raw copy keeps it as a string.
    ev = _event(client_ip="203.0.113.49")
    ev["custom_metric"] = float("nan")
    assert _post({"events": [ev]}).status_code == 202
    raw = _fetch_event("WHERE client_ip = '203.0.113.49'")[10]
    assert raw["custom_metric"] == "NaN"


def test_event_type_length_capped_for_label_cardinality():
    # event_type is a Prometheus label: unbounded length/value = unbounded
    # cardinality and memory (review blocker 5).
    ev = _event(event_type="t" * 65, client_ip="203.0.113.50")
    ev_ok = _event(event_type="t" * 64, client_ip="203.0.113.51")
    assert _post({"events": [ev, ev_ok]}).status_code == 202
    assert _quarantine_count("event_type_too_long") == 1
    assert _fetch_event("WHERE client_ip = '203.0.113.51'")[1] == "t" * 64


def test_events_per_batch_cap_413():
    # 1 MB of `{}` is ~350k events -> as many quarantine rows; the cap is a
    # request-level contract violation (413), not silent trimming.
    n = 5001
    events = [{"schema_version": 1, "ts": time.time(), "event_type": "x"}
              for _ in range(n)]
    r = _post({"events": events})
    assert r.status_code == 413
    assert "5000" in r.json()["detail"]


def test_docs_not_exposed():
    # public listener must not serve /docs /redoc /openapi.json
    for path in ("/docs", "/redoc", "/openapi.json"):
        resp = client.get(path, follow_redirects=False)
        assert resp.status_code == 404, path


def test_insert_fallback_quarantines_only_the_bad_event(monkeypatch):
    """The savepoint machinery: a poison that validation somehow missed
    (simulated here by injecting a DB-constraint violation after validation)
    must quarantine ONE event, not 503 the batch."""
    import app.main as m
    real_validate = m.validate_event

    def let_one_poison_through(e):
        row, reason = real_validate(e)
        if row is not None and e.get("user_agent") == "poison-me":
            row["http_status"] = 999  # violates the DB CHECK, not validation
        return row, reason

    monkeypatch.setattr(m, "validate_event", let_one_poison_through)
    r = _post({"events": [
        _event(user_agent="poison-me", client_ip="203.0.113.52"),
        _event(user_agent="fine", client_ip="203.0.113.53"),
    ]})
    monkeypatch.undo()
    assert r.status_code == 202
    assert _quarantine_count("insert_failed") == 1
    assert _fetch_event("WHERE client_ip = '203.0.113.52'") is None
    assert _fetch_event("WHERE client_ip = '203.0.113.53'") is not None


def test_validator_error_quarantined_batch_survives(monkeypatch):
    """Round-4 blocker 1: NOTHING validate_event does may 503 the batch.
    A validator crash (simulated: raises on one event) must quarantine that
    event as validator_error with a 202, and the rest of the batch must
    still land."""
    import app.main as m
    real_validate = m.validate_event

    def explodes_on_marked(e):
        if isinstance(e, dict) and e.get("user_agent") == "crash-me":
            raise RuntimeError("simulated validator bug")
        return real_validate(e)

    monkeypatch.setattr(m, "validate_event", explodes_on_marked)
    try:
        r = _post({"events": [
            _event(user_agent="crash-me", client_ip="203.0.113.54"),
            _event(user_agent="fine", client_ip="203.0.113.55"),
        ]})
    finally:
        monkeypatch.undo()
    assert r.status_code == 202
    assert _quarantine_count("validator_error") == 1
    assert _fetch_event("WHERE client_ip = '203.0.113.54'") is None
    assert _fetch_event("WHERE client_ip = '203.0.113.55'") is not None


def test_huge_int_ts_quarantined_batch_survives():
    """Round-4 blocker 1, end to end: Python's JSON parser accepts a
    400-digit integer; float(10**400) raises OverflowError and used to 503
    the whole batch from inside the validator."""
    ev_ok = _event(client_ip="203.0.113.56")
    ev_big = _event(client_ip="203.0.113.57")
    ev_big["ts"] = 10**400  # int, not float — json.dumps writes it verbatim
    ev_hint = _event(client_ip="203.0.113.58")
    ev_hint["manifest_refresh_hint"] = -(10**400)
    r = _post({"events": [ev_big, ev_ok, ev_hint]})
    assert r.status_code == 202
    assert _quarantine_count("ts_in_future") == 1
    assert _quarantine_count("invalid_manifest_refresh_hint") == 1
    assert _fetch_event("WHERE client_ip = '203.0.113.56'") is not None
    assert _fetch_event("WHERE client_ip = '203.0.113.57'") is None


def test_deeply_nested_raw_quarantined_batch_survives():
    """Round-4 blocker 1, end to end: a 500-deep raw used to die with
    RecursionError in the sanitizer / jsonb dump -> 503 -> dropped batch.
    Now: quarantined whole as raw_too_deep, and the quarantine row itself
    is storable (stub raw)."""
    ev_ok = _event(client_ip="203.0.113.59")
    ev_deep = _event(client_ip="203.0.113.60")
    node = "deep"
    for _ in range(500):
        node = {"n": [node]}
    ev_deep["payload"] = node
    r = _post({"events": [ev_deep, ev_ok]})
    assert r.status_code == 202
    assert _quarantine_count("raw_too_deep") == 1
    assert _fetch_event("WHERE client_ip = '203.0.113.60'") is None
    assert _fetch_event("WHERE client_ip = '203.0.113.59'") is not None
    # the quarantine row (with its stubbed raw) actually landed in the DB
    with conn_factory() as conn, conn.cursor() as cur:
        cur.execute("SELECT raw FROM quarantine WHERE reason = 'raw_too_deep'")
        assert cur.fetchone() is not None


def test_unknown_event_type_label_is_allowlist_bounded():
    """Round-4 should-fix: the 64-char cap bounded label LENGTH, not COUNT.
    Unknown types are counted under the single label 'other'; which types
    arrived stays queryable in events.raw."""
    from app import metrics as m
    before = m.EVENTS_UNKNOWN_TYPE.labels(event_type="other")._value.get()
    r = _post({"events": [
        _event(event_type="mystery_type_aaa", client_ip="203.0.113.61"),
        _event(event_type="mystery_type_bbb", client_ip="203.0.113.62"),
    ]})
    assert r.status_code == 202
    after = m.EVENTS_UNKNOWN_TYPE.labels(event_type="other")._value.get()
    assert after - before == 2
    # no per-value series were ever created — the label set cannot grow
    assert ("mystery_type_aaa",) not in m.EVENTS_UNKNOWN_TYPE._metrics
    assert ("mystery_type_bbb",) not in m.EVENTS_UNKNOWN_TYPE._metrics
    # the events themselves still carry their real type (queryable)
    assert _fetch_event("WHERE client_ip = '203.0.113.61'")[1] == "mystery_type_aaa"
