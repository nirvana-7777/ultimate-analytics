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
        cur.execute("TRUNCATE events, quarantine")
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
