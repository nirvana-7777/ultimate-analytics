"""Pure unit tests for validate_event — no DB needed.

Validates the round-3 corrections:
  * never raises (one bad field = one quarantined event, never a 500/503)
  * every typed column is type-checked (a bad boolean/float used to reach the
    INSERT and 503 the whole batch)
  * unknown event_type passes through; missing channel_id passes as NULL
"""
import sys
import pathlib

REPO = pathlib.Path(__file__).resolve().parents[2]
for p in (str(REPO), str(REPO / "ingester")):
    if p not in sys.path:
        sys.path.insert(0, p)

from app.validate import validate_event  # noqa: E402


def base(**over):
    e = {
        "schema_version": 1, "ts": 1700000000.5, "event_type": "manifest_request",
        "client_ip": "203.0.113.1", "provider": "edge-a", "channel_id": "ch1",
        "content_type": "channel", "http_status": 200, "delivery": "proxied",
        "user_agent": "ultimate-backend-analytics/1", "country": "DE",
        "is_catchup": False, "client_drm": True, "no_proxy": False,
        "highest_quality_only": False, "drm_variant": "widevine",
        "manifest_refresh_hint": 2.0,
    }
    e.update(over)
    return e


def ok(e):
    row, reason = validate_event(e)
    assert reason is None, f"unexpected quarantine: {reason}"
    return row


def quarantined(e):
    row, reason = validate_event(e)
    assert row is None
    return reason


def test_happy_path():
    row = ok(base())
    assert row["provider"] == "edge-a"
    assert row["ts"] == 1700000000.5
    assert row["raw"] is not None


def test_unknown_event_type_accepted():
    row = ok(base(event_type="proxy_segment_request"))
    assert row["event_type"] == "proxy_segment_request"


def test_missing_channel_id_passes_as_null():
    e = base()
    del e["channel_id"]
    row = ok(e)
    assert row["channel_id"] is None


def test_omitted_keys_become_none():
    e = base()
    for k in ("client_ip", "provider", "content_type", "http_status", "delivery",
              "user_agent", "country", "is_catchup", "drm_variant",
              "manifest_refresh_hint"):
        e.pop(k, None)
    row = ok(e)
    assert all(row[k] is None for k in
               ("client_ip", "provider", "content_type", "http_status", "delivery",
                "user_agent", "country", "is_catchup", "drm_variant",
                "manifest_refresh_hint"))


def test_bad_types_quarantine_not_crash():
    assert quarantined(base(provider=123)) == "invalid_provider"
    assert quarantined(base(client_ip=True)) == "invalid_client_ip"
    assert quarantined(base(country=["DE"])) == "invalid_country"
    assert quarantined(base(user_agent=7)) == "invalid_user_agent"


def test_bad_booleans_quarantine():
    # This is the class of bug that used to 503 the whole batch at INSERT time.
    assert quarantined(base(is_catchup="yes")) == "invalid_is_catchup"
    assert quarantined(base(client_drm=1)) == "invalid_client_drm"
    assert quarantined(base(no_proxy="false")) == "invalid_no_proxy"
    ok(base(highest_quality_only=None))  # None is fine


def test_bad_numbers_quarantine():
    assert quarantined(base(ts="1700000000")) == "invalid_ts"
    assert quarantined(base(ts=0)) == "invalid_ts"
    assert quarantined(base(ts=-5)) == "invalid_ts"
    assert quarantined(base(ts=True)) == "invalid_ts"
    assert quarantined(base(manifest_refresh_hint="soon")) == "invalid_manifest_refresh_hint"


def test_schema_version():
    assert quarantined(base(schema_version=2)) == "unsupported_schema_version"
    assert quarantined(base(schema_version="1")) == "invalid_schema_version"
    assert quarantined(base(schema_version=True)) == "invalid_schema_version"
    e = base()
    del e["schema_version"]
    assert quarantined(e) == "invalid_schema_version"


def test_event_type():
    assert quarantined(base(event_type="")) == "missing_event_type"
    assert quarantined(base(event_type=5)) == "missing_event_type"
    assert quarantined("not a dict") == "event_not_object"
    assert quarantined([1, 2]) == "event_not_object"


def test_enums_and_ranges():
    assert quarantined(base(content_type="livestream")) == "invalid_content_type"
    assert quarantined(base(delivery="cached")) == "invalid_delivery"
    assert quarantined(base(http_status=99)) == "invalid_http_status"
    assert quarantined(base(http_status=600)) == "invalid_http_status"
    assert quarantined(base(http_status="200")) == "invalid_http_status"
    assert quarantined(base(http_status=True)) == "invalid_http_status"


def test_truncation():
    row = ok(base(provider="p" * 300))
    assert len(row["provider"]) == 256
    row = ok(base(channel_id="c" * 1000))
    assert len(row["channel_id"]) == 512


def test_valid_enums_pass():
    for ct in ("channel", "event", "vod", "recording"):
        assert ok(base(content_type=ct))["content_type"] == ct
    for d in ("redirect", "proxied", "error"):
        assert ok(base(delivery=d))["delivery"] == d
