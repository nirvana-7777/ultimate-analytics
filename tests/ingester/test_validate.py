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


# ------------------------------------------------------------- round-3 poison
# Every case here used to pass validation and blow up later: at INSERT time
# (NUL / surrogates / NaN in jsonb) or inside the worker (int(nan // 60),
# to_timestamp(1e308)).


def test_nonfinite_ts_quarantined():
    assert quarantined(base(ts=float("nan"))) == "invalid_ts"
    assert quarantined(base(ts=float("inf"))) == "invalid_ts"
    assert quarantined(base(ts=float("-inf"))) == "invalid_ts"
    assert quarantined(base(ts=1e308)) == "ts_in_future"


def test_far_future_ts_quarantined():
    import time as _t
    assert quarantined(base(ts=_t.time() + 3 * 86400)) == "ts_in_future"
    # a day of skew tolerance remains acceptable (steady-skew is C-scenario
    # domain data, not poison)
    ok(base(ts=_t.time() + 3600))


def test_nul_stripped_from_strings():
    row = ok(base(user_agent="bad\x00agent"))
    assert row["user_agent"] == "bad\ufffdagent"
    assert row["_sanitized"] == ["user_agent"]


def test_lone_surrogate_replaced():
    row = ok(base(channel_id="ch\ud8001"))
    assert row["channel_id"] == "ch\ufffd1"
    assert row["_sanitized"] == ["channel_id"]


def test_raw_sanitized_recursively():
    e = base(metadata={"nested": ["keep", "nul\x00", {"deep": "sur\ud800"}]},
             custom=1.5)
    row = ok(e)
    assert row["raw"]["metadata"]["nested"][1] == "nul\ufffd"
    assert row["raw"]["metadata"]["nested"][2]["deep"] == "sur\ufffd"
    assert row["raw"]["custom"] == 1.5
    # unknown/nested keys are attributed to "raw" so the metric label stays
    # bounded (a client-controlled label would be a cardinality bomb)
    assert set(row["_sanitized"]) == {"raw"}


def test_nan_in_raw_becomes_string():
    row = ok(base(custom_metric=float("nan")))
    assert row["raw"]["custom_metric"] == "NaN"
    row = ok(base(custom_metric=float("inf")))
    assert row["raw"]["custom_metric"] == "Infinity"


def test_nonfinite_number_fields_quarantined():
    assert quarantined(base(manifest_refresh_hint=float("nan"))) \
        == "invalid_manifest_refresh_hint"
    assert quarantined(base(manifest_refresh_hint=float("inf"))) \
        == "invalid_manifest_refresh_hint"


def test_event_type_cap():
    ok(base(event_type="t" * 64))
    assert quarantined(base(event_type="t" * 65)) == "event_type_too_long"


def test_sanitized_event_is_storable():
    """The full guarantee: after validate_event, json.dumps(raw).encode()
    succeeds and contains no NUL escape — i.e. Postgres jsonb accepts it."""
    import json as _json
    e = base(user_agent="a\x00b", channel_id="c\ud800d", pad="x" * 10)
    e["weird"] = {"k\x00": ["s\ud800t", 1, None, True]}
    row, reason = validate_event(e)
    assert reason is None
    encoded = _json.dumps(row["raw"]).encode("utf-8")
    assert b"\\u0000" not in encoded
    encoded.decode("utf-8")  # no surrogates left either


# ------------------------------------------------------------- round-4 poison
# The validator itself must never raise (review blocker 1, round 4):
# Python's JSON parser accepts 400-digit ints and 10k-deep structures, and
# float()/math.isfinite()/the recursive sanitizer all blow up on them —
# which used to escape as a 503 and drop the WHOLE batch.


def test_huge_int_ts_quarantined_not_overflowerror():
    # math.isfinite(10**400) and float(10**400) raise OverflowError; ints
    # are compared as ints now (exact, unraisable).
    assert quarantined(base(ts=10**400)) == "ts_in_future"
    assert quarantined(base(ts=-(10**400))) == "invalid_ts"
    assert quarantined(base(ts=10**12)) == "ts_in_future"   # hard cap, int
    assert quarantined(base(ts=2**60)) == "ts_in_future"    # far beyond cap
    assert quarantined(base(ts=1e12)) == "ts_in_future"     # hard cap, float
    # plausible int ts from a backend (whole seconds, in the past) is fine
    row = ok(base(ts=1700000000))
    assert row["ts"] == 1700000000.0 and isinstance(row["ts"], float)
    # int just past now+MAX_TS_FUTURE_S is poison, not skew (same as floats)
    import time as _t
    assert quarantined(base(ts=int(_t.time()) + 86401)) == "ts_in_future"


def test_huge_int_number_fields_quarantined():
    # float(10**400) in _clean_num used to raise OverflowError. No future
    # check on this field — only the float-exactness bound.
    assert quarantined(base(manifest_refresh_hint=10**400)) \
        == "invalid_manifest_refresh_hint"
    assert quarantined(base(manifest_refresh_hint=-(10**400))) \
        == "invalid_manifest_refresh_hint"
    ok(base(manifest_refresh_hint=2**53))


def _nest(depth, leaf):
    o = leaf
    for _ in range(depth):
        o = {"n": [o]}
    return o


def test_too_deep_raw_quarantined_not_recursionerror():
    # _sanitize_obj recurses once per level and would hit RecursionError
    # (~1000 deep); json.dumps for the jsonb INSERT recurses too. The depth
    # cap fires first and the event is quarantined whole.
    assert quarantined(base(nested=_nest(500, "deep"))) == "raw_too_deep"
    assert quarantined(base(nested=_nest(5000, "deeper"))) == "raw_too_deep"
    # ...while plausible nesting sails through untouched
    row = ok(base(nested=_nest(20, {"still": "here"})))
    node = row["raw"]["nested"]
    while isinstance(node, dict) and "n" in node:
        node = node["n"][0]  # unwrap {"n": [child]} levels
    assert node == {"still": "here"}


def test_validate_event_never_raises():
    """The contract, swept: none of these may raise, whatever happens —
    each returns (row|None, reason|None) and the batch survives."""
    nasty = [
        base(ts=10**400),
        base(ts=float("nan")),
        base(ts=float("inf")),
        base(manifest_refresh_hint=10**999),
        base(nested=_nest(3000, "deep")),
        base(nested=_nest(3000, [float("nan"), {"k\x00": 1}])),
        base(event_type=10**100),
        "not a dict",
        [1, 2, 3],
        None,
        42,
        {"only": "keys", "no": "ts"},
        {"schema_version": 1, "ts": 10**400, "event_type": None},
        {"schema_version": 1, "ts": 1.0, "event_type": "x",
         "raw poison": _nest(2000, "deep")},
    ]
    for e in nasty:
        row, reason = validate_event(e)
        assert (row is None) == (reason is not None), e
        assert reason is None or isinstance(reason, str)
    # the good event still passes through the same gauntlet
    assert ok(base())["provider"] == "edge-a"
