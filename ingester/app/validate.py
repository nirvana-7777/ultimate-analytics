"""Per-event validation. Returns (row | None, quarantine_reason | None).

Never raises: every malformed field is converted into a quarantine reason.
Unknown event_type is *not* quarantined (round-2 resolution) — it is stored in
`events` with typed columns NULL and `raw` filled, so future senders (media
proxy) are queryable from day one. Missing channel_id is allowed through as
NULL (doc §3: error events may omit it).

Round-3 hardening (review blockers 1 / 3 / 5 — all empirically confirmed
against postgres 16, see scripts/verify_review_claims*.py):

* ts must be finite and at most MAX_TS_FUTURE_S ahead. ``json.loads``
  accepts NaN/Infinity literals and ``float("nan") <= 0`` is False, so the
  old check waved them through; ``int(nan // 60)`` then raised in
  rollup._bucket and ``to_timestamp(1e308)`` failed in every rollup DELETE —
  with a one-transaction tick and a last_run watermark that only advances on
  success, that single poisoned event wedged the worker permanently.
* every string (values AND keys, including inside raw) is sanitized for
  Postgres: NUL is rejected by text/jsonb ("\\u0000 cannot be converted to
  text") and lone UTF-16 surrogates fail UTF-8 encoding; both reached
  executemany as a 503 and the backend then dropped the FULL batch
  (at-most-once). user_agent is client-controlled, so this is reachable.
* raw is sanitized recursively; NaN/Infinity floats become strings ("NaN" /
  "Infinity" / "-Infinity") because jsonb rejects those tokens.
* event_type is capped: it is a Prometheus label (analytics_events_unknown_
  type_total), so an unbounded label is an unbounded memory/cardinality bomb.
  Over-long types are quarantined (truncating would silently invent types).
"""
import math
import os
import time
from typing import Any

CAPS = {
    "client_ip": 64,
    "provider": 256,
    "channel_id": 512,
    "user_agent": 256,
    "country": 64,
    "drm_variant": 32,
    "proxy": 2048,
    "content_type": 32,
    "delivery": 32,
    "event_type": 64,
}
CONTENT_TYPES = {"channel", "event", "vod", "recording"}
DELIVERIES = {"redirect", "proxied", "error"}
BOOL_FIELDS = ("is_catchup", "client_drm", "no_proxy", "highest_quality_only")
KNOWN_FIELDS = set(CAPS) | {"schema_version", "ts", "http_status",
                            "manifest_refresh_hint"} | set(BOOL_FIELDS)

# A ts more than this far in the future is poison, not skew (default 1 day).
MAX_TS_FUTURE_S = float(os.environ.get("ANALYTICS_TS_MAX_FUTURE_S", "86400"))

# str.translate table: every lone surrogate -> U+FFFD. Python's
# errors="replace" on ENCODE emits "?"; we want the replacement character so
# the damage is visible, greppable and round-trippable.
_SURROGATE_TABLE = dict.fromkeys(range(0xD800, 0xE000), 0xFFFD)


def _sanitize_str(v: str) -> str:
    """Make a Python string storable in Postgres text/jsonb.

    NUL is replaced with U+FFFD (Postgres rejects 0x00 in text and the \\u0000
    escape in jsonb); lone surrogates — which json.loads happily produces from
    "\\ud800" escapes and which cannot be encoded as UTF-8 — are replaced the
    same way. U+FFFD marks the damage instead of silently deleting it.
    """
    try:
        v.encode("utf-8")
    except UnicodeEncodeError:
        v = v.translate(_SURROGATE_TABLE)
    if "\x00" in v:
        v = v.replace("\x00", "\ufffd")
    return v


def _float_token(v: float) -> str:
    return "NaN" if math.isnan(v) else ("Infinity" if v > 0 else "-Infinity")


def _sanitize_obj(o: Any, top: str | None):
    """Recursively sanitize an event for jsonb storage.

    Returns (obj, touched) where ``touched`` is the set of *top-level* keys
    whose subtree needed cleaning. Unknown/nested keys and dirty keys
    themselves are attributed to "raw" so the metrics label stays bounded.
    """
    if isinstance(o, str):
        s = _sanitize_str(o)
        return (o, set()) if s == o else (s, {top if top in KNOWN_FIELDS else "raw"})
    if isinstance(o, float):
        if math.isfinite(o):
            return o, set()
        return _float_token(o), {top if top in KNOWN_FIELDS else "raw"}
    if isinstance(o, dict):
        out = {}
        touched: set = set()
        for k, v in o.items():
            k2 = _sanitize_str(k) if isinstance(k, str) else k
            if k2 != k:
                touched.add("raw")
            v2, t = _sanitize_obj(v, k)
            out[k2] = v2
            touched |= t
        return out, touched
    if isinstance(o, list):
        out = []
        touched: set = set()
        for v in o:
            v2, t = _sanitize_obj(v, top)
            out.append(v2)
            touched |= t
        return out, touched
    return o, set()


def _clean_str(v: Any, cap: int) -> tuple[str | None, bool]:
    if v is None:
        return None, True
    if not isinstance(v, str):
        return None, False
    return v[:cap], True


def _clean_bool(v: Any) -> tuple[bool | None, bool]:
    if v is None:
        return None, True
    if not isinstance(v, bool):  # bool check must exclude ints sent as 0/1? doc uses real booleans
        return None, False
    return v, True


def _clean_num(v: Any) -> tuple[float | None, bool]:
    if v is None:
        return None, True
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None, False
    f = float(v)
    # json.loads accepts NaN/Infinity literals; a double column would store
    # them silently and poison every downstream sum/percentile.
    if not math.isfinite(f):
        return None, False
    return f, True


def validate_event(e: Any) -> tuple[dict | None, str | None]:
    if not isinstance(e, dict):
        return None, "event_not_object"

    # Sanitize BEFORE any use: the typed fields and the raw copy both come
    # from this object, so one pass protects every downstream INSERT.
    e, touched = _sanitize_obj(e, None)

    sv = e.get("schema_version")
    if isinstance(sv, bool) or not isinstance(sv, int):
        return None, "invalid_schema_version"
    if sv != 1:
        return None, "unsupported_schema_version"

    ts = e.get("ts")
    if (isinstance(ts, bool) or not isinstance(ts, (int, float))
            or not math.isfinite(ts) or ts <= 0):
        return None, "invalid_ts"
    if ts > time.time() + MAX_TS_FUTURE_S:
        return None, "ts_in_future"

    et = e.get("event_type")
    if not isinstance(et, str) or not et:
        return None, "missing_event_type"
    if len(et) > CAPS["event_type"]:
        return None, "event_type_too_long"

    row: dict = {"ts": float(ts), "event_type": et, "schema_version": sv, "raw": e}
    if touched:
        row["_sanitized"] = sorted(touched)

    for f in ("client_ip", "provider", "channel_id", "user_agent", "country", "drm_variant", "proxy"):
        v, ok = _clean_str(e.get(f), CAPS[f])
        if not ok:
            return None, f"invalid_{f}"
        row[f] = v

    ct, ok = _clean_str(e.get("content_type"), CAPS["content_type"])
    if not ok or (ct is not None and ct not in CONTENT_TYPES):
        return None, "invalid_content_type"
    row["content_type"] = ct

    dv, ok = _clean_str(e.get("delivery"), CAPS["delivery"])
    if not ok or (dv is not None and dv not in DELIVERIES):
        return None, "invalid_delivery"
    row["delivery"] = dv

    hs = e.get("http_status")
    if hs is not None:
        if isinstance(hs, bool) or not isinstance(hs, int) or not 100 <= hs <= 599:
            return None, "invalid_http_status"
    row["http_status"] = hs

    for f in BOOL_FIELDS:
        v, ok = _clean_bool(e.get(f))
        if not ok:
            return None, f"invalid_{f}"
        row[f] = v

    v, ok = _clean_num(e.get("manifest_refresh_hint"))
    if not ok:
        return None, "invalid_manifest_refresh_hint"
    row["manifest_refresh_hint"] = v

    return row, None
