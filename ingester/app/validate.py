"""Per-event validation. Returns (row | None, quarantine_reason | None).

Never raises: every malformed field is converted into a quarantine reason.
Unknown event_type is *not* quarantined (round-2 resolution) — it is stored in
`events` with typed columns NULL and `raw` filled, so future senders (media
proxy) are queryable from day one. Missing channel_id is allowed through as
NULL (doc §3: error events may omit it).

Round-3 hardening (NaN/Infinity/future ts, NUL/surrogates, event_type cap).

Round-4 hardening (review blocker 1 — "validate_event can still raise"):
Python's JSON parser accepts integers of arbitrary size and structures of
arbitrary depth, and both blow up INSIDE this module, not at the bad field:

* ``math.isfinite(10**400)`` and ``float(10**400)`` raise OverflowError on a
  400-digit integer ts (the review's example). Ints are therefore compared as
  INTs against TS_HARD_MAX before any float conversion — int comparison is
  exact and cannot raise.
* A deeply nested event raises RecursionError in ``_sanitize_obj`` (and again
  in ``json.dumps`` for the jsonb INSERT). ``_sanitize_obj`` now enforces
  MAX_JSON_DEPTH and signals via _RawTooDeep; the event is quarantined whole
  as ``raw_too_deep``.
* The caller (ingester) additionally wraps every call in try/except and
  quarantines as ``validator_error`` — the contract is "one bad event is one
  quarantine row", and the bug list must not be the only thing standing
  between a weird event and a 503 (at-most-once => a dropped batch).
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

# Absolute sanity bound for ts (epoch seconds; 1e12 ≈ year 33 654). Anything
# at/above is poison by definition — rollup's SQL side uses the same cap as
# its range predicate. Deliberately an INT: comparing a 400-digit int against
# it is exact and cannot raise, while float() on such an int raises
# OverflowError. Single source of truth — worker.rollup imports it.
TS_HARD_MAX = 10**12

# json.loads parses arbitrarily deep structures; _sanitize_obj and json.dumps
# (the jsonb INSERT) recurse once per level and die with RecursionError near
# ~1000. Real events are <10 levels deep; 64 is generous and far from the
# limit, and unwinds long before a Python stack overflow.
MAX_JSON_DEPTH = 64

# str.translate table: every lone surrogate -> U+FFFD. Python's
# errors="replace" on ENCODE emits "?"; we want the replacement character so
# the damage is visible, greppable and round-trippable.
_SURROGATE_TABLE = dict.fromkeys(range(0xD800, 0xE000), 0xFFFD)


class _RawTooDeep(Exception):
    """Raised by _sanitize_obj when the structure exceeds MAX_JSON_DEPTH."""


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


def _sanitize_obj(o: Any, top: str | None, depth: int = 0):
    """Recursively sanitize an event for jsonb storage.

    Returns (obj, touched) where ``touched`` is the set of *top-level* keys
    whose subtree needed cleaning. Unknown/nested keys and dirty keys
    themselves are attributed to "raw" so the metrics label stays bounded.

    Raises _RawTooDeep beyond MAX_JSON_DEPTH (round-4: the recursion here, or
    the one in json.dumps later, would otherwise die with RecursionError —
    which escapes as a 503 and drops the whole batch).
    """
    if depth > MAX_JSON_DEPTH:
        raise _RawTooDeep()
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
            v2, t = _sanitize_obj(v, k, depth + 1)
            out[k2] = v2
            touched |= t
        return out, touched
    if isinstance(o, list):
        out = []
        touched: set = set()
        for v in o:
            v2, t = _sanitize_obj(v, top, depth + 1)
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


# Largest int that survives float() exactly; also the largest float() will
# accept at all (larger ints raise OverflowError — round-4 blocker 1).
_FLOAT_EXACT_MAX = 2**53


def _clean_num(v: Any) -> tuple[float | None, bool]:
    if v is None:
        return None, True
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None, False
    if isinstance(v, int):
        # json.loads accepts 400-digit ints; float() on them raises
        # OverflowError. Compare as int first (exact, never raises).
        if not -_FLOAT_EXACT_MAX <= v <= _FLOAT_EXACT_MAX:
            return None, False
        f = float(v)
    else:
        f = v
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
    try:
        e, touched = _sanitize_obj(e, None)
    except _RawTooDeep:
        return None, "raw_too_deep"

    sv = e.get("schema_version")
    if isinstance(sv, bool) or not isinstance(sv, int):
        return None, "invalid_schema_version"
    if sv != 1:
        return None, "unsupported_schema_version"

    ts = e.get("ts")
    if isinstance(ts, bool) or not isinstance(ts, (int, float)):
        return None, "invalid_ts"
    if isinstance(ts, int):
        # Ints are always finite, but float(10**400) raises OverflowError and
        # so does math.isfinite(10**400) (round-4 blocker 1: the validator
        # itself must never raise). Compare as INT — exact, unraisable —
        # against the same hard cap rollup's range predicate uses.
        if ts <= 0:
            return None, "invalid_ts"
        if ts >= TS_HARD_MAX:  # year 33 654+ is poison, not skew
            return None, "ts_in_future"
    elif not math.isfinite(ts) or ts <= 0:
        return None, "invalid_ts"
    # int-vs-float comparison is exact and cannot raise, for any int size.
    if ts > time.time() + MAX_TS_FUTURE_S:
        return None, "ts_in_future"

    et = e.get("event_type")
    if not isinstance(et, str) or not et:
        return None, "missing_event_type"
    if len(et) > CAPS["event_type"]:
        return None, "event_type_too_long"

    # Safe now: ts is a float or an int < 2**53.
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
