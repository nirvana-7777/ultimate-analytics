"""Per-event validation. Returns (row | None, quarantine_reason | None).

Never raises: every malformed field is converted into a quarantine reason.
Unknown event_type is *not* quarantined (round-2 resolution) — it is stored in
`events` with typed columns NULL and `raw` filled, so future senders (media
proxy) are queryable from day one.

Missing channel_id is allowed through as NULL (doc §3: error events may omit
it). They stay in error dashboards; the sessionizer skips them.
"""
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
}
CONTENT_TYPES = {"channel", "event", "vod", "recording"}
DELIVERIES = {"redirect", "proxied", "error"}
BOOL_FIELDS = ("is_catchup", "client_drm", "no_proxy", "highest_quality_only")


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
    return float(v), True


def validate_event(e: Any) -> tuple[dict | None, str | None]:
    if not isinstance(e, dict):
        return None, "event_not_object"

    sv = e.get("schema_version")
    if isinstance(sv, bool) or not isinstance(sv, int):
        return None, "invalid_schema_version"
    if sv != 1:
        return None, "unsupported_schema_version"

    ts = e.get("ts")
    if isinstance(ts, bool) or not isinstance(ts, (int, float)) or ts <= 0:
        return None, "invalid_ts"

    et = e.get("event_type")
    if not isinstance(et, str) or not et:
        return None, "missing_event_type"

    row: dict = {"ts": float(ts), "event_type": et, "schema_version": sv, "raw": e}

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
