"""Panel SQL is code: every postgres panel's query runs against a real
database here, with the Grafana macros substituted. Plus: the committed
analytics.json must be in sync with tools/panels.py (anti-drift).
"""
import datetime as dt
import json
import pathlib
import re
import sys
import time

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
for p in (str(REPO), str(REPO / "ingester")):
    if p not in sys.path:
        sys.path.insert(0, p)

from tools.panels import PANELS  # noqa: E402


def _ts_literal(epoch: int) -> str:
    """Timestamp literal as Grafana expands $__timeFrom() / $__timeTo()."""
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).strftime("'%Y-%m-%dT%H:%M:%SZ'")


def render(sql: str) -> str:
    now = int(time.time())
    frm = now - 3600
    # $__timeGroupAlias(col, $__interval) -> floor(epoch / step) * step AS "time"
    # (the panels set a 1m minimum interval, so the step is 60s here)
    sql = re.sub(r"\$__timeGroupAlias\((\w+),\s*\$__interval\)",
                 r'floor(extract(epoch FROM \1) / 60) * 60 AS "time"', sql)
    return (
        sql
        .replace("$__timeFilter(bucket)",
                 f"bucket > to_timestamp({frm}) AND bucket < to_timestamp({now})")
        .replace("$__unixEpochFilter(started_at)",
                 f"started_at > {frm} AND started_at < {now}")
        .replace("$__unixEpochFilter(ts)",
                 f"ts > {frm} AND ts < {now}")
        .replace("$__unixEpochTo()", str(now))
        .replace("$__timeFrom()", _ts_literal(frm))
        .replace("$__timeTo()", _ts_literal(now))
        .replace("$__interval_ms", "60000")
    )


def test_no_leftover_macros():
    for p in PANELS:
        if p["ds"] != "postgres":
            continue
        rendered = render(p["sql"])
        assert "$__" not in rendered, f"{p['title']}: unhandled macro"


def test_panel_sql_executes(conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO sessions
              (client_ip, provider, content_type, channel_id, delivery, country,
               started_at, ended_at, duration_s, event_count, closed)
            VALUES ('203.0.113.1', 'edge-a', 'channel', 'ch1', 'proxied', 'DE',
                    extract(epoch from now()) - 600, extract(epoch from now()) - 60,
                    540, 10, false)
        """)
        cur.execute("""
            INSERT INTO rollup_1m (bucket, provider, channel_id, content_type,
                                   delivery, country, views, sessions, events, errors)
            VALUES (date_trunc('minute', now()), 'edge-a', 'ch1', 'channel',
                    'proxied', 'DE', 1, 1, 10, 0)
        """)
        cur.execute("""
            INSERT INTO events (ts, event_type, schema_version, client_ip, provider,
                                channel_id, content_type, http_status, delivery, raw)
            VALUES (extract(epoch from now()), 'manifest_request', 1, '203.0.113.1',
                    'edge-a', 'ch1', 'channel', 503, 'error', '{}')
        """)

    executed = 0
    for p in PANELS:
        if p["ds"] != "postgres":
            continue
        with conn.cursor() as cur:
            cur.execute(render(p["sql"]))
            cur.fetchall()
            cols = [c.name for c in cur.description]
        if p["type"] == "timeseries":
            assert "time" in cols, f"{p['title']}: timeseries needs a 'time' column"
        executed += 1
    assert executed >= 10


def test_dashboard_json_in_sync():
    from tools.build_dashboard import build
    path = REPO / "grafana" / "provisioning" / "dashboards" / "analytics.json"
    on_disk = json.loads(path.read_text())
    fresh = build()
    assert on_disk["uid"] == fresh["uid"]
    assert [p["title"] for p in on_disk["panels"]] == [p["title"] for p in fresh["panels"]], (
        "grafana/provisioning/dashboards/analytics.json is stale — "
        "run: python tools/build_dashboard.py"
    )
