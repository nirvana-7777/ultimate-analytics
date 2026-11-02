"""Single source of truth for dashboard panels.

tools/build_dashboard.py generates grafana/provisioning/dashboards/analytics.json
from this list, and tests/panels/test_panel_sql.py executes the same SQL
against a real database — so a query that breaks never ships. CI additionally
fails if the committed analytics.json is out of date.

Macros used (deliberately a small, explicit set):
  $__timeFilter(bucket)        -> bucket BETWEEN '<from>' AND '<to>'
  $__unixEpochFilter(started_at / ts) -> same for epoch columns
  $__unixEpochTo()             -> dashboard "to" as epoch seconds

Panel semantics per the revised design §6:
  * "current watched" reads sessions (fresh, no rollup lag), split by delivery
  * duration reads sessions, never rollup_1m; attribution-at-start disclosed
  * top-* and geo split by delivery; redirect starts are never blended with
    proxied sessions into one number
  * geo is labeled client-declared (query param, NOT geolocation)
"""

DS_PG = {"type": "grafana-postgresql-datasource", "uid": "postgres"}
DS_PROM = {"type": "prometheus", "uid": "prometheus"}

PANELS = [
    dict(
        id=1, title="Current watched channels", type="table", ds="postgres", format="table",
        gridPos={"h": 8, "w": 12, "x": 0, "y": 0},
        description=("Active in last ~3 min, from the sessions table (fresh, no rollup lag). "
                     "Split by delivery — redirect rows are session starts, proxied rows are "
                     "ongoing sessions."),
        sql="""SELECT delivery, provider, content_type, channel_id, count(*) AS active
FROM sessions
WHERE closed = false OR ended_at > $__unixEpochTo() - 180
GROUP BY 1,2,3,4
ORDER BY 5 DESC
LIMIT 20""",
    ),
    dict(
        id=2, title="Views by delivery", type="timeseries", ds="postgres", format="time_series",
        gridPos={"h": 8, "w": 12, "x": 12, "y": 0},
        description=("Redirect = manifest-request events (excluding http_status >= 400); "
                     "proxied = sessions started. Never blended into a single number."),
        sql="""SELECT bucket AS "time", delivery, sum(views) AS views
FROM rollup_1m
WHERE $__timeFilter(bucket)
GROUP BY 1,2
ORDER BY 1""",
    ),
    dict(
        id=3, title="Viewer-seconds (proxied-derived)", type="timeseries", ds="postgres",
        format="time_series", gridPos={"h": 8, "w": 12, "x": 0, "y": 8},
        description=("Redirect sessions contribute 0 — no heartbeat (doc §4). "
                     "Attributed at session start: long sessions spike their start bucket; "
                     "the companion rate panel below corrects that."),
        sql="""SELECT date_trunc('minute', to_timestamp(started_at)) AS "time",
       sum(duration_s) AS viewer_seconds
FROM sessions
WHERE delivery = 'proxied' AND $__unixEpochFilter(started_at)
GROUP BY 1
ORDER BY 1""",
    ),
    dict(
        id=4, title="Concurrent proxied viewers (approx)", type="timeseries", ds="postgres",
        format="time_series", gridPos={"h": 8, "w": 12, "x": 12, "y": 8},
        description=("sum(duration_s)/60 per start-minute — the derived rate metric a 2h "
                     "session should produce, instead of a start-bucket spike."),
        sql="""SELECT date_trunc('minute', to_timestamp(started_at)) AS "time",
       sum(duration_s) / 60.0 AS concurrent_approx
FROM sessions
WHERE delivery = 'proxied' AND $__unixEpochFilter(started_at)
GROUP BY 1
ORDER BY 1""",
    ),
    dict(
        id=5, title="Top channels", type="table", ds="postgres", format="table",
        gridPos={"h": 8, "w": 12, "x": 0, "y": 16},
        description="Split by delivery; delivery='error' rows excluded from most-watched.",
        sql="""SELECT delivery, provider, channel_id, sum(views) AS views
FROM rollup_1m
WHERE $__timeFilter(bucket) AND delivery <> 'error'
GROUP BY 1,2,3
ORDER BY 4 DESC
LIMIT 20""",
    ),
    dict(
        id=6, title="Top providers", type="table", ds="postgres", format="table",
        gridPos={"h": 8, "w": 12, "x": 12, "y": 16},
        description="Split by delivery.",
        sql="""SELECT delivery, provider, sum(views) AS views
FROM rollup_1m
WHERE $__timeFilter(bucket) AND delivery <> 'error'
GROUP BY 1,2
ORDER BY 3 DESC
LIMIT 20""",
    ),
    dict(
        id=7, title="Client-declared country", type="table", ds="postgres", format="table",
        gridPos={"h": 8, "w": 12, "x": 0, "y": 24},
        description=("Query param from the client, NOT geolocation (doc §3). "
                     "Split by delivery."),
        sql="""SELECT delivery, country, sum(views) AS views
FROM rollup_1m
WHERE $__timeFilter(bucket)
GROUP BY 1,2
ORDER BY 3 DESC
LIMIT 20""",
    ),
    dict(
        id=8, title="Sessions started by delivery", type="timeseries", ds="postgres",
        format="time_series", gridPos={"h": 8, "w": 12, "x": 12, "y": 24},
        description="All sessions (any dominant delivery) started per minute.",
        sql="""SELECT bucket AS "time", delivery, sum(sessions) AS sessions
FROM rollup_1m
WHERE $__timeFilter(bucket)
GROUP BY 1,2
ORDER BY 1""",
    ),
    dict(
        id=9, title="Errors", type="timeseries", ds="postgres", format="time_series",
        gridPos={"h": 8, "w": 12, "x": 0, "y": 32},
        description="Events with http_status >= 400 (doc §6).",
        sql="""SELECT bucket AS "time", sum(errors) AS errors
FROM rollup_1m
WHERE $__timeFilter(bucket)
GROUP BY 1
ORDER BY 1""",
    ),
    dict(
        id=10, title="Delivery mix", type="piechart", ds="postgres", format="table",
        gridPos={"h": 8, "w": 12, "x": 12, "y": 32},
        description=("How much traffic is redirect vs proxied vs error — explains duration "
                     "coverage. '' rows are events with unknown delivery (future senders)."),
        sql="""SELECT delivery, sum(events) AS events
FROM rollup_1m
WHERE $__timeFilter(bucket)
GROUP BY 1
ORDER BY 2 DESC""",
    ),
    dict(
        id=11, title="Error detail (raw events)", type="table", ds="postgres", format="table",
        gridPos={"h": 8, "w": 24, "x": 0, "y": 40},
        description="Hits the raw events table — bounded by raw retention.",
        sql="""SELECT to_timestamp(ts) AS time, provider, channel_id, http_status, delivery, country
FROM events
WHERE http_status >= 400 AND $__unixEpochFilter(ts)
ORDER BY ts DESC
LIMIT 100""",
    ),
    dict(
        id=12, title="Events received /s by sender", type="timeseries", ds="prometheus",
        gridPos={"h": 8, "w": 12, "x": 0, "y": 48},
        description="Ingest health. Per-sender requires the backend UA marker (round-1 note).",
        expr='sum by (sender) (rate(analytics_events_received_total[5m]))',
        legend="{{sender}}",
    ),
    dict(
        id=13, title="Seconds since last event", type="timeseries", ds="prometheus",
        gridPos={"h": 8, "w": 12, "x": 12, "y": 48},
        description="Receive-time based. Alert fires at >600s (see prometheus/alerts.yml).",
        expr="time() - analytics_last_event_ts",
        legend="{{sender}}",
    ),
    dict(
        id=14, title="Ingest lag p99 (received_at - ts)", type="timeseries", ds="prometheus",
        gridPos={"h": 8, "w": 12, "x": 0, "y": 56},
        description="Steady-skew alarm; drives REBUILD_WINDOW sizing.",
        expr=("histogram_quantile(0.99, "
              "sum by (sender, le) (rate(analytics_ingest_lag_seconds_bucket[10m])))"),
        legend="{{sender}}",
    ),
    dict(
        id=15, title="Quarantined events /s by reason", type="timeseries", ds="prometheus",
        gridPos={"h": 8, "w": 12, "x": 12, "y": 56},
        description="A schema mismatch or backend regression shows up here first.",
        expr="sum by (reason) (rate(analytics_events_quarantined_total[10m]))",
        legend="{{reason}}",
    ),
]
