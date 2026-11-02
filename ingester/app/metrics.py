from prometheus_client import Counter, Gauge, Histogram

EVENTS_RECEIVED = Counter(
    "analytics_events_received_total", "events accepted into DB", ["sender"])
EVENTS_QUARANTINED = Counter(
    "analytics_events_quarantined_total", "events rejected to quarantine", ["reason"])
EVENTS_UNKNOWN_TYPE = Counter(
    "analytics_events_unknown_type_total", "events with unknown event_type", ["event_type"])
EVENTS_LATE = Counter(
    "analytics_events_late_total",
    "events older than ANALYTICS_MAX_EVENTS_AGE_S at ingest (ts regression / stale sender)")
INGEST_REQUESTS = Counter(
    "analytics_ingest_requests_total", "ingest requests", ["status"])
INGEST_DURATION = Histogram(
    "analytics_ingest_request_duration_seconds", "handler duration", ["status"])
LAST_EVENT_TS = Gauge(
    "analytics_last_event_ts",
    "unix ts of last accepted event (receive-time, NOT backend ts)", ["sender"])
INGEST_LAG = Histogram(
    "analytics_ingest_lag_seconds",
    "received_at - event.ts at ingest; steady skew alarm", ["sender"],
    buckets=(1, 5, 30, 60, 120, 300, 600, 1200, 3600, 21600))
ROLLUP_DROPPED_LATE = Counter(
    "analytics_rollup_dropped_late_total",
    "rollup buckets dropped (ts older than max age, or over the cap)")
WORKER_TICK = Counter(
    "analytics_worker_tick_total", "worker ticks", ["phase"])
WORKER_LAG = Gauge(
    "analytics_worker_lag_seconds", "seconds since last successful tick", ["phase"])
