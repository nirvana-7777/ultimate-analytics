from prometheus_client import Counter, Gauge, Histogram

EVENTS_RECEIVED = Counter(
    "analytics_events_received_total", "events accepted into DB", ["sender"])
EVENTS_QUARANTINED = Counter(
    "analytics_events_quarantined_total", "events rejected to quarantine", ["reason"])
EVENTS_SANITIZED = Counter(
    "analytics_events_sanitized_total",
    "events whose strings/floats needed Postgres sanitizing "
    "(NUL/surrogate stripped, NaN/Infinity stringified) before insert",
    ["field"])  # bounded: known schema fields + "raw"
EVENTS_UNKNOWN_TYPE = Counter(
    "analytics_events_unknown_type_total", "events with unknown event_type", ["event_type"])
EVENTS_LATE = Counter(
    "analytics_events_late_total",
    "events older than ANALYTICS_MAX_EVENTS_AGE_S at ingest (ts regression / stale sender)")
INGEST_REQUESTS = Counter(
    "analytics_ingest_requests_total", "ingest requests", ["status"])
INGEST_DURATION = Histogram(
    "analytics_ingest_request_duration_seconds", "handler duration", ["status"])
INGEST_BATCH_FALLBACK = Counter(
    "analytics_ingest_batch_fallback_total",
    "bulk event inserts that failed and were re-run per-event "
    "(expected ~0 after sanitizer; >0 means an unknown poison reached the DB)")
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
ROLLUP_BUCKET_ERRORS = Counter(
    "analytics_rollup_bucket_errors_total",
    "rollup buckets skipped after a per-bucket DB error (poison isolation)")
SESSION_INSERT_ERRORS = Counter(
    "analytics_session_insert_errors_total",
    "session inserts that failed in bulk and were retried per-row")
EVENTS_IN_DEFAULT = Gauge(
    "analytics_events_in_default",
    "rows parked in the events DEFAULT partition — >0 means partition "
    "maintenance failed/worker was down; ingest still works, fix the worker")
WORKER_TICK = Counter(
    "analytics_worker_tick_total", "worker phase outcomes",
    ["phase", "status"])  # phase: ddl|core|gap  status: ok|fail|skip
WORKER_LAG = Gauge(
    "analytics_worker_lag_seconds", "seconds since last successful core tick", ["phase"])
