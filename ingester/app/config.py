import os
from dataclasses import dataclass


def _b(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Config:
    database_url: str
    auth_token: str | None
    auth_disabled: bool
    session_gap_s: float
    rebuild_window_s: float
    raw_retention_days: int
    quarantine_retention_days: int
    rollup_bucket_cap: int
    max_events_age_s: float
    worker_tick_s: float
    public_port: int
    internal_port: int
    log_level: str
    # Round-3 review: request/batch bounds and worker pool budgets.
    max_events_per_batch: int
    worker_statement_timeout_ms: int
    ddl_lock_timeout_ms: int


def load() -> Config:
    token = os.environ.get("ANALYTICS_AUTH_TOKEN") or None
    disabled = _b("ANALYTICS_AUTH_DISABLED")
    return Config(
        database_url=os.environ["ANALYTICS_DATABASE_URL"],
        auth_token=None if disabled else token,
        auth_disabled=disabled,
        session_gap_s=float(os.environ.get("ANALYTICS_SESSION_GAP_S", "120")),
        rebuild_window_s=float(os.environ.get("ANALYTICS_REBUILD_WINDOW_S", "900")),
        raw_retention_days=int(os.environ.get("ANALYTICS_RAW_RETENTION_DAYS", "14")),
        quarantine_retention_days=int(os.environ.get("ANALYTICS_QUARANTINE_RETENTION_DAYS", "7")),
        rollup_bucket_cap=int(os.environ.get("ANALYTICS_ROLLUP_BUCKET_CAP", "720")),
        max_events_age_s=float(os.environ.get("ANALYTICS_MAX_EVENTS_AGE_S", "3600")),
        worker_tick_s=float(os.environ.get("ANALYTICS_WORKER_TICK_S", "60")),
        public_port=int(os.environ.get("ANALYTICS_PUBLIC_PORT", "8000")),
        internal_port=int(os.environ.get("ANALYTICS_INTERNAL_PORT", "9000")),
        log_level=os.environ.get("ANALYTICS_LOG_LEVEL", "info"),
        # 1 MB of `{}` is ~350k events -> as many quarantine rows (blocker 5).
        max_events_per_batch=int(os.environ.get("ANALYTICS_MAX_EVENTS_PER_BATCH", "5000")),
        # Worker runs day-scans (gap histogram) that must never wear the
        # ingester's 1.5 s statement_timeout (blocker 2).
        worker_statement_timeout_ms=int(os.environ.get(
            "ANALYTICS_WORKER_STATEMENT_TIMEOUT_MS", "60000")),
        # Partition DDL must never queue behind (or hold up) ingest inserts;
        # bail out fast instead and retry next tick (blocker 4).
        ddl_lock_timeout_ms=int(os.environ.get("ANALYTICS_DDL_LOCK_TIMEOUT_MS", "3000")),
    )
