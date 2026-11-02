"""Worker tick — review round-3 restructure (blocker 2 + advisory-lock fix).

The tick is no longer ONE transaction wearing the ingester pool's 1.5 s
statement_timeout. Three phases, each with its own budget and blast radius:

  ddl    short transaction, lock_timeout=ANALYTICS_DDL_LOCK_TIMEOUT_MS,
         isolated try/except (blocker 4: partition DDL takes AccessExclusive
         on `events`; inside the old long tick it stalled every ingest insert)
  core   ONE transaction guarded by pg_try_advisory_xact_lock:
         sessionize + close + rollup, commit together. xact-lock (not session
         lock) so a failed tick cannot poison the pooled connection with a
         lock it never releases, and no `finally: unlock` can mask the real
         error on an aborted transaction (both reproduced on PG16).
  gap    own transaction + own try/except: a day-scan with a window function
         and percentile_disc must never roll back the core work, and must not
         re-run on every restart just because a Python global reset.

Anchors ("last_run_start") live in worker_state and advance in the same
transaction as the work they guard. A failed tick therefore re-processes
exactly its window (everything is idempotent), a restart resumes from the
last successful tick instead of rewinding a full hour, and a poisoned event
can no longer wedge the worker: its discovery window never grows.
"""
import logging
import time

from prometheus_client import start_http_server

from app import metrics
from app.config import load
from app.db import make_pool, migrate, wait_for_pg
from app.partitions import count_default, ensure_ahead, prune
from worker import gap_histogram, rollup, sessionizer

log = logging.getLogger("worker")
WORKER_LOCK_KEY = 0x5E55_AA02
CORE_KEY = "sessionize_last_run_start"
GAP_KEY = "gap_hist_last_run"


def _load_anchor(pool, key: str, default: float) -> float:
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT value FROM worker_state WHERE key = %s", (key,))
        r = cur.fetchone()
    v = float(r[0]) if r else default
    return v if v > 0 else default


def _save_anchor(cur, key: str, value: float) -> None:
    cur.execute(
        """
        INSERT INTO worker_state (key, value, updated_at)
        VALUES (%s, %s, now())
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
        """,
        (key, value),
    )


def _phase_ddl(pool, cfg) -> bool:
    try:
        with pool.connection() as conn, conn.cursor() as cur:
            # int() makes the f-string safe; SET cannot take bind parameters
            # (psycopg3 server-side binding -> "syntax error at or near $1").
            cur.execute(f"SET LOCAL lock_timeout = {int(cfg.ddl_lock_timeout_ms)}")
            ensure_ahead(cur)
            prune(cur, cfg)
            parked = count_default(cur)
        metrics.EVENTS_IN_DEFAULT.set(parked)
        metrics.WORKER_TICK.labels(phase="ddl", status="ok").inc()
        return True
    except Exception:
        # Not fatal: partitions are created days ahead. But page-worthy:
        # if this keeps failing, ingest eventually hits a missing partition.
        log.exception("ddl phase failed; will retry next tick")
        metrics.WORKER_TICK.labels(phase="ddl", status="fail").inc()
        return False


def _phase_core(pool, cfg, since: float) -> tuple[bool, float]:
    """Returns (success, anchor_to_use_next_tick).

    On failure the anchor stays put: the next tick re-processes exactly the
    same window (idempotent), so a poisoned batch cannot wedge the worker —
    its discovery window never grows (the round-3 wedge mechanism).
    """
    try:
        with pool.connection() as conn, conn.cursor() as cur:
            locked = cur.execute(
                "SELECT pg_try_advisory_xact_lock(%s)", (WORKER_LOCK_KEY,)
            ).fetchone()[0]
            if not locked:
                metrics.WORKER_TICK.labels(phase="core", status="skip").inc()
                log.info("another worker holds the tick lock; skipping")
                return False, since
            rewritten, deleted = sessionizer.rebuild_dirty(cur, cfg, since)
            sessionizer.close_stale(cur, cfg)
            rollup.recompute_touched(cur, cfg, rewritten, deleted, since)
            _save_anchor(cur, CORE_KEY, time.time())
    except Exception:
        log.exception("core phase failed; window will be re-processed")
        metrics.WORKER_TICK.labels(phase="core", status="fail").inc()
        return False, since
    metrics.WORKER_TICK.labels(phase="core", status="ok").inc()
    return True, time.time()


def _phase_gap(pool, cfg, last_run: float) -> float:
    if time.time() - last_run < gap_histogram.INTERVAL_S:
        return last_run
    try:
        with pool.connection() as conn, conn.cursor() as cur:
            gap_histogram.update(cur, cfg)
            _save_anchor(cur, GAP_KEY, time.time())
    except Exception:
        log.exception("gap histogram failed (isolated; core work unaffected)")
        metrics.WORKER_TICK.labels(phase="gap", status="fail").inc()
        return last_run
    metrics.WORKER_TICK.labels(phase="gap", status="ok").inc()
    return time.time()


def main():
    cfg = load()
    logging.basicConfig(level=cfg.log_level.upper(),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    wait_for_pg(cfg.database_url)
    migrate(cfg.database_url)  # idempotent; the migrator service normally did it
    pool = make_pool(
        cfg.database_url,
        statement_timeout_ms=cfg.worker_statement_timeout_ms,
        # A wedged tick must not hold locks/connections forever either.
        idle_in_transaction_timeout_ms=10 * 60_000,
        name="analytics-worker-pool",
        max_size=2,
    )
    start_http_server(9100)
    log.info("worker started (tick=%ss gap=%ss rw=%ss)",
             cfg.worker_tick_s, cfg.session_gap_s, cfg.rebuild_window_s)

    # Resume, don't rewind: first boot has no anchor and rewinds one hour
    # (self-healing under the rollup age cap); a restart picks up the
    # persisted value instead of re-bursting.
    core_anchor = _load_anchor(pool, CORE_KEY, default=time.time() - 3600.0)
    gap_anchor = _load_anchor(pool, GAP_KEY, default=0.0)
    last_success = time.time()

    while True:
        t0 = time.time()
        _phase_ddl(pool, cfg)
        success, core_anchor = _phase_core(pool, cfg, core_anchor)
        if success:
            last_success = time.time()
        gap_anchor = _phase_gap(pool, cfg, gap_anchor)
        metrics.WORKER_LAG.labels(phase="tick").set(time.time() - last_success)
        time.sleep(max(0.0, cfg.worker_tick_s - (time.time() - t0)))


if __name__ == "__main__":
    main()
