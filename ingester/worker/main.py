"""Worker tick — round-3 restructure + round-4 anchor semantics.

The tick is no longer ONE transaction wearing the ingester pool's 1.5 s
statement_timeout. Three phases, each with its own budget and blast radius:

  ddl    short transaction, lock_timeout=ANALYTICS_DDL_LOCK_TIMEOUT_MS,
         isolated try/except (round-3 blocker 4: partition DDL takes
         AccessExclusive on `events`; inside the old long tick it stalled
         every ingest insert). Row retention runs in its own phase.
  core   ONE transaction guarded by pg_try_advisory_xact_lock:
         sessionize + close + rollup, commit together. xact-lock (not session
         lock) so a failed tick cannot poison the pooled connection with a
         lock it never releases, and no `finally: unlock` can mask the real
         error on an aborted transaction (both reproduced on PG16).
  gap    own transaction + own try/except: a day-scan with a window function
         and percentile_disc must never roll back the core work, and must not
         re-run on every restart just because a Python global reset. A FAILED
         day-scan backs off GAP_RETRY_S instead of burning its 60 s statement
         budget on every tick (round-4 should-fix).

Anchor semantics (round-4 blockers 2 + 3):

  * The saved anchor is the DB clock captured at the START of the tick,
    before any discovery query ran. An event whose ingest transaction
    commits mid-tick (after discovery, before our commit) has received_at
    <= that value, so the next tick's `received_at >= anchor - 5 s`
    rediscovers it. (The round-3 regression saved time.time() AFTER the
    work: anything committed while a >5 s tick ran was never rediscovered —
    silently stale sessions and rollup buckets.)
  * The discovery window is clamped to ANALYTICS_MAX_EVENTS_AGE_S: a
    persisted anchor can be arbitrarily old (long outage), and without the
    clamp the first tick would try to process the whole backlog in ONE
    transaction, hit the statement timeout, roll back, and retry the same
    unbounded window forever. Rollup drops anything older anyway; the
    abandoned span is counted in analytics_worker_backlog_abandoned_seconds.
  * The anchor still only advances with the work it guards (same
    transaction): a failed tick re-processes exactly its clamped window
    (idempotent), and a restart resumes from the persisted value.

The per-IP rebuild anchor (how far back a dirty IP's history is fetched) is
NOT clamped: rewinding to the IP's oldest event / oldest open session start
is what keeps a long session from being head-truncated (scenario A3), and
it is bounded by raw retention + the per-IP index.
"""
import logging
import time

from prometheus_client import start_http_server

from app import metrics
from app.config import load
from app.db import make_pool, migrate, wait_for_pg
from app.partitions import count_default, ensure_ahead, prune, prune_data
from worker import gap_histogram, rollup, sessionizer

log = logging.getLogger("worker")
WORKER_LOCK_KEY = 0x5E55_AA02
CORE_KEY = "sessionize_last_run_start"
GAP_KEY = "gap_hist_last_run"

# Backoff after a failed gap-histogram day-scan (round-4 should-fix): the
# 60 s statement budget must not be burned on every tick while the day-scan
# keeps failing (that stretched the tick cadence to ~2 min).
GAP_RETRY_S = 600.0


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


def _db_now_epoch(cur) -> float:
    # now() is transaction-start time: whatever the work below costs, this
    # value stays "before discovery" — and it is the same clock that stamps
    # received_at, so a skewed worker box cannot desynchronize the anchor.
    return float(cur.execute("SELECT extract(epoch FROM now())").fetchone()[0])


def _phase_ddl(pool, cfg) -> bool:
    try:
        with pool.connection() as conn, conn.cursor() as cur:
            # int() makes the f-string safe; SET cannot take bind parameters
            # (psycopg3 server-side binding -> "syntax error at or near $1").
            cur.execute(f"SET LOCAL lock_timeout = {int(cfg.ddl_lock_timeout_ms)}")
            ensure_ahead(cur)
            prune(cur, cfg)
        # The commit above released any ACCESS EXCLUSIVE; anything slow goes below.
        with pool.connection() as conn, conn.cursor() as cur:
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


def _phase_retention(pool, cfg) -> None:
    try:
        with pool.connection() as conn, conn.cursor() as cur:
            prune_data(cur, cfg)
        metrics.WORKER_TICK.labels(phase="retention", status="ok").inc()
    except Exception:
        log.exception("retention phase failed (isolated)")
        metrics.WORKER_TICK.labels(phase="retention", status="fail").inc()


def _phase_core(pool, cfg, since: float) -> tuple[bool, float]:
    """Returns (success, anchor_to_use_next_tick).

    On failure the anchor stays put: the next tick re-processes exactly the
    same (clamped) window — idempotent, and bounded, so a poisoned batch
    cannot wedge the worker (the round-3 wedge mechanism) nor stall recovery
    after an outage (the round-4 one).
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
            # Round-4 blocker 2: capture BEFORE discovery, from the DB clock.
            tick_start = _db_now_epoch(cur)
            # Round-4 blocker 3: clamp the window — a persisted anchor can be
            # arbitrarily old, and one transaction must always be able to
            # finish the (bounded) catch-up window. Rollup drops ts older
            # than max_events_age_s anyway.
            work_since = max(since, tick_start - cfg.max_events_age_s)
            if work_since > since:
                log.warning(
                    "anchor %.0f s behind now; clamping discovery window to "
                    "max_events_age_s=%s s (%.0f s of received_at backlog "
                    "abandoned — rollup drops those buckets anyway)",
                    tick_start - since, cfg.max_events_age_s, work_since - since)
                metrics.WORKER_BACKLOG_ABANDONED.inc(work_since - since)
            rewritten, deleted = sessionizer.rebuild_dirty(cur, cfg, work_since)
            sessionizer.close_stale(cur, cfg)
            rollup.recompute_touched(cur, cfg, rewritten, deleted, work_since)
            # Save the pre-discovery tick start, not "now after the work".
            _save_anchor(cur, CORE_KEY, tick_start)
    except Exception:
        log.exception("core phase failed; window will be re-processed")
        metrics.WORKER_TICK.labels(phase="core", status="fail").inc()
        return False, since
    metrics.WORKER_TICK.labels(phase="core", status="ok").inc()
    return True, tick_start


def _phase_gap(pool, cfg, last_run: float, retry_at: float) -> tuple[float, float]:
    """Returns (last_run, retry_at).

    The cadence anchor only advances on success, and is captured at run
    start from the DB clock (same semantics as the core anchor). On failure
    the next attempt waits GAP_RETRY_S — not the next tick: re-running a
    failing 60 s day-scan every minute stretched the core cadence (round-4
    should-fix).
    """
    now = time.time()
    if now < retry_at or now - last_run < gap_histogram.INTERVAL_S:
        return last_run, retry_at
    try:
        with pool.connection() as conn, conn.cursor() as cur:
            run_start = _db_now_epoch(cur)
            gap_histogram.update(cur, cfg)
            _save_anchor(cur, GAP_KEY, run_start)
    except Exception:
        log.exception("gap histogram failed (isolated; core work unaffected)")
        metrics.WORKER_TICK.labels(phase="gap", status="fail").inc()
        return last_run, now + GAP_RETRY_S
    metrics.WORKER_TICK.labels(phase="gap", status="ok").inc()
    return run_start, 0.0


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

    # Resume, don't rewind: a restart picks up the persisted anchor; first
    # boot rewinds one hour (self-healing under the rollup age cap). Both
    # are additionally clamped to max_events_age_s inside every core tick,
    # so no anchor value can wedge catch-up (round-4 blocker 3).
    core_anchor = _load_anchor(pool, CORE_KEY, default=time.time() - 3600.0)
    gap_anchor = _load_anchor(pool, GAP_KEY, default=0.0)
    gap_retry_at = 0.0
    last_retention = 0.0
    last_success = time.time()

    while True:
        t0 = time.time()
        _phase_ddl(pool, cfg)
        success, core_anchor = _phase_core(pool, cfg, core_anchor)
        if success:
            last_success = time.time()
        gap_anchor, gap_retry_at = _phase_gap(pool, cfg, gap_anchor, gap_retry_at)
        if time.time() - last_retention > 3600:
            _phase_retention(pool, cfg)
            last_retention = time.time()
        metrics.WORKER_LAG.labels(phase="tick").set(time.time() - last_success)
        time.sleep(max(0.0, cfg.worker_tick_s - (time.time() - t0)))


if __name__ == "__main__":
    main()
