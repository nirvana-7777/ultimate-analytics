"""Worker tick: ensure partitions -> sessionize -> close -> rollup -> prune ->
gap histogram. Single-tick transaction (pool.connection() commits on exit);
pg_try_advisory_lock makes the implicit single-replica constraint explicit —
a second worker skips instead of corrupting sessions."""
import logging
import time

from prometheus_client import start_http_server

from app import metrics
from app.config import load
from app.db import make_pool, migrate, wait_for_pg
from app.partitions import prune, ensure_ahead
from worker import gap_histogram, rollup, sessionizer

log = logging.getLogger("worker")
WORKER_LOCK_KEY = 0x5E55_AA02


def main():
    cfg = load()
    logging.basicConfig(level=cfg.log_level.upper(),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    wait_for_pg(cfg.database_url)
    migrate(cfg.database_url)  # idempotent; the migrator service normally did it
    pool = make_pool(cfg.database_url)
    start_http_server(9100)
    log.info("worker started (tick=%ss gap=%ss rw=%ss)",
             cfg.worker_tick_s, cfg.session_gap_s, cfg.rebuild_window_s)

    # Cold-start rewind: self-healing under the rollup age cap.
    last_run_start = time.time() - 3600.0
    last_success = time.time()

    while True:
        t0 = time.time()
        try:
            with pool.connection() as conn, conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s)", (WORKER_LOCK_KEY,))
                if not cur.fetchone()[0]:
                    log.info("another worker holds the tick lock; skipping")
                else:
                    try:
                        ensure_ahead(cur)
                        rewritten, deleted_starts = sessionizer.rebuild_dirty(
                            cur, cfg, last_run_start)
                        sessionizer.close_stale(cur, cfg)
                        rollup.recompute_touched(
                            cur, cfg, rewritten, deleted_starts, last_run_start)
                        prune(cur, cfg)
                        gap_histogram.maybe_update(cur, cfg)
                    finally:
                        cur.execute("SELECT pg_advisory_unlock(%s)", (WORKER_LOCK_KEY,))
                    # Advance only after success; a failed tick re-processes
                    # its window — everything here is idempotent by design.
                    last_run_start = t0
                    last_success = time.time()
            metrics.WORKER_TICK.labels(phase="ok").inc()
        except Exception:
            log.exception("worker tick failed")
            metrics.WORKER_TICK.labels(phase="fail").inc()
        metrics.WORKER_LAG.labels(phase="tick").set(time.time() - last_success)
        time.sleep(max(0.0, cfg.worker_tick_s - (time.time() - t0)))


if __name__ == "__main__":
    main()
