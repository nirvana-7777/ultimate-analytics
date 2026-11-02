"""Pool + migrate-on-boot with an advisory lock (race-free under the
ingester/worker/migrator all calling it at boot)."""
import os
import sys
import time
from pathlib import Path

import psycopg
from psycopg_pool import ConnectionPool

# In the image the repo root is /srv, so this resolves to /srv/migrations.
# Locally / in tests, set ANALYTICS_MIGRATIONS_DIR explicitly.
MIGRATIONS_DIR = Path(os.environ.get(
    "ANALYTICS_MIGRATIONS_DIR",
    str(Path(__file__).resolve().parent.parent / "migrations"),
))
MIGRATOR_LOCK_KEY = 0x5E55_AA01  # arbitrary but stable

# Round-4 review, blocker 5: the ONLY exceptions that mean "this ROW is
# poison" and may be contained by a per-row/per-bucket fallback.
#   DataError       — SQLSTATE class 22: invalid text representation, NUL in
#                     text, numeric out of range, bad jsonb, ...
#   IntegrityError  — SQLSTATE class 23: CHECK / UNIQUE / FK violations.
# Everything else is environmental and MUST propagate:
#   QueryCanceled   — statement timeout (subclass of OperationalError): a
#                     timed-out bulk insert must not "fall back per row" and
#                     quarantine rows that never got a fair attempt;
#   OperationalError — dead/reset connection: the fallback rows would all
#                     fail and be silently dropped or skipped;
#   RaiseException / ProgrammingError — a trigger or a code bug: rolling the
#                     tick back (or answering 503) is what makes it visible.
POISON_ERRORS = (psycopg.DataError, psycopg.IntegrityError)


def make_pool(
    dsn: str,
    *,
    statement_timeout_ms: int = 1500,
    idle_in_transaction_timeout_ms: int | None = None,
    name: str = "analytics-pool",
    max_size: int = 8,
) -> ConnectionPool:
    """connect_timeout bounds connect (hung PG); statement_timeout bounds each
    statement so a wedged query cannot pin the (small) sync pool forever.

    The worker passes its own budgets: it runs day-scans (gap histogram) and
    multi-bucket rollups that must never wear the ingester's 1.5 s timeout
    (review blocker 2 — previously every tick rolled back at volume, and with
    a watermark that only advances on success, the window kept growing)."""
    options = f"-c statement_timeout={statement_timeout_ms}"
    if idle_in_transaction_timeout_ms:
        options += f" -c idle_in_transaction_session_timeout={idle_in_transaction_timeout_ms}"
    return ConnectionPool(
        conninfo=dsn,
        min_size=1,
        max_size=max_size,
        timeout=2.0,
        kwargs={"connect_timeout": 1, "options": options},
        name=name,
        open=True,
    )


def migrate(dsn: str) -> None:
    """Idempotent; safe under concurrent callers. Session-level advisory lock:
    the second caller blocks, then sees the applied versions and no-ops.

    The unlock is wrapped because a *connection-level* failure (PG restarted
    mid-migration) would otherwise replace the real exception; the session
    lock is released anyway when this connection closes.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (MIGRATOR_LOCK_KEY,))
            try:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS schema_version (
                        version int PRIMARY KEY,
                        applied_at timestamptz NOT NULL DEFAULT now()
                    )
                """)
                cur.execute("SELECT version FROM schema_version")
                applied = {r[0] for r in cur.fetchall()}
                for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
                    version = int(path.stem.split("_", 1)[0])
                    if version in applied:
                        continue
                    print(f"applying migration {path.name}", file=sys.stderr)
                    with conn.transaction():
                        with conn.cursor() as mcur:
                            # No params -> simple protocol -> multiple statements OK.
                            mcur.execute(path.read_text())
                            mcur.execute(
                                "INSERT INTO schema_version(version) VALUES (%s)",
                                (version,),
                            )
            finally:
                try:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (MIGRATOR_LOCK_KEY,))
                except Exception:
                    # Unwritable connection: close() below releases the lock.
                    pass


def wait_for_pg(dsn: str, attempts: int = 30) -> None:
    for _ in range(attempts):
        try:
            with psycopg.connect(dsn, connect_timeout=1) as c:
                c.execute("SELECT 1")
            return
        except Exception:
            time.sleep(1)
    raise SystemExit("postgres not reachable")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "migrate":
        dsn = os.environ["ANALYTICS_DATABASE_URL"]
        wait_for_pg(dsn)
        migrate(dsn)
    else:
        raise SystemExit(f"unknown command: {cmd}")
