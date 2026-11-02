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


def make_pool(dsn: str) -> ConnectionPool:
    # connect_timeout bounds connect (hung PG); statement_timeout bounds each
    # statement so a wedged query cannot pin the (small) sync pool forever.
    return ConnectionPool(
        conninfo=dsn,
        min_size=1,
        max_size=8,
        timeout=2.0,
        kwargs={"connect_timeout": 1, "options": "-c statement_timeout=1500"},
        name="analytics-pool",
        open=True,
    )


def migrate(dsn: str) -> None:
    """Idempotent; safe under concurrent callers. Session-level advisory lock:
    the second caller blocks, then sees the applied versions and no-ops."""
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
                cur.execute("SELECT pg_advisory_unlock(%s)", (MIGRATOR_LOCK_KEY,))


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
