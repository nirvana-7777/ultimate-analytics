"""Daily partitions for events; retention = DROP PARTITION, not DELETE.

Round-3 review (blocker 4), empirically confirmed on postgres 16:
  * CREATE TABLE ... PARTITION OF takes an ACCESS EXCLUSIVE lock on the
    *parent* `events` — held until the creating transaction commits. When it
    ran inside the (long) worker tick, ingest INSERTs queued behind it and
    burned their 1.5 s statement_timeout: a daily 503 burst at minimum.
  * DROP TABLE of a retention partition takes AEL on parent AND partition —
    same effect.
  * CREATE TABLE IF NOT EXISTS for a partition that already exists takes NO
    heavy lock, so the every-tick path is cheap; only the *daily* create and
    the *daily* drop are hot.

=> ensure_ahead() and prune() now run in the worker's short DDL-only
transaction with lock_timeout=ANALYTICS_DDL_LOCK_TIMEOUT_MS: they either win
fast or bail out and retry next tick, never blocking ingest for long.

The DEFAULT partition is the safety net for "worker down > 2 days": ingest
keeps working (rows land in events_default) and the
analytics_events_in_default gauge pages. Creating a partition whose range
still has rows in DEFAULT fails with a partition-constraint violation, so
_ensure_one() moves them out first (only happens during recovery).
"""
import datetime as dt

import psycopg


def _ensure_one(cur, name: str, d: dt.date, end: dt.date) -> None:
    try:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {name} PARTITION OF events "
            f"FOR VALUES FROM ('{d.isoformat()}') TO ('{end.isoformat()}')"
        )
    except psycopg.errors.CheckViolation:
        # Rows landed in DEFAULT while the worker was down; Postgres refuses
        # to create a partition whose range they overlap. Rescue them.
        # (explicit DROP both sides: works in autocommit too, and a failed
        # rescue must not leave a stale temp table on a pooled connection)
        cur.execute("DROP TABLE IF EXISTS pg_temp.ev_rescue")
        cur.execute(
            "CREATE TEMP TABLE ev_rescue AS "
            "SELECT * FROM events_default "
            "WHERE received_at >= %s AND received_at < %s",
            (d, end),
        )
        cur.execute(
            "DELETE FROM events_default "
            "WHERE received_at >= %s AND received_at < %s",
            (d, end),
        )
        cur.execute(
            f"CREATE TABLE {name} PARTITION OF events "
            f"FOR VALUES FROM ('{d.isoformat()}') TO ('{end.isoformat()}')"
        )
        # Re-insert through the parent so rows land in the new partition
        # (ids are preserved; the parent sequence was already advanced when
        # they first arrived, so no collision with future inserts).
        cur.execute("INSERT INTO events SELECT * FROM ev_rescue")
        cur.execute("DROP TABLE IF EXISTS pg_temp.ev_rescue")


def ensure_ahead(cur, ahead_days: int = 2) -> None:
    # -1 .. +ahead: yesterday (clock-safety) through two days ahead.
    cur.execute(
        "SELECT date_trunc('day', now())::date + generate_series(-1, %s) AS d",
        (ahead_days,),
    )
    for (d,) in cur.fetchall():
        name = f"events_{d.strftime('%Y%m%d')}"
        _ensure_one(cur, name, d, d + dt.timedelta(days=1))


def count_default(cur) -> int:
    """Rows parked in the DEFAULT partition (= partition maintenance failed).
    Instant while empty (normal state) — it has an index from the PK."""
    cur.execute("SELECT count(*) FROM events_default")
    return cur.fetchone()[0]


def prune(cur, cfg) -> None:
    # String comparison works: partition names are events_YYYYMMDD.
    cur.execute("""
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = 'events'
          AND c.relname < 'events_' || to_char(now() - make_interval(days => %s), 'YYYYMMDD')
    """, (cfg.raw_retention_days,))
    for (name,) in cur.fetchall():
        cur.execute(f'DROP TABLE IF EXISTS "{name}"')
    cur.execute(
        "DELETE FROM quarantine WHERE received_at < now() - make_interval(days => %s)",
        (cfg.quarantine_retention_days,),
    )
