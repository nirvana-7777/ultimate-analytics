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

Round-4 review (blocker 4): that rescue only ever worked in autocommit (the
old test's mode). In the real worker the CheckViolation ABORTS the outer DDL
transaction, and the very next rescue statement dies with
InFailedSqlTransaction. Every attempt now runs in its own
``connection.transaction()`` block: a SAVEPOINT inside the worker's DDL
transaction, a plain BEGIN/COMMIT on autocommit connections — one code path,
both modes. The rescue also takes ACCESS EXCLUSIVE on `events` FIRST, so a
live ingest transaction cannot park NEW rows in DEFAULT between the DELETE
and the CREATE (which would re-fail the CREATE inside the rescue).
"""
import datetime as dt

import psycopg


def _ensure_one(cur, name: str, d: dt.date, end: dt.date) -> None:
    try:
        with cur.connection.transaction():
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {name} PARTITION OF events "
                f"FOR VALUES FROM ('{d.isoformat()}') TO ('{end.isoformat()}')"
            )
    except psycopg.errors.CheckViolation:
        # Rows landed in DEFAULT while the worker was down; Postgres refuses
        # to create a partition whose range they overlap. Rescue them as ONE
        # atomic block (a failed rescue must not leave rows moved nor a
        # stale temp table behind).
        with cur.connection.transaction():
            # Take the parent exclusively first: without this, an ingest
            # commit between the DELETE and the CREATE could park new rows
            # in DEFAULT in the new range and re-fail the CREATE. Ingest
            # inserts queue behind it for the (short) rescue; lock_timeout
            # (set by the ddl phase) bounds the wait — bail and retry next
            # tick instead of blocking ingest for long.
            cur.execute("LOCK TABLE events IN ACCESS EXCLUSIVE MODE")
            # Explicit DROP on both sides: a failed rescue must not leave a
            # stale temp table on a pooled connection.
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
    """DDL half only: drops expired partitions. Takes ACCESS EXCLUSIVE on
    `events`, so it must stay in the short DDL transaction."""
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


def prune_data(cur, cfg) -> None:
    """Row-level retention. Plain DELETEs take no heavy lock but can run
    long, so they get their own transaction, never the DDL one."""
    cur.execute(
        "DELETE FROM quarantine WHERE received_at < now() - make_interval(days => %s)",
        (cfg.quarantine_retention_days,),
    )
    # Round-4 privacy item: sessions carry client_ip (personal data under
    # GDPR, like events.raw) — bounded retention. The aggregates (rollup_1m,
    # gap_histogram) hold no personal data and are kept indefinitely. Rides
    # sessions_started_idx; only rows that already crossed the age leave.
    cur.execute(
        "DELETE FROM sessions WHERE started_at < extract(epoch FROM now()) - %s",
        (cfg.sessions_retention_days * 86400,),
    )
