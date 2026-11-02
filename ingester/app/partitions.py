"""Daily partitions for events; retention = DROP PARTITION, not DELETE."""
import datetime as dt


def ensure_ahead(cur, ahead_days: int = 2) -> None:
    # -1 .. +ahead: yesterday (clock-safety) through two days ahead.
    cur.execute(
        "SELECT date_trunc('day', now())::date + generate_series(-1, %s) AS d",
        (ahead_days,),
    )
    for (d,) in cur.fetchall():
        name = f"events_{d.strftime('%Y%m%d')}"
        end = d + dt.timedelta(days=1)
        # Partition bounds must be literals; d/end are date objects, safe to interpolate.
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {name} PARTITION OF events "
            f"FOR VALUES FROM ('{d.isoformat()}') TO ('{end.isoformat()}')"
        )


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
