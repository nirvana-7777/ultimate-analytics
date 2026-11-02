"""Shared fixtures for worker tests: direct event insertion and thin wrappers
around the worker functions (mirroring one tick's phases)."""
import psycopg
from psycopg.types.json import Jsonb


def insert_event(cur, ts, ip="203.0.113.1", channel="ch1", ct="channel", provider="p1",
                 delivery="proxied", country="DE", status=200, event_type="manifest_request",
                 received_at=None):
    if received_at is None:
        cur.execute(
            """INSERT INTO events
               (ts, event_type, schema_version, client_ip, provider, channel_id,
                content_type, http_status, delivery, user_agent, country, raw)
               VALUES (%s, %s, 1, %s, %s, %s, %s, %s, %s, 'test-ua/1', %s, %s)""",
            (ts, event_type, ip, provider, channel, ct, status, delivery, country,
             Jsonb({"ts": ts, "channel_id": channel, "event_type": event_type})),
        )
    else:
        cur.execute(
            """INSERT INTO events
               (received_at, ts, event_type, schema_version, client_ip, provider,
                channel_id, content_type, http_status, delivery, user_agent, country, raw)
               VALUES (to_timestamp(%s), %s, %s, 1, %s, %s, %s, %s, %s, %s,
                       'test-ua/1', %s, %s)""",
            (received_at, ts, event_type, ip, provider, channel, ct, status, delivery,
             country, Jsonb({"ts": ts, "channel_id": channel, "event_type": event_type})),
        )


def rebuild(conn, cfg, last_run_start):
    from worker import sessionizer
    with conn.cursor() as cur:
        return sessionizer.rebuild_dirty(cur, cfg, last_run_start)


def close_stale(conn, cfg):
    from worker import sessionizer
    with conn.cursor() as cur:
        return sessionizer.close_stale(cur, cfg)


def rollup_touched(conn, cfg, rewritten, deleted_starts, last_run_start):
    from worker import rollup
    with conn.cursor() as cur:
        rollup.recompute_touched(cur, cfg, rewritten, deleted_starts, last_run_start)


def one(conn, sql, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()
