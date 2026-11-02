"""1-minute rollups, counts only (duration lives in `sessions`).

Data-driven re-roll (round-2 C fix): recompute every bucket touched by events
RECEIVED since the last tick, plus every bucket of a session inserted or
deleted this run. A steady-skewed backend (ts drifting arbitrarily far from
received_at) is therefore never invisible, and a rewritten session that moved
its started_at also fixes the bucket it left behind.

Per bucket: DELETE then INSERT (idempotent; no ON CONFLICT arithmetic that
has to "lie" about views for proxied). Buckets older than
ANALYTICS_MAX_EVENTS_AGE_S are dropped and counted in
analytics_rollup_dropped_late_total; a hard cap bounds pathological senders.

views definition (design §5):
  redirect: events with delivery='redirect' and http_status < 400
  proxied:  sessions started in the bucket (session.delivery='proxied')
  error:    0
sessions = all sessions started in the bucket.

Round-3 review hardening:
  * bucket discovery is pushed down: SELECT DISTINCT floor(ts/60) in SQL,
    instead of streaming every distinct ts float into Python.
  * the range predicate `ts >= %s AND ts < %s` also filters poison rows
    without touching them: PostgreSQL treats NaN as greater than everything,
    so `NaN < max` is FALSE and NaN never reaches floor(); 1e308 likewise.
    (ingest rejects them and migration 002 scrubs them — this is layer 3.)
  * every bucket runs under its own SAVEPOINT: one data-poisoned bucket is
    counted in analytics_rollup_bucket_errors_total and skipped, not allowed
    to abort the whole core transaction (review blocker 1's wedge shape).

Round-4 review (blocker 5): the per-bucket catch is POISON_ERRORS only
(DataError/IntegrityError). A timeout, connection error or trigger bug
propagates, the whole core transaction rolls back and the anchor does NOT
advance — the old broad `except psycopg.Error` skipped the bucket, kept its
stale data, and counted the tick "ok" while the anchor moved past it.
"""
import logging
import math

from app import metrics
from app.config import DISCOVERY_OVERLAP_S
from app.db import POISON_ERRORS
# TS_HARD_MAX is imported from app.validate (single source of truth: ingest
# rejects at/above it as ts_in_future; the range predicates below are the
# DB-side layer of the same rule). It is re-exported here for
# worker.sessionizer and the tests.
from app.validate import TS_HARD_MAX  # noqa: F401

log = logging.getLogger("worker.rollup")

INSERT_ROLLUP_SQL = """
    INSERT INTO rollup_1m
      (bucket, provider, channel_id, content_type, delivery, country,
       views, sessions, events, errors)
    SELECT to_timestamp(%s),
           COALESCE(ev.provider, sess.provider),
           COALESCE(ev.channel_id, sess.channel_id),
           COALESCE(ev.content_type, sess.content_type),
           COALESCE(ev.delivery, sess.delivery),
           COALESCE(ev.country, sess.country),
           COALESCE(ev.redirect_views, 0) + COALESCE(sess.proxied_sessions, 0),
           COALESCE(sess.sessions, 0),
           COALESCE(ev.events, 0),
           COALESCE(ev.errors, 0)
    FROM (
      SELECT COALESCE(provider, '') AS provider,
             COALESCE(channel_id, '') AS channel_id,
             COALESCE(content_type, '') AS content_type,
             COALESCE(delivery, '') AS delivery,
             COALESCE(country, '') AS country,
             count(*) AS events,
             count(*) FILTER (WHERE http_status >= 400) AS errors,
             count(*) FILTER (WHERE delivery = 'redirect' AND http_status < 400) AS redirect_views
      FROM events
      WHERE ts >= %s AND ts < %s
      GROUP BY 1,2,3,4,5
    ) ev
    FULL OUTER JOIN (
      SELECT COALESCE(provider, '') AS provider,
             COALESCE(channel_id, '') AS channel_id,
             COALESCE(content_type, '') AS content_type,
             COALESCE(delivery, '') AS delivery,
             COALESCE(country, '') AS country,
             count(*) AS sessions,
             count(*) FILTER (WHERE delivery = 'proxied') AS proxied_sessions
      FROM sessions
      WHERE started_at >= %s AND started_at < %s
      GROUP BY 1,2,3,4,5
    ) sess USING (provider, channel_id, content_type, delivery, country)
"""

def _bucket(ts: float) -> int:
    return int(ts // 60) * 60


def recompute_touched(cur, cfg, rewritten, deleted_starts, last_run_start: float) -> None:
    now_epoch = float(cur.execute("SELECT extract(epoch FROM now())").fetchone()[0])
    min_ts = now_epoch - cfg.max_events_age_s
    dropped = 0

    # Buckets touched by freshly-received events (received_at partition-prunes
    # + events_received_idx). Pushdown: DISTINCT in SQL, bounded by ts range —
    # which also keeps NaN/1e308 out of floor() without raising.
    cur.execute("""
        SELECT DISTINCT floor(ts / 60) AS b
        FROM events
        WHERE received_at >= to_timestamp(%s)
          AND ts >= %s AND ts < %s
    """, (last_run_start - DISCOVERY_OVERLAP_S, min_ts, TS_HARD_MAX))
    buckets: set[int] = {int(b) * 60 for (b,) in cur.fetchall()}

    # Too-old events in the discovery window: dropped by the range predicate
    # above, counted here for the ROLLUP_DROPPED_LATE alarm.
    cur.execute("""
        SELECT count(*) FROM events
        WHERE received_at >= to_timestamp(%s) AND ts < %s
    """, (last_run_start - DISCOVERY_OVERLAP_S, min_ts))
    dropped += cur.fetchone()[0]

    # Buckets whose session set changed (inserts AND the starts deleted away).
    for s in list(rewritten) + list(deleted_starts):
        if not math.isfinite(s):
            dropped += 1
        elif s < min_ts:
            dropped += 1
        else:
            buckets.add(_bucket(s))

    if len(buckets) > cfg.rollup_bucket_cap:
        dropped += len(buckets) - cfg.rollup_bucket_cap
        buckets = set(sorted(buckets)[-cfg.rollup_bucket_cap:])
    if dropped:
        metrics.ROLLUP_DROPPED_LATE.inc(dropped)

    for b in sorted(buckets):
        # One data-poisoned bucket must not abort the core transaction (which
        # holds the sessionize work too). conn.transaction() nests as a
        # SAVEPOINT inside the worker's tick transaction and as BEGIN/COMMIT on
        # an autocommit connection (tests) — one code path, both modes.
        # Round-4 blocker 5: POISON_ERRORS only — a timeout / connection
        # error / trigger bug propagates and rolls back the whole tick, so
        # the anchor cannot advance past a bucket that still holds stale
        # data (the old broad catch skipped it and counted the tick "ok").
        try:
            with cur.connection.transaction():
                cur.execute("DELETE FROM rollup_1m WHERE bucket = to_timestamp(%s)", (b,))
                cur.execute(INSERT_ROLLUP_SQL, (b, b, b + 60, b, b + 60))
        except POISON_ERRORS as e:
            metrics.ROLLUP_BUCKET_ERRORS.inc()
            log.warning("rollup bucket %s skipped after data-level error: %s",
                        b, str(e).splitlines()[0])
