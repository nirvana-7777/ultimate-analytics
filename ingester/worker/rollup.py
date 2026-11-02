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
"""
from app import metrics

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

    # Buckets touched by freshly-received events (received_at partition-prunes).
    cur.execute(
        "SELECT DISTINCT ts FROM events WHERE received_at >= to_timestamp(%s)",
        (last_run_start - 5.0,),
    )
    buckets: set[int] = set()
    dropped = 0
    for (ts,) in cur.fetchall():
        ts = float(ts)
        if ts < min_ts:
            dropped += 1
        else:
            buckets.add(_bucket(ts))

    # Buckets whose session set changed (inserts AND the starts deleted away).
    for s in list(rewritten) + list(deleted_starts):
        if s < min_ts:
            dropped += 1
        else:
            buckets.add(_bucket(s))

    if len(buckets) > cfg.rollup_bucket_cap:
        dropped += len(buckets) - cfg.rollup_bucket_cap
        buckets = set(sorted(buckets)[-cfg.rollup_bucket_cap:])
    if dropped:
        metrics.ROLLUP_DROPPED_LATE.inc(dropped)

    for b in sorted(buckets):
        cur.execute("DELETE FROM rollup_1m WHERE bucket = to_timestamp(%s)", (b,))
        cur.execute(INSERT_ROLLUP_SQL, (b, b, b + 60, b, b + 60))
