"""Sessionizer. Implements the round-2 A/B/C fixes.

Clocks (one job each — this split is what makes A/B/C hold):
  received_at -> discovery, pruning, partitioning
  ts          -> domain logic (sessionize, bucket attribution)
  now()       -> staleness / close decisions only

Session key: (client_ip, content_type, channel_id) — split on any change or
gap > SESSION_GAP_S. Canonical operation is a scoped rebuild per dirty IP:

  1. discover dirty IPs by received_at since the last tick (partition-prunes)
  2. delete by OVERLAP (closed=false OR ended_at >= now-RW-GAP), not by
     started_at >= window — a long session is always in scope, however old
  3. recompute anchored at LEAST(min deleted started_at, now-RW); on the
     FIRST build for an IP (no prior rows) anchor rewinds to the IP's oldest
     event, otherwise a run older than the window would be head-truncated
  4. close_stale runs globally, independent of discovery (round-2 B fix)

Idempotent, merge-correct (a bridging late event rebuilds one session where
two were), and start-move-correct (a late earlier event moves started_at).
"""
from collections import Counter

DISCOVERY_OVERLAP_S = 5.0  # small overlap; discovery is idempotent anyway
PRIO = {"error": 2, "proxied": 1, "redirect": 0}  # delivery tie-break priority

INSERT_SESSION_SQL = """
    INSERT INTO sessions
      (client_ip, provider, content_type, channel_id, delivery, country,
       started_at, ended_at, duration_s, event_count, closed)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,false)
"""


def rebuild_dirty(cur, cfg, last_run_start: float) -> tuple[list[float], list[float]]:
    """Returns (rewritten_starts, deleted_starts) — epoch starts of sessions
    inserted and deleted this run. Rollup recomputes both bucket sets: a
    rewritten session can also *leave* its old start bucket behind."""
    cur.execute("""
        SELECT DISTINCT client_ip
        FROM events
        WHERE received_at >= to_timestamp(%s)
          AND client_ip IS NOT NULL
          AND channel_id IS NOT NULL
    """, (last_run_start - DISCOVERY_OVERLAP_S,))
    dirty = [r[0] for r in cur.fetchall()]

    # extract() returns numeric -> Decimal in psycopg3; cast before float math.
    now_epoch = float(cur.execute("SELECT extract(epoch FROM now())").fetchone()[0])

    rewritten: list[float] = []
    deleted_starts: list[float] = []
    for ip in dirty:
        cur.execute("SELECT 1 FROM sessions WHERE client_ip = %s LIMIT 1", (ip,))
        has_prior = cur.fetchone() is not None

        # Delete by overlap: every open session is in scope, however old.
        cur.execute("""
            DELETE FROM sessions
            WHERE client_ip = %s
              AND (closed = false OR ended_at >= %s)
            RETURNING started_at
        """, (ip, now_epoch - cfg.rebuild_window_s - cfg.session_gap_s))
        deleted = [float(r[0]) for r in cur.fetchall()]
        deleted_starts.extend(deleted)

        anchor = now_epoch - cfg.rebuild_window_s
        if deleted:
            anchor = min(anchor, min(deleted))
        if not has_prior:
            # First build for this IP: runs crossing now-RW would otherwise be
            # head-truncated (missing up to RW of their leading events).
            cur.execute("""
                SELECT min(ts) FROM events
                WHERE client_ip = %s AND channel_id IS NOT NULL
            """, (ip,))
            m = cur.fetchone()[0]
            if m is not None:
                anchor = min(anchor, float(m))

        cur.execute("""
            SELECT ts, channel_id, content_type, provider, delivery, country
            FROM events
            WHERE client_ip = %s AND channel_id IS NOT NULL AND ts >= %s
            ORDER BY ts ASC
        """, (ip, anchor))
        rows = cur.fetchall()
        if not rows:
            continue

        # Split on channel change, content_type change, or gap > SESSION_GAP_S.
        buckets: list[list] = []
        prev = None
        current: list = []
        for r in rows:
            if prev is not None and (
                r[1] != prev[1] or r[2] != prev[2] or (r[0] - prev[0]) > cfg.session_gap_s
            ):
                buckets.append(current)
                current = []
            current.append(r)
            prev = r
        if current:
            buckets.append(current)

        for sess in buckets:
            start, end = float(sess[0][0]), float(sess[-1][0])
            counts = Counter(x[4] for x in sess if x[4] is not None)
            # Most frequent delivery; ties broken error > proxied > redirect.
            dom = max(counts, key=lambda d: (counts[d], PRIO.get(d, -1)), default=None)
            cur.execute(INSERT_SESSION_SQL, (
                ip, sess[0][3], sess[0][2], sess[0][1], dom, sess[0][5],
                start, end, end - start, len(sess),
            ))
            rewritten.append(start)

    return rewritten, deleted_starts


def close_stale(cur, cfg) -> int:
    """Global close sweep, independent of discovery (round-2 B fix).
    Rides the sessions_open_idx partial index."""
    cur.execute("""
        UPDATE sessions SET closed = true
        WHERE closed = false
          AND ended_at < extract(epoch FROM now()) - %s
    """, (cfg.session_gap_s,))
    return cur.rowcount
