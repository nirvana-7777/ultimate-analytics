"""Sessionizer. Implements the round-2 A/B/C fixes.

Clocks (one job each — this split is what makes A/B/C hold):
  received_at -> discovery, pruning, partitioning
  ts          -> domain logic (sessionize, bucket attribution)
  now()       -> staleness / close decisions only

Session key: (client_ip, content_type, channel_id) — split on any change or
gap > SESSION_GAP_S. Canonical operation is a scoped rebuild per dirty IP:

  1. discover dirty IPs by received_at since the last tick (partition-prunes,
     served by events_received_idx within the day-partition)
  2. delete by OVERLAP (closed=false OR ended_at >= now-RW-GAP), not by
     started_at >= window — a long session is always in scope, however old
  3. recompute anchored at LEAST(min deleted started_at, now-RW); on the
     FIRST build for an IP (no prior rows) anchor rewinds to the IP's oldest
     event, otherwise a run older than the window would be head-truncated
  4. close_stale runs globally, independent of discovery (round-2 B fix)

DELIBERATE SEMANTICS — key-grouped gap-checking (round-4 review, made
explicit and pinned by tests both ways): events are grouped per
(client_ip, content_type, channel_id) and gaps are measured WITHIN a group,
not in ts-interleaved order across the IP.

  + Two viewers behind one NAT on different channels: each key's stream
    stays continuous — no cross-channel fragmentation
    (test_two_views_behind_nat_do_not_fragment).
  − One viewer zapping A→B→A within SESSION_GAP_S: ONE A session that
    SPANS the B time (duration includes the away-time) plus one B session,
    instead of three fragments (test_zap_aba_...).

The NAT case is the deployed reality (TV boxes behind home routers) and
cannot be solved without a device id; the zap case is rarer and its
distortion is bounded by the gap. Time-ordered splitting per IP is exactly
what produced the NAT fragmentation — reversing this trade needs a device
id in the contract (v2), not a smarter ordering.

Round-3 review: the rebuild was ~5 queries PER DIRTY IP and a restart rewound
a full hour. It is now ~6 queries per TICK (discovery, has-prior, delete,
first-build minima, one fetch, one insert batch), and restarts resume from the
persisted anchor instead of re-bursting. Non-finite ts rows are skipped
(ingest rejects them and migration 002 scrubs them, but the sessionizer reads
the DB, not the API).

Round-4 review (blocker 5): per-row insert fallback catches ONLY data-level
errors now. A timeout or connection error propagates, the whole core
transaction rolls back — including the session DELETEs above — and the
anchor does not advance. The old broad catch swallowed the insert error
AFTER the deletes had committed per-row: those sessions were simply lost,
with the tick counted "ok" and the anchor moved.
"""
import logging
import math
from collections import Counter, defaultdict

from app import metrics
from app.config import DISCOVERY_OVERLAP_S
from app.db import POISON_ERRORS
from app.validate import TS_HARD_MAX

log = logging.getLogger("worker.sessionizer")

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
    if not dirty:
        return [], []

    # extract() returns numeric -> Decimal in psycopg3; cast before float math.
    now_epoch = float(cur.execute("SELECT extract(epoch FROM now())").fetchone()[0])

    # --- batched per PHASE, not per IP -------------------------------------
    cur.execute(
        "SELECT DISTINCT client_ip FROM sessions WHERE client_ip = ANY(%s)",
        (dirty,),
    )
    has_prior = {r[0] for r in cur.fetchall()}

    # Delete by overlap: every open session is in scope, however old.
    cur.execute("""
        DELETE FROM sessions
        WHERE client_ip = ANY(%s)
          AND (closed = false OR ended_at >= %s)
        RETURNING client_ip, started_at
    """, (dirty, now_epoch - cfg.rebuild_window_s - cfg.session_gap_s))
    deleted_starts: list[float] = []
    deleted_by_ip: dict[str, list[float]] = defaultdict(list)
    for ip, started in cur.fetchall():
        started = float(started)
        deleted_starts.append(started)
        deleted_by_ip[ip].append(started)

    # Per-IP anchors: now-RW, rewound to this run's earliest deleted start...
    anchors: dict[str, float] = {
        ip: now_epoch - cfg.rebuild_window_s for ip in dirty
    }
    for ip, starts in deleted_by_ip.items():
        anchors[ip] = min(anchors[ip], min(starts))

    # ...and on first build for an IP, to its oldest event ever (one query
    # for all first-build IPs; a run older than the window would otherwise be
    # head-truncated).
    firsts = [ip for ip in dirty if ip not in has_prior]
    if firsts:
        cur.execute("""
            SELECT client_ip, min(ts) FROM events
            WHERE client_ip = ANY(%s) AND channel_id IS NOT NULL
            GROUP BY client_ip
        """, (firsts,))
        for ip, m in cur.fetchall():
            if m is not None:
                anchors[ip] = min(anchors[ip], float(m))

    # --- one fetch, each IP bounded by ITS OWN anchor ------------------------
    # A single IP with old history must not drag every other dirty IP's events
    # back to its anchor (memory + statement_timeout on first deploy/outage).
    ips = list(anchors)
    cur.execute("""
        SELECT e.client_ip, e.content_type, e.channel_id, e.provider,
               e.delivery, e.country, e.ts
        FROM events e
        JOIN unnest(%s::text[], %s::float8[]) AS a(ip, anchor)
          ON e.client_ip = a.ip AND e.ts >= a.anchor
        WHERE e.channel_id IS NOT NULL
        ORDER BY e.client_ip, e.content_type, e.channel_id, e.ts
    """, (ips, [anchors[ip] for ip in ips]))
    rows = cur.fetchall()

    # Split on channel change, content_type change, or gap > SESSION_GAP_S.
    # Rows arrive grouped by (client_ip, content_type, channel_id): a key
    # change always splits, gaps split within a group — see the DELIBERATE
    # SEMANTICS note in the module header (NAT vs zap tradeoff, pinned by
    # tests). Events below the IP's own anchor are skipped (they
    # belong to sessions outside this rebuild's scope).
    rewritten: list[float] = []
    sess_rows: list[tuple] = []
    group_key = None
    group_ip_anchor = 0.0
    sess: list[tuple] = []
    prev_ts = 0.0

    def flush():
        if not sess:
            return
        start, end = sess[0][6], sess[-1][6]
        counts = Counter(x[4] for x in sess if x[4] is not None)
        # Most frequent delivery; ties broken error > proxied > redirect.
        dom = max(counts, key=lambda d: (counts[d], PRIO.get(d, -1)), default=None)
        first = sess[0]
        sess_rows.append((
            first[0], first[3], first[1], first[2], dom, first[5],
            start, end, end - start, len(sess),
        ))
        rewritten.append(start)

    for r in rows:
        ts = r[6]
        if not (math.isfinite(ts) and 0 < ts < TS_HARD_MAX):
            continue  # belt & braces: ingest rejects, 002 scrubs; 1e308 is
            # finite but would build a year-33000 session with a poisoned
            # started_at (found by the round-3 wedge tests)
        key = (r[0], r[1], r[2])
        if key != group_key:
            flush()
            sess = []
            group_key = key
            group_ip_anchor = anchors.get(r[0], 0.0)
            prev_ts = 0.0
        if ts < group_ip_anchor:
            continue
        if sess and (ts - prev_ts) > cfg.session_gap_s:
            flush()
            sess = []
        sess.append(r)
        prev_ts = ts
    flush()

    if sess_rows:
        _insert_sessions(cur, sess_rows)
    return rewritten, deleted_starts


def _insert_sessions(cur, sess_rows: list[tuple]) -> None:
    """Fast path executemany; on a DATA-LEVEL poison, roll back and retry per
    row so one bad row cannot abort the whole core transaction (symmetry with
    the ingester's per-event fallback). conn.transaction() nests as a
    SAVEPOINT inside the tick transaction, and as BEGIN/COMMIT on autocommit
    connections (tests) — one code path, both modes.

    Round-4 blocker 5: only POISON_ERRORS (DataError/IntegrityError) take the
    fallback. Anything else (timeout, dead connection) propagates: the core
    transaction rolls back — INCLUDING the session DELETEs that already ran
    in rebuild_dirty — and the anchor stays. The old broad `except
    psycopg.Error` swallowed the insert error after those deletes: the
    sessions were gone for good while the tick reported success.
    """
    try:
        with cur.connection.transaction():
            cur.executemany(INSERT_SESSION_SQL, sess_rows)
    except POISON_ERRORS as e:
        metrics.SESSION_INSERT_ERRORS.inc()
        log.warning("bulk session insert failed (%s); per-row retry",
                    str(e).splitlines()[0])
        for row in sess_rows:
            try:
                with cur.connection.transaction():
                    cur.execute(INSERT_SESSION_SQL, row)
            except POISON_ERRORS as e2:
                log.warning("session row skipped after data-level insert error: %s",
                            str(e2).splitlines()[0])


def close_stale(cur, cfg) -> int:
    """Global close sweep, independent of discovery (round-2 B fix).
    Rides the sessions_open_idx partial index."""
    cur.execute("""
        UPDATE sessions SET closed = true
        WHERE closed = false
          AND ended_at < extract(epoch FROM now()) - %s
    """, (cfg.session_gap_s,))
    return cur.rowcount
