"""Nightly empirical inter-event gap distribution per content_type — the data
behind SESSION_GAP_S retuning (not intuition). Writes to gap_histogram.

Round-3 review (blocker 2): the last-run marker was a module global, so the
day-scan re-ran on EVERY worker restart — inside the tick transaction, on the
ingester's 1.5 s statement_timeout, rolling back sessionize+rollup with it at
volume. The worker now calls update() from its own isolated phase and tracks
the cadence in worker_state, so a restart resumes instead of re-bursting.

NaN gaps (from any pre-scrub poison rows) would flow straight into
percentile_disc and store NaN quietly; `gap < 1e9` excludes them — PostgreSQL
orders NaN as greater than everything, so NaN fails the comparison. (1e9 s
≈ 31 years, far beyond any plausible inter-event gap.)
"""
import logging

log = logging.getLogger("worker.gap_histogram")

INTERVAL_S = 86400.0  # once per day, persisted in worker_state


def update(cur, cfg) -> None:
    cur.execute("""
        WITH gaps AS (
          SELECT COALESCE(content_type, '') AS ct, ts - lag(ts) OVER (
            PARTITION BY client_ip, content_type, channel_id ORDER BY ts) AS gap
          FROM events
          WHERE received_at > now() - interval '1 day'
            AND channel_id IS NOT NULL
        )
        INSERT INTO gap_histogram (day, content_type, p50_s, p90_s, p99_s, samples)
        SELECT current_date, ct,
               percentile_disc(0.5) WITHIN GROUP (ORDER BY gap),
               percentile_disc(0.9) WITHIN GROUP (ORDER BY gap),
               percentile_disc(0.99) WITHIN GROUP (ORDER BY gap),
               count(*)
        FROM gaps
        WHERE gap IS NOT NULL AND gap < 1e9
        GROUP BY ct
        ON CONFLICT (day, content_type) DO UPDATE SET
          p50_s = EXCLUDED.p50_s, p90_s = EXCLUDED.p90_s,
          p99_s = EXCLUDED.p99_s, samples = EXCLUDED.samples
    """)
    cur.execute(
        "SELECT content_type, p50_s, p90_s, p99_s, samples FROM gap_histogram "
        "WHERE day = current_date ORDER BY content_type")
    for r in cur.fetchall():
        log.info("gap_histogram %s", r)
