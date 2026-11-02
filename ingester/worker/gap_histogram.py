"""Nightly empirical inter-event gap distribution per content_type — the data
behind SESSION_GAP_S retuning (not intuition). Writes to gap_histogram."""
import time

LAST = {"t": 0.0}


def maybe_update(cur, cfg) -> None:
    if time.time() - LAST["t"] < 86400:
        return
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
        WHERE gap IS NOT NULL
        GROUP BY ct
        ON CONFLICT (day, content_type) DO UPDATE SET
          p50_s = EXCLUDED.p50_s, p90_s = EXCLUDED.p90_s,
          p99_s = EXCLUDED.p99_s, samples = EXCLUDED.samples
    """)
    cur.execute(
        "SELECT content_type, p50_s, p90_s, p99_s, samples FROM gap_histogram "
        "WHERE day = current_date ORDER BY content_type")
    for r in cur.fetchall():
        print("gap_histogram", r, flush=True)
    LAST["t"] = time.time()
