-- 002_round3_hardening.sql — review round-3 fixes that need schema/DDL.
--
-- Everything here is additive and idempotent under the migrator's advisory
-- lock. Deploy on an empty or a running system alike.

-- 1) events(received_at): the sessionizer and rollup discover work by
--    `received_at >= to_timestamp(...)`. Partition pruning removes whole days,
--    but WITHIN a day-partition (tens of millions of rows at volume) that
--    predicate was a sequential scan on every tick. The PK starts with `id`,
--    so it could not serve it.
CREATE INDEX IF NOT EXISTS events_received_idx ON events (received_at);

-- 2) Poison scrub: ts NaN/Infinity/1e308 could not have been inserted through
--    the hardened ingester, but rows may exist from before this fix (or from
--    direct loads). PostgreSQL treats NaN as *greater than everything*, so
--    `ts >= 0 AND ts < 1e12` is FALSE for NaN, +Inf and 1e308 — and TRUE for
--    any plausible epoch-second ts (1e12 s ≈ year 33 654). Deleting them is
--    strictly better than letting rollup's to_timestamp() fail on them
--    forever.
DELETE FROM events WHERE NOT (ts >= 0 AND ts < 1e12);

-- 3) Safety net for a worker that is down longer than the partitions that
--    exist (review blocker 4): without this, ingest hard-fails with
--    "no partition of relation events found for row". Rows that arrive while
--    the worker is down land here; analytics_events_in_default (worker gauge)
--    alarms on them, and partitions.ensure_ahead() moves them out when it
--    creates the partition whose range covers them.
--    NOTE: keep this partition EMPTY in steady state — creating a new
--    partition takes an AccessExclusiveLock on the DEFAULT partition while it
--    verifies no row falls in the new range, and an empty check is instant.
CREATE TABLE IF NOT EXISTS events_default PARTITION OF events DEFAULT;

-- 4) Persisted worker anchors (review blocker 2 / should-fix): last_run_start
--    used to be process memory, so every restart rewound a full hour (heavy
--    cold-start burst) and re-ran the day-scan gap histogram. Anchors now
--    advance in the same transaction as the work they guard, so a restart
--    resumes from the last successful tick instead of guessing.
CREATE TABLE IF NOT EXISTS worker_state (
    key        text PRIMARY KEY,
    value      double precision NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO worker_state (key, value) VALUES
    ('sessionize_last_run_start', 0),
    ('gap_hist_last_run', 0)
ON CONFLICT (key) DO NOTHING;
