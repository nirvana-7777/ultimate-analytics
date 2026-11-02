# Analytics Ingest & Visualization

Postgres-centric analytics for streaming manifest events: synchronous ingest
(single Docker Compose stack, no broker), sessionization per
`(client_ip, content_type, channel_id)`, 1-minute rollups, Grafana provisioned
as code. Aggregate-only, at-most-once delivery, ≤1 min staleness.

```
backend ×≤3 ──HTTPS──▶ caddy (TLS, 1 MiB body cap, rate-limit backstop)
                          └─▶ ingester (FastAPI, 202-after-commit)
                                └─▶ postgres 16 (events PARTITIONED BY day + DEFAULT)
                                      ├─ sessions   (duration lives HERE, not in rollup)
                                      ├─ rollup_1m  (counts only)
                                      ├─ quarantine (validation failures only)
                                      └─ worker_state (resumable tick anchors)
                          worker (ddl → sessionize+rollup → gap histogram; 60 s tick)
                          grafana ◀─ prometheus (ingester/worker metrics)
```

## Layout

```
docker-compose.yml        7 services: caddy, migrator, ingester, worker, postgres, grafana, prometheus
caddy/                    custom image (xcaddy + mholt/caddy-ratelimit) + Caddyfile
ingester/app/             FastAPI app: main, validate, db (pool + migrator), config, metrics, partitions
ingester/worker/          tick phases: main, sessionizer, rollup, gap_histogram
ingester/requirements.lock.txt   hash-pinned runtime deps (pip-compile --generate-hashes)
ingester/requirements-dev.txt    pytest/httpx/pgserver — never in the prod image
migrations/               001 schema + initial partitions; 002 round-3 hardening
prometheus/               scrape config + alert rules (no Alertmanager — see Alerts)
grafana/provisioning/     datasources + dashboard (generated, see below)
tools/panels.py           SINGLE SOURCE OF TRUTH for panel SQL
tools/build_dashboard.py  regenerates grafana/provisioning/dashboards/analytics.json
tests/worker/             the A/B/C scenario fixtures + round-3 poison/wedge tests
tests/contract/           ingest contract as executable spec (incl. poison batches)
tests/panels/             every panel SQL runs against a real DB; JSON anti-drift
.env.example              copy to .env; never commit .env
.dockerignore             tests/tools/secrets stay out of the build context
```

## Quickstart

```bash
openssl rand -hex 16 > secrets/pg_password
openssl rand -hex 20 > secrets/grafana_admin
cp .env.example .env
# edit .env: put the pg_password value into the DSN, set ANALYTICS_AUTH_TOKEN
docker compose up -d --build
```

Migrations run in their own one-shot container (`migrator`) before the
ingester and worker start; both also self-migrate at boot under an advisory
lock, so running the worker standalone is safe.

Grafana: `https://grafana.example.com` (via Caddy) or
`ssh -L 3000:127.0.0.1:3000 <host>` → `http://localhost:3000`.

Local testing without DNS: change the site addresses in `caddy/Caddyfile` to
`localhost` (Caddy uses its internal CA). If the Caddyfile adapter ever
errors `rate_limit is not an ordered directive`, add a global options block
`{ order rate_limit before reverse_proxy }`.

## Tests

```bash
pip install -r ingester/requirements-dev.txt   # incl. pgserver = embedded Postgres
python tools/build_dashboard.py               # regenerate dashboard JSON
python -m pytest tests -q
```

Or point at an existing Postgres:
`ANALYTICS_TEST_DATABASE_URL=postgresql://... python -m pytest tests -q`.
No database ⇒ DB tests skip; `tests/ingester/test_validate.py` always runs.

### Pinned scenarios (tests/worker/)

The regression tests for the review's ship-blockers; if you change the
sessionizer, these tell you which contract you broke:

| Test | Scenario | Pins |
|---|---|---|
| `test_a1_steady` | 2 h continuous viewing | one session, full duration, idempotent re-runs |
| `test_a2_stops` | viewer departs | closed only via the global sweep once gap elapses |
| `test_a3_long_across_rebuild` | 3 h session vs 15-min rebuild window | no head truncation on first build, no ~15-min fracture |
| `test_b_departing` | no new events at all | close works even when the IP is not dirty |
| `test_c_skew` | ts 20 min in the past, received now | data-driven re-roll; >1 h old dropped + counted; moved sessions fix the bucket they left |
| `test_split_rules` | channel/gap/content_type changes; mixed deliveries; **two TVs behind one NAT**; **one viewer zapping A→B→A** | split key incl. content_type; dominant delivery by count, ties error > proxied > redirect; per-key gaps (not per-event interleave) — a deliberate, module-header-documented trade: NAT streams stay continuous, a zap-back yields ONE spanning session instead of three fragments |
| `test_poison` | NaN / ±Inf / 1e308 / huge-int rows in events; data-level vs environmental landmines on one bucket; worker restart; anchor clamp after outage; DDL/gap failure | the tick never wedges: poison rows produce no sessions, data-level bucket errors are skipped + counted while environmental ones roll the tick back, anchors resume instead of rewinding and are clamped after an outage, phase failures are isolated (with gap backoff), DEFAULT-partition rows get rescued — inside the real worker's DDL transaction, not just autocommit — and per-session rows (client_ip) expire on retention |

Contract tests additionally pin: poison batches stay 202 (only the bad event
is quarantined or sanitized), the 5000-event request cap answers 413,
`/docs` & `/openapi.json` are 404 on the public listener, the per-event
insert fallback (simulated DB constraint violation) quarantines exactly one
event instead of 503ing the batch, a crashing validator quarantines as
`validator_error` (round-4: nothing a single event does may 503 a batch),
huge-int `ts` and 500-deep `raw` land in quarantine (not OverflowError /
RecursionError), and unknown `event_type` values collapse into the single
metric label `other` (allowlist-bounded cardinality).

## How the worker thinks about time

Three clocks, one job each — this is what makes the scenarios above hold:

- **`received_at`** (server clock): discovery of dirty IPs, rollup bucket
  discovery, partitioning, pruning.
- **`ts`** (backend clock): sessionize, split, bucket attribution.
- **`now()`**: staleness only (the global close sweep).

A tick has three phases, each its own transaction (round-3 review, blocker 2):

1. **ddl** — `ensure_ahead` + `prune` + DEFAULT-partition gauge, in a short
   transaction with `lock_timeout`. `CREATE/DROP PARTITION` take an
   AccessExclusiveLock on `events` (empirically confirmed on PG16), so this
   must never ride the long core transaction: ingest inserts would queue
   behind it until the tick commits. On lock timeout it bails and retries
   next tick. The DEFAULT-partition rescue runs as a savepoint under
   `LOCK TABLE events IN ACCESS EXCLUSIVE MODE` (round-4: it only ever
   worked in autocommit before — inside the DDL transaction the
   CheckViolation left it in InFailedSqlTransaction).
2. **core** — sessionize + close + rollup in ONE transaction guarded by
   `pg_try_advisory_xact_lock` (auto-released at rollback; the session-level
   variant leaked locks onto pooled connections and masked real errors).
   Data-level failures (DataError/IntegrityError) roll back to savepoints
   and are counted; environmental ones (timeout, connection, trigger bug)
   roll back the whole tick — round-4 blocker 5: the old broad
   `except psycopg.Error` swallowed them and advanced the anchor past
   stale buckets / deleted-away sessions.
3. **gap** — the day-scan gap histogram, isolated in its own transaction with
   its own failure handling; its cadence anchor lives in `worker_state`, so
   it runs once per day, not once per restart. A FAILED day-scan backs off
   10 minutes instead of burning its 60 s budget on every tick (round-4).

Anchor semantics (round-4 blockers 2 + 3):

- The saved anchor is the **DB clock captured at the start of the tick**,
  before any discovery query ran. An event whose ingest transaction commits
  mid-tick (after discovery, before the tick's commit) has `received_at ≤`
  that value, so the next tick's `received_at ≥ anchor − 5 s` rediscovers
  it. (The round-3 regression saved `time.time()` *after* the work:
  anything committed while a >5 s tick ran was never rediscovered — silently
  stale sessions and rollup buckets.)
- The discovery window is **clamped to `ANALYTICS_MAX_EVENTS_AGE_S`**: a
  persisted anchor can be arbitrarily old (long outage), and without the
  clamp the first catch-up tick would try to process the whole backlog in
  one transaction, hit the statement timeout, roll back, and retry the same
  unbounded window forever. The abandoned span is counted in
  `analytics_worker_backlog_abandoned_seconds` (alerted).
- Anchors still only advance in the same transaction as the work they
  guard: a failed tick re-processes exactly its (clamped) window, and a
  restart resumes from the persisted value.

Rollup re-rolls every bucket touched by events *received* since the last tick
(plus buckets of sessions inserted/deleted this run) — not a fixed trailing
window, so steady clock skew cannot make a sender invisible. Buckets with ts
older than `ANALYTICS_MAX_EVENTS_AGE_S` (default 1 h) are dropped and counted
in `analytics_rollup_dropped_late_total`.

## Input hardening (rounds 3–4 review)

Everything below is enforced in `validate.py` and covered by tests; each item
used to be a dropped batch (503 = at-most-once loss) or a worker wedge:

- **ts** must be finite, > 0, and at most `ANALYTICS_TS_MAX_FUTURE_S` ahead.
  `json.loads` accepts `NaN`/`Infinity` and `nan <= 0` is False — one such
  value used to fail every tick at `int(nan // 60)` / `to_timestamp(1e308)`
  forever. Round-4: `json.loads` also accepts **400-digit integers**, and
  `float(10**400)` / `math.isfinite(10**400)` raise OverflowError *inside
  the validator* — ints are now compared as ints against the 1e12 hard cap
  (`TS_HARD_MAX`, shared with the rollup's SQL range predicates) before any
  float conversion.
- **raw is depth-capped** at 64 levels (round-4): `json.loads` parses
  arbitrarily deep structures, and the recursive sanitizer / `json.dumps`
  for the jsonb INSERT die with RecursionError near ~1000 — which escaped as
  a 503 and dropped the whole batch. Deeper events are quarantined whole as
  `raw_too_deep` (the quarantine row stores a stub raw — it must stay
  insertable too).
- **A crashing validator cannot 503 the batch** (round-4): every
  `validate_event` call is wrapped; an unexpected exception quarantines the
  event as `validator_error` and the batch still gets its 202. The contract
  is "one bad event = one quarantine row", and the bug list is not the only
  thing enforcing it.
- **NUL and lone surrogates** are replaced with U+FFFD in every string
  (values, keys, nested `raw`); Postgres rejects them in text and jsonb, and
  `user_agent` is client-controlled. Sanitized fields are counted in
  `analytics_events_sanitized_total{field}` (bounded label set).
- **NaN/Infinity inside `raw`** become the strings `"NaN"` / `"Infinity"`
  (jsonb rejects those tokens).
- **event_type** is capped at 64 chars and **allowlist-bounded as a metric
  label** (round-4): the length cap alone did not bound cardinality. Values
  outside `ANALYTICS_KNOWN_EVENT_TYPES` are counted under the single label
  `other` in `analytics_events_unknown_type_total`; which types actually
  arrived stays queryable in `events.raw` (14-day retention).
- **Requests are capped** at `ANALYTICS_MAX_EVENTS_PER_BATCH` events (413
  otherwise; 1 MB of `{}` would otherwise become ~350k quarantine rows).
  Caddy enforces `request_body max_size 1MiB` so chunked requests (no
  Content-Length) are also rejected at the edge before buffering.
- **Unknown poison at INSERT time** (the layer that cannot be enumerated) is
  contained by a per-event savepoint fallback: the bulk insert is retried
  row-by-row and only the offending event is quarantined
  (`insert_failed`, counted in `analytics_ingest_batch_fallback_total`).
  Round-4 blocker 5: the fallback catches only data-level errors
  (DataError/IntegrityError) — a statement timeout or connection error
  propagates as 503 instead of "quarantining" rows that never got a fair
  insert attempt. The quarantine rows are sanitized with the same rules — a
  NaN ts must not poison the quarantine INSERT either (a raw too deep to
  sanitize is stored as a stub).
- **Pre-existing poison rows** are scrubbed by migration 002
  (`DELETE ... WHERE NOT (ts >= 0 AND ts < 1e12)` — NaN sorts greatest in
  Postgres, so the predicate catches NaN/±Inf/1e308 and spares real epochs).
- **The public listener** serves only `/ingest`: `docs_url=None`,
  `redoc_url=None`, `openapi_url=None`.

## Dashboards & alerts

- Dashboard JSON is **generated** from `tools/panels.py`
  (`python tools/build_dashboard.py`); CI fails if the committed file is
  stale, and every panel's SQL runs in tests — panels cannot drift from CI.
- Semantics kept explicit: "current watched" reads `sessions` (fresh, split
  by delivery); viewer-seconds are proxied-derived with the attribution
  caveat in the panel description; geo is labeled *client-declared country*
  (query param, not geolocation).
- Prometheus rules in `prometheus/alerts.yml`: ingest gap per sender,
  quarantine spike, ingest-lag p99 (steady-skew alarm), worker stuck, rollup
  dropped-late, **rows in DEFAULT partition** (partition maintenance is
  behind), **per-event fallback firing** (an unknown poison class is passing
  validation). **No Alertmanager is deployed** — rules are visible at
  Prometheus `/alerts`; wire an Alertmanager when paging must actually reach
  someone.

## Configuration

| Env | Default | Meaning |
|---|---|---|
| `ANALYTICS_DATABASE_URL` | — | Postgres DSN (password must match `secrets/pg_password`) |
| `ANALYTICS_AUTH_TOKEN` | — | bearer token; **required** unless `ANALYTICS_AUTH_DISABLED=1` |
| `ANALYTICS_SESSION_GAP_S` | 120 | session split gap; retune from `gap_histogram` |
| `ANALYTICS_REBUILD_WINDOW_S` | 900 | session-merge lookback; size ≥ max expected backend skew (see lag p99 panel) |
| `ANALYTICS_MAX_EVENTS_AGE_S` | 3600 | rollup drops buckets with ts older than this |
| `ANALYTICS_ROLLUP_BUCKET_CAP` | 720 | hard cap on buckets re-rolled per tick |
| `ANALYTICS_RAW_RETENTION_DAYS` | 14 | events partitions dropped after this |
| `ANALYTICS_QUARANTINE_RETENTION_DAYS` | 7 | quarantine rows deleted after this |
| `ANALYTICS_WORKER_TICK_S` | 60 | tick cadence |
| `ANALYTICS_MAX_EVENTS_PER_BATCH` | 5000 | requests with more events are rejected whole (413) |
| `ANALYTICS_TS_MAX_FUTURE_S` | 86400 | ts further ahead is quarantined as poison, not skew |
| `ANALYTICS_KNOWN_EVENT_TYPES` | `manifest_request` | comma-separated allowlist; other event_type values are metric-counted under the single label `other` (cardinality bound) |
| `ANALYTICS_SESSIONS_RETENTION_DAYS` | 90 | per-session rows (carry `client_ip`) deleted after this; aggregates (rollup, gap histogram) keep forever |
| `ANALYTICS_WORKER_STATEMENT_TIMEOUT_MS` | 60000 | worker pool budget (gap-histogram day-scans need more than the ingester's 1.5 s) |
| `ANALYTICS_DDL_LOCK_TIMEOUT_MS` | 3000 | partition DDL gives up instead of queueing behind ingest |

## Operations runbook

- **Retention is DROP PARTITION**, not DELETE: daily partitions are created 2
  days ahead by the worker and pruned at `ANALYTICS_RAW_RETENTION_DAYS`.
- **The DEFAULT partition is a safety net, not a parking lot**: if the worker
  is down longer than partitions exist, ingest keeps working (rows land in
  `events_default`, `analytics_events_in_default` alarms) and the worker's
  `ensure_ahead` rescues them into the proper partition when it catches up.
- **The rate limit is a data-loss knob.** A 429 is a dropped batch upstream
  (at-most-once, no retry). Tightening it means choosing to lose data.
- **One worker replica.** The tick holds `pg_try_advisory_xact_lock`; a
  second replica skips its ticks rather than corrupting `sessions`. Scaling
  the worker needs a shard key, not more replicas.
- **SESSION_GAP_S tuning** is data-driven: the worker writes the empirical
  inter-event gap distribution (p50/p90/p99 per content_type) to
  `gap_histogram` once per day (persisted cadence — restarts do not re-run
  it).
- **Backend contract drift** fails CI, not dashboards: the contract test
  asserts typed columns in the DB (a renamed field still gets 202 — that is
  exactly the drift that must be caught).
- **Ingest-gap alert needs the sender marker**: per-sender series require the
  backend's User-Agent (or a header); without it all senders collapse into
  `unknown`.

## Privacy (GDPR position — needs owner sign-off)

`events.raw` is a faithful (post-sanitization) copy of the client payload and
`events.client_ip` is the connection IP; both are personal data under GDPR.
So is `sessions.client_ip` (round-4: it had no retention at all — it is now
pruned after `ANALYTICS_SESSIONS_RETENTION_DAYS`, default 90 d, in the same
maintenance sweep as events/quarantine). The current position: purpose =
sessionization + aggregate dashboards only (no per-user analytics product),
retention bounded (14 days events, 7 days quarantine, 90 days sessions,
DROP PARTITION / DELETE in the ddl phase), aggregates (`rollup_1m`,
`gap_histogram`) hold no personal data and are kept indefinitely, access via
authenticated Grafana only. If your DPO requires more, the knobs are: shorter
`ANALYTICS_RAW_RETENTION_DAYS` / `ANALYTICS_SESSIONS_RETENTION_DAYS`, a
`dims`-only rollup with raw disabled, or hashing `client_ip` with a server
side salt (sessionization survives — the key just becomes opaque). The
latter two are not implemented yet; they are deliberate product decisions.

## Known limitations (accepted in design)

- Loss on outage is accepted (at-most-once; 503 fast, backend drops batch).
- Redirect sessions have structurally zero duration (no heartbeat, doc §4);
  dashboards label this instead of blending.
- Per-IP sessions are approximate under NAT/truncation/hashing (doc §5).
  Events are gap-checked per session key, so two TVs behind one NAT on
  different channels no longer fragment into one-event sessions. Merging
  (two viewers, one channel, one IP) remains unavoidable without a device id
  — a v2 contract change.
- A single viewer flipping back to a channel within the gap produces one
  spanning session instead of three fragments; duration therefore includes
  the away-time. This is a **deliberate, pinned trade** (round-4: documented
  in `worker/sessionizer.py`'s module header, pinned by
  `test_zap_aba_is_one_spanning_session_per_key` on one side and
  `test_two_views_behind_nat_do_not_fragment` on the other). The alternative
  (split on interleave) is exactly what produced the NAT fragmentation; a
  device id in the contract is what resolves the trade properly.
- Segment-level events (doc §9.3): unknown event types land in `events.raw`
  (typed columns NULL) — queryable from day one, no migration when the
  contract lands.
