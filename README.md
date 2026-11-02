# Analytics Ingest & Visualization

Postgres-centric analytics for streaming manifest events: synchronous ingest
(single Docker Compose stack, no broker), sessionization per
`(client_ip, content_type, channel_id)`, 1-minute rollups, Grafana
provisioned as code. Aggregate-only, at-most-once delivery, ≤1 min staleness.

```
backend ×≤3 ──HTTPS──▶ caddy (TLS, rate-limit backstop)
                          └─▶ ingester (FastAPI, 202-after-commit)
                                └─▶ postgres 16 (events PARTITIONED BY day)
                                      ├─ sessions   (duration lives HERE, not in rollup)
                                      ├─ rollup_1m  (counts only)
                                      └─ quarantine (validation failures only)
                          worker (sessionizer + rollup + prune, 60 s tick)
                          grafana ◀─ prometheus (ingester/worker metrics)
```

## Layout

```
docker-compose.yml        7 services: caddy, migrator, ingester, worker, postgres, grafana, prometheus
caddy/                    custom image (xcaddy + mholt/caddy-ratelimit) + Caddyfile
ingester/app/             FastAPI app: main, validate, db (pool + migrator), config, metrics, partitions
ingester/worker/          tick loop: sessionizer, rollup, gap_histogram
migrations/001_init.sql   schema; creates initial daily partitions
prometheus/               scrape config + alert rules (no Alertmanager — see Alerts)
grafana/provisioning/     datasources + dashboard (generated, see below)
tools/panels.py           SINGLE SOURCE OF TRUTH for panel SQL
tools/build_dashboard.py  regenerates grafana/provisioning/dashboards/analytics.json
tests/worker/             the A/B/C scenario fixtures (see "Pinned scenarios")
tests/contract/           ingest contract as executable spec
tests/panels/             every panel SQL runs against a real DB; JSON anti-drift
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
pip install -r ingester/requirements.txt pgserver   # pgserver = embedded Postgres
python tools/build_dashboard.py                     # regenerate dashboard JSON
python -m pytest tests -q
```

Or point at an existing Postgres: `ANALYTICS_TEST_DATABASE_URL=postgresql://... python -m pytest tests -q`.
No database ⇒ DB tests skip; `tests/ingester/test_validate.py` always runs.

### Pinned scenarios (tests/worker/)

These are the regression tests for the review's ship-blockers; if you change
the sessionizer, these tell you which contract you broke:

| Test | Scenario | Pins |
|---|---|---|
| `test_a1_steady` | 2 h continuous viewing | one session, full duration, idempotent re-runs (was: never re-selected) |
| `test_a2_stops` | viewer departs | closed only via the global sweep once gap elapses |
| `test_a3_long_across_rebuild` | 3 h session vs 15-min rebuild window | no head truncation on first build, no ~15-min fracture |
| `test_b_departing` | no new events at all | close works even when the IP is not dirty |
| `test_c_skew` | ts 20 min in the past, received now | data-driven re-roll lands them in their ts buckets; >1 h old dropped + counted; moved sessions fix the bucket they left |
| `test_split_rules` | channel/gap/content_type changes; mixed deliveries | split key incl. content_type; dominant delivery by count, ties error > proxied > redirect |

## How the worker thinks about time

Three clocks, one job each — this is what makes the scenarios above hold:

- **`received_at`** (server clock): discovery of dirty IPs, rollup bucket
  discovery, partitioning, pruning.
- **`ts`** (backend clock): sessionize, split, bucket attribution.
- **`now()`**: staleness only (the global close sweep).

The rollup re-rolls every bucket touched by events *received* since the last
tick (plus buckets of sessions inserted/deleted this run) — not a fixed
trailing window, so steady clock skew cannot make a sender invisible.
Buckets with ts older than `ANALYTICS_MAX_EVENTS_AGE_S` (default 1 h) are
dropped and counted in `analytics_rollup_dropped_late_total`.

## Dashboards & alerts

- Dashboard JSON is **generated** from `tools/panels.py`
  (`python tools/build_dashboard.py`); CI fails if the committed file is
  stale, and every panel's SQL runs in tests — panels cannot drift from CI.
- Semantics kept explicit: "current watched" reads `sessions` (fresh, split
  by delivery); viewer-seconds are proxied-derived with the attribution
  caveat in the panel description; geo is labeled *client-declared country*
  (query param, not geolocation).
- Prometheus rules in `prometheus/alerts.yml`: ingest gap per sender
  (`time() - analytics_last_event_ts > 600` with prior-hour traffic),
  quarantine spike, ingest-lag p99 (steady-skew alarm), worker stuck, rollup
  dropped-late. **No Alertmanager is deployed** — rules are visible at
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

## Operations runbook

- **Retention is DROP PARTITION**, not DELETE: daily partitions are created 2
  days ahead by the worker and pruned at `ANALYTICS_RAW_RETENTION_DAYS`.
- **The rate limit is a data-loss knob.** A 429 is a dropped batch upstream
  (at-most-once, no retry). Tightening it means choosing to lose data.
- **One worker replica.** The tick holds `pg_try_advisory_lock`; a second
  replica skips its ticks rather than corrupting `sessions`. Scaling the
  worker needs a shard key, not more replicas.
- **SESSION_GAP_S tuning** is data-driven: the worker writes the empirical
  inter-event gap distribution (p50/p90/p99 per content_type) to
  `gap_histogram` nightly.
- **Backend contract drift** fails CI, not dashboards: the contract test
  asserts typed columns in the DB (a renamed field still gets 202 — that is
  exactly the drift that must be caught).
- **Ingest-gap alert needs the sender marker**: per-sender series require the
  backend's User-Agent (or a header); without it all senders collapse into
  `unknown`.

## Known limitations (accepted in design)

- Loss on outage is accepted (at-most-once; 503 fast, backend drops batch).
- Redirect sessions have structurally zero duration (no heartbeat, doc §4);
  dashboards label this instead of blending.
- Per-IP sessions are approximate under NAT/truncation/hashing (doc §5).
- Segment-level events (doc §9.3): unknown event types land in `events.raw`
  (typed columns NULL) — queryable from day one, no migration when the
  contract lands.
