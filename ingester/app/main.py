"""Ingest service.

Public listener (:8000, behind Caddy): POST /ingest, bearer auth, 202 after
commit, 503 fast on DB-down. Internal listener (:9000, Docker-network only,
auth-exempt): /healthz (DB-independent), /readyz (DB ping), /metrics.

The DB work runs via run_in_threadpool: blocking psycopg calls must not stall
the event loop (round-1 hardening: a hung PG must not take /healthz with it —
healthz is also on a separate listener).
"""
import hmac
import json
import logging
import time

import psycopg
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from psycopg.types.json import Jsonb
from starlette.concurrency import run_in_threadpool

from . import metrics
from .config import load
from .db import make_pool, wait_for_pg
from .partitions import ensure_ahead
from .validate import _sanitize_obj, validate_event

cfg = load()
logging.basicConfig(level=cfg.log_level.upper(),
                    format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("ingester")

if cfg.auth_token is None and not cfg.auth_disabled:
    # Misconfigured .env must never silently disable auth.
    raise SystemExit(
        "ANALYTICS_AUTH_TOKEN is required (or set ANALYTICS_AUTH_DISABLED=1 for local dev)")
if cfg.auth_disabled:
    log.warning("AUTH DISABLED — local dev only")

wait_for_pg(cfg.database_url)
pool = make_pool(cfg.database_url)
try:
    with pool.connection() as conn, conn.cursor() as cur:
        ensure_ahead(cur)  # first inserts must never hit a missing partition
except Exception:
    log.exception("initial partition ensure failed; worker will retry each tick")

# docs/openapi on a *public* listener are a recon gift: pass None for all.
public_app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
internal_app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

MAX_BODY = 1_048_576  # 1 MB (doc §6)

INSERT_EVENT_SQL = """
    INSERT INTO events (
      ts, event_type, schema_version, client_ip, provider, channel_id,
      content_type, http_status, delivery, user_agent, country, is_catchup,
      drm_variant, client_drm, no_proxy, highest_quality_only, proxy,
      manifest_refresh_hint, raw
    ) VALUES (
      %(ts)s, %(event_type)s, %(schema_version)s, %(client_ip)s, %(provider)s,
      %(channel_id)s, %(content_type)s, %(http_status)s, %(delivery)s,
      %(user_agent)s, %(country)s, %(is_catchup)s, %(drm_variant)s,
      %(client_drm)s, %(no_proxy)s, %(highest_quality_only)s, %(proxy)s,
      %(manifest_refresh_hint)s, %(raw)s
    )
"""


def _sender(headers) -> str:
    # Bound label cardinality: product token before "/", not the full UA.
    ua = headers.get("user-agent", "")
    s = ua.split("/", 1)[0].strip()[:64]
    return s or "unknown"


def _check_auth(authorization: str | None) -> None:
    if cfg.auth_token is None:
        return
    if not authorization or not authorization.startswith("Bearer "):
        metrics.INGEST_REQUESTS.labels(status="401").inc()
        raise HTTPException(status_code=401)
    presented = authorization[7:].encode()
    expected = cfg.auth_token.encode()
    if not hmac.compare_digest(presented, expected):
        metrics.INGEST_REQUESTS.labels(status="401").inc()
        raise HTTPException(status_code=401)


def _ingest_sync(events: list, sender: str) -> Response:
    t0 = time.perf_counter()
    status = "202"
    try:
        good, quarantined = [], []
        for e in events:
            row, reason = validate_event(e)
            if reason is None:
                sanitized = row.pop("_sanitized", None)
                if sanitized:
                    for f in sanitized:
                        metrics.EVENTS_SANITIZED.labels(field=f).inc()
                good.append(row)
            else:
                # The quarantine row must be jsonb-storable TOO: a NaN ts or a
                # NUL string in the raw would make the quarantine executemany
                # fail -> 503 -> dropped batch (found by the round-3 tests —
                # the poison was hiding in the handler's other INSERT).
                e2, _touched = _sanitize_obj(e, None)
                quarantined.append((reason, e2))

        now_ts = time.time()
        with pool.connection() as conn, conn.cursor() as cur:
            inserted_rows, insert_failed = _insert_events(cur, good) if good else (good, [])
            if quarantined or insert_failed:
                cur.executemany(
                    "INSERT INTO quarantine (reason, raw) VALUES (%s, %s)",
                    [(reason, Jsonb(raw)) for reason, raw in quarantined + insert_failed],
                )

        for reason, _raw in quarantined + insert_failed:
            metrics.EVENTS_QUARANTINED.labels(reason=reason).inc()
        if inserted_rows:
            metrics.EVENTS_RECEIVED.labels(sender=sender).inc(len(inserted_rows))
            metrics.LAST_EVENT_TS.labels(sender=sender).set(now_ts)
            for r in inserted_rows:
                lag = now_ts - r["ts"]
                metrics.INGEST_LAG.labels(sender=sender).observe(lag)
                if lag > cfg.max_events_age_s:
                    metrics.EVENTS_LATE.inc()
                if r["event_type"] != "manifest_request":
                    metrics.EVENTS_UNKNOWN_TYPE.labels(event_type=r["event_type"]).inc()
        metrics.INGEST_REQUESTS.labels(status="202").inc()
        return Response(status_code=202)
    except Exception:
        # Backend backs off and drops the batch (doc §2, at-most-once) — intended.
        # Only truly environmental failures (DB down, disk full) land here;
        # every *event-level* poison is handled by validation or the
        # per-event fallback below, so this is no longer reachable by a
        # single malformed event.
        log.exception("ingest failed")
        status = "503"
        metrics.INGEST_REQUESTS.labels(status="503").inc()
        raise HTTPException(status_code=503)
    finally:
        metrics.INGEST_DURATION.labels(status=status).observe(time.perf_counter() - t0)


def _insert_events(cur, good: list) -> tuple[list, list]:
    """Fast path: one executemany. Poison path: fall back to per-event inserts
    under savepoints so only the offending event is quarantined (review
    blocker 3) — a 503 here means the backend drops the whole batch.

    Defense in depth: validate_event already rejects/sanitizes everything we
    know can poison Postgres (NUL, surrogates, NaN/Infinity/huge ts, oversized
    fields); this loop catches the unknown unknowns.

    Returns (inserted_rows, [(reason, raw), ...] to quarantine).
    """
    params = [dict(r, raw=Jsonb(r["raw"])) for r in good]
    try:
        with cur.connection.transaction():
            cur.executemany(INSERT_EVENT_SQL, params)
        return good, []
    except psycopg.Error as first:
        # The failed block was rolled back by the context manager; the
        # transaction (pool connection) is healthy again — start over row by
        # row, each in its own nested block.
        log.warning("bulk insert failed (%s); falling back to per-event inserts",
                    str(first).splitlines()[0])
        metrics.INGEST_BATCH_FALLBACK.inc()

    inserted, failed = [], []
    for r in good:
        try:
            with cur.connection.transaction():
                cur.execute(INSERT_EVENT_SQL, dict(r, raw=Jsonb(r["raw"])))
            inserted.append(r)
        except psycopg.Error as e:
            log.warning("event quarantined at insert: %s", str(e).splitlines()[0])
            failed.append(("insert_failed", r["raw"]))
    return inserted, failed


@public_app.post("/ingest")
async def ingest(request: Request, authorization: str | None = Header(default=None)):
    _check_auth(authorization)

    # Reject oversized bodies before buffering them.
    cl = request.headers.get("content-length")
    if cl is not None:
        try:
            n = int(cl)
        except ValueError:
            n = -1
        if n < 0 or n > MAX_BODY:
            metrics.INGEST_REQUESTS.labels(status="413").inc()
            raise HTTPException(status_code=413)

    body = await request.body()
    if len(body) > MAX_BODY:
        metrics.INGEST_REQUESTS.labels(status="413").inc()
        raise HTTPException(status_code=413)

    try:
        payload = json.loads(body)
        events = payload["events"]
        if not isinstance(events, list):
            raise ValueError("events not a list")
    except Exception:
        # Malformed *envelope* is a backend bug -> 400. Per-event problems are
        # never 4xx (doc §2): they go to quarantine with a 202.
        metrics.INGEST_REQUESTS.labels(status="400").inc()
        raise HTTPException(status_code=400)

    # Bounded batches: 1 MB of `{}` is ~350k events -> ~350k quarantine rows in
    # one request (review blocker 5). The cap is a request-level contract
    # violation, so the whole request is rejected, not silently trimmed.
    if len(events) > cfg.max_events_per_batch:
        metrics.INGEST_REQUESTS.labels(status="413").inc()
        raise HTTPException(
            status_code=413,
            detail=f"events per request capped at {cfg.max_events_per_batch}")

    sender = _sender(request.headers)
    return await run_in_threadpool(_ingest_sync, events, sender)


@internal_app.get("/healthz")
def healthz():
    return {"ok": True}  # DB-independent on purpose


@internal_app.get("/readyz")
def readyz():
    try:
        with psycopg.connect(cfg.database_url, connect_timeout=1,
                             options="-c statement_timeout=250") as c:
            c.execute("SELECT 1")
    except Exception:
        raise HTTPException(status_code=503)
    return {"ok": True}


@internal_app.get("/metrics")
def metrics_endpoint():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def run():
    import threading

    def run_internal():
        uvicorn.run(internal_app, host="0.0.0.0", port=cfg.internal_port, log_level="warning")

    threading.Thread(target=run_internal, daemon=True).start()
    uvicorn.run(public_app, host="0.0.0.0", port=cfg.public_port, log_level=cfg.log_level)


if __name__ == "__main__":
    run()
