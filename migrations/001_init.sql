-- 001_init.sql — initial schema.
--
-- Notes on two deliberate deviations from the round-2 design DDL:
--  * events.id is bigserial, not GENERATED AS IDENTITY: identity columns on
--    PARTITIONED tables are a PostgreSQL 17 feature; the stack pins postgres:16.
--  * sessions gains `country` (first event's client-declared country): the
--    rollup PK includes country and proxied views are counted from sessions,
--    so without it proxied views cannot be attributed to a country at all.
--  * sessionizer_state is gone: discovery is received_at-based, so there is
--    no per-IP watermark to persist.
--  * rollup_1m has no separate bucket index: the PK already starts with bucket.

CREATE TABLE events (
    id             bigserial,
    received_at    timestamptz NOT NULL DEFAULT now(),
    ts             double precision NOT NULL CHECK (ts > 0),
    event_type     text NOT NULL,
    schema_version int NOT NULL,
    client_ip      text CHECK (client_ip IS NULL OR length(client_ip) <= 64),
    provider       text CHECK (provider IS NULL OR length(provider) <= 256),
    channel_id     text CHECK (channel_id IS NULL OR length(channel_id) <= 512),
    content_type   text CHECK (content_type IS NULL OR content_type IN
                     ('channel','event','vod','recording')),
    http_status    int CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    delivery       text CHECK (delivery IS NULL OR delivery IN ('redirect','proxied','error')),
    user_agent     text CHECK (user_agent IS NULL OR length(user_agent) <= 256),
    country        text CHECK (country IS NULL OR length(country) <= 64),
    is_catchup     boolean,
    drm_variant    text,
    client_drm     boolean,
    no_proxy       boolean,
    highest_quality_only boolean,
    proxy                 text,              -- reserved (doc §9.1)
    manifest_refresh_hint double precision, -- reserved
    raw            jsonb NOT NULL,
    PRIMARY KEY (id, received_at)
) PARTITION BY RANGE (received_at);

CREATE INDEX events_ts_idx          ON events (ts);
CREATE INDEX events_ip_ch_ts_idx    ON events (client_ip, content_type, channel_id, ts);
CREATE INDEX events_provider_ts_idx ON events (provider, ts);
CREATE INDEX events_delivery_ts_idx ON events (delivery, ts);

CREATE TABLE sessions (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    client_ip     text NOT NULL,
    provider      text,
    content_type  text,
    channel_id    text NOT NULL,
    delivery      text,
    country       text,
    started_at    double precision NOT NULL,
    ended_at      double precision NOT NULL,
    duration_s    double precision NOT NULL,
    event_count   int NOT NULL,
    closed        boolean NOT NULL DEFAULT false
);
CREATE INDEX sessions_ip_key_idx  ON sessions (client_ip, content_type, channel_id, started_at);
CREATE INDEX sessions_started_idx ON sessions (started_at);
CREATE INDEX sessions_open_idx    ON sessions (closed) WHERE closed = false;

CREATE TABLE rollup_1m (
    bucket        timestamptz NOT NULL,
    provider      text NOT NULL DEFAULT '',
    channel_id    text NOT NULL DEFAULT '',
    content_type  text NOT NULL DEFAULT '',
    delivery      text NOT NULL DEFAULT '',
    country       text NOT NULL DEFAULT '',
    views         bigint NOT NULL DEFAULT 0,
    sessions      bigint NOT NULL DEFAULT 0,
    events        bigint NOT NULL DEFAULT 0,
    errors        bigint NOT NULL DEFAULT 0,
    dims          jsonb NOT NULL DEFAULT '{}'::jsonb,  -- extension point
    PRIMARY KEY (bucket, provider, channel_id, content_type, delivery, country)
);

CREATE TABLE quarantine (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    received_at timestamptz NOT NULL DEFAULT now(),
    reason      text NOT NULL,
    raw         jsonb NOT NULL
);
CREATE INDEX quarantine_received_idx ON quarantine (received_at);

CREATE TABLE gap_histogram (
    day          date NOT NULL,
    content_type text NOT NULL DEFAULT '',
    p50_s        double precision,
    p90_s        double precision,
    p99_s        double precision,
    samples      bigint,
    PRIMARY KEY (day, content_type)
);

-- Initial daily partitions (yesterday..+2d). Without this, inserts between
-- "migrator finished" and "worker's first tick" would fail with
-- "no partition of relation events found for row". The worker keeps 2 days
-- ahead after this; both paths use IF NOT EXISTS so they compose.
DO $$
DECLARE
    d date;
BEGIN
    FOR i IN -1..2 LOOP
        d := date_trunc('day', now())::date + i;
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS events_%s PARTITION OF events FOR VALUES FROM (%L) TO (%L)',
            to_char(d, 'YYYYMMDD'), d, d + 1);
    END LOOP;
END $$;
