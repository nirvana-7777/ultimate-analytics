-- Read-only role for the Grafana datasource.
-- Idempotent (re-run keeps the password in sync). docker-compose.yml runs it via the
-- grafana-role service after migrations. Manually:
--   psql -U analytics -d analytics -v ON_ERROR_STOP=1 -v grafana_pw="$GRAFANA_PG_PASSWORD" -f postgres/grafana_ro.sql
-- Assumes tables live in schema "public"; adjust if not.

SELECT 'CREATE ROLE grafana_ro LOGIN'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_ro')
\gexec

ALTER ROLE grafana_ro WITH LOGIN PASSWORD :'grafana_pw' CONNECTION LIMIT 10;

GRANT CONNECT ON DATABASE analytics TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;
-- Privileges on the partitioned parent cover its partitions for SELECT.
GRANT SELECT ON sessions, rollup_1m, events TO grafana_ro;

-- Dashboard SQL is user-editable; cap what it can cost.
ALTER ROLE grafana_ro SET statement_timeout = '30s';
ALTER ROLE grafana_ro SET default_transaction_read_only = on;
ALTER ROLE grafana_ro SET idle_in_transaction_session_timeout = '60s';
