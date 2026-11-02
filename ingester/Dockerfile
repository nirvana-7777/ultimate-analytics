# Build context is the REPO ROOT (dockerfile: ingester/Dockerfile) because the
# image also bakes in migrations/ at /srv/migrations.
# .dockerignore at the repo root keeps tests/, tools/, secrets/ and VCS data
# out of the context (they were never copied into the image, but they did
# travel with every build context upload).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /srv

# Locked + hash-pinned (review: "requirements have no lockfile or hashes").
# pip-verify fails the build if a hash doesn't match. pydantic arrives as
# fastapi's transitive; pytest/httpx are NOT installed here (dev file only).
COPY ingester/requirements.lock.txt /srv/requirements.lock.txt
RUN pip install --no-cache-dir --require-hashes -r /srv/requirements.lock.txt

COPY ingester/ /srv/
COPY migrations/ /srv/migrations/

# Never run as root (review round-3). /srv needs no write access at runtime.
RUN useradd --system --no-create-home appuser
USER appuser

EXPOSE 8000 9000 9100
# HEALTHCHECK lives in docker-compose.yml (ingester: :9000/healthz,
# worker: :9100/metrics) — one image, two services, two checks.
CMD ["python", "-m", "app.main"]
