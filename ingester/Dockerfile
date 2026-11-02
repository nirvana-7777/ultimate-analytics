# Build context is the REPO ROOT (dockerfile: ingester/Dockerfile) because the
# image also bakes in migrations/ at /srv/migrations.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /srv

COPY ingester/requirements.txt /srv/requirements.txt
RUN pip install --no-cache-dir -r /srv/requirements.txt

COPY ingester/ /srv/
COPY migrations/ /srv/migrations/

EXPOSE 8000 9000
CMD ["python", "-m", "app.main"]
