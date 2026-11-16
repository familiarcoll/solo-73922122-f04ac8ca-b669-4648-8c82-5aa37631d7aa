FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY oaitime.py ./
COPY demo_oai ./demo_oai
COPY docker/entrypoint.sh ./docker/entrypoint.sh
RUN chmod +x ./docker/entrypoint.sh

# Run as a non-root user; /data is a named volume owned by it.
RUN useradd --create-home --uid 10001 harvester \
    && mkdir -p /data \
    && chown -R harvester:harvester /data /srv
USER harvester

EXPOSE 8000 8001
ENTRYPOINT ["/srv/docker/entrypoint.sh"]
