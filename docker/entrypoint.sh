#!/bin/sh
set -eu

case "${1:-}" in
  app)
    exec uvicorn app.main:app \
      --host 0.0.0.0 --port "${APP_PORT:-8000}" \
      --proxy-headers --no-server-header
    ;;
  demo-oai)
    exec uvicorn demo_oai.main:app \
      --host 0.0.0.0 --port "${DEMO_OAI_PORT:-8001}" \
      --proxy-headers --no-server-header
    ;;
  *)
    exec "$@"
    ;;
esac
