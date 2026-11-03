#!/usr/bin/env bash
# Start (or reuse) a disposable Postgres for local tests and print its URL.
# Works with a local or remote Docker daemon; set BZ_DOCKER_HOST_IP when the daemon's
# published ports are reachable on a different address than localhost.
set -euo pipefail
NAME=bezisync-dev-pg
PORT=${BZ_DEV_PG_PORT:-25432}
if ! docker ps -q -f "name=^${NAME}$" | grep -q .; then
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run -d --name "$NAME" --label app.bezisync.io/e2e=true -p "${PORT}:5432" \
    -e POSTGRES_USER=bezisync -e POSTGRES_PASSWORD=bezisync -e POSTGRES_DB=bezisync \
    postgres:17 -c wal_level=logical -c max_connections=300 >/dev/null
fi
until docker exec "$NAME" pg_isready -U bezisync >/dev/null 2>&1; do sleep 1; done
echo "postgresql://bezisync:bezisync@${BZ_DOCKER_HOST_IP:-localhost}:${PORT}/bezisync"
