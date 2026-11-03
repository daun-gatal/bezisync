#!/usr/bin/env bash
# Smoke-test the demo stack (deploy/compose): bring it up, create a pipeline from the
# sandbox template, wait until it streams live orders into Kafka, then tear everything down.
#
#   scripts/demo-smoke.sh            # BZ_DOCKER_HOST_IP for a remote Docker daemon
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
COMPOSE=(docker compose -f "$ROOT/deploy/compose/docker-compose.yml")
export BEZISYNC_DEMO_PORT=${BEZISYNC_DEMO_PORT:-28080}
export BEZISYNC_IMAGE=bezisync-local/demo-backend:smoke BEZISYNC_WEB_IMAGE=bezisync-local/demo-frontend:smoke
URL="http://${BZ_DOCKER_HOST_IP:-localhost}:${BEZISYNC_DEMO_PORT}"

cleanup() {
  # Connectors started by the sandbox runtime are not compose services.
  docker ps -aq --filter network=bezisync-demo --filter label=app.bezisync.io/managed-by=bezisync | xargs -r docker rm -f >/dev/null
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  docker rmi -f "$BEZISYNC_IMAGE" "$BEZISYNC_WEB_IMAGE" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[demo] building and starting the stack"
"${COMPOSE[@]}" up -d --build --wait --wait-timeout 600 >/dev/null
echo "[demo] exercising the sandbox at $URL"
if ! (cd "$ROOT/tools" && go run ./cmd/demo-smoke "$URL"); then
  echo "[demo] FAILED — controller and connector logs:"
  "${COMPOSE[@]}" logs --tail 80 controller api 2>&1 | tail -120
  for c in $(docker ps -aq --filter network=bezisync-demo --filter label=app.bezisync.io/managed-by=bezisync); do
    docker logs --tail 80 "$c" 2>&1
  done
  exit 1
fi
echo "[demo] PASSED"
