#!/usr/bin/env bash
# Smoke-test the Codespaces lab (.devcontainer/lab, ADR 0018) the way a codespace starts it:
# build the images from this checkout, run the lab's up.sh (random secrets), run it a second time
# (a restart must change nothing), then check the Sandbox extras, the teammates' roles,
# preflight on the source that isn't ready, and streams into Kafka and the warehouse. Tears
# everything down at the end.
#
#   scripts/lab-smoke.sh            # BZ_DOCKER_HOST_IP for a remote Docker daemon
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
LAB="$ROOT/.devcontainer/lab"
export BEZISYNC_DEMO_PORT=${BEZISYNC_DEMO_PORT:-28081}
export BEZISYNC_IMAGE=bezisync-local/lab-backend:smoke BEZISYNC_WEB_IMAGE=bezisync-local/lab-frontend:smoke
export LAB_URL="http://${BZ_DOCKER_HOST_IP:-localhost}:${BEZISYNC_DEMO_PORT}" LAB_BUILD=1

if [ -f "$LAB/.env" ] || docker ps -aq --filter label=com.docker.compose.project=bezisync-lab | grep -q .; then
  echo "[lab] a lab is already running here; remove it first: bash .devcontainer/lab/down.sh" >&2
  exit 1
fi

cleanup() {
  bash "$LAB/down.sh" >/dev/null 2>&1 || true
  docker rmi -f "$BEZISYNC_IMAGE" "$BEZISYNC_WEB_IMAGE" >/dev/null 2>&1 || true
}
trap cleanup EXIT

logs() {
  for s in migrate controller api; do docker logs --tail 80 "bezisync-lab-$s-1" 2>&1; done | tail -200
  for c in $(docker ps -aq --filter network=bezisync-demo --filter label=app.bezisync.io/managed-by=bezisync); do
    docker logs --tail 80 "$c" 2>&1
  done
}

echo "[lab] building the images and starting the lab"
if ! bash "$LAB/up.sh" >/dev/null; then
  echo "[lab] FAILED to start"
  logs
  exit 1
fi
for f in .env .state/login.txt .state/banner.txt; do
  if [ "$(stat -c %a "$LAB/$f")" != 600 ]; then
    echo "[lab] FAILED: $f is not private (mode $(stat -c %a "$LAB/$f"))"
    exit 1
  fi
done
echo "[lab] starting it again (a restart must change nothing)"
bash "$LAB/up.sh" >/dev/null

echo "[lab] exercising the lab at $LAB_URL"
export BEZISYNC_SMOKE_ADMIN_PASSWORD BEZISYNC_SMOKE_TEAM_PASSWORD
BEZISYNC_SMOKE_ADMIN_PASSWORD=$(grep '^BEZISYNC_ADMIN_PASSWORD=' "$LAB/.env" | cut -d= -f2-)
BEZISYNC_SMOKE_TEAM_PASSWORD=$(grep '^LAB_TEAM_PASSWORD=' "$LAB/.env" | cut -d= -f2-)
if ! (cd "$ROOT/tools" && go run ./cmd/demo-smoke -lab "$LAB_URL"); then
  echo "[lab] FAILED"
  logs
  exit 1
fi
rows=$(docker exec bezisync-lab-lab-warehouse-1 psql -U bz_writer -d warehouse -Atc 'SELECT count(*) FROM public.demo_shop_orders')
if [ "${rows:-0}" -lt 1 ]; then
  echo "[lab] FAILED: no orders in the warehouse"
  exit 1
fi
echo "[lab] $rows orders in the warehouse"
echo "[lab] PASSED"
