#!/usr/bin/env bash
# Capture the screenshots the docs show (docs/images/*.jpg), from THIS working tree.
#
#   scripts/docs-screenshots.sh            # or: make docs-screenshots
#
# Starts the Codespaces lab stack (.devcontainer/lab, built from this checkout), creates and
# deploys the pipelines the pages show, and captures every screen with Playwright from INSIDE the
# stack's Docker network (on a remote Docker daemon the published ports are not reachable from
# here). The JPEGs replace docs/images/*.jpg; docs pages reference them by name. Never run in CI.
#
# Env:
#   SHOTS_KEEP=1          leave the lab running afterwards (to iterate)
#   SHOTS_SKIP_UP=1       use a lab that is already running (bash .devcontainer/lab/up.sh)
#   SHOTS_ONLY=a,b        capture only these screenshots
#   SHOTS_WAIT_SECONDS    how long pipelines stream before the capture (default 90)
set -euo pipefail

cd "$(dirname "$0")/.."

LAB=.devcontainer/lab
NETWORK=bezisync-demo
PLAYWRIGHT_VERSION=1.64.1
PLAYWRIGHT_IMAGE="mcr.microsoft.com/playwright:v${PLAYWRIGHT_VERSION}-noble"
OUT_DIR=docs/images

docker info >/dev/null 2>&1 || { echo "Docker daemon unreachable (DOCKER_HOST=${DOCKER_HOST:-unset})" >&2; exit 1; }

TMP_TAR="$(mktemp)"
cleanup() {
  rm -f "${TMP_TAR:?}"
  if [ "${SHOTS_KEEP:-0}" != "1" ] && [ "${SHOTS_SKIP_UP:-0}" != "1" ]; then
    bash "$LAB/down.sh" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [ "${SHOTS_SKIP_UP:-0}" != "1" ]; then
  echo "Building BeziSync from this checkout and starting the lab..."
  LAB_BUILD=1 bash "$LAB/up.sh" >&2
fi
PASSWORD="$(grep '^BEZISYNC_ADMIN_PASSWORD=' "$LAB/.env" | cut -d= -f2-)"

echo "Seeding and capturing (this takes a few minutes)..."
# The script is streamed over stdin, not bind-mounted (a remote daemon resolves volume sources on
# its own host); the screenshots come back as a tar on stdout.
docker run --rm -i --network "$NETWORK" \
  -e SHOTS_ADMIN_PASSWORD="$PASSWORD" -e SHOTS_ONLY="${SHOTS_ONLY:-}" -e SHOTS_WAIT_SECONDS="${SHOTS_WAIT_SECONDS:-90}" \
  "$PLAYWRIGHT_IMAGE" \
  bash -c "cat > /tmp/shots.mjs && cd /tmp && npm init -y >/dev/null 2>&1 && npm i --silent playwright-core@${PLAYWRIGHT_VERSION} >&2 && { node shots.mjs >&2; status=\$?; mkdir -p /out; tar -C /out -cf - .; exit \$status; }" \
  < scripts/docs-screenshots.mjs > "$TMP_TAR" || {
    status=$?
    mkdir -p "$OUT_DIR"
    tar -C "$OUT_DIR" -xf "$TMP_TAR" --wildcards '_debug-*' 2>/dev/null || true
    echo "Capture failed (exit $status); see $OUT_DIR/_debug-*" >&2
    exit "$status"
  }

mkdir -p "$OUT_DIR"
if [ -z "${SHOTS_ONLY:-}" ]; then
  find "${OUT_DIR:?}" -maxdepth 1 -name '*.jpg' -delete
fi
tar -C "$OUT_DIR" -xf "$TMP_TAR"
echo "Captured $(find "$OUT_DIR" -maxdepth 1 -name '*.jpg' | wc -l) screenshots into $OUT_DIR"
