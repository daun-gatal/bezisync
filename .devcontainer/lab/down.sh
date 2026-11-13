#!/usr/bin/env bash
# Removes the BeziSync lab completely: its containers, the connectors its pipelines started, its
# volumes, and its generated secrets (.env) and sign-in files. `bash up.sh` then starts a new one.
set -euo pipefail
LAB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT=bezisync-lab
docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" | xargs -r docker rm -f >/dev/null
docker ps -aq --filter network=bezisync-demo --filter label=app.bezisync.io/managed-by=bezisync | xargs -r docker rm -f >/dev/null
docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT" | xargs -r docker volume rm >/dev/null
docker network rm bezisync-demo >/dev/null 2>&1 || true
rm -rf "$LAB_DIR/.env" "$LAB_DIR/.state"
echo "The lab is removed."
