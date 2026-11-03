#!/usr/bin/env bash
# End-to-end gate: build images, deploy BeziSync + data plane into a disposable k3s
# cluster running in Docker, run the Playwright suite, collect logs on failure.
#
#   scripts/e2e.sh            # full run
#   scripts/e2e.sh down       # delete everything this script created
#   E2E_GREP="pipeline" scripts/e2e.sh   # run a subset
#   scripts/e2e.sh quick tests/23-admin.spec.ts …   # iterate: reuse the cluster and deployment
#
# Quick mode tags images by the content of their source, so only changed images are rebuilt
# and imported; it upgrades the running release in place (its database and the data-plane
# fixtures are kept) and runs only the given specs. Data from earlier runs remains, so a spec
# that needs a clean database still wants a full run.
#
# Works with a local or remote Docker daemon. When the daemon's published ports are
# reachable on another address than localhost, set BZ_DOCKER_HOST_IP.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
NET=bezisync-e2e
K3S=bezisync-e2e-k3s
NS=bezisync-e2e                 # BeziSync itself: recreated every run (clean database)
DATA_NS=bezisync-e2e-data       # source/sink fixtures: kept between runs (slow to start)
RUNTIME_NS=bezisync-e2e-runtime # where the Kubernetes runtime starts connectors: recreated
AGENT_NS=bezisync-e2e-agent     # agent-mode runtimes (agents + their connectors): recreated
LABEL=app.bezisync.io/e2e=true
CUSTOM_IMAGE=bezisync-local/debezium-custom:e2e
K3S_IMAGE=rancher/k3s:v1.33.5-k3s1
HOST_IP=${BZ_DOCKER_HOST_IP:-localhost}
API_PORT=${BZ_E2E_K8S_PORT:-26443}
STATE=$ROOT/.e2e
export KUBECONFIG=$STATE/kubeconfig
TAG=${E2E_TAG:-e2e-$(date +%s)}
BACKEND_TAG=$TAG WEB_TAG=$TAG RUNNER_TAG=$TAG
QUICK=false
mkdir -p "$STATE"

log() { printf '\033[1m[e2e]\033[0m %s\n' "$*"; }

down() {
  log "removing e2e cluster, containers, network, volumes and images"
  docker ps -aq --filter "label=$LABEL" | xargs -r docker rm -f >/dev/null
  docker network ls -q --filter "label=$LABEL" | xargs -r docker network rm >/dev/null
  docker volume ls -q --filter "label=$LABEL" | xargs -r docker volume rm >/dev/null
  docker images --format '{{.Repository}}:{{.Tag}}' | grep '^bezisync-local/' | xargs -r docker rmi -f >/dev/null || true
  rm -rf "$STATE"
}

ensure_cluster() {
  if docker ps -q -f "name=^${K3S}$" | grep -q . && kubectl get nodes >/dev/null 2>&1; then return; fi
  # Imported images have no registry to pull them from again, and the Docker host's disk is often
  # shared: kubelet's image GC runs only when the disk is nearly full.
  log "starting k3s cluster ($K3S_IMAGE)"
  docker rm -f "$K3S" >/dev/null 2>&1 || true
  docker network inspect "$NET" >/dev/null 2>&1 || docker network create --label "$LABEL" "$NET" >/dev/null
  docker volume create --label "$LABEL" bezisync-e2e-k3s-data >/dev/null
  docker run -d --name "$K3S" --hostname "$K3S" --privileged --network "$NET" --label "$LABEL" \
    -p "${API_PORT}:6443" --tmpfs /run --tmpfs /var/run -v bezisync-e2e-k3s-data:/var/lib/rancher/k3s \
    "$K3S_IMAGE" server --disable=traefik --disable=metrics-server \
    "--kubelet-arg=eviction-hard=imagefs.available<1%,nodefs.available<1%" \
    --kubelet-arg=eviction-minimum-reclaim=imagefs.available=1%,nodefs.available=1% \
    --kubelet-arg=image-gc-high-threshold=99 --kubelet-arg=image-gc-low-threshold=98 \
    --tls-san "$HOST_IP" --tls-san "$K3S" >/dev/null
  until docker exec "$K3S" kubectl get nodes 2>/dev/null | grep -q ' Ready'; do sleep 2; done
  docker exec "$K3S" cat /etc/rancher/k3s/k3s.yaml | sed "s#https://127.0.0.1:6443#https://${HOST_IP}:${API_PORT}#" > "$KUBECONFIG"
  until kubectl get nodes >/dev/null 2>&1; do sleep 1; done
}

# A tag that changes only when the files under a directory change (tracked or new, not ignored).
content_tag() {
  (cd "$ROOT" && git ls-files -co --exclude-standard -- "$1" | LC_ALL=C sort | xargs -d '\n' sha1sum) | sha1sum | cut -c1-12
}

# Build one image unless that exact tag exists; import it into k3s unless containerd has it.
build_image() {
  local name=$1 tag=$2 dir=$3 import=$4
  shift 4
  if ! docker image inspect "bezisync-local/$name:$tag" >/dev/null 2>&1; then
    log "building $name ($tag)"
    docker build -q "$@" -t "bezisync-local/$name:$tag" "$dir" >/dev/null
  fi
  if $import && ! docker exec "$K3S" ctr -n k8s.io images ls -q | grep -qx "docker.io/bezisync-local/$name:$tag"; then
    log "importing $name into k3s"
    docker save "bezisync-local/$name:$tag" | docker exec -i "$K3S" ctr -n k8s.io images import - >/dev/null
  fi
}

build() {
  local args=(--build-arg COMMIT_SHA="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo dev)")
  build_image backend "$BACKEND_TAG" "$ROOT/backend" true "${args[@]}" --build-arg VERSION="$BACKEND_TAG"
  build_image frontend "$WEB_TAG" "$ROOT/frontend" true "${args[@]}" --build-arg VERSION="$WEB_TAG"
  build_image e2e "$RUNNER_TAG" "$ROOT/e2e" false
  for image in $(grep -ho 'quay.io/debezium/server:[^"]*' "$ROOT/backend/internal/config/config.go" | sort -u); do
    if ! docker exec "$K3S" ctr -n k8s.io images ls -q | grep -qx "$image"; then
      log "pre-loading $image into k3s"
      docker image inspect "$image" >/dev/null 2>&1 || docker pull -q "$image" >/dev/null
      docker save "$image" | docker exec -i "$K3S" ctr -n k8s.io images import - >/dev/null
    fi
  done
  # Sinks with their own Debezium Server build (Iceberg): pulled inside k3s by digest.
  for image in $(sink_images); do
    if ! docker exec "$K3S" crictl inspecti "$image" >/dev/null 2>&1; then
      log "pulling $image into k3s"
      docker exec "$K3S" crictl pull "$image" >/dev/null
    fi
  done
  # A "custom image" (same Debezium build under its own name) for the version-management
  # journey; tagged inside containerd, so nothing is transferred.
  local base
  base=$(grep -ho 'quay.io/debezium/server:[^"]*' "$ROOT/backend/internal/config/config.go" | head -1)
  docker exec "$K3S" ctr -n k8s.io images tag --force "$base" "docker.io/$CUSTOM_IMAGE" >/dev/null
}

# The pinned images of sinks that need their own Debezium Server build (descriptors.json).
sink_images() {
  jq -r '.connectors[].descriptor.sink.image // empty | "\(.repository):\(.tag)@\(.digest)"' "$ROOT/backend/internal/connectors/descriptors.json"
}

# Quick mode: upgrade the running release in place when there is one with ready fixtures.
# Fixtures are applied too (unchanged ones are untouched), so new ones start without a full run.
deploy_in_place() {
  helm status bezisync -n "$NS" >/dev/null 2>&1 || return 1
  kubectl -n "$DATA_NS" wait --for=condition=ready pod -l app.bezisync.io/fixture=true --timeout=5s >/dev/null 2>&1 || return 1
  log "applying data plane fixtures"
  kubectl apply -n "$DATA_NS" -f "$ROOT/deploy/e2e/fixtures/" >/dev/null
  log "upgrading the running release in place (backend $BACKEND_TAG, web $WEB_TAG)"
  helm upgrade bezisync "$ROOT/deploy/helm/bezisync" -n "$NS" \
    -f "$ROOT/deploy/e2e/values.yaml" --set image.tag="$BACKEND_TAG" --set webImage.tag="$WEB_TAG" \
    --wait --timeout 6m >/dev/null || { diagnose "$NS"; exit 1; }
  kubectl -n "$NS" wait --for=condition=available deployment --all --timeout=5m >/dev/null
  kubectl -n "$DATA_NS" wait --for=condition=ready pod -l app.bezisync.io/fixture=true --timeout=10m >/dev/null \
    || { diagnose "$DATA_NS"; exit 1; }
}

deploy() {
  log "resetting app and runtime namespaces"
  kubectl delete namespace "$NS" "$RUNTIME_NS" "$AGENT_NS" --ignore-not-found --wait=true >/dev/null
  kubectl delete clusterrole,clusterrolebinding -l app.kubernetes.io/instance=bezisync --ignore-not-found >/dev/null
  kubectl delete clusterrole,clusterrolebinding -l app.kubernetes.io/name=bezisync-agent --ignore-not-found >/dev/null
  for ns in "$NS" "$RUNTIME_NS" "$DATA_NS"; do
    kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  done
  if compgen -G "$ROOT/deploy/e2e/fixtures/*.yaml" >/dev/null; then
    log "applying data plane fixtures"
    kubectl apply -n "$DATA_NS" -f "$ROOT/deploy/e2e/fixtures/" >/dev/null
  fi
  log "helm install"
  helm upgrade --install bezisync "$ROOT/deploy/helm/bezisync" -n "$NS" \
    -f "$ROOT/deploy/e2e/values.yaml" --set image.tag="$BACKEND_TAG" --set webImage.tag="$WEB_TAG" \
    --wait --timeout 6m >/dev/null || { diagnose "$NS"; exit 1; }
  kubectl -n "$NS" wait --for=condition=available deployment --all --timeout=5m >/dev/null
  if compgen -G "$ROOT/deploy/e2e/fixtures/*.yaml" >/dev/null; then
    kubectl -n "$DATA_NS" wait --for=condition=ready pod -l app.bezisync.io/fixture=true --timeout=15m >/dev/null \
      || { diagnose "$DATA_NS"; exit 1; }
    prepare_dind
  fi
}

# The DinD "remote Docker host": drop connectors left by earlier runs, pre-pull Debezium.
prepare_dind() {
  local d=(kubectl -n "$DATA_NS" exec deploy/dind -- docker -H tcp://127.0.0.1:2375)
  "${d[@]}" ps -aq --filter label=app.bezisync.io/managed-by=bezisync | xargs -r "${d[@]}" rm -f >/dev/null
  for image in $(grep -ho 'quay.io/debezium/server:[^"]*' "$ROOT/backend/internal/config/config.go" | sort -u); do
    "${d[@]}" image inspect "$image" >/dev/null 2>&1 || { log "pre-pulling $image into DinD"; "${d[@]}" pull -q "$image" >/dev/null; }
  done
}

# Print why pods in a namespace are not ready (into the job log), then collect everything.
diagnose() {
  log "pods not ready in $1:"
  kubectl -n "$1" get pods -o wide || true
  for p in $(kubectl -n "$1" get pods -o jsonpath='{range .items[?(@.status.phase!="Succeeded")]}{.metadata.name}{" "}{range .status.conditions[?(@.type=="Ready")]}{.status}{end}{"\n"}{end}' 2>/dev/null | awk '$2!="True"{print $1}'); do
    log "--- $p"
    kubectl -n "$1" describe pod "$p" | sed -n '/^Containers:/,$p' | grep -E 'State|Reason|Message|Image:|Last State|Warning|Failed|Back-off|pull' || true
    kubectl -n "$1" logs "$p" --all-containers --tail=30 2>&1 || true
  done
  kubectl describe nodes | grep -A8 -E '^Conditions:|^Allocated resources:' || true
  collect_logs
}

collect_logs() {
  local dir=$STATE/logs-$TAG
  mkdir -p "$dir"
  kubectl -n "$NS" get pods -o wide > "$dir/pods.txt" 2>&1 || true
  kubectl get pods -A -o wide > "$dir/all-pods.txt" 2>&1 || true
  kubectl describe nodes > "$dir/nodes.txt" 2>&1 || true
  kubectl get events -A --sort-by=.lastTimestamp > "$dir/events.txt" 2>&1 || true
  kubectl -n "$NS" describe pods > "$dir/describe-pods.txt" 2>&1 || true
  df -h > "$dir/df.txt" 2>&1 || true
  for p in $(kubectl -n "$NS" get pods -o name 2>/dev/null); do
    kubectl -n "$NS" logs --all-containers "$p" > "$dir/$(basename "$p").log" 2>&1 || true
  done
  for ns in "$RUNTIME_NS" "$DATA_NS"; do
    for p in $(kubectl -n "$ns" get pods -o name 2>/dev/null); do
      kubectl -n "$ns" logs --all-containers "$p" > "$dir/$ns-$(basename "$p").log" 2>&1 || true
    done
  done
  log "logs in $dir"
}

run_tests() {
  log "running Playwright suite"
  local name=bezisync-e2e-runner-$RUNNER_TAG status=0
  # The runner reaches the cluster API by container name; agent charts are copied in.
  mkdir -p "$STATE/kube"
  sed "s#https://${HOST_IP}:${API_PORT}#https://${K3S}:6443#" "$KUBECONFIG" > "$STATE/kube/config"
  docker create --name "$name" --label "$LABEL" --network "$NET" \
    -e BASE_URL="http://${K3S}:30080" -e ADMIN_PASSWORD=e2e-admin-password -e CI=1 \
    -e KUBECONFIG=/e2e/.kube/config -e AGENT_NS="$AGENT_NS" -e DATA_NS="$DATA_NS" -e E2E_TAG="$BACKEND_TAG" -e CUSTOM_IMAGE="$CUSTOM_IMAGE" \
    "bezisync-local/e2e:$RUNNER_TAG" ${E2E_GREP:+--grep "$E2E_GREP"} "${SPECS[@]}" >/dev/null
  docker cp "$STATE/kube" "$name:/e2e/.kube" >/dev/null
  docker cp "$ROOT/deploy" "$name:/e2e/deploy" >/dev/null
  docker start -a "$name" || status=$?
  rm -rf "$STATE/report" "$STATE/test-results"
  docker cp "$name:/e2e/playwright-report" "$STATE/report" >/dev/null 2>&1 || true
  docker cp "$name:/e2e/test-results" "$STATE/test-results" >/dev/null 2>&1 || true
  docker rm "$name" >/dev/null
  return $status
}

prune_old_images() {
  local keep=":($BACKEND_TAG|$WEB_TAG|$RUNNER_TAG)$"
  docker images --format '{{.Repository}}:{{.Tag}}' | grep '^bezisync-local/' | grep -Ev "$keep" | xargs -r docker rmi -f >/dev/null 2>&1 || true
  # Only stale BeziSync builds: Debezium images stay cached (pruning them made every run re-pull).
  docker exec "$K3S" ctr -n k8s.io images ls -q | grep '^docker.io/bezisync-local/\(backend\|frontend\):' \
    | grep -Ev "$keep" | xargs -r docker exec "$K3S" ctr -n k8s.io images rm >/dev/null 2>&1 || true
}

SPECS=()
if [[ "${1:-}" == "down" ]]; then down; exit 0; fi
if [[ "${1:-}" == "quick" ]]; then
  QUICK=true
  shift
  SPECS=("$@")
  BACKEND_TAG=c-$(content_tag backend) WEB_TAG=c-$(content_tag frontend) RUNNER_TAG=c-$(content_tag e2e)
  TAG=$RUNNER_TAG
fi
ensure_cluster
build
if ! $QUICK || ! deploy_in_place; then deploy; fi
prune_old_images
if run_tests; then log "PASSED ($TAG)"; else collect_logs; log "FAILED ($TAG) — report in $STATE/report"; exit 1; fi
