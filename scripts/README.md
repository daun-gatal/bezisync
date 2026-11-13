# scripts

## Overview
Bash entry points for the repository's development, test and release workflows. The `Makefile` and the GitHub workflows call them (`make dev-db`, `make migration`, `make e2e*`, `make demo-smoke`, `make lab-smoke`; `ci.yml` runs `dev-db.sh`, `demo-smoke.sh`, `lab-smoke.sh` and `release.sh`, `build-branch.yml` and `release.yml` run `release.sh`, `release.yml` runs `build-cli.sh`); the logic of the release and demo tools lives in the Go module under `tools/`, which two of these scripts only wrap.

## Interface
- `[dev-db.sh:NAME]` — `dev-db.sh` starts or reuses the `bezisync-dev-pg` Postgres 17 container (logical WAL, 300 connections) on `BZ_DEV_PG_PORT` (25432) and prints its URL; the Makefile uses it as the default test database.
- `[new-migration.sh:next]` — `new-migration.sh "what it does"` creates the next numbered goose SQL file in `backend/internal/migrate/sql/` with empty Up/Down sections and prints its path (`make migration m=…`).
- `[e2e.sh:ensure_cluster]`, `[e2e.sh:build]`, `[e2e.sh:deploy]`, `[e2e.sh:run_tests]`, `[e2e.sh:down]` — `e2e.sh` builds the images, deploys BeziSync and the data-plane fixtures into a disposable k3s-in-Docker cluster and runs the Playwright suite; `quick SPECS…` reuses the cluster and upgrades in place (`[e2e.sh:deploy_in_place]`, images tagged by `[e2e.sh:content_tag]`); `down` removes everything.
- `[demo-smoke.sh:cleanup]` — `demo-smoke.sh` brings up the compose demo stack, runs `go run ./cmd/demo-smoke` from `tools/` against it and tears it down (logs printed on failure).
- `[lab-smoke.sh:cleanup]` — `lab-smoke.sh` starts the Codespaces lab (ADR 0018) with images built from the checkout through its own `.devcontainer/lab/up.sh`, starts it a second time (a restart must change nothing), checks that its `.env` and sign-in files are private, runs `go run ./cmd/demo-smoke -lab` with the generated passwords, checks that orders reached the warehouse, and removes it with `.devcontainer/lab/down.sh`. It refuses to run while a lab exists in the same Docker.
- `[build-cli.sh:TARGETS]` — `build-cli.sh VERSION` cross-compiles `backend/cmd/bezisync` for six OS/arch pairs into `dist/` archives with `LICENSE`, `NOTICE`, `LICENSES.txt` and a `checksums.txt`.
- `release.sh` — runs `go run ./cmd/release "$@"` from `tools/` (see `tools/cmd/release`).

## Invariants
- Every script runs under `set -euo pipefail` and resolves paths from its own location, so it works from any directory.
- Docker resources `e2e.sh` and `dev-db.sh` create carry the label `app.bezisync.io/e2e=true` (`[e2e.sh:LABEL]`, `dev-db.sh`), and `[e2e.sh:down]` removes only containers, networks and volumes with that label (the dev database included) plus `bezisync-local/*` images and `.e2e/`.
- `[e2e.sh:prune_old_images]` keeps the current backend, web and runner tags and prunes only its own `bezisync-local/(backend|frontend|e2e)` builds, so Debezium images and an `E2E_FLINK_IMAGE` build stay cached between runs.
- `BZ_DOCKER_HOST_IP` replaces `localhost` wherever a script publishes or reaches a port (remote Docker daemons).
- `[build-cli.sh:TARGETS]` builds with `CGO_ENABLED=0 -trimpath` and stamps `internal/version.Version`; its platform list matches `[../tools/licenses/licenses.go:Platforms]` so the shipped licence file covers every archive.
- `new-migration.sh` numbers the file one above the highest existing `NNNNN_*.sql`; the new migration also needs `internal/migrate/testdata/schema.txt` updated (the script's own instructions).

## Side Effects
- Docker: `dev-db.sh` (the `bezisync-dev-pg` container), `e2e.sh` (k3s container `bezisync-e2e-k3s`, network `bezisync-e2e`, volume, images, a Playwright runner container; Debezium and sink images pulled and imported into k3s and the DinD fixture; namespaces `bezisync-e2e*` and a Helm release in the cluster), `demo-smoke.sh` (compose stack, its volumes and two `bezisync-local/demo-*:smoke` images, plus connector containers on the `bezisync-demo` network), `lab-smoke.sh` (the `bezisync-lab` compose project, its volumes, two `bezisync-local/lab-*:smoke` images and connector containers; it also writes and deletes `.devcontainer/lab/.env` and `.state/`).
- Files: `e2e.sh` writes `.e2e/` (kubeconfig, report, test results, `logs-<tag>/` on failure); `build-cli.sh` deletes and recreates `dist/` and uses a temp dir; `new-migration.sh` writes one file under `backend/internal/migrate/sql/`.
- Environment read: `BZ_DOCKER_HOST_IP`, `BZ_DEV_PG_PORT`, `BZ_E2E_K8S_PORT`, `E2E_TAG`, `E2E_GREP`, `BEZISYNC_DEMO_PORT` for `lab-smoke.sh` too, `E2E_FLINK_IMAGE` (an Admin-built Flink CDC image already in the Docker daemon; `e2e.sh` imports it into k3s and passes it to the runner so the Flink CDC journeys run), `BEZISYNC_DEMO_PORT`.

## Verification
- Run: `shellcheck -S warning scripts/*.sh` (what `ci.yml` runs); the scripts themselves are exercised by `make e2e`, `make demo-smoke`, `make lab-smoke` and the release workflow.
