# Running locally

Prerequisites: Docker with BuildKit (the `buildx` plugin: Docker Desktop, or docker-ce 23 and
later; the Dockerfiles cross-compile with `--platform=$BUILDPLATFORM`, which the legacy builder
cannot do), Go (the version in `backend/go.mod`), `golangci-lint` v2, `bun`, Node 24, `kubectl`,
`helm`.

```bash
make dev-db                                   # disposable Postgres; prints the URL
export BEZISYNC_DATABASE_URL=<that url> BEZISYNC_BOOTSTRAP_ADMIN_PASSWORD=admin-password
cd backend && go run ./cmd/bezisync migrate && go run ./cmd/bezisync api   # API on :8000
cd backend && go run ./cmd/bezisync controller                   # reconcile loop (another terminal)
cd frontend && bun run dev                                        # UI on :8080 (proxies /api)
```

Sign in as `admin@bezisync.local` with the bootstrap password.

`make demo` runs the evaluation stack from `deploy/compose` (BeziSync + sample shop database +
Kafka; first start creates the Sandbox project via `BEZISYNC_SANDBOX_*`). `make demo-smoke`
brings it up, streams through the sandbox template and removes it — run it after changing the
compose file, bootstrap or the Docker runtime.

The full stack (API, controller, web, Postgres, data plane, connectors) runs in the e2e cluster:
`make e2e` leaves it running; `make e2e-down` deletes it. With a remote Docker daemon set
`BZ_DOCKER_HOST_IP` to the address where its published ports are reachable.
