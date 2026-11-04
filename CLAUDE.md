# CLAUDE.md — BeziSync Engineering Guide

Entry point for anyone (human or agent) changing **BeziSync**. Read it first; keep the code
production-grade. Area rules live in [`.rules/`](.rules/) — read the relevant file before
working in that area.

> **Self-updating mandate.** A new pattern, core dependency, or change to how the project is
> built/run/tested updates the matching `.rules/*.md` (or this file) **in the same change**.
> **Guide parity.** `AGENTS.md` and `CLAUDE.md` are identical apart from their filename.

## Rules index (`.rules/`)

- [`backend.md`](.rules/backend.md) — Go layout and toolchain, config, db/migrations (goose), auth/RBAC, API conventions, connectors, controller, runtimes.
- [`frontend.md`](.rules/frontend.md) — layout, `bun`, routing, server state, design system, UX rules.
- [`testing.md`](.rules/testing.md) — `make verify`, `make e2e`, what to test, definition of done.
- [`running-locally.md`](.rules/running-locally.md) — dev database, API + Vite dev servers, the e2e cluster.
- [`releases.md`](.rules/releases.md) — workflows (CI, push, deploy), published images/charts, changelog fragments, releases.

## 1. What BeziSync is

A **self-service control plane for Debezium Server** (and, optionally, **Flink CDC** for
lakehouse and analytics destinations). Admins connect runtimes and set guardrails once; any engineer — Debezium expert or not — creates and operates change-data-capture
pipelines safely. Its value is removing Debezium's real pain:

- **Preflight checks with exact fixes** before anything deploys (per connector, per cloud provider).
- **Source protection**: WAL/binlog/oplog retention visibility, heartbeats, snapshot windows.
- **One-click operations**: snapshots, offsets, pause/resume/restart, versions + rollback.
- **Honest health**: never green without fresh evidence; Unknown is grey.
- **Encrypted, write-only secrets**, RBAC, SSO configured in Settings, audit, approvals and policies.
- **Personal data chosen per pipeline**: mask, remove or keep each column, never a silent leak.
- **AI agents over MCP**, with exactly their person's permissions and never a credential.

## 2. Architecture

```
Browser / CLI ──▶ api (Go)       ─┐
                                  ├─ PostgreSQL: metadata, jobs, metrics, audit, Debezium offsets (JDBC store), Flink positions
            controller (reconcile ┘
            loops + job queue) ──▶ runtime driver ──▶ Docker / Kubernetes ──▶ Debezium Server or Flink CDC (1 per pipeline)
                                         ▲ remote runtimes: outbound-only agent
```

- One Go binary (`backend/`, `cmd/bezisync`) provides the `api`, `controller` and `agent` roles
  (`bezisync api|controller|agent|migrate`) and the CLI; the image is distroless. No external
  queue: Postgres is the queue. The API also serves MCP (`/api/v1/mcp`) for AI agents, whose
  tools call the REST API in process as the caller.
- Debezium **3.7.0.Final** is the default image; offsets/schema history use its JDBC store.
- Flink CDC (**3.7.0** on Flink **2.2.1**) is an optional second engine, on once an Admin registers
  an image (built in [daun-gatal/image-repo](https://github.com/daun-gatal/image-repo), `flink-cdc/`).
  Each pipeline runs its own JobManager + TaskManagers; BeziSync's helper (`bezisync engine
  flink-cdc …`, copied in from the BeziSync image) runs the job and stores encrypted saved positions
  in the state database.
- Frontend (`frontend/`) is a React SPA served by nginx, talking only to `/api/v1`.
- `bezisync remote …` is the CLI for users and CI (API tokens); `deploy/` holds the Helm charts
  (control plane, agent), the compose demo stack, the e2e fixtures and an optional AppArmor profile
  (`deploy/apparmor/`). Pull requests target `preview`; `main` only moves by releases. Four
  workflows: `ci.yml` and `helm.yml` (every check on pull requests; the e2e suite runs locally), `build-branch.yml`
  (pre-release images and charts `-pre` from a branch) and `release.yml` (from preview, after an
  approval: images, charts, CLI and the GitHub Release) — see `.rules/releases.md`.
- `tools/` is a small Go module (no dependencies, outside the image build contexts) with the
  release tool (`scripts/release.sh`), the licence-file generator (`make licenses`) and the demo smoke test. There is
  no Python anywhere in the repository.
- User-facing docs live in [`docs/`](docs/README.md): the guide, the generated preflight-check
  reference (`bezisync check-docs`) and the [threat model](docs/security/threat-model.md) — a
  user-visible or security-relevant change updates them in the same change.

## 3. Golden rules

1. **Never weaken security.** Secrets are encrypted at rest, write-only, never logged or returned.
2. **Thin routers/components.** Logic lives in `services/`, connectors, controller, or hooks.
3. **Type everything.** `golangci-lint` and `tsc` strict pass; no untyped `any` in API or domain types without reason.
4. **Every behavioural change ships with tests.** A PR passes the full `make verify` and, before
   merge, `make e2e` (run by a maintainer; contributors run the specs they touch where they can).
   Maintainers also keep each of their own commits passing `make verify-quick` and `make e2e-quick SPECS=…`;
   see `.rules/testing.md`.
5. **Health is evidence-based.** Missing or stale data is Unknown/stale, never healthy.
6. **State-aware safety.** Destructive or disruptive actions show their effect and are audited.
7. **Keep guides current** (self-updating mandate) and `AGENTS.md` == `CLAUDE.md`.
