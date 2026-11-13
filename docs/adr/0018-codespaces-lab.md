# 0018 — A private lab in the visitor's own GitHub Codespace, provisioned by the Sandbox

- **Status:** Accepted
- **Date:** 2026-11-13
- **Scope:**
  - `.devcontainer/lab/` (new): devcontainer, compose override, start script, visitor guide.
  - `internal/services/bootstrap.go` (`EnsureSandbox`) and `internal/config` (new optional `BEZISYNC_SANDBOX_*` settings).
  - `scripts/lab-smoke.sh` (new), `tools/cmd/demo-smoke` (a lab mode) and `ci.yml`.
  - The README badge, and the guide's demo section and settings table.
  - A `minor` changelog fragment.

## Context

BeziSync had a hosted lab (`bezisync-lab.dglabs-ai.dev`): one shared instance with about 25
sources and sinks on the homelab cluster, behind Google sign-in. It was deleted on 2026-11-13,
together with its cluster resources, to stop hosting and exposing it. That left visitors two ways
to try BeziSync: install it, or run the Compose demo (`deploy/compose`) on their own Docker.

The owner wants what CHouse UI got the same day (its ADR 0032): a button in the README that
opens **a private BeziSync in the visitor's own GitHub Codespace**. The visitor is admin and
the lab costs the project nothing. Three constraints set the shape:

- **It must be released.** A Codespaces link opens the default branch, `main`, and `main` only
  moves through `release.yml`. A release needs a changelog fragment, and CI refuses a fragment
  unless an image changes. A lab made only of devcontainer, Compose and CI files would sit on
  `preview` until some unrelated release.
- **It must fit a free codespace.** The free 2-core / 8 GB machine has to hold the control plane,
  its Postgres, the sample databases, Kafka and one Debezium Server per running pipeline. The old
  lab's full catalogue doesn't fit.
- **It must look like a real install, not an empty one:** several sources (one of them not ready
  for CDC, so preflight has something to fix), several destinations, templates, and teammates in
  each role.

BeziSync already has most of this as a product feature. When the `BEZISYNC_SANDBOX_*` settings
are set, `EnsureSandbox` creates a **Sandbox** project with a PostgreSQL source, a Kafka sink, a
local Docker runtime and one template. The Compose demo uses it. What it lacks is everything
beyond that first pair.

### Goals

- One click from the README gives a working BeziSync with live change data, where the visitor is
  admin, in a few minutes, on the visitor's free Codespaces quota.
- Variety: PostgreSQL and MySQL sources plus one source that preflight rejects with its fixes;
  Kafka, a JDBC warehouse and an HTTP endpoint as destinations, each with a template; teammates
  as Editor, Operator and Viewer.
- The visitor creates and deploys the first pipeline, which is the journey the demo is built
  around.
- No fixed secrets: the master key and every password are random per lab.
- CI proves the lab starts and streams, before a release puts it on `main`.

### Non-goals

- A public, always-on lab, and any telemetry about visitors.
- The old lab's full catalogue (SQL Server, Oracle, MongoDB, Flink CDC, lakehouse sinks).
- Changes to the Helm charts or the Compose demo's defaults.
- Recreating Sandbox content after a visitor deletes it.

## Decision

### D1 — The Sandbox provisions the lab's extras (product change)

`EnsureSandbox` gains optional settings. Each one, when set, adds one piece to the Sandbox it
creates:

| Setting | Adds |
|---|---|
| `BEZISYNC_SANDBOX_MYSQL_HOST`, `_PORT` (3306), `_USER` (`cdc_reader`), `_PASSWORD` | source `demo-mysql` (MySQL with GTIDs) |
| `BEZISYNC_SANDBOX_LEGACY_HOST`, `_PORT` (5432), `_DATABASE` (`legacy`), `_USER` (`app_user`), `_PASSWORD` | source `legacy-postgres`, described as not ready for CDC on purpose (`wal_level=replica`) |
| `BEZISYNC_SANDBOX_WAREHOUSE_HOST`, `_PORT` (5432), `_DATABASE` (`warehouse`), `_USER` (`bz_writer`), `_PASSWORD` | JDBC sink `demo-warehouse` (PostgreSQL, schema `public`) and template **Shop → Warehouse (demo)** |
| `BEZISYNC_SANDBOX_HTTP_URL` | HTTP sink `demo-http` and template **Shop → HTTP (demo)** |
| `BEZISYNC_SANDBOX_TEAM_PASSWORD` | users `editor@`, `operator@` and `viewer@bezisync.local` (names Erin Editor, Oscar Operator, Vera Viewer), each a member of the Sandbox with that role |

Rules:

- **Same path as the existing pair.** Connections go through `ValidateConnection`, their secrets
  are encrypted (`EncryptOrNil`) and they are marked `Managed`, exactly like `demo-shop` and
  `demo-kafka`. Templates use the same spec shape as `demo-shop-to-kafka`.
- **Created with the Sandbox, never after.** Everything is created in the same transaction as the
  project, only on the run that creates it. An existing Sandbox is left alone. A visitor's
  deletions are never undone, and an older demo stack is not changed on upgrade.
- **Unset means absent.** Without these settings the Sandbox is exactly what it is today, so the
  Compose demo and the Helm chart are unchanged.
- **Teammates are created only when their email is free.** If a user with that email already
  exists, it is not touched and no membership is added. The team password must pass the same
  weak-password refusal as the bootstrap admin's.
- **Audited** like the project: one `project.created` event as today, plus `member.upserted` events
  for the teammates.

### D2 — The lab: `.devcontainer/lab/`

- **`devcontainer.json`:** Ubuntu base image with docker-in-docker, 2 CPUs / 8 GB, port 8080
  forwarded and opened in the browser. It runs `up.sh` on every start and opens `WELCOME.md`.
- **`compose.yml`:** an override applied on top of `deploy/compose/docker-compose.yml`, so the lab
  *is* the demo plus extras:
  - `lab-mysql`, MySQL 8.4 with GTIDs, a `shop` database and live traffic;
  - `lab-legacy`, PostgreSQL 17 with `wal_level=replica`;
  - `lab-warehouse`, PostgreSQL 17;
  - `lab-http-echo`;
  - the `BEZISYNC_SANDBOX_*` settings of D1, and random secrets from `.env`.

  Init SQL is written by the containers themselves, as in the demo, so the stack also runs against
  a remote Docker daemon. Images are pulled (`ghcr.io/daun-gatal/bezisync:latest`, overridable by
  `BEZISYNC_IMAGE` / `BEZISYNC_WEB_IMAGE`), never built in the codespace.
- **`up.sh`:**
  - On the first run, it writes a gitignored `.env` (mode 600) with a random
    `BEZISYNC_MASTER_KEY`, admin password and team password.
  - It sets `BEZISYNC_PUBLIC_URL` to the codespace's forwarded URL (or `LAB_URL`, else
    `http://localhost:8080`).
  - It runs `docker compose up --wait` and prints a private sign-in banner.
  - It is safe to run again. A leftover lab without its `.env` is removed first, because data
    encrypted with a lost master key is unreadable.
- **`WELCOME.md`:** what's inside, the sign-in, a first pipeline in a few steps, the preflight
  fixes on `legacy-postgres`, signing in as each teammate, and how to delete the codespace.
- **Fixed internal service passwords stay.** The demo's sample databases keep their fixed internal
  passwords, as they do today: they are reachable only on the lab's Docker network, and only
  port 8080 is published.

### D3 — CI: start the lab and stream through it

`scripts/lab-smoke.sh` brings up the demo plus the lab override with images built from the
change (as `demo-smoke.sh` does) and random secrets. It then runs `tools/cmd/demo-smoke` in a new
`-lab` mode, which checks:

- the admin signs in with the generated password;
- the Sandbox has 5 connections and 3 templates;
- each teammate signs in and has the expected role;
- a pipeline created from **Shop → Kafka** streams live orders;
- a pipeline created from **Shop → Warehouse** writes rows;
- preflight on `legacy-postgres` fails with the `wal_level` fix.

`ci.yml` runs it in the job that runs the demo smoke test, under the same conditions (the demo
or an image changes), plus whenever `.devcontainer/lab/**` changes. `make lab-smoke` runs it
locally.

### D4 — Docs and release

- The README gets an **Open in GitHub Codespaces** badge and a *Try it* section.
- The guide's demo section gains a *Run your own lab* subsection, and the settings table lists
  the new `SANDBOX_*` settings.
- A `minor` fragment (0.9.0) describes the Sandbox extras. The release then moves `main`, and the
  badge works from that moment.

### Implementation plan

1. `feat(sandbox)`: config settings, `EnsureSandbox` extras, backend tests (`TestSandboxBootstrap`
   extended plus new cases), contracts (`services`, `config`), guide settings table, fragment.
2. `feat(lab)`: `.devcontainer/lab/` (devcontainer, compose override, `up.sh`, `WELCOME.md`,
   contract), `.gitignore`.
3. `ci(lab)`: `demo-smoke -lab`, `scripts/lab-smoke.sh`, `make lab-smoke`, `ci.yml`, contracts
   (`tools/cmd`, `scripts`), `.rules/testing.md` and `releases.md`.
4. `docs`: README badge and section, the guide's *Run your own lab*, the ADR index.

### Verification plan

- `make verify` (backend lint and tests, tools, frontend, licences, contracts),
  `make demo-smoke` and `make lab-smoke`.
- The real `devcontainer.json`, with the official Dev Containers CLI in a nested Docker daemon
  (the same docker-in-docker setup a codespace uses): first start, restart, lost `.env`.
- The UI in a browser at 390/768/1280 px: admin and each teammate.

**Edge-case register:**

| ID | Case | Expected | Covered by |
|---|---|---|---|
| E1 | No extra settings | The Sandbox is the same as today: 2 connections, 1 template, no users | backend test |
| E2 | All settings | 5 connections, 3 templates, 3 teammates with their roles, all secrets encrypted | backend test |
| E3 | Second run, or Sandbox already exists | Nothing created or changed | backend test |
| E4 | Teammate email already taken | That user is untouched, gets no membership; the rest are created | backend test |
| E5 | Weak team password (`admin`, `password`) | Refused like the bootstrap admin's | config test |
| E6 | Invalid extra (for example a bad port) | `EnsureSandbox` fails; migrate reports it and nothing is half-created (one transaction) | backend test |
| E7 | Visitor deletes `demo-mysql`, then restarts | Not recreated | backend test (E3) |
| E8 | Lab restart (`up.sh` again) | Same secrets, same data, nothing duplicated | `lab-smoke.sh` second start plus devcontainer run |
| E9 | `.env` lost, volumes kept | Old lab removed, fresh lab starts | devcontainer run |
| E10 | Teammate permissions | A Viewer can't deploy (403); an Editor can | `demo-smoke -lab` |
| E11 | `legacy-postgres` preflight | Fails with the `wal_level=logical` fix | `demo-smoke -lab` |
| E12 | Browser on the forwarded URL | Sign-in works with `PUBLIC_URL` set to that URL | devcontainer run with `LAB_URL` |

## Consequences

- Visitors get a private, admin BeziSync with real CDC in a few minutes, at no cost to the
  project. They pay with a GitHub account, a cold start, and an editor window around the app.
- The Compose demo and Helm installs can opt in to the same extras.
- The lab goes live only when this change is released. Until then, the badge on `main` would open
  plain VS Code, so the README badge ships in this same PR, and `main` gets it only with the
  release.
- The Sandbox now creates users when asked. That adds a threat-model note: a known team password
  set by the operator, the same class as the bootstrap admin password.
- The real Codespaces path (the `app.github.dev` origin and the auto-open tab) can't run in CI and
  needs a manual click after the release.

## Alternatives considered

- **An external seeder through the API (what CHouse UI did).** Rejected by the owner: it changes no
  image, so it can't be released, and BeziSync's `main` only moves by releases.
- **Link the badge to the `preview` branch.** Rejected by the owner: visitors would run unreleased
  code.
- **Deploy a pipeline from `EnsureSandbox`.** Rejected: it runs in `migrate`, which has no Docker
  socket and runs before the sample databases are ready. Deploying is the visitor's first step
  anyway.
- **Recreate missing Sandbox pieces on every start.** Rejected: it would undo a visitor's
  deletions.
- **The old lab's full catalogue.** Rejected: it doesn't fit a free 2-core / 8 GB codespace (owner
  chose the lean lab).
