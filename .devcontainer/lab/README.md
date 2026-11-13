# .devcontainer/lab

## Overview
The BeziSync lab (ADR 0018): a private BeziSync with sample databases and live change data, in the
visitor's own GitHub Codespace, opened from the README's "Open in GitHub Codespaces" badge. It is
the compose demo (`deploy/compose`) plus a MySQL shop, a PostgreSQL that isn't ready for change data
capture, a PostgreSQL warehouse and an HTTP endpoint, which the image's Sandbox registers through
`BEZISYNC_SANDBOX_*`. It pulls the published images and changes nothing in them. `make lab` runs
it on any Docker; `scripts/lab-smoke.sh` tests it in CI.

## Interface
- `[devcontainer.json]` — Ubuntu with docker-in-docker on a 2-CPU / 8 GB codespace; forwards and opens port 8080; runs `up.sh` on every start and opens `WELCOME.md`.
- `[up.sh]` — starts the lab and prints the sign-in banner. Reads `BEZISYNC_IMAGE`, `BEZISYNC_WEB_IMAGE`, `LAB_URL`, `LAB_BUILD` (1: build this checkout), `BEZISYNC_DEMO_PORT`.
- `[down.sh]` — removes the lab: containers, the connectors its pipelines started, volumes, `.env` and `.state/`.
- `[compose.yml]` — the override applied on top of `deploy/compose/docker-compose.yml` (project `bezisync-lab`): `lab-mysql`, `lab-mysql-traffic`, `lab-legacy`, `lab-warehouse`, `lab-http-echo`, and the Sandbox settings for `migrate`, `api` and `controller`.
- `WELCOME.md` — the visitor guide the codespace opens.

## Invariants
- No fixed secrets reach BeziSync: the first `up.sh` writes `.env` (mode 600, git-ignored) with a random master key, admin password and team password, and `compose.yml` refuses to start without them (`${…:?}`). The sample databases keep fixed passwords, reachable only on the stack's network.
- `BEZISYNC_PUBLIC_URL` follows the browser's address: `LAB_URL`, else the codespace's forwarded URL, else `http://localhost:8080`, rewritten on every start.
- `up.sh` is safe to run again (same secrets, same data); a lab whose `.env` is gone is removed first with `down.sh`, because its data is encrypted with a lost key.
- `.state/login.txt` and `.state/banner.txt` hold passwords: mode 600, recreated on every start.
- `migrate` waits for the lab's databases, because the Sandbox and its extras are created on the first migrate and never after.

## Side Effects
- Docker: the `bezisync-lab` compose project (images pulled or built, volumes, port 8080 published), the `bezisync-demo` network, and one Debezium Server container per deployed pipeline.
- Files: `.env` and `.state/` in this directory.

## Verification
- Run: `make lab-smoke` (needs Docker; `BZ_DOCKER_HOST_IP` for a remote daemon); `shellcheck -S warning .devcontainer/lab/*.sh`.
