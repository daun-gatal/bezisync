# demo-smoke

## Overview
The Go half of the demo smoke test: `scripts/demo-smoke.sh` starts the compose demo stack and runs this command against it to prove the sandbox works end to end. It signs in, creates a pipeline from the sandbox template, deploys it, waits until it streams, reads order events back from Kafka and deletes it again. With `-lab`, `scripts/lab-smoke.sh` runs it against the Codespaces lab (ADR 0018) instead.

## Interface
- `[main.go:main]` — `go run ./cmd/demo-smoke [-lab] URL`; exits 2 on wrong usage, 1 with a `[demo]` message on failure.
- `[main.go:run]` — the scenario, using `apiclient` (`[../../apiclient/apiclient.go:New]`).
- `[lab.go:runLab]` — the lab scenario: passwords from `BEZISYNC_SMOKE_ADMIN_PASSWORD` and `BEZISYNC_SMOKE_TEAM_PASSWORD` (both required).
- `[main.go:wait]` — polls a condition every 3 s until done, an error, or the timeout (which reports the last state when the caller gives a describer, as the streaming wait does).

## Invariants
- Signs in as the demo stack's seeded admin (`admin@bezisync.local` / `bezisync-demo`), finds the project with slug `sandbox` and `is_sandbox` in `GET /auth/me` (none is an error), and requires exactly one template in it.
- The pipeline (`orders-demo`, source connection `demo-shop`, table `shop.orders`) must reach `streaming` + `healthy` within 15 min (lifecycle `failed` aborts at once), return at least one record from `GET …/events?table=shop.orders&limit=50` within 3 min (event read errors are retried), and reach `deleted` within 4 min after the delete (an API error while waiting aborts). It never signs out.
- `-lab` requires exactly the connections `[lab.go:labConnections]` and 3 templates in the Sandbox; each teammate in `[lab.go:labTeam]` must sign in with its role, and the viewer's pipeline create must be refused with 403. Preflight on `legacy-postgres` must fail `pg_wal_level` with a `wal_level = logical` fix (`[lab.go:checkLegacyPreflight]`). The editor then deploys `shop.orders` from `demo-shop` to Kafka (events read back) and from `demo-mysql` to the warehouse, each to `streaming` + `healthy` (`[lab.go:streamTo]`). Its pipeline names carry a per-run suffix (`[lab.go:runName]`), and every pipeline it creates is deleted again, also when a check fails.

## Side Effects
- HTTP against the given server: creates, deploys and deletes one pipeline in the sandbox project (three with `-lab`, one of them never deployed) (which starts and stops a Debezium Server container in the demo stack). Prints progress to stdout.

## Verification
- Run: No co-located tests; `cd tools && go vet ./cmd/demo-smoke/`, `make demo-smoke` runs it against a real demo stack and `make lab-smoke` against the lab (both need Docker).
