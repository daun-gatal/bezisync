# agent

## Overview
`bezisync agent`: the outbound-only process that runs next to a remote Docker host or Kubernetes cluster for runtimes in agent mode. Each poll it reports what runs locally (phase, health, metrics or Flink CDC status, capacity), receives the desired workloads, converges the local runtime driver off the poll loop, and answers on-demand requests (logs, connection checks, HTTP reads). It never listens on a port. The control-plane side of the protocol is `services.AgentDriver` (see [services](../services/README.md)); the local driver comes from [runtime](../runtime/README.md).

## Interface
- `[agent.go:New]` — builds the agent: a local driver via `services.BuildDriver`, an HTTP client (30 s timeout) whose transport adds `Authorization: Bearer <token>`; `verifyTLS=false` skips certificate checks.
- `[agent.go:NewWithDriver]` — same around an existing `runtime.Driver` and client (tests).
- `[agent.go:Agent]` — state: last desired workloads, pending results, owner runtime id, `synced` flag.
- `[agent.go:Run]` — starts `convergeLoop` and polls until the context is cancelled.
- `[agent.go:Poll]` — one `POST /api/v1/agent/poll`; returns the delay before the next poll.
- `[agent.go:Report]` — the poll body: `version`, `kind`, `features` (`health_port`, `runtime.FeatureFlinkCDC`), `capacity`, `local_test`, `workloads`, `results`.
- `[agent.go:Converge]`, `[agent.go:ApplyDesired]` — apply desired workloads now and delete managed ones no longer desired.
- `[agent.go:Answer]` — runs requests `logs` (tail default 300, optionally per `component` via `runtime.ComponentLogs`), `check` (`services.RunCheck`, provider default `self_hosted`), `http_get` (4 s timeout); anything else errors `unknown op`.
- `[agent.go:Owner]`, `[agent.go:PendingResults]` — accessors.

## Invariants
- Never converge (and so never delete) before the first desired state arrives: `[agent.go:convergeLoop]` applies only when `synced`, which `[agent.go:Poll]` sets on the first successful answer; deletion in `[agent.go:apply]` is skipped while `owner` is unknown.
- Converging runs off the poll loop (`[agent.go:convergeLoop]`, woken by a change or every 30 s) so image pulls never stall reporting; `Poll` only signals `changed` when the desired set differs (`reflect.DeepEqual`) from the last seen.
- Workloads in `Report` are the desired ids plus, once the owner is known, `Driver.ListWorkloads(owner)`, sorted; an `Observe` error reports `phase: pending` with the reason truncated to 200 runes (`[agent.go:truncate]`).
- For running workloads the agent probes `/q/health` on the workload's health port (default 8080) and either Debezium metrics (`:9404/metrics`) or, for Flink CDC (`Workload.IsFlink`), `/bz/status` reported as `status`; each probe has a 4 s timeout.
- `[agent.go:localTest]` passes `flink_cdc` only when the driver's test details carry it as a bool (Kubernetes).
- Request errors are returned as `ok: false` with the message truncated to 500 runes; results are cleared when a poll answer arrives and re-filled by `Answer`, and a pending result shortens the next poll to 200 ms.
- Poll backoff: unreachable or an undecodable answer 5 s, HTTP 401 or non-2xx 2 s (logged), otherwise the server's `poll_seconds` (default 2). The response body is read up to 64 MiB.
- Invalid desired workloads are skipped with a warning (`[agent.go:decodeDesired]`), keyed by `ShortID`.

## Side Effects
- Network: `POST <url>/api/v1/agent/poll` with the bearer token; HTTP GETs into workloads through the driver; connection checks dial sources/sinks from the agent's network (`services.RunCheck`).
- Local runtime: `Driver.Apply` / `Driver.Delete` create and remove containers or Kubernetes objects.
- Goroutine: `convergeLoop` with a 30 s ticker, stopped with the context.
- Logs via `bezisync.agent`; no DB access, no env vars read here.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/agent/"`
