# controller

## Overview
`bezisync controller`: the leader-elected loop that turns the desired state the API writes into running workloads and evidence. Each tick it prepares source/sink artifacts, applies each deployed pipeline's workload through its runtime driver, observes health and metrics, derives the verdict with `domain.Derive`, and writes `pipelines.status`; it then evaluates alerts, runs queued jobs (`restart`, `cleanup`, `snapshot`, `notify`) and periodic maintenance. It also holds the logic behind `bezisync uninstall`. Business rules come from [services](../services/README.md); drivers from [runtime](../runtime/README.md).

## Interface
- `[controller.go:Main]` / `[controller.go:New]` / `[controller.go:Controller]` — the process; tick from `config.Get().ControllerTickSeconds`.
- `[controller.go:RunForever]` — competes for `pg_try_advisory_lock(LeaderLock)` and leads while holding it; `[controller.go:LeaderLock]` is shared with the Python controller.
- `[controller.go:TickOnce]` — one round: reconcile every deployed pipeline, `EvaluateAlerts`, `RunJobs(ctx, 10)`, maintenance every 10 min. On the first tick after start it also logs a warning naming running workloads whose config hash would change (`[controller.go:Controller.reportHashDrift]`).
- `[controller.go:Reconcile]` — converge and record one pipeline.
- `[controller.go:EvaluateAlerts]` — `services.EvaluateProject` per project, one transaction each.
- `[controller.go:CarriedStatus]` — status keys written outside the observe loop that each tick keeps.
- `[controller.go:ParseHealth]` — SmallRye health: 200 + `UP` up, 503 down, anything else unknown (nil).
- `[controller.go:EnsureStateSchema]` — creates Debezium's state schema (`compiler.StateSchema`) in the state database.
- `[controller.go:CanaryPoll]`, `[controller.go:CanaryTailTimeout]`, `[controller.go:Identity]` (hostname-pid, used in job locks).
- `[jobs.go:RunJob]` — dispatches `restart`, `cleanup`, `snapshot`, `notify`; `[jobs.go:RunJobs]` claims and runs due jobs; `[jobs.go:Defer]` reschedules without counting an attempt.
- `[uninstall.go:Plan]` / `[uninstall.go:Uninstall]` with `[uninstall.go:PlanItem]`, `[uninstall.go:UninstallEntry]` — list, then remove, every live pipeline's workload, source artifacts and offsets.
- `[flinkcdc.go:observeFlink]` — Flink CDC evidence from the helper's `/q/health` and `/bz/status`, mapped onto `domain.Metrics` (`flinkMetrics`), a DOWN reason (`flinkDown`) and `status.flink` (`flinkStatus`).

## Invariants
- One leader: the lock is held on a dedicated connection pinged with `SELECT 1` every tick (`[controller.go:Controller.lead]`); losing it ends leadership.
- `Reconcile` locks the pipeline row with `FOR UPDATE SKIP LOCKED` and only acts on non-deleted pipelines with a deployed version (`[controller.go:Reconcile]`); at most 8 pipelines reconcile concurrently and a panic is recovered and logged (`[controller.go:TickOnce]`).
- Artifacts are prepared once per deployed version (`artifacts.prepared_version_id`); a sink result with outcome `failed` fails the apply (`[controller.go:Controller.prepareIfNeeded]`). Flink CDC refreshes the tables' columns first (`services.RefreshFlinkColumns`).
- Health is evidence-based: the verdict comes only from `domain.Derive`; an apply error is prepended as `apply_failed` unless streaming, and a pipeline still deploying 2 min after `DeployedAt` with an apply error becomes failed/down.
- `[controller.go:Controller.storeStatus]` rebuilds `status` from scratch keeping only `CarriedStatus` keys; any new key written outside the observe loop must be added there or the next tick drops it. A lifecycle change records a `pipeline.<lifecycle>` event with `NotAudit: true` (first 3 reasons). `source_impact`, `flink` and the position note are written after it on the same tick.
- `[controller.go:Controller.checkPosition]` reads the stored Postgres LSN after each fresh source probe, writes `last_lsn`, and sets `position_regressed` when it is lower than the previous `last_lsn`. `last_lsn` is not in `CarriedStatus`, so `storeStatus` drops it on the next tick and `position_regressed` never fires — known issue, see [ARCHITECTURE.md › Known issues](../../../ARCHITECTURE.md#known-issues).
- Throttles: source probe every 30 s (`probeEvery`), metric samples at most every 15 s (`[controller.go:Controller.recordSample]`, rate from counter deltas, negative deltas dropped), schema check every `SchemaCheckSeconds` and only while streaming or snapshotting for sources with `TracksSchema` (`[controller.go:Controller.trackSchema]`), canary (policy `DeliveryCanary`, streaming, both connectors support it) every `CanaryIntervalSeconds`, polled every `CanaryPoll`, `ok: false` after `CanaryTimeoutSeconds`; a new canary is written only while none is pending (`[controller.go:Controller.canary]`). A timed-out canary keeps its `pending` entry, so no later canary is ever written for that pipeline — known issue, see [ARCHITECTURE.md › Known issues](../../../ARCHITECTURE.md#known-issues).
- Only non-agent runtimes get `runtimes.last_seen_at` from the controller, and only when the driver answered; agent runtimes are refreshed by the agent's own poll.
- Jobs are delivered at least once and every handler is idempotent. Claiming uses `FOR UPDATE SKIP LOCKED` ordered by `run_after`, sets `running`, `locked_by`, `locked_at` and increments `attempts` (`[jobs.go:RunJobs]`). The handler runs in a savepoint whose writes are kept even when it then fails; failures retry with `[jobs.go:backoff]` (5 s × 2^attempts, so the first retry waits 10 s, capped at 300 s) until `max_attempts`, then `failed`; a `Defer` requeues at `Until` and gives the attempt back. `services.Retry` errors are not logged as errors. Only `queued` jobs are ever claimed: a job left `running` by a crashed controller is never reclaimed — known issue, see [ARCHITECTURE.md › Known issues](../../../ARCHITECTURE.md#known-issues).
- `[jobs.go:jobSnapshot]` waits (`services.Retry`) until the pipeline streams, defers to the snapshot window unless `ignore_window` (status `scheduled_snapshot`), marks queued snapshot jobs whose tables it covers `done` with `superseded_by`, then sends an `execute-snapshot` signal (default mode `incremental`) and records a `pipeline.snapshot` event (`NotAudit`).
- `[jobs.go:jobCleanup]` deletes the workload and waits up to 30 × 2 s for `missing`; then, only for a pipeline with a deployed version, source cleanup, destination drop only with payload `drop_destinations`, `SinkReleaser` helpers always, offsets cleared, and Flink CDC saved positions dropped. A failed artifact step sets lifecycle `cleanup_failed`, else `deleted`; errors loading connections or clearing offsets fail the job instead. It records a `pipeline.cleanup` event (`NotAudit`). `[jobs.go:jobRestart]` only deletes the workload; the next reconcile recreates it.
- `[uninstall.go:Uninstall]` covers every pipeline whose lifecycle is not `deleted`: it sets `desired_state = paused`, lifecycle `deleting` and `deleted_at`, then runs the cleanup handler with an in-memory job (no `jobs` row) in its own transaction and savepoint (rolled back on error); pipelines without a runtime are just marked deleted. `[uninstall.go:Plan]` changes nothing.

## Side Effects
- DB writes: `pipelines` (status, lifecycle, health, artifacts via `services.SavePipeline`; `desired_state`/`deleted_at` in uninstall), `metric_samples` (resolution 0), `schema_changes`, `jobs` (claim, result, superseded snapshots), `runtimes.last_seen_at`, events/audit via `services.Record`; reads offsets (`services.ReadOffsets`) and clears them (`services.ClearOffsets`, `services.DropSavedPositions`).
- Maintenance every 10 min: `services.EnforceMetricRetention`, `services.MaybeSendTelemetry` (may call out), `services.ExpireSavedPositions`.
- Runtimes: `Apply`/`Delete`/`Observe`/`Logs`/`HTTPGet` on Docker, Kubernetes or agent drivers.
- Sources and sinks over the network: `Prepare`/`PrepareSink`, `Discover`, source impact probes, canary writes (`WriteCanary`) and reads (`Tail`), snapshot `Signal`, cleanup.
- Separate connections: the leader-lock connection to `DatabaseURL` and, at start, one to the state database (`EffectiveStateDatabaseURL`); env via `config.Get()` only.
- Goroutines: up to 8 reconcilers per tick; logs via `bezisync.controller`.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/controller/"`
