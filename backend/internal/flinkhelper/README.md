# flinkhelper

## Overview
The process BeziSync injects into every container of a Flink CDC pipeline
(`bezisync engine flink-cdc jobmanager|taskmanager`, copied in with `bezisync install-self`).
On a TaskManager it builds the pipeline's classpath and `exec`s Flink's TaskManager. On the
JobManager it starts Flink, waits for the TaskManagers, submits the pipeline restoring the
requested saved position, stores the newest completed checkpoint it sees on each poll encrypted
in BeziSync's state database, serves health/status to the controller, and on SIGTERM stops with a savepoint it
stores. The controller (`internal/controller/flinkcdc.go`) reads its `Status`.

## Interface
- `[run.go:RunJobManager]` / `[run.go:RunTaskManager]` — role entry points; `[run.go:InstallSelf]` copies the running binary to `DIR/bezisync`; `[run.go:RESTBase]`.
- `[settings.go:Settings]` / `[settings.go:LoadSettings]` — `settings.json` (connectors, drivers, TaskManagers, `restore`, `restore_after`, state dir, health port, stop timeout); container path constants `[settings.go:DefaultConfigDir]` etc.
- `[layout.go:Layout]` / `[layout.go:DefaultLayout]` — where the image keeps Flink (`/opt/flink`, `/opt/flink-cdc/{lib,connectors,drivers}`) and the work dirs; `Prepare` (lib links, conf copy, keystore, console-only log4j), `LibDir`, `ConfDir`, `DynamicProperties` (`flink-conf.yaml` as `-D key=value` pairs), `Classpath`, `Env`, `KeystorePath`; `[layout.go:SecretSettingsFile]`.
- `[jobmanager.go:JobManager]` — the JobManager lifecycle (`Run`, `Status`); `[jobmanager.go:Status]` and the `Phase*` constants (`[jobmanager.go:PhaseRunning]` …) served at `/bz/status`; `[jobmanager.go:StateStore]` and `[jobmanager.go:Launcher]` seams for tests.
- `[jobmanager.go:RenderSecrets]` — fills `${BZ_…}` placeholders in `pipeline.yaml` as quoted YAML scalars.
- `[health.go:Health]` / `[health.go:ServeHealth]` — `/q/health`, `/q/health/ready`, `/q/health/live` (Debezium Server's paths) and `/bz/status`.
- `[rest.go:REST]` / `[rest.go:NewREST]` — Flink REST client: `TaskManagers`, `Jobs`, `Job`, `Checkpoints`, `Exceptions`, `StopWithSavepoint`, `Cancel`, `SubtaskMetrics`, `MetricNames`; `[rest.go:ErrSavepointFailed]`.
- `[store.go:Store]` / `[store.go:OpenStore]` — encrypted saved positions: `Save`, `List`, `LatestAfter`, `Extract`, `Prune`, `Close`; `[store.go:Saved]`, `[store.go:TableName]`, `[store.go:StateSchema]`, `[store.go:MaxStateBytes]`, `[store.go:ErrStateTooLarge]`.

## Invariants
- Settings are checked on load: `short_id` is 12 hex chars, at least one connector, connector/driver names match `^[a-z0-9][a-z0-9-]*$` (they become file paths), `state_dir` absolute; defaults: 1 TaskManager, `/state`, keep 5 checkpoints, health port 8080, 90 s stop timeout (`[settings.go:check]`).
- Connectors load from Flink's classpath (`FLINK_LIB_DIR` = `LibDir`), never per job; a missing Flink, `flink-cdc-dist-*.jar`, connector JAR or required driver is a `config_error` that says how to fix the image (`[layout.go:Prepare]`).
- Secrets never go on a command line: secret Flink settings are appended to the private `config.yaml` from `flink-secrets.conf` (`[layout.go:appendSecretSettings]`), and `pipeline.yaml` secrets are rendered only into a 0600 work-dir file that is removed after the submit (`[jobmanager.go:submit]`). Each substituted value is a JSON-quoted scalar so it cannot add YAML keys (`[jobmanager.go:quoteScalar]`).
- Saved state is tar.gz, sealed with AES-256-GCM (random nonce, the relative path as associated data), its SHA-256 checked on extract; sealed size over `MaxStateBytes` (64 MiB) is refused (`[store.go:Save]`, `[store.go:Extract]`). The key is 32 base64-encoded bytes (`[store.go:OpenStore]`).
- Archive paths must stay under the state dir (`[store.go:validRel]`); extraction rejects any entry type other than directories and regular files, and truncates each file at 4 × `MaxStateBytes` (`[store.go:unarchive]`).
- `restore`: `""` starts fresh, `latest` takes the newest stored state with id above `restore_after` (Start over floor), otherwise a numeric stored id (`[jobmanager.go:restorePoint]`, `[jobmanager.go:latestPoint]`).
- Each poll stores the newest completed checkpoint once (checkpoints that complete between two polls are not stored), then deletes every non-`kept` checkpoint outside the newest `keep_checkpoints` checkpoints (a `kept` one still takes a slot); savepoints are never pruned here (`[jobmanager.go:storeCheckpoint]`, `[store.go:Prune]`).
- Known non-healing failures (destination mismatch/missing, replica identity, transform errors) cancel the job before reporting `failed` (`[jobmanager.go:fatal]`, `[jobmanager.go:watch]`). The schema-registry start-up race is resubmitted at most `maxResubmits` (3) times (`[jobmanager.go:schemaRace]`); a job that exhausts Flink's restarts or is cancelled outside BeziSync is resubmitted from the latest stored state, and a failed submit is retried from the same point, both with one backoff doubling from 30 s up to 10 min (`[jobmanager.go:lifecycle]`).
- Readiness is UP only when phase is `running`, the job is `RUNNING` and every wanted TaskManager is registered (`[health.go:Health]`).
- On SIGTERM a `RUNNING` job is stopped with a canonical savepoint under `state_dir/sp`, `drain: false`, within `stop_timeout_seconds`; on failure the newest checkpoint is stored instead (`[jobmanager.go:stop]`, `[rest.go:StopWithSavepoint]`).

## Side Effects
- PostgreSQL (state database): `CREATE SCHEMA IF NOT EXISTS debezium_state`, `CREATE TABLE IF NOT EXISTS debezium_state.flink_state_<short_id>`; inserts and deletes rows there (`[store.go:OpenStore]`, `[store.go:Prune]`). At most 2 connections, 15 s connect timeout, retried every 5 s while unreachable (`[run.go:openStore]`, `[run.go:openStoreRetrying]`).
- Env vars read: `BZ_STATE_DSN`, `BZ_STATE_USER`, `BZ_STATE_PASSWORD`, `BZ_STATE_KEY` (`[run.go:openStore]`), `JAVA_HOME` (`[run.go:Submit]`), every `BZ_*` referenced by `pipeline.yaml`.
- Processes: runs `bin/jobmanager.sh start-foreground` (SIGTERM then kill after 20 s), `exec`s `bin/taskmanager.sh`, runs the Flink CDC `CliFrontend` client with a 5 min timeout (`[run.go:execLauncher]`).
- Files: recreates `WorkDir/lib` (symlinks) and `WorkDir/conf` (image conf copied, secret settings appended to `config.yaml`, `log4j-console.properties`), writes `keystore.p12`, the rendered `pipeline.yaml` (0600, removed), restored state archives under `state_dir`; `InstallSelf` writes `DIR/bezisync` (0755).
- Network: HTTP to the local Flink REST API (`127.0.0.1:8081`, 15 s timeout, 8 MiB response cap); listens on `health_port`.
- Goroutines: the health server, the job lifecycle loop (waits for TaskManagers polling every 1 s, then watches the job every 5 s by default), a waiter on the JobManager process; logs to stderr with the `bezisync-helper:` prefix (`[jobmanager.go:logf]`).

## Verification
- Run: `make verify-quick GO_PKGS="./internal/flinkhelper/"`
