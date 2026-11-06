# cli

## Overview
The `bezisync` command line (cobra). It starts the server roles (`api`, `controller`, `agent`),
administers an install (`migrate`, `uninstall`, key rotation, recovery), hosts the in-container
helpers (SSH tunnel sidecar, Flink CDC engine, healthcheck), and provides `bezisync remote …`,
a client that drives a control plane over the REST API with an API token. `cmd/bezisync`
calls `Execute`.

## Interface
- `[cli.go:Execute]` — runs the command tree; exit 0, `ExitError.Code`, 2 for BeziSync's own `usageError` (bad or missing option values), 1 otherwise, including cobra's own errors (unknown flag, wrong argument count, a missing required flag) and the plain validation errors in `positions.go`.
- `[cli.go:ExitError]` — a command's chosen exit code.
- Server roles: `[cli.go:apiCmd]` (`api`, HTTP on `--host`/`--port`, default `0.0.0.0:8000`; `--workers` is accepted and ignored), `[cli.go:controllerCmd]` (`controller`), `[cli.go:agentCmd]` (`agent`, flags with `BEZISYNC_AGENT_*` env defaults).
- Administration: `[cli.go:migrateCmd]` (`migrate`: schema, then `services.Bootstrap` and `services.EnsureSandbox`), `[cli.go:uninstallCmd]` (`uninstall`), `[cli.go:rotateKeyCmd]` (`rotate-master-key`), `[cli.go:passwordLoginCmd]` (`enable-password-login`), `[cli.go:generateKeyCmd]` (`generate-master-key`), `[cli.go:licensesCmd]` (`licenses`).
- Documents: `[cli.go:OpenAPIDocument]` / `[cli.go:openapiCmd]` (`openapi`), `[cli.go:checkDocsCmd]` (`check-docs`).
- Container helpers: `[cli.go:tunnelCmd]` (`tunnel`), `[cli.go:tunnelProbeCmd]` (hidden `tunnel-probe`), `[cli.go:healthcheckCmd]` (`healthcheck`), `[engine.go:engineCmd]` (hidden `engine flink-cdc jobmanager|taskmanager`), `[engine.go:installSelfCmd]` (hidden `install-self DIR`).
- Remote client: `[remote.go:Client]` / `[remote.go:NewClient]` (overridable in tests) with `[remote.go:Client.Call]`, `[remote.go:Client.Base]` (project slug → `/api/v1/projects/<id>`), `[remote.go:Client.PipelineID]`; `[remote.go:remoteCmd]` adds `login`, `get`, `export`, `apply`, `diff`, `pipelines pause|resume|restart`; `[positions.go:positionsCmd]`, `[positions.go:restoreCmd]`, `[positions.go:startOverCmd]` add `pipelines positions|restore|start-over`; `[remote.go:ConfigPath]`.

## Invariants
- `api` refuses to start when `httpapi.CheckProduction` fails, warns about retired settings (`config.Settings.Retired`), uses a 15 s read-header and 120 s idle timeout, and shuts down gracefully within 20 s on SIGTERM/SIGINT (`[cli.go:apiCmd]`).
- `uninstall` only prints the plan unless `--yes`; it exits 1 when any pipeline could not be fully cleaned up (`[cli.go:uninstallCmd]`).
- `rotate-master-key` and `enable-password-login` run in one database transaction (`[cli.go:rotateKeyCmd]`, `[cli.go:passwordLoginCmd]`); `migrate` runs `services.Bootstrap` and `services.EnsureSandbox` together in one transaction after `migrate.Up` (`[cli.go:migrateCmd]`).
- `pipelines restore` and `start-over` refuse to run without `--yes` and send the pipeline name as the API's typed `confirm` (`[positions.go:restoreCmd]`, `[positions.go:startOverCmd]`).
- `remote login` reads the token from `--token`, else from the terminal without echo, else one line of stdin; it verifies the token against `/api/v1/auth/me` before storing it in `[remote.go:ConfigPath]` with mode 0600 in a 0700 directory; `BEZISYNC_URL`/`BEZISYNC_TOKEN`/`BEZISYNC_PROJECT` override the stored values (`[remote.go:loadSettings]`).
- `remote diff` (and `remote apply --dry-run`) exits 3 when deploying the manifest would break running pipelines (unless `--confirm`), and `remote apply --deploy` reads a dry-run plan first and stops with exit 3 on a breaking change unless `--confirm` is given; applied settings changes are printed with their impact (`[remote.go:impactWords]`), and raw advanced properties only by key and class (passed through or refused), never by value (`[remote.go:printApply]`, `[remote.go:breaking]`).
- `tunnel` reads the SSH key only from `BZ_SSH_KEY`, never from a flag (`[cli.go:tunnelCmd]`).
- `frontend/openapi.json` must equal `OpenAPIDocument()` (`[cli_test.go:TestOpenAPIDocumentMatchesTheFrontend]`; regenerate with `make openapi`).

## Side Effects
- `api` listens on TCP and serves `api.Router()`; `controller` runs `controller.Main`; `agent` connects outbound to the control plane; all three install signal handlers and configure logging.
- Database writes via `migrate.Up`, `services.Bootstrap`/`EnsureSandbox`, `services.RotateKeys`, `services.SetPasswordLogin` and `controller.Uninstall` (which removes connectors, replication slots, publications and BeziSync tables on sources, and with `--drop-destinations` topics/streams).
- Processes: `engine flink-cdc` runs the JobManager/TaskManager through `flinkhelper.RunJobManager` / `RunTaskManager`; `install-self DIR` copies this binary to `DIR/bezisync` (`flinkhelper.InstallSelf`).
- Files: `openapi`/`check-docs` write `--output` (default `-`, stdout; files mode 0644); `remote login` writes `~/.config/bezisync/cli.json` (or under `XDG_CONFIG_HOME`); `remote apply|diff -f` read a manifest file, a directory of `*.yaml`/`*.yml` joined in name order (`[remote.go:readManifests]`), or stdin with `-f -`.
- Network: `remote …` calls the control plane with `Authorization: Bearer <token>` (60 s timeout); `healthcheck` GETs `--url` (default `http://127.0.0.1:8000/readyz`, 5 s timeout, exit 1 unless 2xx); `tunnel` holds an SSH tunnel and listens on `--listen-host` (default 127.0.0.1) / `--listen-port`; `tunnel-probe` dials 127.0.0.1 with a 2 s timeout.
- Env vars read: `BEZISYNC_AGENT_*`, `BEZISYNC_URL`, `BEZISYNC_TOKEN`, `BEZISYNC_PROJECT`, `XDG_CONFIG_HOME`, `BZ_SSH_KEY`, `BZ_SSH_HOST_KEY`, plus every `BEZISYNC_*` setting through `config`.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/cli/ ./internal/api/"` (`internal/api/cli_test.go` covers `remote`, `uninstall`, documents/keys and Flink positions end to end).
