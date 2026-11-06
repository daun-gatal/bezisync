# config

## Overview
Reads the process configuration from `BEZISYNC_*` environment variables, plus an optional
`.env` file in the working directory that real variables override. It holds only what must be
known before the database is reachable; anything users change at runtime lives in the
database. Every role and most packages read settings through `config.Get()`.

## Interface
- `[config.go:Settings]` — every setting (database URLs, master keys, public URL, cookies and session TTL, bootstrap admin, CORS origins, SSRF CIDRs, controller/schema-check/canary timings, metrics retention and token, telemetry, lab mode, SMTP, logging, default Debezium/Flink CDC/Iceberg images, agent and sandbox settings) plus `Retired`.
- `[config.go:Get]` / `[config.go:Set]` / `[config.go:Reset]` — the process-wide settings, loaded once under a mutex; `Set`/`Reset` are for tests and key rotation.
- `[config.go:Load]` — parses settings through a lookup function (`os.LookupEnv` in production) and `./.env`.
- `[config.go:Settings.ValidateForProduction]` — fatal misconfigurations when `Environment` is `production` (the HTTP server refuses to start on any).
- `[config.go:Settings.EffectiveStateDatabaseURL]` / `[config.go:Settings.ConnectorStateDatabaseURL]` — where Debezium offsets live, as BeziSync and as connectors reach it.
- `[config.go:DevMasterKey]` / `[config.go:GenerateMasterKey]` — the development key and a fresh base64 32-byte key.

## Invariants
- Lookup order per name: exact `BEZISYNC_X`, then lower-case `bezisync_x`, then the `.env` entry (`[config.go:Load]`).
- Invalid values (bad bool/int/float/JSON list, `ENVIRONMENT` outside `development|production|test`) are collected and returned together; `[config.go:Get]` prints them and exits with status 2.
- `MASTER_KEY` must be strict base64 decoding to exactly 32 bytes; it is checked only after the other values parse (`[config.go:Load]`).
- Production refuses the dev master key, non-https `PUBLIC_URL` or `COOKIE_SECURE=false` unless `ALLOW_PLAIN_HTTP`, and the bootstrap passwords `admin`/`password` (`[config.go:Settings.ValidateForProduction]`).
- Retired `BEZISYNC_OIDC_*` names are reported in `Retired`, never read (`[config.go:retired]`).
- Other packages must not read `os.Getenv` for settings (`.rules/backend.md`).

## Side Effects
- Reads environment variables `BEZISYNC_*` and the file `./.env`.
- `[config.go:Get]` writes to stderr and calls `os.Exit(2)` on invalid configuration; `[config.go:GenerateMasterKey]` panics if the system RNG fails.

## Verification
- No co-located tests; covered by `cd backend && go vet ./internal/config/` plus `make verify-quick GO_PKGS="./internal/api/ ./internal/secrets/ ./internal/ssrf/"`, whose tests set and load settings.
