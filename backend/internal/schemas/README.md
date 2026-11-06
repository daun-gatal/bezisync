# schemas

## Overview
Embeds the JSON Schemas of the secret-store, runtime and alert-channel models
(`model_schemas.json`, captured from the Python implementation). The API serves them so the
UI can render forms, and `api` / `services` validate configs against them with `schemaval`. Connector
schemas are not here; they live in `connectors/descriptors.json`.

## Interface
- `[schemas.go:Kind]` — one model family entry: `Kind`, `Title`, `Description`, `ConfigSchema`, `SecretSchema`.
- `[schemas.go:Schema]` — a key-ordered JSON Schema (`*om.Map`).
- `[schemas.go:Stores]` / `[schemas.go:Store]` — secret stores (`vault`, `aws`, `gcp`, `kubernetes`).
- `[schemas.go:Runtimes]` / `[schemas.go:Runtime]` — runtimes (`kubernetes`, `docker`).
- `[schemas.go:Channels]` / `[schemas.go:Channel]` — alert channels (`slack`, `webhook`, `pagerduty`, `email`).

## Invariants
- `model_schemas.json` is decoded once in `init` with key order kept; a malformed file panics at start-up (`[schemas.go:init]`).
- List order in the JSON is display order (`[schemas.go:Stores]`).
- Every entry must have `config_schema` and `secret_schema` objects; the loader type-asserts them (`[schemas.go:init]`).

## Side Effects
- None (reads only the embedded file).

## Verification
- Run: No co-located tests; covered by `cd backend && go vet ./internal/schemas/` and `make verify-quick GO_PKGS="./internal/services/ ./internal/api/"`.
