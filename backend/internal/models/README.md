# models

## Overview
Mirrors the database schema: one struct per table, with columns mapped by `db` tags. The
exception is `sso_replays` (SAML assertion IDs), which `services` reads and writes with plain SQL. The structs
are scanned by the `internal/db` query helpers and written by `db.Insert` / `db.Update`.
JSONB columns use `db.Map` / `db.List`; encrypted columns are `*_enc` strings holding sealed
values.

## Interface
- Accounts and access: `[models.go:User]`, `[models.go:UserSession]`, `[models.go:APIToken]`, `[models.go:Project]`, `[models.go:Membership]`, and the project roles `[models.go:RoleEditor]`, `[models.go:RoleOperator]`, `[models.go:RoleViewer]`.
- Sign-in and MCP: `[models.go:SSOProvider]`, `[models.go:SSORequest]`, `[models.go:OAuthClient]`, `[models.go:OAuthRequest]`, `[models.go:OAuthGrant]`.
- Resources: `[models.go:Connection]`, `[models.go:SecretStore]`, `[models.go:Runtime]`, `[models.go:Pipeline]`, `[models.go:PipelineVersion]`, `[models.go:PipelineTemplate]`, `[models.go:DebeziumImage]`.
- Operations: `[models.go:Job]`, `[models.go:MetricSample]`, `[models.go:AgentWorkload]`, `[models.go:AgentRequest]`, `[models.go:ChangeRequest]`, `[models.go:SchemaChange]`, `[models.go:Event]`, `[models.go:InstanceState]`.
- Alerts and feedback: `[models.go:AlertChannel]`, `[models.go:AlertRule]`, `[models.go:Alert]`, `[models.go:Notification]`, `[models.go:Feedback]`.
- Helpers: `[models.go:User.Muted]`, `[models.go:Pipeline.LabelMap]` (string-valued entries only), `[models.go:ShortID]` / `[models.go:Pipeline.ShortID]` (first 12 hex characters of the id, used in workload names).

## Invariants
- Struct fields and `db` tags must match the schema in `internal/migrate/sql`; a new column needs a migration and a tagged field.
- Admin is account-wide (`User.IsSuperuser`), not a project role (`[models.go:RoleEditor]` block).
- `*_enc` fields (`Connection.SecretsEnc`, `SecretStore.SecretsEnc`, `Pipeline.EngineSecretsEnc`, …) hold sealed (encrypted) values only; secrets are write-only and never returned (`AGENTS.md` golden rule 1).
- `Pipeline.Engine` is `debezium` or `flink-cdc` (a database check constraint, migration `00007_engines.sql`) and is fixed once deployed; `Runtime.PlacementProfiles` holds a Kubernetes runtime's placement profiles as a JSONB list (`[models.go:Runtime]`, migration `00009_runtime_placement.sql`).
- `MetricSample.ID` is an `int` serial key, so `db.Insert` omits it (`[models.go:MetricSample]`).

## Side Effects
- None.

## Verification
- No co-located tests; covered by `cd backend && go vet ./internal/models/` plus `make verify-quick GO_PKGS="./internal/services/ ./internal/api/"`.
