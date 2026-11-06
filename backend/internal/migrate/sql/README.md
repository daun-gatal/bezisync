# sql

## Overview
The goose migrations, embedded into the binary by the parent package
([migrate](../README.md), `//go:embed sql/*.sql`) and applied in file-number order by
`bezisync migrate`. Each file has a `-- +goose Up` and a `-- +goose Down` section.
`00001_baseline.sql` is the schema exactly as the last Alembic revision (0018) left it; every
later file is a forward change on top of it.

## Interface
- `[00001_baseline.sql:goose]` — the full Alembic-0018 schema; databases already at 0018 are stamped with version 1 instead of running it (see `[../migrate.go:adoptAlembic]`).
- `[00002_sso_providers.sql:sso_providers]` — single sign-on providers managed in Settings → Sign-in (replace the `BEZISYNC_OIDC_*` settings).
- `[00003_mcp_oauth.sql:oauth_clients]` — MCP OAuth tables `oauth_clients`, `oauth_requests`, `oauth_grants` (codes and tokens stored hashed).
- `[00004_sso_provider_kinds.sql:sso_replays]` — `sso_providers.kind`/`config`, nullable `client_secret_enc`, and the SAML tables `sso_requests` and `sso_replays`.
- `[00005_pipeline_version_sealed.sql:sealed_at]` — `pipeline_versions.sealed_at`; a data step seals every existing version except the current one of a never-deployed, never-submitted pipeline.
- `[00006_secret_store_credentials_replaced_at.sql:secrets_updated_at]` — `secret_stores.secrets_updated_at`, backfilled from `updated_at` where credentials exist.
- `[00007_engines.sql:engine_secrets_enc]` — `pipelines.engine` (`debezium` | `flink-cdc`, CHECK-constrained), `pipelines.engine_secrets_enc`, and `debezium_images.engine`.

## Invariants
- Files are numbered `NNNNN_name.sql` and never edited once released; a schema change is a new file written by `make migration m="…"` (`.rules/backend.md`).
- Every schema change updates `../testdata/schema.txt` in the same change: `[../migrate_test.go:TestBaselineEqualsAlembicHead]` compares a freshly migrated database with it.
- Encrypted columns (`*_enc`) hold only sealed values; tokens and codes are stored hashed (`00003_mcp_oauth.sql`).
- `00001_baseline.sql` must stay identical to Alembic revision 0018's schema (`[../migrate.go:AlembicHead]`).

## Side Effects
- DDL and data updates on the BeziSync PostgreSQL database when goose applies them (tables above; `00005` and `00006` rewrite existing rows).

## Verification
- Run: `make verify-quick GO_PKGS="./internal/migrate/"`
