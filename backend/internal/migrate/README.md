# migrate

## Overview
Applies the database schema with goose from the embedded SQL files in [sql](./sql/README.md).
It also adopts databases created by the Python/Alembic releases: a database at Alembic
revision 0018 is stamped with the goose baseline without running DDL, an older revision is
refused, and an empty database gets the baseline. `bezisync migrate`
(`internal/cli`) calls it before first-start bootstrap, and `internal/testdb` calls it for test
databases.

## Interface
- `[migrate.go:Up]` — takes the advisory lock, adopts Alembic if needed, then runs `goose.UpContext` on `sql/`.
- `[migrate.go:LockID]` — advisory lock key `0x62657A69` ("bezi"), shared with the Python implementation.
- `[migrate.go:AlembicHead]` — `"0018"`, the only Alembic revision that can be adopted.
- `[migrate.go:adoptAlembic]` (internal) — creates `goose_db_version` and records versions 0 and `baselineVersion` (1) in one transaction.
- `testdata/schema.txt` — golden description of the migrated schema.
- Subdirectories: [sql](./sql/README.md) — the numbered goose migrations.

## Invariants
- All work runs on one physical session (`SetMaxOpenConns(1)`) because `pg_advisory_lock` is session-scoped; concurrent `Up` calls are serialised by `[migrate.go:LockID]`.
- Adoption only happens when `alembic_version` exists and `goose_db_version` does not; a revision other than `[migrate.go:AlembicHead]` is an error telling the operator to upgrade with v0.1.1 first.
- A schema change must update `testdata/schema.txt`; `[migrate_test.go:TestBaselineEqualsAlembicHead]` fails on drift (`UPDATE_GOLDEN=1` rewrites it; with `BEZISYNC_ALEMBIC_DATABASE_URL` it compares against a real Alembic database instead).

## Side Effects
- Opens its own `database/sql` connection (pgx stdlib driver) to the given URL; takes and releases PostgreSQL advisory lock `LockID`.
- Writes DDL and the `goose_db_version` table; logs adoption and goose progress through `slog` (`gooseLogger`).

## Verification
- Run: `make verify-quick GO_PKGS="./internal/migrate/"`
