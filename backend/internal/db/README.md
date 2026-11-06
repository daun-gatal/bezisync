# db

## Overview
Owns the PostgreSQL connection pool, transactions, generic query helpers and the JSONB column
types. Queries are handwritten SQL scanned into the structs of `internal/models`; the schema
itself is applied by `internal/migrate`. Services, the API and the controller all go through it.

## Interface
- `[db.go:Querier]` — what the pool, a connection and a `pgx.Tx` share (`Exec`, `Query`, `QueryRow`).
- `[db.go:Pool]` / `[db.go:MustPool]` / `[db.go:SetPool]` / `[db.go:Close]` — the process pool, created lazily from `BEZISYNC_DATABASE_URL`.
- `[db.go:Tx]` — runs a function in a transaction (commit on success, rollback on error or panic); `[db.go:Savepoint]` nests one inside a transaction (on the pool it opens a plain transaction; a `Querier` that cannot `Begin` runs `fn` directly, without a savepoint).
- `[db.go:Get]` / `[db.go:Find]` / `[db.go:Select]` / `[db.go:Scalar]` / `[db.go:FindScalar]` — generic scans (columns matched by `db` tag); `Find*` return nil on no rows, `Select` never returns a nil slice.
- `[crud.go:Insert]` / `[crud.go:Update]` / `[crud.go:Delete]` — row writes from model structs by their `db` tags.
- `[db.go:JSON]` / `[db.go:Map]` / `[db.go:List]` — JSONB columns; numbers decode as `json.Number`.
- `[db.go:NormalizeURL]` (SQLAlchemy → libpq URL), `[db.go:Redact]` (password hidden), `[db.go:Now]`, `[db.go:NewID]`, `[db.go:IsNoRows]`, `[db.go:IsUniqueViolation]` (code 23505, optional constraint names), `[db.go:ToJSON]`.

## Invariants
- Pool budget is fixed: 30 max connections, 5 min idle time, 30 s health checks (`[db.go:Pool]`).
- `[db.go:Now]` truncates to microseconds so values compare equal after a PostgreSQL round trip.
- JSONB values decode through `pyjson.DecodeGeneric`, keeping integers and floats exact, because stored hashes depend on them (`[db.go:JSON]`); encoding disables HTML escaping (`[db.go:marshal]`).
- A nil `[db.go:List]` is stored as `[]`; a nil `[db.go:Map]` as SQL NULL.
- `[crud.go:Insert]` skips an `int` `id` column (serial key); `[crud.go:Update]` writes every tagged column keyed by `id` or the given column.
- Table names in `[crud.go:Insert]`/`Update`/`Delete` are interpolated into SQL unquoted: callers pass constant table names, never input.
- Use `[db.go:Redact]` whenever a database URL reaches a log or message.

## Side Effects
- Opens and holds a pgx connection pool to `BEZISYNC_DATABASE_URL` (via `config.Get()`); runs whatever SQL callers pass.

## Verification
- No co-located tests; covered by `cd backend && go vet ./internal/db/` plus `make verify-quick GO_PKGS="./internal/services/ ./internal/api/"`.
