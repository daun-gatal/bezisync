# contract

## Overview
Test-only helper that loads the JSON fixtures captured from the former Python implementation,
so cross-implementation tests can assert that Go produces the same output. The fixtures live in
`backend/testdata/contract/`. It is imported only by `*_test.go` files (domain, difflib, pyjson,
pyyaml, compiler, services, security, connectors, val).

## Interface
- `[contract.go:Dir]` — absolute path of `backend/testdata/contract`, resolved from this source file's location.
- `[contract.go:Load]` — decodes `<name>.json` from `Dir()` into `v`, calling `t.Fatalf` when it is missing or invalid.

## Invariants
- Only test code may import it: it depends on `testing` and on the source tree being present (`[contract.go:Dir]` uses `runtime.Caller`).
- Fixture names are passed without the `.json` suffix (`[contract.go:Load]`).

## Side Effects
- Reads fixture files from `backend/testdata/contract/`.

## Verification
- No co-located tests; covered by `cd backend && go vet ./internal/contract/` plus `make verify-quick GO_PKGS="./internal/difflib/ ./internal/pyjson/ ./internal/pyyaml/ ./internal/domain/"`.
