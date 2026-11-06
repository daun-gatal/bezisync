# cmd/contracts

## Overview
`go run ./cmd/contracts` (from `tools/`), which is `make contracts-check`: lists every directory-contract
problem [tools/contracts](../../contracts/README.md) finds and exits 1, or prints
`contracts are current`.

## Interface
- `[main.go:main]` / `[main.go:run]` — resolve the repository root, run `contracts.Check`, and print the problems to stderr.
- `[main.go:repoRoot]` — `BEZISYNC_ROOT`, otherwise `git rev-parse --show-toplevel`.

## Invariants
- Exit 0 (printing `contracts are current` to stdout) only when there are no problems. Otherwise every problem is printed to stderr (one per line), followed by `contracts: N problem(s); see .rules/egm.md (Directory contracts)` and exit 1; a failure to find the root or run `git` also exits 1 (`[main.go:run]`).

## Side Effects
- Reads the `BEZISYNC_ROOT` environment variable and runs `git`. Read-only.

## Verification
- No co-located tests; covered by `cd tools && go test ./contracts/` and by running `make contracts-check`.
