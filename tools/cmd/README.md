# cmd

## Overview
The executables of the `tools/` module, each a thin `main` over a package one level up. They are run with `go run ./cmd/<name>` from `tools/`, usually through a script or Make target, and never ship in an image.

## Subdirectories & Boundaries
- [demo-smoke](./demo-smoke/README.md) — drives the compose demo sandbox through the API (`scripts/demo-smoke.sh`); uses `apiclient`.
- [licenses](./licenses/README.md) — writes or checks the shipped licence files (`make licenses`, `make licenses-check`); uses `licenses`.
- [release](./release/README.md) — plans, checks and assembles releases (`scripts/release.sh`); uses `release`.
- [contracts](./contracts/README.md) — checks the directory contracts against the code (`go run ./cmd/contracts`, `make contracts-check`); uses `tools/contracts`.
