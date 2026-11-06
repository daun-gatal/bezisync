# backend

## Overview

The Go module (`github.com/daun-gatal/bezisync/backend`) behind the `bezisync` image: one
distroless binary with the roles `api`, `controller`, `agent` and `migrate`, the `remote` CLI and the
Flink CDC helper. Built from this directory by `Dockerfile`; `.dockerignore` keeps tests,
`testdata/` and these contracts out of the image. `testdata/contract/` holds the corpus recorded
from the Python implementation (JSON fixtures read through `internal/contract`).

Verification: `make verify-quick GO_PKGS=…` with the packages you changed, `make verify` for all
(here: golangci-lint, vet and `go test -race ./...`, whose tests include the schema, corpus and
check-reference drift checks; it also runs the tools, frontend, licence and contract checks).

## Subdirectories & Boundaries

- cmd: [./cmd/README.md](./cmd/README.md) - `cmd/bezisync`, the binary's `main`, which hands over to `internal/cli`.
- internal: [./internal/README.md](./internal/README.md) - every package of the binary: API and MCP, auth and security, services, connectors and compiler, controller, runtimes and agent, storage and validation.
