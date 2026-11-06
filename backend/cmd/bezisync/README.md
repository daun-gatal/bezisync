# bezisync

## Overview
The entry point of the single BeziSync binary. It holds no logic: `main` hands the arguments and standard streams to the command tree in `internal/cli`, which provides the `api`, `controller`, `agent` and `migrate` roles, the administration commands and the `remote` client. The same package is built into the image (`backend/Dockerfile`) and into the release CLI archives (`scripts/build-cli.sh`).

## Interface
- `[main.go:main]` — calls `[../../internal/cli/cli.go:Execute]` with `os.Args[1:]`, stdin, stdout and stderr, and exits with the code it returns.

## Invariants
- Keep `[main.go:main]` a one-line delegation: commands, flags and exit-code mapping live in `[../../internal/cli/cli.go:Execute]`, so they stay testable without a process.
- Release builds stamp the version through `-X github.com/daun-gatal/bezisync/backend/internal/version.Version` on this package (`scripts/build-cli.sh` always; `backend/Dockerfile` when `RELEASE_VERSION` is set, otherwise the image keeps `internal/version`'s value); `tools/licenses` lists the modules `./cmd/bezisync` links (`[../../../tools/licenses/licenses.go:GoEntries]`), so the licence file follows what this package imports.

## Side Effects
- Exits the process with `os.Exit`. Everything else (servers, database, Docker/Kubernetes calls) happens in the role started by `internal/cli`.

## Verification
- Run: No co-located tests; covered by `cd backend && go vet ./cmd/bezisync/` and `make verify-quick GO_PKGS="./internal/cli/"` (which tests `Execute`).
