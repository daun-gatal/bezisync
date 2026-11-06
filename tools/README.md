# tools

## Overview
A small, separate Go module (`github.com/daun-gatal/bezisync/tools`) with no third-party dependencies, outside the image build contexts. It holds the repository's maintenance tooling: the release tool, the licence-file generator, the demo smoke test and the directory-contract checker. Nothing here is linked into the `bezisync` binary.

## Subdirectories & Boundaries
- [cmd](./cmd/README.md) — the `main` packages (`demo-smoke`, `licenses`, `release`, `contracts`); thin wrappers over the packages below.
- [apiclient](./apiclient/README.md) — a signed-in API session (cookies + CSRF) for tools that drive a running BeziSync.
- [licenses](./licenses/README.md) — builds and vets the server and web licence files.
- [release](./release/README.md) — changelog fragments, versions, chart versions and the CI change check.
- [contracts](./contracts/README.md) — checks the directory contracts (README.md per code directory, ARCHITECTURE.md) against the code; the logic behind `cmd/contracts`.
- Boundary: `go.mod` declares no `require`s; keep it dependency-free. Checked by `make verify-tools` (`cd tools && golangci-lint run ./... && go vet ./... && go test ./...`).
