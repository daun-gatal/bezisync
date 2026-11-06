# licenses

## Overview
Generates the licence files BeziSync ships: one for the server binary (every Go module `./cmd/bezisync` links on any release platform, plus the Go standard library) and one for the web UI (every npm package the frontend's production dependencies pull in). Each file starts with BeziSync's own NOTICE and LICENSE, then lists third-party packages with their licence texts and notices. `cmd/licenses` (`make licenses`, `make licenses-check`) drives it.

## Interface
- `[licenses.go:ServerFile]` / `[licenses.go:WebFile]` — the outputs: `backend/internal/legal/licenses.txt` (embedded in the binary) and `frontend/public/licenses.txt` (served at `/licenses.txt`).
- `[licenses.go:GoEntries]` — runs `go list -deps` for `./cmd/bezisync` per `[licenses.go:Platforms]` and reads each module's licence files; `[licenses.go:ParseGoList]` parses that output.
- `[licenses.go:NpmEntries]` — walks the frontend's `dependencies` and `optionalDependencies` from `node_modules`, resolving like Node (devDependencies are excluded; a missing optional package is skipped, a missing required one is an error).
- `[licenses.go:ReadFiles]` (`[licenses.go:File]`) — a package's top-level licence and notice files; `[licenses.go:Classify]` recognises SPDX identifiers from a licence text, used for every Go module and for npm packages whose `package.json` declares no licence.
- `[licenses.go:Vet]` — fails on any package whose licence is not allowed or not recognised; `[licenses.go:AllowedExpression]` evaluates SPDX `OR`/`AND` expressions against `[licenses.go:Allowed]`.
- `[licenses.go:Render]` — writes a file from `[licenses.go:OwnFiles]` (`NOTICE`, `LICENSE`) and the `[licenses.go:Entry]` list.
- `[licenses.go:Overrides]` (`[licenses.go:Override]`) — maintainer-reviewed licences for packages whose files cannot be recognised.

## Invariants
- Only the SPDX identifiers in `[licenses.go:Allowed]` (permissive licences, MPL-2.0 and OFL-1.1) may ship; GPL-family, SSPL, BUSL or unrecognised licences make `[licenses.go:Vet]` fail. An `[licenses.go:Override]` clears a package only when its own SPDX expression is allowed, so it settles an unrecognised licence, not a disallowed one. Every licence an entry carries must be allowed.
- `[licenses.go:AllowedExpression]`: one allowed alternative of an `OR` is enough; every part of an `AND` must be allowed; an empty expression is not allowed.
- `[licenses.go:Platforms]` must list every platform the release builds (`scripts/build-cli.sh` and the images), or a platform-only dependency would be missing from the server file.
- `[licenses.go:Render]` sorts packages by lower-cased name, so `--check` can compare its output byte for byte with the committed files; a licence text identical to an earlier package's is printed once and referred to afterwards (`[licenses.go:Render]`).

## Side Effects
- Runs `go list` in `backend/` (with `GOOS`, `GOARCH`, `CGO_ENABLED=0`, `GOFLAGS=-mod=readonly`) and `go env GOROOT`; reads the Go module cache (a module not downloaded is an error), `$GOROOT/LICENSE`, `frontend/node_modules`, and the repository's `NOTICE`/`LICENSE`. Writing the output files is done by `cmd/licenses`.

## Verification
- Run: `cd tools && go test ./licenses/`
