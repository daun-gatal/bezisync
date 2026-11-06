# licenses

## Overview
The command behind `make licenses` and `make licenses-check`: it writes (or checks) the server and web licence files using the `tools/licenses` package. It runs from `tools/` and finds the repository root with git or `BEZISYNC_ROOT`.

## Interface
- `[main.go:main]` — flags `--check` (fail when a file is stale instead of writing it) and `--only server|web`.
- `[main.go:run]` — for each target, lists entries (`[../../licenses/licenses.go:GoEntries]` / `[../../licenses/licenses.go:NpmEntries]`), vets them, renders and writes or compares the file.
- `[main.go:repoRoot]` — `BEZISYNC_ROOT`, else `git rev-parse --show-toplevel`.

## Invariants
- Every run, writing or checking, fails when `[../../licenses/licenses.go:Vet]` rejects a licence.
- `--only` accepts only `server` or `web`; anything else is an error before any work.
- `--check` fails with "is stale: run make licenses" unless the file matches the rendered content exactly; a missing file also counts as stale.

## Side Effects
- Writes `backend/internal/legal/licenses.txt` and `frontend/public/licenses.txt` (mode 0644) unless `--check`; runs `git` and `go list`; reads `BEZISYNC_ROOT`. The web target needs `bun install` in `frontend/`.

## Verification
- Run: `make licenses-check` (logic tested by `cd tools && go test ./licenses/`).
