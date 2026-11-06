# release

## Overview
The command line of the release tool, run through `scripts/release.sh` by contributors and by `ci.yml`, `build-branch.yml` and `release.yml`. It parses one mode, finds the repository root, and calls the `tools/release` package.

## Interface
- `[main.go:main]` / `[main.go:run]` — modes (at most one): no flag (assemble an app release, optional explicit version), `--plan [VERSION]`, `--dry-run`, `--next`, `--current`, `--notes VERSION`, `--chart-notes NAME`, `--changes BASE`.
- `[main.go:dispatch]` — maps a mode to `[../../release/release.go:MakePlan]`, `[../../release/release.go:Notes]`, `[../../release/release.go:ChartNotes]` (unknown chart name is an error), `[../../release/release.go:ReadVersion]`, `[../../release/release.go:TargetVersion]` or `[../../release/release.go:Assemble]`.
- `[main.go:changes]` — `--changes BASE` prints `build=`, `demo=` and `charts=` lines (GitHub output format) and the problems found by `[../../release/release.go:Classify]`.
- `[main.go:repoRoot]` — `BEZISYNC_ROOT`, else `git rev-parse --show-toplevel`.

## Invariants
- A flag parse error, more than one mode or more than one positional argument exits 2; any failure exits 1 with `release: …` on stderr.
- `--plan` prints exactly the `kind`, `version`, `charts`, `pre_charts` and `agent_chart` lines the workflows read.
- `--dry-run` fails without fragments; `--next` falls back to a patch bump.
- `--changes` exits 1 when there are problems, printing each to stderr prefixed with `::error::` when `GITHUB_ACTIONS=true`, else `release: `.

## Side Effects
- The default mode rewrites `CHANGELOG.md`, the version files and charts and deletes fragments (via `[../../release/release.go:Assemble]`); other modes only read the tree and run `git`. Reads `BEZISYNC_ROOT` and `GITHUB_ACTIONS`.

## Verification
- Run: No co-located tests; `cd tools && go vet ./cmd/release/`, logic covered by `cd tools && go test ./release/`.
