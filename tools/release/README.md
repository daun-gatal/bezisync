# release

## Overview
The release logic behind `scripts/release.sh` and `cmd/release`: it turns changelog fragments in `changelogs/unreleased/` into a version and a `CHANGELOG.md` section, keeps the app version in step across the backend, frontend, OpenAPI document and Helm charts, versions each chart independently by its published git tags, and checks in CI that a change carries a fragment exactly when it reaches a published image.

## Interface
- Locations: `[release.go:GoVersion]`, `[release.go:PackageJSON]`, `[release.go:OpenAPI]`, `[release.go:FragmentDir]`, `[release.go:ChartDir]`, `[release.go:Charts]`, `[release.go:AppFiles]`, `[release.go:VersionFiles]`, `[release.go:ImageContexts]`, `[release.go:DemoPaths]`.
- Versions: `[release.go:ReadVersion]`, `[release.go:BumpVersion]`, `[release.go:TargetVersion]`, `[release.go:SetVersion]`.
- Fragments and notes: `[release.go:ParseFragments]` (`[release.go:Fragment]`), `[release.go:ChangelogBlock]`, `[release.go:Notes]`, `[release.go:ChartNotes]`.
- Charts: `[release.go:ReadCharts]` (`[release.go:Chart]`), `[release.go:ChartTag]`, `[release.go:PublishedVersions]`, `[release.go:Published]`, `[release.go:NextChartVersion]`, `[release.go:SetChartVersion]`.
- Releases: `[release.go:MakePlan]` (`[release.go:Plan]`, `[release.go:ChartRelease]`, `[release.go:AgentChartName]`) — what a release publishes now; `[release.go:Assemble]` — performs an app release.
- CI check: `[release.go:ChangedPaths]`, `[release.go:Classify]` (`[release.go:Changes]`, `[release.go:Changes.Problems]`), `[release.go:ReachesImage]`.
- `[release.go:Error]` — a user-facing failure message.

## Invariants
- `[release.go:ReadVersion]` fails unless `version.go`, `frontend/package.json` and every chart's `appVersion` hold the same version.
- A fragment's first line is `type: major|minor|patch` and it needs at least one bullet under `### Added|Changed|Fixed|Removed` (`[release.go:ParseFragments]`); `README.md` and `.gitkeep` are skipped.
- Plain semver: the highest fragment type wins, defaulting to patch (`[release.go:TargetVersion]`); an override must match `X.Y.Z`; `[release.go:Assemble]` refuses a version not newer than the current one and fails without fragments.
- `[release.go:Assemble]` writes `CHANGELOG.md` and the version files before deleting the fragments, and patch-bumps only charts whose current version is already tagged (`[release.go:NextChartVersion]`).
- `[release.go:Changes.Problems]`: changes reaching an image (`[release.go:ReachesImage]`: a `backend/` or `frontend/` path not excluded by that context's `.dockerignore`, or the Dockerfile / `.dockerignore` itself; a `.dockerignore` with `!` exceptions is an error) need a fragment, and a fragment without such a change is an error (a deleted fragment does not count); a changed chart must carry a version newer than every published `chart-<name>-v<version>` tag (`[release.go:Classify]`). `Changes.Demo` is set by an image change or a `[release.go:DemoPaths]` change.
- Git is invoked with a fixed argv, never through a shell.

## Side Effects
- `[release.go:Assemble]`, `[release.go:SetVersion]` and `[release.go:SetChartVersion]` rewrite `CHANGELOG.md`, `backend/internal/version/version.go`, `frontend/package.json`, `frontend/openapi.json` and `deploy/helm/*/Chart.yaml`, and delete the assembled fragments.
- Runs `git` (`tag --list`, `diff --name-only`, `log`) in the repository root.

## Verification
- Run: `cd tools && go test ./release/`
