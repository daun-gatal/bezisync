# Changelog Fragments

A PR needs a fragment exactly when it changes a published image (`backend/` or `frontend/`
outside their `.dockerignore`); CI fails otherwise. The next release collects every fragment on
`preview`. Docs, tests, CI and chart-only changes take none (a chart change bumps that chart's
`version` in its `Chart.yaml` instead). Drop a file here named `<pr-number>-<slug>.md` when the
PR number is known. Before the PR exists, use a descriptive temporary filename such as
`kafka-data-preview.md`; renaming to the PR-number form is preferred but the fragment content is
what CI validates.

## Format

```md
type: minor

### Added
- **Feature name** - description of what was added and why
```

## Rules

- `type` is required: `major`, `minor` or `patch`. The highest type among the pending fragments
  decides the release version (plain semver):
  - `major` → `X+1.0.0`: a breaking change once BeziSync is 1.0 or later.
  - `minor` → `X.Y+1.0`: a backwards-compatible feature, and below 1.0 also a breaking change.
  - `patch` → `X.Y.Z+1`: a bug fix or internal improvement.
- Use one or more of: `### Added`, `### Changed`, `### Fixed`, `### Removed`.
- Follow the existing changelog style: bold name, hyphen, description.

Do not edit `CHANGELOG.md` or version fields directly in feature PRs. The release workflow
assembles fragments into `CHANGELOG.md` and sets the versions.
