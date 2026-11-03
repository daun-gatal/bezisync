# Changelog Fragments

A PR needs a fragment exactly when it changes a published image or chart (`backend/` or
`frontend/` outside their `.dockerignore`, or `deploy/helm/`); CI fails otherwise, and merging
one releases. Docs, tests and CI changes take none. Drop a file here named `<pr-number>-<slug>.md` when the PR
number is known. Before the PR exists, use a descriptive temporary filename such
as `kafka-data-preview.md`; renaming to the PR-number form is preferred but the
fragment content is what CI validates.

## Format

```md
type: minor

### Added
- **Feature name** - description of what was added and why
```

## Rules

- `type` is required: use `major`, `minor`, or `patch`.
- `major` means a breaking change (below 1.0 it bumps the minor: `0.1.0` → `0.2.0`).
- `minor` and `patch` below 1.0 bump the patch.
- `minor` means a backwards-compatible feature.
- `patch` means a bug fix or internal improvement.
- Use one or more of: `### Added`, `### Changed`, `### Fixed`, `### Removed`.
- Follow the existing changelog style: bold name, hyphen, description.

Do not edit `CHANGELOG.md` or version fields directly in feature PRs. The release workflow assembles fragments into `CHANGELOG.md` and bumps versions.
