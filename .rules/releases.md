# Releases, images and deployment

> Part of the BeziSync engineering guide. See [CLAUDE.md](../CLAUDE.md) / [AGENTS.md](../AGENTS.md)
> for the overview, architecture, and golden rules. What CI runs lives in [testing.md](testing.md).

## Workflows (`.github/workflows/`)

| Workflow | Trigger | What it does |
|---|---|---|
| `ci.yml` | pull requests to `main` | Every check (backend, scripts, frontend, charts, compose, versions/changelog, vulnerability scan, e2e type check) and, when the PR changes an image or chart, the image build and the demo smoke test (the demo also runs for `deploy/compose/` and its script). It does **not** run again on `main` after a merge |
| `push.yml` | push to `main` (every merge) | With fragments in `changelogs/unreleased/`: **releases** — see below. Without: nothing (no build, no publish). Images are signed (cosign keyless) with an SBOM attestation; both Helm charts are published as signed OCI artifacts. Optionally calls `deploy.yml` after a release (repository variable `AUTO_DEPLOY=true`) |
| `deploy.yml` | manual, or called by `push.yml` | Deploys our cluster from [`k8s/`](../k8s/README.md): `diff`, `dry-run`, `apply`, `restart`, `uninstall`; `apply` and `playground-apply` first install `k8s/apparmor` (the AppArmor profile loader every pod relies on); `playground-apply` / `playground-delete` manage the [playground](../k8s/playground/README.md) (`k8s/playground/playground.sh`). The image tag defaults to the current version (`scripts/release.sh --current`: the release that commit carries) |

Published on a release `vX.Y.Z` (nothing else publishes):

| | |
|---|---|
| Images `ghcr.io/daun-gatal/bezisync` (api, controller, agent, migrate, CLI: one binary) and `ghcr.io/daun-gatal/bezisync-web` (UI) | `:X.Y.Z`, `:vX.Y.Z`, `:latest`, `:sha-<short>`, `:main` |
| Charts `oci://ghcr.io/daun-gatal/charts/{bezisync,bezisync-agent}` | `X.Y.Z` (appVersion `X.Y.Z`) |

The charts' `image.tag` / `webImage.tag` default to `latest` (`pullPolicy: Always`) and the compose
demo uses `:latest` too — pin a tag for reproducible installs. The control plane passes its chart
version to agent install commands.

## What releases: changes that reach an artifact

A pull request carries a changelog fragment **exactly when** it changes a published artifact; CI
fails otherwise (`scripts/release.sh --changes HEAD^1` on the PR's merge commit; locally
`--changes origin/main`). A change reaches an artifact when it touches:

- an image's build context: `backend/` or `frontend/` minus what that directory's
  `.dockerignore` excludes (so `backend/testdata/`, `backend/**/*_test.go`, `frontend/**/__tests__`, `frontend/src/test/`,
  `frontend/openapi.json` do not), including the `Dockerfile` and `.dockerignore` themselves;
- a Helm chart: anything under `deploy/helm/`.

Everything else — `docs/`, `e2e/`, `k8s/` (deploy it with `deploy.yml`), `deploy/compose/`,
`deploy/e2e/`, `scripts/`, `.github/`, `.rules/`, the guides — ships with **no** fragment and
merges without a release. The `.dockerignore` files decide what counts: keep them excluding
everything an image does not need (no `!` exceptions; the check refuses them).

## Changelog fragments

A PR that changes an image or chart adds a fragment under `changelogs/unreleased/`
(`<pr-number>-<slug>.md`, or a descriptive slug before the PR exists); never edit `CHANGELOG.md`
or a version field in a feature PR.

```md
type: minor

### Added
- **Feature name** - description of what changed and why
```

Valid types: `major`, `minor`, `patch`; sections: `Added`, `Changed`, `Fixed`, `Removed`. CI
validates every fragment.

## Releases

There is no manual step: **merging a pull request that carries fragments is the release.** On that
push to `main`, `push.yml`:

1. runs `scripts/release.sh` (the Go tool `tools/cmd/release`) on the newest `main`: the highest fragment type decides the bump
   (`major` > `minor` > `patch`; while the version is below 1.0, `major` bumps the minor and
   `minor`/`patch` the patch, so 1.0.0 is only ever an explicit decision); it writes the `CHANGELOG.md` section, deletes the fragments and
   sets the version in `backend/internal/version/version.go`, `frontend/package.json`,
   `frontend/openapi.json` and both `Chart.yaml` (`version`, `appVersion`); CI checks they agree
   (`scripts/release.sh --current`);
2. commits `chore: release vX.Y.Z` to `main` and tags `vX.Y.Z` in one atomic push (pushed with
   `GITHUB_TOKEN`, so it triggers no workflow; if `main` moved meanwhile, the newer merge's run
   releases instead);
3. builds the images from the release commit (the version is baked in), publishes images and
   charts, and creates the GitHub Release from the `CHANGELOG.md` section.

A PR that reaches no artifact (docs, tests, CI) adds no fragment and releases nothing. Locally,
`scripts/release.sh --dry-run` prints the version the pending fragments give and
`--notes X.Y.Z` a release's notes.
