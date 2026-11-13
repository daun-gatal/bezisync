# Releases, images and charts

> Part of the BeziSync engineering guide. See [AGENTS.md](../AGENTS.md)
> for the overview, architecture, and golden rules. What CI runs lives in [testing.md](testing.md).

## Branches

- **`preview`** is where work lands: every pull request targets it, and CI runs there.
- **`main`** holds released code only. Only the Release workflow moves it, by fast-forwarding it
  to preview, so it never diverges from preview and always equals the latest release.
- Merging releases nothing. A maintainer releases when preview is ready, after testing a
  pre-release built from it.

## Workflows (`.github/workflows/`)

| Workflow | Trigger | What it does |
|---|---|---|
| `ci.yml` | pull requests to `preview` | Every check: backend lint and tests, tools, the licence files (current, every licence allowed), scripts, frontend, versions and fragments (`release.sh --changes`), the manual-edit guard (CHANGELOG.md and the app version), the source scan, the images built and scanned (when an image changes), the demo and Codespaces lab smoke tests (when the demo, the lab or an image changes). Read-only token, no secrets |
| `helm.yml` | pull requests touching `deploy/helm/`, `backend/` or `frontend/` | `helm lint --strict` with every `ci/*-values.yaml`, helm-unittest (`unittests/`), kubeconform on three Kubernetes versions, the chart READMEs current (helm-docs), and installs on kind with the change's own images: bundled and external PostgreSQL, `helm test`, an agent that must report in, an in-place upgrade |
| `build-branch.yml` | manual: branch (not `main`), include charts, version override | Pre-release images `X.Y.Z-pre` (and charts `<chart version>-pre`) of the version the branch would release (`release.sh --plan`), replacing the previous `-pre`. Commits, tags and releases nothing |
| `release.yml` | manual, from `preview`: version override | The release: see below |

Shared steps: `.github/actions/build-images` (both images, linux/amd64 + linux/arm64, push,
attest) and `.github/actions/helm-publish` (package one chart, push unless that version exists,
attest, pull and render).

## Versions

- **App version** (images, CLI, every chart's `appVersion`): from the changelog fragments, plain
  semver, the highest type wins (`patch` → X.Y.Z+1, `minor` → X.Y+1.0, `major` → X+1.0.0; below
  1.0 a breaking change takes `minor`). It lives in `backend/internal/version/version.go`,
  `frontend/package.json`, `frontend/openapi.json` and the charts' `appVersion`, which CI checks
  agree (`release.sh --current`). An override (`release.sh 1.0.0`, the workflows' `version` input)
  sets it explicitly.
- **Chart versions**: each chart (`deploy/helm/bezisync`, `deploy/helm/bezisync-agent`) has its own
  `version` in its `Chart.yaml`. A chart version counts as published once its git tag
  `chart-<name>-v<version>` exists. An app release bumps the patch of a chart whose version is
  already published (only its `appVersion` changed); a chart change bumps it by hand.
- **The control-plane chart pins the agent chart**: the release fills its
  `bezisync.io/agent-chart-version` annotation with the agent chart version released with it (or
  the newest published one); agent install commands pass it as `--version`.

## What a pull request carries

- **A changelog fragment exactly when an image changes**: an image's build context is `backend/`
  or `frontend/` minus what that directory's `.dockerignore` excludes (so `backend/testdata/`,
  `backend/**/*_test.go`, `frontend/**/__tests__`, `frontend/src/test/`, `frontend/openapi.json`
  do not count), including the `Dockerfile` and `.dockerignore` themselves. Keep the
  `.dockerignore` files excluding everything an image does not need (no `!` exceptions; the check
  refuses them). CI fails a change to an image without a fragment, and a fragment without one.
- **A new chart version when a chart changes** (anything under `deploy/helm/<chart>/`): unpublished
  and newer than every published one (several changes before a release share one bump). No
  fragment for chart-only changes. Regenerate the chart READMEs (`make chart-docs`).
- Everything else — `docs/`, `e2e/`, `deploy/compose/`, `deploy/e2e/`, `deploy/apparmor/`,
  `scripts/`, `tools/`, `.github/`, `.rules/`, the guides — ships with neither.
- Never edit `CHANGELOG.md` or the app version by hand: CI refuses both.

### Changelog fragments

`changelogs/unreleased/<pr-number>-<slug>.md` (or a descriptive slug before the PR exists):

```md
type: minor

### Added
- **Feature name** - description of what changed and why
```

Valid types: `major`, `minor`, `patch`; sections: `Added`, `Changed`, `Fixed`, `Removed`.

## Releasing

1. **Build branch** on `preview`: images `X.Y.Z-pre` and charts `<chart version>-pre`. Test them
   (e.g. deploy them into a test cluster). Any other branch can be built the same way to try it
   out; every build replaces the previous `-pre`.
2. **Release** (run from `preview`). It plans with `release.sh --plan`:
   - **app** — fragments are pending: images `X.Y.Z` + `latest`, the CLI, every chart;
   - **charts** — no fragments, but a chart's version is unpublished: only those charts;
   - **nothing** — it stops.

   It **stops unless the `-pre` of everything it would ship was built from preview's latest
   commit** (the images' `org.opencontainers.image.revision` index annotation and the charts'
   `bezisync.io/revision` annotation). The summary shows the version and the notes.
3. **Approve** the `release-approval` environment (rejecting changes nothing). Then:
   - app releases: `release.sh` writes the `CHANGELOG.md` section, deletes the fragments, sets the
     app version everywhere and bumps published charts; `make chart-docs`; commit
     `chore: release vX.Y.Z` to preview;
   - `main` is fast-forwarded to preview (an atomic, non-force push of both: if preview moved since
     the plan, nothing is pushed and a new `-pre` and run are needed);
   - images, charts (immutable: an existing version is never overwritten), the tags `vX.Y.Z` and
     `chart-<name>-v<version>`, the GitHub Release (the `CHANGELOG.md` section, the chart
     versions, the CLI archives for Linux, macOS and Windows on amd64 and arm64 with
     `checksums.txt`) and a post in Discussions (category *Announcements*). A chart-only release
     gets a GitHub Release per chart (not marked latest), its notes the pull requests that changed
     the chart since its previous release (`release.sh --chart-notes`);
   - every `-pre` image and chart is deleted.

Locally: `scripts/release.sh --plan` shows what a release would publish, `--dry-run` the app
version, `--notes X.Y.Z` a release's notes.

## Published artifacts

| | Tags |
|---|---|
| Images `ghcr.io/daun-gatal/bezisync` (api, controller, agent, migrate, CLI: one binary) and `ghcr.io/daun-gatal/bezisync-web` (UI), linux/amd64 + linux/arm64 | `X.Y.Z`, `latest`; `X.Y.Z-pre` while testing |
| Charts `oci://ghcr.io/daun-gatal/charts/{bezisync,bezisync-agent}` | `<chart version>`; `<chart version>-pre` while testing |
| CLI archives on the GitHub Release (binary, `LICENSE`, `NOTICE`, `LICENSES.txt`) | `bezisync_X.Y.Z_<os>_<arch>.tar.gz` / `.zip`, `checksums.txt` |

Not published: the Flink CDC engine image. Admins build it (the guide's Flink CDC engine section
lists its layout), push it to their registry and register it in Settings.

Build provenance is attested with GitHub artifact attestations (stored by GitHub, so no
signature tags in the registry): `gh attestation verify oci://ghcr.io/daun-gatal/bezisync:X.Y.Z
--owner daun-gatal`. They need the repository to be public. buildx's own provenance and SBOM are
off, so images carry no "unknown/unknown" entries. The charts' `image.tag` / `webImage.tag`
default to `latest` (`pullPolicy: Always`) and the compose demo uses `:latest` too — pin a tag for
reproducible installs.

## Repository settings the workflows rely on

- Environment **`release-approval`**: required reviewer (a maintainer), deployments only from
  `preview`, administrators cannot bypass it. It holds the secret **`RELEASE_SECRET`**: a
  fine-grained token of a repository admin (this repository, Contents: read and write), which
  pushes the release commit to `preview` and fast-forwards `main`.
- Rulesets (Repository admin bypasses all three, which is how the release token pushes):
  - **`preview`**: no deletion or force push, linear history, a pull request with one approval
    (stale approvals dismissed, conversations resolved; squash merges only) and these checks:
    Backend lint, Backend tests, Tools, Frontend, Versions and changelog, Demo compose file,
    Vulnerability scan (source), Build and scan images, Demo stack smoke test,
    Helm charts result. Not "up to date with the base".
    A job skipped by its `if` counts as passed; `helm.yml` starts on every pull request and its
    `Helm charts result` job is the one chart check to require (skipped matrix jobs report under
    an unexpanded name). Renaming a required job means updating the ruleset.
  - **`main`**: updates restricted to the bypass, no deletion or force push, linear history.
  - **`release tags`** (`v*`, `chart-*`): no update or deletion — a published version's tag never
    moves. Creation stays open: the release creates them with `GITHUB_TOKEN`, and a personal
    account cannot name GitHub Actions as a bypass actor.
- Merging: squash only (the PR title becomes the commit subject, with `(#N)`), head branches
  deleted after merge.
- GHCR packages public and linked to the repository (the images' `org.opencontainers.image.source`
  label links them); Discussions with an *Announcements* category; Actions approval required for
  outside contributors.
