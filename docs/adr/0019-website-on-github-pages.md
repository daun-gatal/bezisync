# 0019 — The website lives in this repository, its docs are pages, and GitHub Pages serves it

- **Status:** Accepted
- **Date:** 2026-11-18
- **Scope:**
  - `website/` (new, moved from `daun-gatal/bezisync-lab`'s `site/`): the generator, landing page and tests.
  - `docs/`: the single-file guide becomes an index (`docs/README.md`) and pages (`docs/guide/*.md`) with screenshots (`docs/images/`).
  - `scripts/docs-screenshots.sh` + `.mjs` (new): the screenshots, from the Codespaces lab stack.
  - `.github/workflows/pages.yml` (new), `release.yml` (calls it), `ci.yml` (a Website job).
  - Links into the old guide (README, Codespaces welcome, rules), `.rules/*`, `AGENTS.md`, `ARCHITECTURE.md`.
  - Outside this repository: `bezisync-lab` drops `site/` and its workflows; `k3s-cloudflare-lab`
    drops the Ingress; the `bezisync-site` namespace is deleted.

## Context

The website, `https://bezisync.dglabs-ai.dev` (landing page and `/docs/`), was built in a second
repository. A Bun script downloaded this repository's `docs/` at the latest release tag, cut the
1,600-line `docs/README.md` into one page per `##` section, packed the result into an nginx image
and rolled it out to a home k3s cluster over Tailscale; a third repository routed the host name
through a Cloudflare tunnel. So a change to the docs and a change to how they are shown landed in
different pull requests; the generator carried code for docs older or newer than itself (renamed
sections, phrases that might be missing, feature detection on the landing page); and the site
depended on a home cluster. The guide had no screenshots, and its pages read like a
specification: correct, but dense.

CHouse UI, by the same maintainer, keeps its site in its own repository, publishes it with GitHub
Pages under a `dglabs-ai.dev` subdomain, and writes its docs as task pages with a screenshot of
each screen. The goal: the same here, without breaking a URL people or search engines already use.

Non-goals: versioned docs (the site shows one version), a different framework for the generator,
app changes (no image changes, so no changelog fragment).

## Decision

1. **`website/` builds from the checkout.** The generator reads `docs/` and `CHANGELOG.md` from the
   same commit and writes static HTML. The code that coped with other versions of the docs goes;
   the landing page states what this commit does.
2. **The docs are pages.** `docs/README.md` is the index: a `##` per section, an item
   `[Title](guide/page.md) — description` per page. The site takes its sidebar, page order, cards
   and page descriptions from it, so the docs read the same on GitHub and on the site. Each page
   follows one shape: what it is, the screenshot, where to find it and who can use it, tasks as
   numbered steps, options as tables, fine print last.
3. **Screenshots are committed** in `docs/images/` (1440×900 JPEG, about 1.5 MB in all), made by
   `scripts/docs-screenshots.sh`: it starts the Codespaces lab stack from the working tree,
   creates and deploys the pipelines the pages show, and captures each screen with Playwright
   inside the stack's network. It never runs in CI. The build fails on a missing or unused image.
4. **GitHub Pages serves it**, deployed by `pages.yml`:
   - **after every release**: `release.yml` calls it with the released commit (app and chart-only
     releases; a chart-only release has no pending app change, so the docs describe the released
     app). It deploys after the images, charts and GitHub Release exist, never before;
   - **by hand, from `preview`**, for a docs fix between releases: allowed while
     `changelogs/unreleased/` holds no fragment (the app is unchanged since the release), otherwise
     only with `unreleased: true`, and the pages then say "+ unreleased changes".
   Both run from `preview`, the default branch, which the `github-pages` environment allows. The
   custom domain is a Pages setting (a `CNAME` file is ignored by `actions/deploy-pages`).
5. **URLs stay.** Every page keeps the URL its section had (`/docs/<section anchor>/`); the
   reference pages keep theirs. Renamed sections redirect; the two sections that became several
   pages (Install → Codespaces lab; Destinations → four pages by kind) forward their old anchors.
   `docs/README.md` keeps an `<a id>` per old section, so GitHub links to `docs/README.md#install`
   still land on the right entry. The test suite lists every old URL.
6. **The build refuses to publish a broken page**: a link to a missing page or anchor, a missing
   image, an SEO override whose phrases left the page, a description outside 70–160 characters.
   CI's Website job runs it on every pull request.

### Implementation plan

1. Move `site/` to `website/`; read docs from the checkout; `--release` / `--ref`; fail on problems.
2. Split `docs/README.md` into `docs/guide/*.md` and an index; repoint every link into it.
3. The screenshot script; capture; show each image on its page and on the landing page.
4. Rewrite the pages in the shared shape.
5. `pages.yml`, `release.yml`'s Website job, `ci.yml`'s Website job.
6. Contracts, rules, `AGENTS.md`, `ARCHITECTURE.md`; remove the old site elsewhere.

### Verification plan

- `cd website && bun run typecheck && bun test`, in CI on every pull request.
- The built site rendered at 1280 px (light) and 390 px (dark): no horizontal scroll, no console
  errors under the Content-Security-Policy, old URLs and anchors land on their new page.

| ID | Case | Expected | Covered by |
|---|---|---|---|
| E1 | A saved link to `/docs/install/` or another section | The same page | test *every old URL is still a page* |
| E2 | `/docs/destinations/#apache-iceberg` | Forwards to `/docs/destinations-lakehouse/#apache-iceberg` | test *sections that became their own pages*; rendered check |
| E3 | `/docs/debezium-versions/`, `/docs/flink-cdc-engine-optional/` | Redirect, noindex, canonical to the new page | test *renamed sections redirect* |
| E4 | `docs/README.md#install` from a chart README on GitHub | Lands on the Install entry of the index | the `<a id>` stubs; build resolves `README.md#install` |
| E5 | A page links an anchor that a rewrite renamed | Build fails, CI red | build `fail` |
| E6 | A screenshot is deleted but still referenced, or added but not shown | Build fails | build `fail` |
| E7 | A guide page not listed in the index | Build fails | build `fail` |
| E8 | Release pipeline: website deploys before the release exists | Impossible: the job needs the GitHub Release (app) or the charts (chart-only) | `release.yml` `website.needs` |
| E9 | Manual deploy with unreleased app changes on preview | Refused unless `unreleased: true`, then labelled | `pages.yml` *Which docs* |
| E10 | `--release` with a changelog whose latest release isn't the app version | Build fails | build `fail`; test *builds a release too* |

## Consequences

- One pull request changes behaviour, docs and the site together, and CI shows a broken docs
  link on that pull request. The home cluster, its image and its Tailscale secrets no longer
  serve the site.
- Docs merged to `preview` reach the site at the next release, or by hand when nothing unreleased
  is pending. That is the price of never showing unreleased behaviour by accident.
- Screenshots go stale as the UI changes; refreshing them is one command, and a pull request that
  changes a screen should refresh its image.
- GitHub Pages sets no response headers: the CSP is a `<meta>` tag (no `frame-ancestors`), and
  there is no nginx cache control; the stylesheet and script are fingerprinted by hash instead.
- Cutover (once, outside the code): set the Pages source to GitHub Actions and the custom domain
  `bezisync.dglabs-ai.dev`; a DNS-only CNAME to `daun-gatal.github.io`; wait for the certificate,
  then enforce HTTPS.

## Alternatives considered

- **Keep the site in `bezisync-lab`, only move hosting to Pages.** Docs and site would still change
  in different pull requests, and the version-compatibility code would stay.
- **Deploy on every push to `main`.** `release.yml` pushes `main` before the images and the GitHub
  Release exist; the site would announce a version not yet published.
- **Deploy on every push to `preview`.** The site would describe behaviour no release has.
- **Port CHouse UI's Vite + React docs app.** A framework and a bundle for static text; the Bun
  generator already has search, the sidebar, SEO and the check-reference split.
- **Keep one long guide and let the site split it.** Hard to read on GitHub, no place for a
  screenshot per screen, and no per-page descriptions.
- **Screenshots under `website/public/`.** The docs on GitHub would show broken images.
