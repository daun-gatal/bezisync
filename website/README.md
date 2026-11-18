# website

## Overview
The BeziSync website, <https://bezisync.dglabs-ai.dev>: the landing page and the documentation.
A small Bun static-site generator (no framework, no client bundle) turns this checkout's
`docs/` (the index `docs/README.md`, the pages `docs/guide/*.md`, the screenshots `docs/images/`,
the check reference and the threat model) and `CHANGELOG.md` into plain HTML under `dist/`.
GitHub Pages serves it ([ADR 0019](../docs/adr/0019-website-on-github-pages.md)): `release.yml`
publishes it after every release through `.github/workflows/pages.yml`. Nothing here ships in an
image or a chart.

## Subdirectories & Boundaries
- [src](./src/README.md) — the generator: pages, layout, landing page, SEO, styles and the small client script.
- `test/` — `bun test`: builds the site and checks every page (titles, descriptions, canonical
  URLs and sitemap, headings, JSON-LD, links, the URLs the site has always had, screenshots).
- `public/` — files copied as they are (Google Search Console's ownership file).
- `package.json` / `bun.lock` — scripts `build`, `test`, `typecheck`, `dev`; the dependencies are
  `marked`, `highlight.js` and two IBM Plex fonts.
- Boundary: the site reads the repository; nothing in the repository reads the site. Screenshots
  are made by `scripts/docs-screenshots.sh`, never by the build.

## Verification
- Run: `cd website && bun install && bun run typecheck && bun test`
