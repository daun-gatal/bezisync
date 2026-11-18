# src

## Overview
The generator behind the website: `build.ts` reads the docs from the checkout and writes every
page; `layout.ts` is the page shell; `landing.ts` the home page; `seo.ts` the titles,
descriptions and structured data; `styles.css` and `site.js` are copied as they are.

## Interface
- `[build.ts]` — the entry point: `bun src/build.ts [--release] [--ref <git ref>] [--out dist]`.
  `--release` labels the docs with the app version (`backend/internal/version/version.go`) and
  links GitHub at its tag; without it they say "+ unreleased changes" and link `--ref` (default
  `preview`).
- `[layout.ts:layout]` — the HTML of a docs page or a plain page; `[layout.ts:head]`,
  `[layout.ts:header]`, `[layout.ts:footer]` its parts; `[layout.ts:SITE]` the public origin.
- `[landing.ts:landing]` — the home page.
- `[seo.ts:OVERRIDES]` — hand-written titles and descriptions for the pages people search for,
  each with phrases its page must contain; `[seo.ts:autoDescription]`, `[seo.ts:docTitle]` the
  defaults; `[seo.ts:jsonLdScript]` structured data.
- `[site.js]` — theme toggle, copy buttons, docs search over `/docs/search.json`, the mobile menu.

## Invariants
- The sidebar, page order and the docs home's cards come from `docs/README.md`: each `##` is a
  section, and each item (a link to `guide/<page>.md`, an em dash, a description) a page at `/docs/<page>/`; a guide
  page missing from it fails the build `[build.ts:sections]`.
- Reference pages keep their URLs: `/docs/reference/checks/`, `/docs/security/threat-model/`,
  `/docs/changelog/` `[build.ts:FIXED_URLS]`.
- Old URLs keep working: renamed sections redirect `[build.ts:REDIRECTS]`; anchors of sections
  that became their own pages forward from where they were `[build.ts:SPLIT]`; check anchors
  forward to their group page `[build.ts:movedChecks]`.
- The build fails, and writes nothing, on a link to a missing page or anchor, a missing or unused
  image, an SEO override that no longer holds, or a description outside 70–160 characters
  `[build.ts:fail]`.
- Pages carry a Content-Security-Policy meta tag (GitHub Pages sets no headers): scripts and
  styles only from the site itself `[layout.ts:head]`.

## Side Effects
- Writes the output directory (`--out`, default `website/dist/`), deleting it first.

## Verification
- Run: `cd website && bun test`
