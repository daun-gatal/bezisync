# frontend

## Overview
The BeziSync web UI: a React SPA built with Vite and served by nginx in the `bezisync-web` image. It talks only to `/api/v1` (plus the OAuth discovery documents for MCP agents), which nginx proxies to the API. This directory holds the build and serving configuration; the app is in `src/`, static files in `public/`. The `openapi.json` here is generated from the backend and drives the typed client in `src/lib/api-schema.ts`.

## Interface
- `[vite.config.ts:defineConfig]` — Vite + React + Tailwind v4 plugins, the `@` alias to `src/`, the dev proxy of `/api` and `/.well-known/oauth-` to `http://localhost:8000`, source maps, and the vitest config (jsdom, `src/test/setup.ts`).
- `index.html` — the page shell: loads `/theme.js` before the app, then `/src/main.tsx` into `#root`.
- `nginx.conf.template` — the image's server on port 8080: proxies `/api/`, `/healthz` and `/.well-known/oauth-` to `${BEZISYNC_API_UPSTREAM}`, serves `/assets/` immutable for a year and every other path as `index.html` (SPA fallback).
- `Dockerfile` — builds the assets with `oven/bun` (`bun install --frozen-lockfile`, then `vite build` without the type check) and copies them and the template into `nginxinc/nginx-unprivileged:stable-alpine`, running as user 101.
- `package.json` scripts: `dev` (Vite on port 8080), `build` (`tsc -b && vite build`), `typecheck`, `test` (`vitest run`), `generate:api`, `check:api`.
- `openapi.json` — the API description `make openapi` writes; `generate:api` turns it into `src/lib/api-schema.ts`.
- Subdirectories: [src](./src/README.md) — the application source; [public](./public/README.md) — static files copied to the site root.

## Invariants
- Every HTML and asset response carries `Content-Security-Policy` with `default-src 'self'` (no inline scripts; inline styles and `data:` images are allowed), `frame-ancestors 'none'`, plus `X-Frame-Options DENY`, `X-Content-Type-Options nosniff` and `Referrer-Policy same-origin` (`nginx.conf.template`); keep the two `location` blocks' headers identical when changing them.
- `/api/` is proxied unbuffered with a 1 h read timeout (server-sent events), forwarding `Host`, `X-Forwarded-For` and `X-Forwarded-Proto`; request bodies are capped at 5 MB (`client_max_body_size 5m`).
- `index.html` is served `no-cache` and `/assets/` `immutable`, so a deploy takes effect on the next page load.
- `openapi.json` and `src/lib/api-schema.ts` are generated: change the backend and run `make openapi`; `bun run check:api` (and `make openapi-check`) fail on drift.
- The image runs as the unprivileged user 101 and upgrades Alpine packages at build time (`Dockerfile`).

## Side Effects
- None at runtime from this directory's files beyond nginx serving and proxying; the image reads `BEZISYNC_API_UPSTREAM` (default `http://bezisync-api:8000`) when nginx renders the template.

## Verification
- Run: `cd frontend && bun run typecheck && bun run test`
