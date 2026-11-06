# src

## Overview
The web UI's source: a React 19 single-page app that talks only to `/api/v1`. `main.tsx` boots it (fonts, design tokens, the TanStack Query client, the router and the top-level error boundary); the shell and routes are in `app/`, product areas in `features/`, shared components in `components/` and the API/data layer in `lib/`. `styles.css` holds the Tailwind v4 design tokens and the rem scale.

## Interface
- `[main.tsx:queryClient]` — the one `QueryClient`: no retry on 4xx `ApiError`s, at most two retries otherwise, refetch on window focus, `staleTime` 5 s.
- `[main.tsx:router]` — `makeRouter(queryClient)` from `app/router.tsx`, rendered inside `ErrorBoundary` and `QueryClientProvider` under `StrictMode`.
- `styles.css` — design tokens (`--bz-*`, each `light-dark(light, dark)`), the Tailwind `@theme` names (`bg-surface`, `text-muted`, …) and the root font-size steps at 1920 px and 2560 px.
- `test/` — vitest setup (`test/setup.ts`), the `renderAt` router test helper (`test/render.tsx`) and `test/source-rules.test.ts`, the source-level layout and UX rules every screen must pass.
- Subdirectories: [app](./app/README.md) — router, guard, shell and error surfaces; [features](./features/README.md) — product areas (pipelines, connections, alerts, settings, …); [components](./components/README.md) — shared components and the `ui` design system; [lib](./lib/README.md) — API client, session, query hooks and helpers.

## Invariants
- The unauthorized handler set in `main.tsx` (`setUnauthorizedHandler`) opens the sign-in dialog (`sessionExpiry.set(true)`) when a session existed (`meKey` is cached) and otherwise navigates to `/login?next=…`, unless already on `/login`.
- `main.tsx` throws `missing #root` when `index.html` has no `#root` element.
- Colours come only from the tokens in `styles.css`; `test/source-rules.test.ts` fails on rule breaks (bare `fr` tracks, `min-w-[…px]`, raw `<h1>`, `toast.error`, arbitrary text sizes or padding, native checkboxes/radios/selects, links styled as buttons) in any `.tsx` file outside `components/ui`, `__tests__` and `test/`.

## Side Effects
- Mounts the React tree into `#root` and loads the IBM Plex Sans/Mono font files. Network, storage and event-stream effects belong to `lib/` and `app/` (see their contracts).

## Verification
- Run: `cd frontend && bun run test`
