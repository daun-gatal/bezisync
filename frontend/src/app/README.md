# app

## Overview
The application frame around the feature screens: the code-based TanStack Router route tree, the route guard that decides who may see a page, the project shell (sidebar, inbox, command palette, help menu), the mid-session sign-in dialog and the error boundaries. Features under `../features/` provide the pages; this directory wires them to URLs and to the session and project context from `../lib/session.tsx`.

## Interface
- `[router.tsx:makeRouter]` — builds the router (context `[router.tsx:RouterContext]` with the `QueryClient`, intent preloading, scroll restoration, `RouteError` as the default error component) over the tree: `/login`, `/oauth/consent` (MCP OAuth consent), `/testing-policy`, `/__ui`, `/` (Home) and `/p/$projectSlug/…` (`pipelines`, `pipelines/new`, `pipelines/$pipelineId`, `connections`, `alerts`, `welcome`, `learn`, `tour`, `account`, `settings/$section`, legacy `admin/$section`); the root renders `[SessionDialog.tsx:SessionDialog]` and the toaster; `[router.tsx:pageSearch]` parses `?page=`; `[router.tsx:PipelinesSearch]`, `[router.tsx:NewPipelineSearch]`, `[router.tsx:PIPELINE_TABS]` type the pipeline search params.
- `[guard.tsx:Guard]` — renders, redirects or explains per `[guard.tsx:Access]` (`public`, `signed-out`, `signed-in`) using `[guard.tsx:sessionState]` and `[guard.tsx:decide]`; `[guard.tsx:safeNext]` sanitises the post-sign-in target.
- `[guard.tsx:sessionExpiry]` / `[guard.tsx:useSessionExpired]` — the external store that opens `[SessionDialog.tsx:SessionDialog]` over the page after a 401.
- `[guard.tsx:homeProject]` / `[guard.tsx:rememberProject]` — Home opens the last project used.
- `[AppShell.tsx:AppShell]` — sidebar from `[AppShell.tsx:NAV]` with the project switcher and firing-alert badge, footer (help menu, feedback, `[InboxBell.tsx:InboxBell]`, user menu with theme, Guided/Expert mode, notification settings and sign-out), the sandbox banner and live updates for the project.
- `[CommandPalette.tsx:CommandPalette]` — ⌘K / Ctrl+K palette over pipelines, connections, pages and visible settings sections; `[CommandPalette.tsx:OPEN_PALETTE]` is the window event that opens it.
- `[ErrorPanel.tsx:ErrorBoundary]`, `[ErrorPanel.tsx:RouteError]`, `[ErrorPanel.tsx:NotFound]`, `[ErrorPanel.tsx:QueryError]`, `[ErrorPanel.tsx:FullPageError]`, `[ErrorPanel.tsx:FullPageLoading]`, `[ErrorPanel.tsx:ErrorPanel]` — error and loading surfaces.
- `[AboutDialog.tsx:AboutDialog]` (version, licence links), `[NotificationSettings.tsx:NotificationSettings]` (the person's notification preferences via `useUpdateMe`), `[Gallery.tsx:Gallery]` (the `/__ui` component gallery).

## Invariants
- Only a 401 means signed out: `[guard.tsx:sessionState]` maps a network failure or 5xx to `unreachable`, which shows `FullPageError` with Try again instead of sending people to `/login`.
- `[guard.tsx:safeNext]` accepts only same-site paths: it must start with `/`, not `//` or `/\`, and not `/login`; anything else goes to `/`.
- `[guard.tsx:Guard]` freezes the `?next=` address when a `to-login` redirect starts so it is not nested in itself.
- Every project route renders inside `ProjectScope` (`[router.tsx:makeRouter]`'s tree): an unknown or inaccessible slug shows "Project not found" rather than a redirect, and the shell is wrapped in `ProjectProvider`.
- Settings appears in the sidebar and the palette only when `canOpenSettings(ctx)` allows it. The palette keeps its own page list (`[CommandPalette.tsx:CommandPalette]`), separate from `[AppShell.tsx:NAV]`: a new top-level page goes in both. The palette shows at most 50 matches.
- `/__ui` lazily loads `[Gallery.tsx:Gallery]` only when `import.meta.env.DEV`; production builds route it to `[ErrorPanel.tsx:NotFound]`.
- `[SessionDialog.tsx:SessionDialog]` keeps the page on re-sign-in; signing in as a different user clears the query cache and navigates home.
- Legacy URLs keep working: old `/admin/$section` and renamed settings sections redirect through `LEGACY_ADMIN_SECTIONS` / `LEGACY_SECTIONS`, and old pipeline tabs map through `LEGACY_PIPELINE_TABS` (`router.tsx`).

## Side Effects
- `localStorage`: `bz.lastProject` (`guard.tsx`), `bz.sidebar.collapsed` (`AppShell.tsx`), both through `components/ui/hooks`.
- `[AppShell.tsx:AppShell]` opens the project's server-sent event stream (`useLiveUpdates`) and polls open alerts for the badge (`useAlertCount`); the inbox polls through `useInbox` and marks everything read when its popover closes with unread items (`useReadAll`). The user menu changes `ui_mode` through `useUpdateMe` and signs out through `useLogout`; `localStorage` `bz.tour.seen` is set when the Tour is opened or its card dismissed (`lib/tour.ts`).
- Global listeners: `keydown` (⌘K) and the `OPEN_PALETTE` event in `[CommandPalette.tsx:CommandPalette]`.
- `[ErrorPanel.tsx:ErrorBoundary]` logs caught errors with `console.error`; its panel can reload the page.
- In an open-lab install (`useProviders().lab`) the help menu also opens `/testing-policy` in a new window (`window.open`, `noopener`).

## Verification
- Run: `cd frontend && bun run test src/app`
