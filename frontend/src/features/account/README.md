# account

## Overview
The personal Account page every member has whatever their role: profile name, theme, Guided/Expert mode, notification settings, personal API tokens and AI agents connected over MCP. It is a feature screen under `features/`, routed by `app/router.tsx`, and reuses the MCP setup helpers from [../mcp](../mcp/README.md).

## Interface
- `[AccountPage.tsx:AccountPage]` — the page: Profile card (name), Preferences card (theme via `useTheme`, mode switch via `useUpdateMe`, opens `NotificationSettings`), then the two sections below.
- `[TokensSection.tsx:TokensSection]` — lists, creates and revokes personal API tokens; after creation shows the token once with a CLI login line and, when MCP allows tokens, a Claude Code setup line.
- `[AgentsSection.tsx:AgentsSection]` — "AI agents" card: per-agent MCP setup snippets for the connect modes an Admin left on, and the caller's sign-in (OAuth) grants with Disconnect.

## Invariants
- A new token is shown only once: the result modal cannot be closed (`closable={false}`) and Done stays disabled until "I've copied it" is ticked (`[TokensSection.tsx:copied]`).
- Token scope defaults to the current project; `''` means all the caller's projects and is sent as `project_id: null`; expiry `never` is sent as `expires_in_days: null` (`[TokensSection.tsx:EXPIRY]`).
- `[AgentsSection.tsx:AgentsSection]` renders nothing when MCP is disabled (`!i.enabled`), only offers modes from `connectModes`, and drops agents for which `agentSetup` returns null (ChatGPT has no token mode); grants are listed only when `oauth_enabled`.
- The mode switch flips `ui_mode` between `guided` and `expert`; it is the same server-side preference `useProject().expert` reads (`[../../lib/session.tsx:useUpdateMe]`).

## Side Effects
- HTTP: `PATCH /api/v1/auth/me` (name, `ui_mode`) via `useUpdateMe`; `GET/POST /api/v1/auth/tokens`, `DELETE /api/v1/auth/tokens/{token_id}`; `GET /api/v1/mcp/info`, `GET /api/v1/mcp/grants`, `DELETE /api/v1/mcp/grants/{grant_id}`. Invalidates the `tokens` and `mcp-grants` queries.
- Theme choice is stored in `localStorage` (`bz.theme`) by `[../../lib/theme.ts:useTheme]`.
- Reads `window.location.origin` for the CLI login line.

## Verification
- Run: No co-located tests; covered by `cd frontend && bun run typecheck`, `cd frontend && bun run test src/features/mcp` (the setup helpers it renders) and `cd frontend && bun run test src/test` (source rules).
