# mcp

## Overview
Everything the browser needs for AI agents over MCP: the OAuth consent page an agent sends a person to, and pure helpers that build per-agent setup snippets and shape the Admin tool catalogue. The helpers are shared by Account → AI agents ([../account](../account/README.md)) and Settings → MCP ([../settings](../settings/README.md)).

## Interface
- `[ConsentPage.tsx:ConsentPage]` — OAuth consent for `request` (from the URL): choose one project or all, role vs read-only, duration; Allow (approve) or Deny then follows the server's `redirect_to`.
- `[mcp.ts:connectModes]` — the connect modes an Admin left on, sign-in first (`ConnectMode`).
- `[mcp.ts:agentSetup]` / `[mcp.ts:checkSetup]` / `[mcp.ts:AGENTS]` — setup snippet per `Agent` and mode (null when unsupported), and a curl `tools/list` check; `TOKEN_PLACEHOLDER` stands in for the token.
- `[mcp.ts:grantDays]` — duration options for consent, capped at the Admin maximum.
- `[mcp.ts:groupTools]` / `[mcp.ts:toolOverrides]` / `[mcp.ts:GROUP_ORDER]` / `[mcp.ts:KIND_LABEL]` — group tools by area and compute the overrides map to save.
- `[mcp.ts:hostError]` / `[mcp.ts:originError]` — validators for redirect host names (`*.` wildcard allowed) and browser origins.

## Invariants
- `[mcp.ts:grantDays]` never offers more than `max` and always includes `max` itself.
- `[mcp.ts:toolOverrides]` saves only choices that differ from each tool's `default_enabled`.
- `[mcp.ts:agentSetup]` returns null for ChatGPT in token mode; callers drop such agents.
- Consent for a client still `pending`: non-Admins are blocked ("An Admin has to allow this agent first"); an Admin must tick "Allow this agent for everyone" (`allow_agent`) before Allow is enabled (`[ConsentPage.tsx:ConsentPage]`).
- The default duration is `min(30, max_days)`; an empty project choice is sent as `project_id: null` (all projects).
- With no `request` or with an `error` param the page shows only a notice; the request is fetched whenever `request` is set (`enabled: Boolean(request)`), even alongside `error`, but no approve/deny is possible.

## Side Effects
- HTTP: `GET /api/v1/oauth/requests/{request_id}` (no retry), `POST …/approve`, `POST …/deny`; on success `window.location.assign(redirect_to)` leaves the app.

## Verification
- Run: `cd frontend && bun run test src/features/mcp`
