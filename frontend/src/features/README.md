# features

## Overview
The product areas of the React SPA, one directory per area. Each holds the pages and dialogs of that area plus any pure logic they need (tested in a co-located `__tests__/`); routes are wired in `app/router.tsx`, screens are built only from `components/ui`, and server state goes through `lib/api.ts` and the `lib/*` query hooks (see `.rules/frontend.md`).

## Subdirectories & Boundaries
- [account](./account/README.md) — personal settings: profile, theme, mode, API tokens, connected AI agents.
- [alerts](./alerts/README.md) — open alerts, alert rules and notification channels.
- [auth](./auth/README.md) — public sign-in page and the lab testing policy.
- [connections](./connections/README.md) — sources and destinations: add, check, copy, secrets from stores.
- [feedback](./feedback/README.md) — the in-app feedback dialog.
- [learn](./learn/README.md) — the plain-language CDC glossary.
- [mcp](./mcp/README.md) — OAuth consent page for AI agents and MCP setup helpers.
- [onboarding](./onboarding/README.md) — the Welcome setup checklist.
- [pipelines](./pipelines/README.md) — pipeline list, wizard and pipeline page.
- [settings](./settings/README.md) — Admin-only Settings sections.
- [tour](./tour/README.md) — the product tour page.
