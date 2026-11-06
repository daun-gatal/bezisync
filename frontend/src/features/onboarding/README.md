# onboarding

## Overview
The Welcome page for a new project: a three-step setup checklist (runtime, source database, first pipeline) whose ticks come from live project data.

## Interface
- `[WelcomePage.tsx:WelcomePage]` — the checklist; each step links to the page that does it or says "Ask an Admin".

## Invariants
- A step is done only on evidence: a runtime whose `last_test.ok` is true, a connection of kind `source`, a pipeline with a `deployed_version` (`[WelcomePage.tsx:WelcomePage]`).
- Each call to action is shown only with its permission: `runtime.manage`, `connection.manage`, `pipeline.edit` via `useProject().can`.

## Side Effects
- HTTP reads only: runtimes (`[../settings/RuntimesSection.tsx:useRuntimes]`), connections (`[../../lib/connections.ts:useConnections]`), pipelines (`[../../lib/pipelines.ts:usePipelines]`).

## Verification
- Run: No co-located tests; covered by `cd frontend && bun run typecheck` and `cd frontend && bun run test src/test` (source rules).
