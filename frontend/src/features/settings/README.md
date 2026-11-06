# settings

## Overview
The Admin-only Settings page and its sections: people and projects, feedback inbox, sign-in providers, MCP, runtimes, secret stores, engine versions, policies, templates and the audit log. `SettingsPage` routes `/p/$projectSlug/settings/$section`; per-project sections can be pointed at another project with `?scope=`. Each section is a thin screen over the REST API (directly via `lib/api.ts` or a `lib/*` hook); `signin.ts` holds the pure logic of the Sign-in forms.

## Interface
- `[SettingsPage.tsx:SettingsPage]` / `[SettingsPage.tsx:SETTINGS_SECTIONS]` — the shell and the section registry (id, label, group, `perProject`, render); also read by the command palette. `[SettingsPage.tsx:LEGACY_SECTIONS]` / `[SettingsPage.tsx:LEGACY_ADMIN_SECTIONS]` map old ids; `[SettingsPage.tsx:canOpenSettings]` is the gate.
- `[PeopleSection.tsx:PeopleSection]` — every account: invite, Admin on/off, per-project role and approver right, deactivate/reactivate; `ROLES`, `usersKey`, `ApproverToggle` are reused.
- `[ProjectsSection.tsx:ProjectsSection]` / `[ProjectsSection.tsx:addressError]` — create, rename, delete projects; the slug validator mirrors the server pattern.
- `[FeedbackSection.tsx:FeedbackSection]` / `[FeedbackSection.tsx:useOpenFeedbackCount]` — the feedback inbox (cursor-paged, resolve/reopen) and the nav badge count.
- `[SignInSection.tsx:SignInSection]` — SSO providers (OIDC, OAuth2, SAML): add from a preset, edit, test, enable/disable, remove; email-and-password on/off.
- `[signin.ts:draftBody]` / `[signin.ts:draftProblem]` / `[signin.ts:draftFromProvider]` / `[signin.ts:draftFromPreset]` / `[signin.ts:PRESETS]` — draft model, request body and form validation (`urlError`, `issuerError`, `scopeError`); `[signin.ts:passwordOffBlocker]` / `[signin.ts:lastOptionBlocker]` keep a way to sign in; summaries `ruleSummary`, `defaultSummary`, `certExpiryDays`.
- `[McpSection.tsx:McpSection]` — MCP settings (on/off, sign-in vs tokens, new-agent policy, max grant days, redirect hosts, browser origins, per-tool overrides), registered agents (allow/block/remove) and everyone's grants.
- `[RuntimesSection.tsx:RuntimesSection]` / `[RuntimesSection.tsx:useRuntimes]` — Kubernetes/Docker runtimes, direct or agent mode: add, test, edit, enable/disable, rotate agent token, delete.
- `[SecretStoresSection.tsx:SecretStoresSection]` — secret stores: add/edit (tested first), test, try a reference, copy to another project, delete.
- `[EngineVersionsSection.tsx:EngineVersionsSection]` — Debezium and Flink CDC image catalogue per engine (`?engine=`): add, label, default, deprecate, remove.
- `[PoliciesSection.tsx:PoliciesSection]` / `[PoliciesSection.tsx:blockedBy]` — project policies (allow-lists, approvals, force deploy, size cap, pipelines-per-source limit, transform switches) and which deployed pipelines a draft would block on their next deploy.
- `[TemplatesSection.tsx:TemplatesSection]` — published templates: "Use template" (with `pipeline.edit`, opens the wizard with `?template=`), delete (with `project.manage`); publishing happens from a pipeline's History tab.
- `[AuditSection.tsx:AuditSection]` — the project's audit log (cursor-paged) with CSV / JSON Lines export links.

## Invariants
- Only Admins open Settings: `[SettingsPage.tsx:canOpenSettings]` is `me.user.is_superuser`; others get an empty state pointing to Account. Per-project sections render inside a `ProjectProvider` for the `?scope=` project, so `can()` and `project` follow that project.
- An Admin cannot remove their own Admin role or deactivate themselves (`self` in `[PeopleSection.tsx:ManagePersonDialog]`); granting Admin (in the confirmation and the invite form) lists `ADMIN_POWERS`; removing it states what they lose. An initial password is optional, but one shorter than 12 characters is refused client-side; invite creates the user, then (for a non-Admin with a project chosen) grants the project role in a second request.
- Role and Admin changes are optimistic on the `usersKey` cache and re-fetched on settle; changing one's own access also refreshes `meKey`.
- A project with pipelines cannot be deleted (the button is disabled client-side); deleting one requires typing its slug (`[ProjectsSection.tsx:ProjectsSection]`).
- Write-only secrets: SSO client secrets are sent only when typed on edit (`[signin.ts:draftBody]`); runtime secrets only in direct mode; store and channel credentials are never displayed. Agent tokens and the runtime install command are shown once and must be copied before closing (`[RuntimesSection.tsx:RuntimesSection]`).
- Optimistic concurrency: SSO provider, runtime and secret-store updates send `if-match` with the stored `version`.
- Email-and-password sign-in can be turned off only when an enabled provider has passed its test and an Admin has signed in with SSO (`[signin.ts:passwordOffBlocker]`); the last enabled option cannot be turned off (`[signin.ts:lastOptionBlocker]`).
- [mcp](../mcp/README.md)'s `toolOverrides` sends only tool choices that differ from the default; redirect hosts and origins are validated with `hostError`/`originError` (`[McpSection.tsx:McpSection]`).
- Saving a secret store tests it first and saves nothing on failure unless "Save anyway" (`test_first: !force`); a store used by connections cannot be deleted, and deleting needs the typed name (`[SecretStoresSection.tsx:SecretStoresSection]`).
- Policy allow-lists: `null` means Any, an empty list is refused; leaving with unsaved policy edits is blocked (`useBlocker`) (`[PoliciesSection.tsx:PoliciesSection]`).
- A deprecated engine image cannot be made default (`[EngineVersionsSection.tsx:EngineVersionsSection]`).
- Deleting a runtime requires its typed name; disabling or rotating its token states the effect on running pipelines.

## Side Effects
- HTTP, people/projects: `GET/POST /api/v1/users`, `PATCH /api/v1/users/{user_id}`, `PUT/DELETE /api/v1/users/{user_id}/projects/{project_id}`, `GET/POST /api/v1/projects`, `PATCH/DELETE /api/v1/projects/{project_id}`.
- HTTP, feedback: `GET /api/v1/feedback` (badge with 30 s `staleTime`), `PATCH /api/v1/feedback/{feedback_id}`.
- HTTP, sign-in: `GET /api/v1/projects` (rule targets), `GET/POST /api/v1/sso-providers`, `PATCH/DELETE /api/v1/sso-providers/{provider_id}`, `POST …/{provider_id}/test`, `GET/PUT /api/v1/auth/settings`.
- HTTP, MCP: `GET/PUT /api/v1/mcp/settings`, `GET /api/v1/mcp/tools`, `GET /api/v1/mcp/clients`, `PATCH/DELETE /api/v1/mcp/clients/{client_id}`, `GET /api/v1/mcp/grants?all=true`, `DELETE /api/v1/mcp/grants/{grant_id}`.
- HTTP, runtimes: `GET …/runtime-kinds`, `GET/POST …/runtimes`, `PATCH/DELETE …/runtimes/{runtime_id}`, `POST …/runtimes/{runtime_id}/test`, `POST …/runtimes/{runtime_id}/agent-token`.
- HTTP, secret stores: `GET …/secret-store-kinds`, `GET/POST …/secret-stores`, `PATCH/DELETE …/secret-stores/{store_id}`, `POST …/test`, `POST …/check-ref`, `POST …/copy`.
- HTTP, engines: `GET /api/v1/engines`, `GET /api/v1/debezium-catalog?engine=` (and without `engine` for the runtime image picker), `POST /api/v1/debezium-catalog`, `PATCH/DELETE /api/v1/debezium-catalog/{image_id}`.
- HTTP, guardrails: `GET/PUT …/policies` (plus connector types, runtimes and pipelines for the blocked-pipelines list), `GET …/templates`, `DELETE …/templates/{template_id}`, `GET …/events?audit_only=true`; export links download `…/audit:export?format=csv|jsonl`.

## Verification
- Run: `cd frontend && bun run test src/features/settings`
