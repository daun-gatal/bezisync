# connections

## Overview
The Connections page and its dialogs: the project's sources (databases captured from) and destinations (where events go), with add/edit, readiness checks, delete, copy to another project, and secrets typed or referenced from a secret store. Server state goes through the hooks in `lib/connections.ts`, `lib/copy.ts` and `lib/secretRefs.ts`; forms are rendered from each connector type's JSON schema by `components/SchemaForm`.

## Interface
- `[ConnectionsPage.tsx:ConnectionsPage]` — list for `/p/$projectSlug/connections` (`?kind=` source/sink, `?page=`), Re-check, Edit, Copy, Delete per row, and (when Copy is offered) multi-select with a bulk "Copy to project…" that opens `CopyDialog` with every picked connection; closing the dialog clears the selection; `[ConnectionsPage.tsx:summaryLine]` gives the one-line address per connector type.
- `[ConnectionDialog.tsx:ConnectionDialog]` — add/edit modal (`onSaved` callback): type and provider preset, schema-driven config and secret fields, "Managed by admins", draft readiness check, save.
- `[CopyDialog.tsx:CopyDialog]` — copy connections to another project, re-planning (dry run) on every choice: name clashes (rename or update), missing secret stores (use one or copy it), saved secrets (leave or copy); `initialTarget` and `onCopied` let Promote ([../pipelines/page](../pipelines/page/README.md)) reuse it.
- `[SecretFromStore.tsx:SecretFromStore]` / `[SecretFromStore.tsx:RefStatus]` — a secret field that is typed or picked as `ref+<store>://<path>#<key>` from a store, reporting its resolve state to the dialog.

## Invariants
- Edit and delete are offered only with `connection.manage`, and for a `managed` connection only with `project.manage` too (`[ConnectionsPage.tsx:editable]`); only `project.manage` can set `managed`.
- Delete is disabled while the connection is `used_by` pipelines; its confirmation needs the typed name and states that the stored secret is destroyed. Re-check is offered to every member (no `can()` check).
- Save is blocked until every store-backed secret resolves (`refWhy`) and the name passes `slugError` on create (`[ConnectionDialog.tsx:missing]`); a reference the server reports as unresolved (store not reachable yet) can still be saved with "Save anyway". A new name is sent through `slugify`; config and secrets are cleaned with `cleanValues` against the type's schemas before sending. Connector types whose engine gate is unavailable (`engineGate`) cannot be picked. From the wizard (`fitting`: the source type and the policies) it lists only destination types `sinkFit` keeps and `allowed_sink_types` allows, and disables those whose allowed engines are off (ADR 0011).
- An edit sends the connection's `version` for optimistic concurrency; the name is fixed after creation.
- Only the reference string is saved for store-backed secrets, and a reference unchanged from the saved one is sent blank so the stored one is kept; secret values never reach the browser (`[SecretFromStore.tsx:SecretFromStore]`).
- Copy is offered only with `connection.manage` and when another project grants the caller `connection.manage` (`[../../lib/copy.ts:useCopyTargets]`).
- Copy is one request with `dry_run: false` only after the latest dry-run plan is `ready` (`[CopyDialog.tsx:blocked]`); copying saved secrets or a missing store is offered only to Admins (`is_superuser`).

## Side Effects
- HTTP via `[../../lib/connections.ts:useConnections]` and siblings: `GET …/connector-types`, `GET/POST …/connections`, `PATCH/DELETE …/connections/{connection_id}`, `POST …/connections/{connection_id}/check`, `POST …/connections:check` (draft check).
- `POST /api/v1/projects/{project_id}/connections:copy` (dry-run plans as queries fired from `CopyDialog` through `client`, the copy as a mutation).
- Secret stores via `[../../lib/secretRefs.ts:useSecretStores]` and siblings: `GET …/secret-stores`, `GET …/secret-stores/{store_id}/secrets`, `POST …/secret-stores/{store_id}/keys`, `POST …/secret-stores/{store_id}/check-ref`.

## Verification
- Run: `cd frontend && bun run test src/features/connections`
