# 0017 — Discard a deployed pipeline's undeployed changes

- **Status:** Accepted
- **Date:** 2026-11-10
- **Scope:** `backend/internal/services` (`DiscardChanges`, `UpdatePipeline`), `backend/internal/api`
  (`POST …/pipelines/{pipeline_id}/changes:discard`, OpenAPI), `backend/internal/cli`
  (`bezisync remote pipelines discard`), `backend/internal/mcpserver` (`discard_pipeline_changes`),
  the pipeline page's "saved but not deployed" notice, the wizard's Close, History's draft pill,
  the guide (Change / Rollback).

## Context

Changing a deployed pipeline saves every edit straight away into a working version (ADR 0001's
draft: `UpdatePipeline` amends the author's unsealed version, otherwise starts one). Closing the
wizard leaves `current_version_id` on that version while the connector keeps running
`deployed_version_id`. The pipeline page then says "vN is saved but not deployed" and its primary
button becomes **Review and deploy**. Nothing takes the change back:

- **Rollback in History** deploys an older version: the connector restarts, and `Deploy` never
  moves `current_version_id`, so the notice stays.
- **Editing the settings back by hand** amends the same draft: its spec now equals the deployed
  one, but `has_undeployed_changes` compares version ids, so the notice stays too.

`services/images.go` already tells people to "Deploy or discard them first"; there is no discard.

What makes it less simple than deleting the draft:

- `change_requests.version_id` is `ON DELETE CASCADE`: deleting a version erases its approval
  history.
- Versions are numbered `max(number)+1`; a deleted v5 would come back as a different v5, while
  audit text, approvals, notifications and the History deploy dialog refer to versions by number.
- The version that stops being current is not always an unsealed draft: a version sent for
  approval stays sealed after the request is cancelled or rejected, migration 00005 sealed every
  draft that existed at upgrade, another author's edit sits on top as its own version, and a
  rollback leaves the newer versions above the deployed one.
- Description, owner, labels and the runtime are columns of the pipeline row, not of a version.

## Decision

**D1. Discard moves the pointer and deletes nothing.** `DiscardChanges` sets
`current_version_id = deployed_version_id`. Every version row stays in History, deployable as
before; numbers never repeat (the next edit is `max(number)+1` and, since the deployed version is
sealed, `UpdatePipeline` starts a new version). The connector is not touched: no job, no restart,
no change to offsets, Flink saved positions or `flink_written`. Undo is "Deploy vN" in History.

**D2. When it refuses.** 409 with a code each:

| Code | When |
|---|---|
| `never_deployed` | The pipeline has no deployed version (a draft is deleted instead). |
| `nothing_to_discard` | `current_version_id` already equals `deployed_version_id`. |
| `pending_change_request` | A change request is pending; the message names its version, and the response carries `change_request_id` and `version`. Cancel or decide it first. |

Deleted or deleting pipelines are 404/409 as for every action (`GetPipeline` hides deleted ones).

**D3. Concurrency.** The request carries `If-Match` with the pipeline `version` the caller read
(428 without it, 412 `version_conflict` when stale), like `PATCH`. Inside the transaction the
service re-reads the row `FOR UPDATE`, so a deploy, an approval decision or another discard that
committed first is seen, then checks D2, moves the pointer and bumps `version`. A wizard left open
elsewhere gets 412 on its next autosave and reloads, instead of reviving the draft.

**D4. Editing back to the deployed spec is a discard.** When `UpdatePipeline` receives a spec
whose hash equals the deployed version's (also after writing the project's pinned settings into
it, as a deploy would), and current isn't the deployed version, it points current back at the
deployed version instead of amending or creating one. So "ids differ" stays the only meaning of
`has_undeployed_changes` everywhere (API, `pipelines:apply`, the Debezium image change, the UI),
and an apply of a manifest equal to what runs clears a stale draft. The version left behind stays
in History.

**D5. Permission and approvals.** `pipeline.edit` (the same as saving the change), any author's
version. It needs no approval in projects that require one for deploys: nothing deployed changes.

**D6. Audit.** `pipeline.changes_discarded`, summary "Discarded vN of NAME; v M stays deployed",
details `{discarded_version, discarded_version_id, discarded_spec_hash, discarded_by_author,
deployed_version, deployed_version_id, reason}` where `reason` is `discarded` (D1) or
`edited_back` (D4).

**D7. Surfaces.**

- API: `POST /api/v1/projects/{project_id}/pipelines/{pipeline_id}/changes:discard`, no body,
  `If-Match`, returns `PipelineRead`.
- Pipeline page: the "vN is saved but not deployed" notice gets **Discard vN** for people who can
  edit. The confirmation says: the connector keeps running vM and doesn't restart; settings go back
  to vM; vN stays in History and can be deployed from there; description, owner, labels and the
  runtime stay as they are.
- Wizard: **Close** on a deployed pipeline whose current version isn't deployed (and nothing is
  waiting for approval) asks "Keep vN?" the way Close on a new draft asks "Keep this draft?":
  **Keep vN** (it stays saved, deploy it later) or **Discard vN** (the same confirmation text).
  After a discard the wizard leaves without the unsaved-step prompt.
- History: the "draft" pill's tooltip says a draft may also be one that was discarded.
- CLI: `bezisync remote pipelines discard NAME` (reads the pipeline for If-Match).
- MCP: `discard_pipeline_changes` (`Write`, `pipeline.edit`, optional `expected_version`).

### Implementation plan

1. This ADR and its index row.
2. Backend: `services.DiscardChanges`, D4 in `UpdatePipeline`, the route and OpenAPI entry
   (`make openapi`), tests, `services` and `api` contracts.
3. CLI and MCP: the command, the tool, server instructions, tests, `cli` and `mcpserver` contracts.
4. UI: the discard dialog, the notice action, the wizard's Close, the History tooltip, vitest,
   e2e journey, the `pipelines` contracts.
5. Guide (Change / Rollback) and the changelog fragment.

### Verification plan

Go tests in `internal/api` (`discard_test.go`) and the updated `TestOneVersionPerChange`; MCP
and CLI tests; vitest for the dialog; e2e journey in `e2e/tests/` (change a deployed pipeline,
close, discard, the notice is gone and the connector was not restarted).

| ID | Case | Expected | Test |
|---|---|---|---|
| E1 | Caller's unsealed draft above deployed | current = deployed; draft row kept; audit written; desired state, lifecycle and deployed image unchanged; no job queued | `TestDiscardChanges` |
| E2 | Another author's version on top of the caller's | Both rows kept; current = deployed | `TestDiscardChanges` |
| E3 | Version sealed by a cancelled request | Discardable; the request row is still there | `TestDiscardAfterApproval` |
| E4 | A pending change request | 409 `pending_change_request` with its id | `TestDiscardAfterApproval` |
| E5 | After a rollback (current above deployed, sealed) | Discardable | `TestDiscardChanges` |
| E6 | Never deployed | 409 `never_deployed` | `TestDiscardRefusals` |
| E7 | Nothing to discard | 409 `nothing_to_discard` | `TestDiscardRefusals` |
| E8 | Missing / stale If-Match | 428 / 412 | `TestDiscardRefusals` |
| E9 | Autosave with the If-Match read before the discard | 412; the draft isn't revived | `TestDiscardChanges` |
| E10 | Edit after discard | New version `max+1`; no number reused | `TestDiscardChanges` |
| E11 | Spec edited back to the deployed one | current = deployed, `edited_back` audit | `TestOneVersionPerChange`, `TestEditBackWithPins` |
| E12 | Edited back, project pins a setting the spec leaves out | Converges (pins applied before comparing) | `TestEditBackWithPins` |
| E13 | Operator (no `pipeline.edit`) / other project | 403 / 404 | `TestDiscardRefusals` |
| E14 | Approvals required for deploys | Discard works without a request | `TestDiscardAfterApproval` |
| E15 | Flink CDC pipeline | Artifacts (`flink_written`) unchanged | `TestDiscardChanges` (artifacts compared) |
| E16 | CLI and MCP | Discard by name; MCP refuses a viewer | `TestRemoteDiscard`, MCP tool test |
| E17 | Wizard Close with a draft; Keep / Discard | Keep leaves it; Discard clears the notice, no unsaved-step prompt | e2e journey, vitest |

## Consequences

- People can abandon a change without restarting anything, from the page, the wizard, the CLI or
  an agent. Editing back by hand now clears the notice too.
- History keeps versions that were never deployed and are no longer current; they show as drafts.
  The audit log says who discarded which one. A `discarded_at` column could label them later.
- A save after a discard starts a new version, so the discarded number is skipped: v5 discarded,
  the next change is v6.
- `pipelines:apply` with a manifest that differs from what runs recreates a draft after a discard,
  as it would after any edit.
- Rollback still leaves `current_version_id` where it was; Discard (or D4) clears the notice after
  one. Moving current on deploy is a separate decision.

## Alternatives considered

- **Delete the unsealed drafts above deployed.** Reuses numbers, cascades into change requests,
  deletes other authors' work, and can't clear sealed leftovers or the state after a rollback.
- **Delete, plus a monotonic number counter.** Fixes reuse only; still drops rows that audit and
  approvals point at, with no undo.
- **Revert by saving a new version equal to the deployed spec.** Its id differs from the deployed
  one, so it is "not deployed" again: the bug itself.
- **Compare spec hashes when showing the notice.** The page would say "nothing to deploy" while
  History, `current_version`, apply and the image change still saw a draft.
- **A `discarded_at` tombstone.** Needs a migration and a filter in every version reader for a
  label the audit log already provides; left for later.
