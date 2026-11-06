# pipelines

## Overview
Everything about pipelines in the browser: the list (`/p/$projectSlug/pipelines`), the wizard that creates and edits a draft (`/pipelines/new`), and one pipeline's page (`/pipelines/$pipelineId`). This directory holds the pages, the import/upgrade dialogs and the client-side spec model (`spec.ts`) every step and tab reads; the wizard steps, pipeline-page tabs and Transform step live in the subdirectories. Server state comes from `lib/pipelines.ts`.

## Interface
- `[PipelinesPage.tsx:PipelinesPage]` — the list with fleet-health filter (`[FleetHealth.tsx:FleetHealth]`), search/paging in the URL, multi-select Change Debezium version, Apply YAML and Adopt.
- `[NewPipelinePage.tsx:NewPipelinePage]` — the wizard shell: picks the step from `?step=`/`?draft=`, saves the current step before moving, asks before leaving with unsaved changes, and offers to keep or delete a draft created in this tab.
- `[PipelinePage.tsx:PipelinePage]` — header with health/lifecycle, one primary action by state, operate menu (pause/resume/restart), Delete, approval banner, and the tabs from [page](./page/README.md).
- `[ImportDialogs.tsx:ApplyYamlDialog]` / `[ImportDialogs.tsx:AdoptDialog]` — apply `kind: Pipeline` YAML (preview, then apply, optionally deploy) and adopt an existing Debezium Server's properties as a draft; `DiffLines`, `ApplyResults` render results.
- `[ChangeDebeziumDialog.tsx:ChangeDebeziumDialog]` — set the Debezium image (or the default) for one or more pipelines, optionally deploying.
- `[spec.ts:fullSpec]` / `[spec.ts:FullSpec]` / `[spec.ts:specForEngine]` — the spec with every default filled, and stripped to one engine's settings; `TransformStep` and related types; `stageOf`, `withShape`, `pruneToTables`; naming (`destinationFor`, `namingError`, `sinkDestinationFor`, `sinkNamingError`), `parseAdvanced`, `deriveSinkOptions`, `optionsSchemaFor`, `SNAPSHOT_MODES`, `RESOURCES`.
- `[useSaveSpec.ts:useSaveSpec]` / `[useSaveSpec.ts:StepHandle]` / `[useSaveSpec.ts:SaveBody]` — the save every wizard step uses and the contract a step gives the wizard.
- Subdirectories: [page](./page/README.md) — the pipeline page's tabs; [transforms](./transforms/README.md) — the Transform step and its model; [wizard](./wizard/README.md) — the other wizard steps.

## Invariants
- `[spec.ts:fullSpec]` keeps fields it does not know, so saving from the wizard never drops settings made through YAML or the API.
- `[spec.ts:specForEngine]`: Flink CDC drops `values` and resets `debezium_image` (null), `advanced` (`{}`) and the snapshot delay/window (0/null); Debezium drops `flink_cdc` and `flink_cdc_image`.
- `[spec.ts:namingError]` requires `{table}`, only `{schema}`, `{table}`, `{pipeline}` placeholders, and otherwise letters, digits, `.`, `_`, `-`; `[spec.ts:sinkNamingError]` adds each destination's pattern and length limits for every captured table.
- `[useSaveSpec.ts:useSaveSpec]` saves through `useUpdatePipeline`, which sends `if-match` with the pipeline `version` (optimistic concurrency); it resolves to false instead of throwing and leaves the error for the step to show inline.
- Leaving the wizard with a dirty step is blocked (router `useBlocker` plus `beforeunload`); a locked step in the URL falls back to the first unlocked, unfinished one (`[NewPipelinePage.tsx:NewPipelinePage]`).
- On the pipeline page: operate actions need `pipeline.operate` and a deployed, not-deleting pipeline; edits need `pipeline.edit`; Delete needs `pipeline.deploy` and the typed pipeline name, with "delete destination topics" an explicit choice (`[PipelinePage.tsx:DeleteDialog]`). A `deleting`, `deleted` or `cleanup_failed` pipeline shows only Overview and History and offers no operate, edit or Delete.
- Approvals: Approve/Reject need `pipeline.approve` and are hidden from the requester; Reject needs a comment; the requester or an approver may cancel (`[PipelinePage.tsx:ApprovalBanner]`).
- The list hides `deleted` pipelines; Apply YAML, Adopt and the multi-select for Change Debezium version need `pipeline.edit`, and only deployed pipelines can be selected (`[PipelinesPage.tsx:PipelinesPage]`).
- Apply YAML must be previewed (dry run) before applying; its deploy option needs `pipeline.deploy`. Adopted drafts are labelled `adopted: "true"` and opened at the Tables step for review (`[ImportDialogs.tsx:AdoptDialog]`).
- Change Debezium version lists only non-deprecated catalogue images plus a typed "Another image…" reference and "Follow the default" (`image: null`), and deploys only with `pipeline.deploy` (`[ChangeDebeziumDialog.tsx:ChangeDebeziumDialog]`).

## Side Effects
- HTTP via `[../../lib/pipelines.ts:usePipelines]` and siblings: `GET/POST …/pipelines`, `GET/PATCH/DELETE …/pipelines/{pipeline_id}` (delete with `drop_destinations`), `POST …/pause|resume|restart`, `POST …/change-requests/{request_id}/approve|reject|cancel`.
- `POST …/pipelines:apply`, `POST …/pipelines:adopt`, `POST …/debezium-images:apply`; reads `GET /api/v1/debezium-catalog` and `GET …/debezium-images`.
- Polling: the list refetches every 5 s and one pipeline every 5 s (2 s while `deploying`, `deleting` or `snapshotting`), defined in `[../../lib/pipelines.ts:usePipeline]`.

## Verification
- Run: `cd frontend && bun run test src/features/pipelines`
