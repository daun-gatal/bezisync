# wizard

## Overview
The steps of the pipeline wizard (`/p/$projectSlug/pipelines/new?draft=&step=`), driven by `../NewPipelinePage.tsx`: Source → Tables → Destination → Transform (in [../transforms](../transforms/README.md)) → Review & deploy. Every step edits the same draft pipeline spec and saves through `useSaveSpec` when it is left; `personal.ts` holds the pure personal-data rules logic.

## Interface
- `[shared.tsx:STEPS]` / `[shared.tsx:stepState]` / `[shared.tsx:StepNav]` — step order, done/locked state with the reason, and the step navigation; `StepHeader`, `ConnectionCards` are shared UI; `[shared.tsx:createdHere]` remembers drafts created in this tab.
- `[SourceStep.tsx:SourceStep]` — pick a source connection (and optionally a template), run its readiness check (automatically when the source was never checked or its last check is over 5 minutes old, otherwise on Re-check), create the draft pipeline.
- `[TablesStep.tsx:TablesStep]` — choose captured tables from discovery and the personal-data choice per column (`[PersonalDataCard.tsx:PersonalDataCard]`).
- `[SinkStep.tsx:SinkStep]` — destination connection, engine (Debezium or Flink CDC), naming template, event format and destination options.
- `[ReviewStep.tsx:ReviewStep]` — name, owner (`[OwnerPicker.tsx:OwnerPicker]`), runtime, snapshot, size, policies, advanced/Flink settings, final checks (preflight) and deploy; `[ReviewStep.tsx:flinkSizeHelp]` describes the Flink cluster size.
- `[FlinkSettings.tsx:FlinkRunSettings]` / `[FlinkSettings.tsx:FlinkConfEditor]` / `[FlinkSettings.tsx:BATCH_LOAD_SINKS]` — Flink CDC run settings and the Expert catalogue editor.
- `[personal.ts:defaultsFor]` / `[personal.ts:setChoice]` / `[personal.ts:normalize]` / `[personal.ts:personalRows]` / `[personal.ts:personalProblem]` / `[personal.ts:rulesToSave]` / `[personal.ts:personalSummary]` — mask / remove / keep rules per column.

## Invariants
- A step unlocks only when the ones before it are done: tables need a pipeline, the destination needs tables, Transform and Review need a destination whose naming passes `namingError` (`[shared.tsx:stepState]`).
- Each step implements `StepHandle`: `commit` refuses while the step is invalid (no tables, blocking personal-data problem, `missing` destination, invalid review settings) and saves only when dirty, so visiting a step never makes a new version.
- Personal data is protected by default: a newly picked table masks each detected column when the source can hash its type, else removes it (`[personal.ts:defaultsFor]`); remove wins over mask, mask over keep (`[personal.ts:normalize]`); a column set to mask that cannot be masked blocks the step (`[personal.ts:personalProblem]`); untouched saved rules are kept as is (`[personal.ts:rulesToSave]`).
- Tables step prunes transforms and keys to the captured tables (`pruneToTables`); the Destination step saves the spec through `specForEngine` so each engine gets only its own settings, and a deployed pipeline keeps its engine and its destination connection (`[SinkStep.tsx:SinkStep]`).
- Draft names are `slugify(<source>-cdc)`, then `<source>-cdc-2`, `-3`… until unique in the project (`[SourceStep.tsx:SourceStep]`).
- Review autosaves 800 ms after the last change (`AUTOSAVE_MS`), one save at a time; preflight re-runs after every save (on `pipeline.version`) when a runtime and destination are set; a single usable runtime (enabled, allowed by the policy's `allowed_runtime_ids`, last test not failed) is picked automatically (`[ReviewStep.tsx:ReviewStep]`).
- Deploy needs `pipeline.deploy`, valid and saved settings, a runtime and passing final checks (`deployWhy`); forcing past failed checks is offered only when the policy does not set `allow_force_deploy: false`, every failure is a source or destination check, the settings are saved, and a reason of at least 10 characters is given. With approvals required the deploy becomes an approval request.

## Side Effects
- HTTP: `POST …/pipelines` (create draft), `PATCH …/pipelines/{pipeline_id}` with `if-match` (via `useSaveSpec`), `POST …/connections/{connection_id}/check`, `POST …/connections/{connection_id}/discover`, `POST …/pipelines/{pipeline_id}/preflight`, `POST …/pipelines/{pipeline_id}/deploy`; reads connections, connector types, engines, templates, policies, runtimes, pipelines.
- Timers: the Review autosave `window.setTimeout`, cleared on each change and on unmount.
- Module state: `[shared.tsx:createdHere]` is a process-wide `Set` of draft ids created in this browser tab.

## Verification
- Run: `cd frontend && bun run test src/features/pipelines` (covers `wizard/__tests__` and `../__tests__/wizard.test.ts`, `personal.test.ts`)
