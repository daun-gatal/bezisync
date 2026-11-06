# transforms

## Overview
The wizard's Transform step: an ordered chain of steps that changes events on their way (event shape, keep only some events, rename, add field, flatten, outbox, custom SMT), value formats and message key columns. `model.ts` is the pure, tested model (stages, validation, impact); the components render and preview it. On Flink CDC the same spec describes rows and columns written into tables, and Debezium-only steps are refused.

## Interface
- `[TransformStep.tsx:TransformStep]` — the wizard step; implements `StepHandle` (`dirty`/`commit`) from `../useSaveSpec.ts` and saves `transforms`, `values` and `keys` into the spec.
- `[editors.tsx:ShapeEditor]`, `[editors.tsx:FilterEditor]`, `[editors.tsx:RenameEditor]`, `[editors.tsx:AddFieldEditor]`, `[editors.tsx:FlattenEditor]`, `[editors.tsx:CustomEditor]`, `[editors.tsx:ValuesEditor]`, `[editors.tsx:KeysEditor]` — one editor per step type plus value formats and keys (the outbox step has no editor: YAML or the API only); `EditorContext`, `Row`, `Segmented` are shared.
- `[PreviewPanel.tsx:PreviewPanel]` — server-rendered sample event before/after the chain; `[FlinkPreviewPanel.tsx:FlinkPreviewPanel]` — the columns a Flink CDC table gets (`flinkColumns`), computed locally.
- `[Impact.tsx:OutputImpact]` / `[Impact.tsx:StepsSummary]` — what changes for consumers versus the deployed version, and a one-line-per-step summary.
- `[EventsMatch.tsx:EventsMatch]` — on the Events tab, compares sampled rows with the fields the deployed chain should produce.
- `[model.ts:stepErrors]` / `[model.ts:addStep]` / `[model.ts:moveStep]` / `[model.ts:defaultStep]` / `[model.ts:fieldsBefore]` / `[model.ts:outputImpact]` / `[model.ts:stepCaps]` / `[model.ts:flinkColumns]` — the chain model; plus validators `nameError`, `textError`, `parseRuleValue`, column typing `columnKind`/`operatorsFor`, catalogues `TYPE_TITLE`, `KNOWN_CLASSES`, `SCRIPTED_CLASSES`, `DEBEZIUM_ONLY_STEPS`, `FLINK_OPERATIONS`.

## Invariants
- Stage order (`stageOf` in `../spec.ts`): keep-events filters (0) run before the event shape (`unwrap`, 1), field changes (2) after it; `[model.ts:stepErrors]` flags violations and `[model.ts:addStep]` / `[model.ts:moveStep]` preserve the order (moves only within stage 2).
- `[model.ts:stepErrors]` mirrors the backend decoder: a filter needs an operation, a rule or a raw `condition`, every rule has a field and its value(s), renames are valid, distinct and not to the same name, added fields pass `nameError` (`NAME_RE`) and, for a static value, `textError` (no `${`, no control characters, at most 200 characters), custom steps need a dotted class name. The step cannot advance while any error exists (`[TransformStep.tsx:TransformStep]`).
- Step availability (`locked` in `[TransformStep.tsx:TransformStep]`): `DEBEZIUM_ONLY_STEPS` are refused on Flink CDC; custom needs the project policy `allow_custom_transforms` and Expert mode; types outside `allowed_transforms` are refused; Debezium field steps need the event shape first (on Flink CDC it is added silently). The event shape cannot be removed while field steps exist or when the destination requires it (`unwrap.required`).
- `[model.ts:stepCaps]` drops value formats on Flink CDC and the `epoch` timestamp format for destinations with `time_precision`; Flink CDC never saves `values`.
- `[model.ts:newStepId]` gives each step a stable random id unique within the chain; `outputImpact` matches steps by id.
- `[EventsMatch.tsx:EventsMatch]` compares only plain-JSON, non-MongoDB pipelines with an event shape and no embedded schema.

## Side Effects
- HTTP: `POST /api/v1/projects/{project_id}/pipelines/{pipeline_id}/transforms:preview` via `[../../../lib/transforms.ts:useTransformPreview]`; reads discovery, connector types, policies and versions; saving goes through `useSaveSpec` (`PATCH …/pipelines/{pipeline_id}`).

## Verification
- Run: `cd frontend && bun run test src/features/pipelines/transforms`
