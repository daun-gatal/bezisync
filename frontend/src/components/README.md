# components

## Overview
Shared, domain-aware components used by several feature screens: the descriptor-driven form, the preflight check list and the time-series chart. They build on the design system in `ui/`, which holds every primitive screens are allowed to use.

## Interface
- `[SchemaForm.tsx:SchemaForm]` — renders a descriptor JSON Schema (`[SchemaForm.tsx:JsonSchema]`, `[SchemaForm.tsx:JsonSchemaProp]` with its `ui` hints: group, expert, labels, descriptions, when, hidden, connection) as grouped fields; plain or write-only secret inputs, with `renderSecret` (`[SchemaForm.tsx:SecretFieldProps]`) to wrap secret fields and `links` (`[SchemaForm.tsx:LinkOptions]`) for `ui.connection` pick-lists.
- `[SchemaForm.tsx:fieldsOf]`, `[SchemaForm.tsx:isVisible]`, `[SchemaForm.tsx:defaultsOf]`, `[SchemaForm.tsx:cleanValues]` — the pure schema helpers the form and its callers share.
- `[CheckList.tsx:CheckList]` — a preflight or connection check report (`[CheckList.tsx:CheckReport]`, `[CheckList.tsx:Check]`) sorted by status (fail, warn, unknown, pass, skip) with each check's category as a label, with fixes and a "Send to my DBA" message; `[CheckList.tsx:reportBadge]` is its one-pill summary.
- `[TimeSeries.tsx:TimeSeries]` — a uPlot chart of at most two series with a hover readout, peak label and a screen-reader `summary`.
- Engine settings: `[SettingsEditor.tsx:SettingsEditor]` (registry settings grouped by stage, with `[SettingsEditor.tsx:ImpactPill]` and `[SettingsEditor.tsx:settingText]`), `[RawProperties.tsx:RawProperties]` (`[RawProperties.tsx:RawRow]` key/value rows judged by the server's classify, `[RawProperties.tsx:Verdict]`, `[RawProperties.tsx:FIX_LABEL]`), `[Recommendations.tsx:Recommendations]` / `[Recommendations.tsx:freshRecommendations]` (`[Recommendations.tsx:Recommendation]`, only within `[Recommendations.tsx:FRESHNESS_MS]` of their evidence), `[SettingsChange.tsx:SettingsChangeList]` / `[SettingsChange.tsx:settingsChangeLines]` / `[SettingsChange.tsx:changeText]` (what a deploy changes, with impacts).
- `[ClusterDiagram.tsx:ClusterDiagram]` — a Flink CDC cluster shape (`[ClusterDiagram.tsx:ClusterShape]`, `[ClusterDiagram.tsx:readersPerTaskManager]`, `[ClusterDiagram.tsx:clusterSummary]`).
- Subdirectories: [ui](./ui/README.md) — the design system primitives.

## Invariants
- Secret fields are write-only: `[SchemaForm.tsx:SchemaForm]` never shows a stored secret back — it shows `••••••` for an already-stored secret (`setSecrets`) whose blank input keeps it. A single-line secret is an `Input` with `type="password"` and `autoComplete="new-password"` (a `ref+…` secret-store reference shows as plain text); a `ui.multiline` secret is a plain `Textarea`, so the value being typed or loaded is visible.
- `[SchemaForm.tsx:cleanValues]` drops fields hidden by `ui.when` (`[SchemaForm.tsx:isVisible]`), empty strings and null nullable values, splits lists on commas and converts numbers; callers submit its output, not the raw form values.
- `[SchemaForm.tsx:isVisible]` resolves a `ui.when` name from the form's values, then the sibling `context`, then the schema default.
- Expert-only fields are hidden outside expert mode unless they hold a non-default value, so a saved setting is never invisible.
- A check with status `unknown` is grey ("Not verified") and sorts after warnings; a report with unknowns but no warnings shows its summary grey, never "Ready" (`[CheckList.tsx:reportBadge]`).
- Raw properties are never judged in the browser: `[RawProperties.tsx:RawProperties]` shows what the server's `settings:classify` says, so the wizard never copies the classifier. The wizard's `rawErrors` (`lib/settings.ts`) only compares those verdicts with the stored spec, and flags a key set twice, to decide what blocks a save.
- `[CheckList.tsx:reportBadge]` never shows Ready without a report: no report is "Not checked" (grey), a failed report shows its summary in the bad tone.
- `[CheckList.tsx:CheckList]` hides passed and skipped checks behind a "Show N passed checks" toggle, and shows a check's `technical` detail only in expert mode or when the check failed.
- `[SettingsEditor.tsx:SettingsEditor]` shows `pinned` (project policy) values with "Set by project policy." and the row reads the pinned value first; number, choice and text rows are disabled, but a boolean row's `Segmented` is disabled the same way; `caps` lower a setting's maximum below the registry's; a key in the value that is not one of `settings` is flagged for removal; removing a setting drops the key so its default applies. Each row is checked with `settingError` from `lib/settings.ts`.
- Impacts are shown as `[SettingsEditor.tsx:ImpactPill]` (glyph and words, never colour alone); `[SettingsChange.tsx:settingsChangeLines]` and `[SettingsChange.tsx:SettingsChangeList]` list the heaviest impact first, and a missing from/to value reads "default".
- `[RawProperties.tsx:RawProperties]` flags a key used twice ("Twice"), shows an `allowed` verdict as grey "Not verified" (never green) and every other class in the bad tone; a key with no verdict yet shows "Checking…".
- `[Recommendations.tsx:FRESHNESS_MS]` (10 min) mirrors the backend's `services.RecommendationFreshness`; `[Recommendations.tsx:freshRecommendations]` also drops a recommendation for a setting the pipeline can't use or has already set.

## Side Effects
- `[CheckList.tsx:CheckList]` writes the DBA message to the clipboard (`navigator.clipboard.writeText`) and shows a success toast.
- `[SchemaForm.tsx:SchemaForm]` offers "Load from a file" under each multiline field: it reads a file the person picks into the field with `file.text()` (nothing is uploaded) and silently ignores files over 1,000,000 bytes.
- `[TimeSeries.tsx:TimeSeries]` creates a uPlot instance, a `ResizeObserver`, a `prefers-color-scheme` media listener and a `MutationObserver` on `<html>` (`data-theme`, `class`, `style`) to re-theme, and removes them on unmount; it briefly appends a hidden probe element to `document.body` to resolve colour tokens.

## Verification
- Run: `cd frontend && bun run test src/components`
