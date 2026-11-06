# components

## Overview
Shared, domain-aware components used by several feature screens: the descriptor-driven form, the preflight check list and the time-series chart. They build on the design system in `ui/`, which holds every primitive screens are allowed to use.

## Interface
- `[SchemaForm.tsx:SchemaForm]` — renders a descriptor JSON Schema (`[SchemaForm.tsx:JsonSchema]`, `[SchemaForm.tsx:JsonSchemaProp]` with its `ui` hints: group, expert, labels, descriptions, when, hidden, connection) as grouped fields; plain or write-only secret inputs, with `renderSecret` (`[SchemaForm.tsx:SecretFieldProps]`) to wrap secret fields and `links` (`[SchemaForm.tsx:LinkOptions]`) for `ui.connection` pick-lists.
- `[SchemaForm.tsx:fieldsOf]`, `[SchemaForm.tsx:isVisible]`, `[SchemaForm.tsx:defaultsOf]`, `[SchemaForm.tsx:cleanValues]` — the pure schema helpers the form and its callers share.
- `[CheckList.tsx:CheckList]` — a preflight or connection check report (`[CheckList.tsx:CheckReport]`, `[CheckList.tsx:Check]`) grouped by category, with fixes and a "Send to my DBA" message; `[CheckList.tsx:reportBadge]` is its one-pill summary.
- `[TimeSeries.tsx:TimeSeries]` — a uPlot chart of at most two series with a hover readout, peak label and a screen-reader `summary`.
- Subdirectories: [ui](./ui/README.md) — the design system primitives.

## Invariants
- Secret fields are write-only: `[SchemaForm.tsx:SchemaForm]` renders them as `type="password"` with `autoComplete="new-password"`, shows `••••••` for an already-stored secret (`setSecrets`) whose blank input keeps it, and shows only a `ref+…` secret-store reference as plain text.
- `[SchemaForm.tsx:cleanValues]` drops fields hidden by `ui.when` (`[SchemaForm.tsx:isVisible]`), empty strings and null nullable values, splits lists on commas and converts numbers; callers submit its output, not the raw form values.
- `[SchemaForm.tsx:isVisible]` resolves a `ui.when` name from the form's values, then the sibling `context`, then the schema default.
- Expert-only fields are hidden outside expert mode unless they hold a non-default value, so a saved setting is never invisible.
- `[CheckList.tsx:reportBadge]` never shows Ready without a report: no report is "Not checked" (grey), a failed report shows its summary in the bad tone.

## Side Effects
- `[CheckList.tsx:CheckList]` writes the DBA message to the clipboard (`navigator.clipboard.writeText`) and shows a success toast.
- `[TimeSeries.tsx:TimeSeries]` creates a uPlot instance, a `ResizeObserver`, a `prefers-color-scheme` media listener and a `MutationObserver` on `<html>` (`data-theme`, `class`, `style`) to re-theme, and removes them on unmount; it briefly appends a hidden probe element to `document.body` to resolve colour tokens.

## Verification
- Run: `cd frontend && bun run test src/components`
