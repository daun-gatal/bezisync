# 0001 — Wizard truth: Keep deleted rows, the Review autosave loop, and a badge that names the field

- **Status:** Accepted
- **Date:** 2026-11-06
- **Scope:** the pipeline wizard (`frontend/src/features/pipelines`: `transforms/`, `wizard/`, `spec.ts`, `PipelinePage.tsx`, `page/overview.ts`), `frontend/src/lib/settings.ts`, `frontend/src/lib/cluster.ts`, their directory contracts, `ARCHITECTURE.md` (Known issues), `docs/README.md`, `.rules/frontend.md`, e2e specs `27`, `32` and `33`, and one changelog fragment. No backend, API, OpenAPI or migration change.

## Context

### Terms used here

- **Transform chain and stages.** A pipeline's `transforms` is an ordered list of steps. Each step belongs to a stage, defined by the backend's `domain.Stage()` and copied by the frontend's `stageOf` (`frontend/src/features/pipelines/spec.ts:109`):
  - **Stage 0:** keep-only-some-events filters built from rules.
  - **Stage 1:** the *event shape*. This is `unwrap` ("Row after change", which turns Debezium's change envelope into the changed row) and `soft_delete` ("Keep deleted rows").
  - **Stage 2:** field changes (`rename`, `add_field`, `flatten`, `custom`).
  - **Stage -1:** every other step, such as outbox, a filter written as a script, truncate, route and computed. These run in the order they are written.

  The server refuses a chain where a stage-0 step comes after the first stage-1 step, or a stage-2 step comes before it.
- **Draft, version, dirty, signature.** Editing a pipeline in the wizard writes the author's *draft version*. Each save updates that one draft until it is deployed or sent for approval. A step is *dirty* when what it would save differs from the pipeline's stored spec. Review's *signature* is the JSON of the body it would save (`ReviewStep.tsx:193`), so two drafts with the same signature save the same thing.
- **Engine settings.** Review's Engine settings card (Expert mode only) holds two kinds of entry:
  - **Typed settings** (`tuning`), picked from BeziSync's registry. The ones this pipeline can use are `tunable`.
  - **Raw advanced properties** (`advanced`), shown as key and value rows (`rawRows`). The server classifies each row as allowed or refused (`settings:classify`).
- **Guided and Expert mode.** Guided is the default. Expert is turned on in the account menu and shows raw properties and extra settings. Both edit the same spec.
- **Preflight** is the final check that runs before a deploy. **`config_hash`** is the hash of a pipeline's compiled workload. The controller replaces a running pipeline whose hash changes, which restarts it.

### The problem

The wizard must show what the server saves and what the compiler runs. Three reported bugs break that, and the review found a fourth.

1. **Keep deleted rows is saved but never drawn.**
   - The Transform step draws stage 0, the `unwrap` row, stage 2 and stage -1 (`TransformStep.tsx:293-318`). A `soft_delete` (stage 1) has no row, so once added it can't be seen, edited or removed. Review still lists it. This is Known issue 14.
   - Around it, the screen also disagrees with the compiler. On Debezium, `withSoftDelete` (`backend/internal/compiler/sinkcaps.go:84`) forces deletes to be rows marked `__deleted`, and inserts a "Row after change" step where none exists. The Transform step can still say "Full change envelope" or "deletes dropped", and Review's summary and the Overview page repeat it.
   - The client's ordering check (`stepErrors`) treats only `unwrap` as the event shape. The server treats the first stage-1 step as the shape (`backend/internal/domain/transforms.go:663-668`). So on Flink CDC, where there is usually no `unwrap`, the client accepts `[soft_delete, filter]` and the server refuses it.
2. **Review switches between "Saving…" and "Not saved" forever.**
   - The autosave effect re-runs whenever `saving` changes (`ReviewStep.tsx:196-203`). After a refused save, `saving` goes back to false, the draft is still unsaved, and the step saves again 800 ms later. That is about one request a second for as long as the page is open.
   - Many things trigger it: a refused advanced key (`managed_property`), a tuning rule the client doesn't check, a 412 `version_conflict`, a network error, a 5xx.
   - The page already asks the server which raw keys are refused, but never uses the answer to stop a save, and doesn't ask at all in Guided mode.
3. **"Not saved: fix the highlighted field" names no field.**
   - The error chain at `ReviewStep.tsx:190` keeps only the first problem, and nothing says which field it is.
   - The Debezium custom-size error covers CPU and memory but sits only under CPU (`:392`). The Flink streaming-size and manual TaskManager errors also sit on one field each.
   - The Deploy reason says "Fix the highlighted settings first."
4. **A blocking error can be on no visible field.** With Expert mode off, the Engine settings card isn't drawn, but its errors still block the save. With Expert on and no typed settings to offer, `SettingsEditor` isn't drawn, so an unknown saved setting blocks with nothing to remove.

### Goals

- **Every Keep deleted rows step is drawn and removable** in the Transform step, on Debezium and Flink CDC. This includes saved specs with none, one or several of them.
- **The Transform step, Review's summary, the deploy impact and the Overview describe deletes the way the compiler runs them**, using the destination's own Deletes option where the compiler does.
- **The client's ordering check (`stepErrors`) matches the server's**, so a chain it flags is one the server refuses.
- **A refused save is never repeated on its own.** The next attempt comes from a change to the draft, a Try again click (network errors and 5xx only), or leaving the step.
- **A refused raw key blocks saving exactly when the server would refuse it:** when it is new, or its value changed, against the stored spec.
- **Every error that blocks saving names its field, sits under that field, and has a visible field in both modes.** The badge and the Deploy reason name it, and clicking the badge focuses it.
- **Opening a step never changes a spec.** An unedited pipeline keeps its version, its spec and its compiled config.

### Non-goals

- Any backend, API, OpenAPI or migration change, including a compiler fix for the ordering issue in Consequences.
- Rewriting or reordering saved specs.
- Changing `stageOf`, which mirrors `domain.Stage()`.
- A form-error framework, or a shared autosave hook (only Review autosaves).
- Automatic retries or backoff.
- Showing the full Engine settings editor in Guided mode.
- Naming fields from the server's error locations (`errors[].loc`). See Future improvements.

### Critical user journeys

1. **Debezium, full envelope, add Keep deleted rows.**
   - The step appears under Event shape, below a locked line: "Row after change · Keep deleted rows needs it; BeziSync adds it".
   - The menu offers Keep deleted rows no more ("Already added: see Event shape").
   - On a JDBC destination, the Destination step's Deletes option carries the note "Deletes: kept, marked deleted. Set by Keep deleted rows in Transform."
   - Removing the step brings the earlier view back.
2. **Debezium with Row after change set to "Drop delete events", then add Keep deleted rows.**
   - The Deletes select shows "Keep as an event with __deleted = true", disabled, with "Set by Keep deleted rows (step N). Remove that step to choose."
   - "Use the full change envelope instead" is disabled with its reason.
   - Removing the step shows "Drop delete events" again. The stored value was never changed.
3. **Flink CDC, add Keep deleted rows, then a filter.** Both appear under Keep rows, the filter first. The draft saves.
4. **A pipeline whose saved spec has `soft_delete` and no unwrap, or two `soft_delete` steps, is opened.** Every step is drawn with Remove, no save request is sent, and the version, spec and compiled config are unchanged.
5. **A refused raw key in Review (Expert).** The user adds `debezium.format.value` = `json`, which is refused ("The event format is chosen on the Destination step", `compiler/advanced.go:166`).
   - The row turns red and the badge reads "Not saved: fix Advanced Debezium properties". Clicking the badge focuses the row's key.
   - The verdict usually arrives before the 800 ms autosave. If it doesn't, one save is refused and autosave pauses (journey 6).
   - Removing the row saves the draft, and the badge reads "All changes saved".
6. **The server refuses a save the client thought valid** (a tuning rule the client doesn't check, a 412, a network error or a 5xx).
   - One request is sent. "Couldn't save your changes" shows with the server's message, and the badge reads "Not saved". Try again appears only for network errors and 5xx.
   - No further request is sent until the draft changes, Try again is clicked, or the step is left.
7. **An older pipeline holds a refused raw key with the same value; the user changes its owner.** The save goes through, the key's row stays red, and preflight still blocks the deploy.
8. **Debezium custom size over the project limit.** A Memory above the limit shows its error under Memory, not CPU. The badge reads "Not saved: fix Memory", and clicking it focuses Memory. Flink cluster errors sit on their own fields in the same way.
9. **A blocking setting in Guided mode.** A card "Settings that block saving" lists only the blocking rows, each with Remove, and says "Switch to Expert mode (account menu) to edit these." The badge names the row's section. After Remove, the draft saves.

## Decision

Every rule becomes a pure function with table tests. The components call it and draw the result. A function that copies a backend rule names the Go function in its doc comment, and its directory contract lists it under "Mirrors of backend rules". That list, and review, is how drift is caught, the same as for the existing mirrors (`globMatch`, `routeTargets`, `SOFT_DELETE_FIELD`…). No test runs the Go and TypeScript rules against each other.

```text
 stored spec (pipeline.spec; never rewritten on load)
        │
        ├─► transforms/model.ts: shapeIndex · addStep · sectionOf · drawOrder · effectiveDeletes · summarizeShape · outputImpact
        │        └─► TransformStep · ShapeEditor · Impact (StepsSummary, OutputImpact) · page/overview.ts · SinkStep (note)
        │
        ├─► lib/settings.ts: rawErrors          lib/cluster.ts: streamingErrors · manualErrors
        │        └──────────────┬──────────────────────┘
        │                       ▼
        └─► wizard/review.ts: autosaveDecision · reviewProblems · problemsSummary · rawLabel · focusReviewField
                 └─► ReviewStep (badge, Deploy reason, field errors, autosave) · FlinkClusterEditor (errors as props) · BlockingSettings
```

### Fix A: the Review autosave never repeats a refused save

**A1. Pause after a failure.**
- ReviewStep keeps `failedSignature: string | null` in state, set to the body's signature when a save resolves false and cleared when one resolves true.
- `paused = failedSignature !== null && signature === failedSignature`.
- A pure `autosaveDecision({ dirty, saving, invalid, paused })` checks top to bottom, and the first match wins:

| Check | Result |
| --- | --- |
| not dirty | `idle` |
| saving | `wait` |
| invalid (a client-side problem, Fix B) | `blocked` |
| paused | `paused` |
| otherwise | `schedule`: save in 800 ms |

- The timer lives in a small `useAutosave` hook next to `autosaveDecision` in `wizard/review.ts`, used only by Review, so the loop can be tested with fake timers. It re-arms whenever the signature or the decision changes. Because `invalid` feeds the decision, a draft that just became valid saves without another edit.

**A2. The next attempt comes only from:**
- **a change to the draft**, because the signature moves off `failedSignature`;
- **Try again**, passed as `onRetry` to the existing `InlineError`. It clears `failedSignature`, and `InlineError` shows it only for network errors and 5xx (`isRetryable`, `lib/errors.ts:45`);
- **leaving the step**: the save-on-leave handler (`StepHandle.commit`) makes one attempt whenever the draft has no client-side problem, paused or not. A draft with a client-side problem can't be left, as today.

A 412 pauses like any other failure, and its message, "Someone else changed this pipeline. Reload and try again.", says what to do.

**A3. The status is always about the current draft.** A new save clears the previous error, because the mutation resets on `mutate`. When the draft becomes clean again, ReviewStep calls `reset()` so no red message is left behind. The badge shows the first of these that applies:

| Badge | When |
| --- | --- |
| "Saving…" | a save is in flight |
| "Not saved: fix {fields}" (a button, Fix B) | the draft has a client-side problem |
| "Not saved" (with the server's message below) | paused |
| "Saving soon…" | dirty, save scheduled |
| "All changes saved" | clean |

`deployWhy` checks in the same order. When paused it says "Not saved: {the server's message}".

**A4. A refused raw key blocks saving only when the server would refuse it.** `rawErrors(rows, verdicts, stored)` in `lib/settings.ts` returns a blocking message per trimmed key in two cases:
- the key is set twice;
- its verdict's class isn't `allowed` (the test `RawProperties.tsx:72` already uses), and `stored[key]` is missing or differs from the row's value.

`stored` is the stored spec's `advanced`, from `fullSpec(pipeline.spec)` (the pipeline as the server returned it, not the draft). The server compares against the same thing: `RefuseNewAdvanced(eng, prev, next)` with `prev` = the pipeline's current version (`backend/internal/services/pipelines.go:535-544`). A key whose verdict hasn't arrived doesn't block. If it turns out refused, one save is refused and A1 pauses.

**A5. Classification runs whenever raw rows exist**, in both modes: `useClassify(…, rawRows.some((r) => r.key.trim()))`.

**A6.** One `rawLabel(flink)` returns "Advanced Flink CDC options" or "Advanced Debezium properties". It is used for the field label and for the `managed_property` fix text, which today names Debezium on Flink CDC too.

### Fix B: every blocking error names its field

**B1. One list of client-side problems.** `reviewProblems(input): ReviewProblem[]` in `wizard/review.ts` replaces the `??` chain. A `ReviewProblem` is `{ field, label, message, key? }`.
- `field` is one of `name`, `cpu`, `memory`, `streaming-cpu`, `streaming-memory`, `task-managers`, `tm-memory`, `tuning` or `advanced`.
- `label` is the visible label:
  - "Name", "CPU", "Memory";
  - "Streaming reader CPU", "Streaming reader memory";
  - "TaskManagers", "TaskManager memory";
  - for a typed setting, its title, or its key when it is unknown;
  - for raw properties, `rawLabel(flink)`.
- `key` is set for `tuning` and `advanced` problems.

Entries come in page order. These read the list:
- `invalid` is `problems.length > 0`;
- the badge reads "Not saved: fix {problemsSummary}", for example "Memory" or "Memory and 2 more";
- `deployWhy` reads "Fix {first label} first.";
- each field takes its own error from the list.

The server's error locations are not merged in (see Non-goals). Placement is never a problem, because preflight checks it.

**B2. Each error on its own control.**
- **Debezium custom size:** `customErrors(amount, max, min, sinkLabel)` returns `{ cpu?, memory? }`. Each amount is checked against the project's maximum and the destination's minimum on its own.
- **Flink cluster:** `lib/cluster.ts` gains two functions. `streamingError` and `manualError` stay, returning the first message in the order CPU then memory, and TaskManagers then TaskManager memory. Their existing messages are unchanged, and `usePackCluster` keeps using them.
  - **`streamingErrors(c)` → `{ cpu?, memory? }`.** It returns `{}` for a manual shape, which is never resized and whose streaming fields aren't drawn (`toManual` clears the streaming size). A bigger CPU goes on CPU and a bigger memory on memory. "The same size" (CPU and memory both equal) goes on both fields.
  - **`manualErrors(c)` → `{ taskManagers?, memory? }`.** It returns `{}` for an automatic shape. "Can't hold N readers" goes on TaskManagers, and "each slot needs at least 512 MiB" goes on TaskManager memory.
- **`FlinkClusterEditor`** takes these errors as props instead of computing them again.

**B3. Focus.**
- Each control that can hold a problem sits in a `<div data-review-field="…">`. ReviewStep wraps its own fields, FlinkClusterEditor wraps the cluster fields, and the Engine settings and raw property fields are wrapped as whole sections.
- The badge becomes a `button` that calls `focusReviewField(field)`. It finds the wrapper, calls `scrollIntoView?.({ block: 'center' })` (the `?.` because jsdom has none), then focuses the first `[aria-invalid="true"]` control inside, or else the first focusable one.
- A refused raw row's key input already sets `aria-invalid` (`RawProperties.tsx:70`, through `Input`'s `invalid`).

**B4. Never block on a field that isn't drawn.**
- **Guided mode:** with Expert off and any `tuning` or `advanced` problem, a new `wizard/BlockingSettings.tsx` card titled "Settings that block saving" is drawn where the Engine settings card would be. It has one row per problem (key, message, Remove), plus "Switch to Expert mode (account menu) to edit these."
- **Expert mode with nothing typed to offer:** when `tunable` is empty, the same rows are drawn inside the Engine settings card.
- **Remove** deletes the key from `tuning` (rebuilt with `jsonbSorted`) or the row from `rawRows`.
- **Everything else is drawn by construction:**
  - the custom-size fields are drawn whenever Size is Custom, which is the only time `customErrors` runs;
  - the streaming fields whenever the shape is automatic with a streaming size;
  - the TaskManager fields whenever the shape is manual.

  B2's `{}` results keep each problem inside those conditions.

### Fix C: Keep deleted rows is drawn, and deletes are described as they run

**C1. One shape boundary for ordering.**
- `shapeIndex(steps)` returns the index of the first stage-1 step. It mirrors `unwrapAt` in `domain.validateTransforms`.
- `stepErrors` uses it: a stage-0 step after it is an error, and a stage-2 step before it, or with none, is an error.
- **The add menu doesn't change.** On Debezium a field step still needs an explicit "Row after change" before it can be added (`locked`, `TransformStep.tsx:150`), and on Flink CDC `add()` still inserts the unwrap silently. The menu therefore never builds `[soft_delete, rename]` without an unwrap, which the compiler orders wrongly for destinations that require the shape (see Consequences).

**C2. Where a new step goes** (`addStep`):

| Step added | Goes |
| --- | --- |
| stage-0 filter | just before `shapeIndex`, or at the end when there is no boundary |
| `soft_delete` | just after the `unwrap` if there is one; otherwise at the end, as today |
| anything else | at the end, as today |

"At the end" is safe for `soft_delete` with no unwrap. No field step can exist without a boundary, because both validators refuse it. So the unwrap the compiler inserts at the `soft_delete` lands after every other step, as it does today. Journey 3 holds, because a filter added after `[soft_delete]` goes before it.

**C3. Where a step is drawn.** `sectionOf(step, flink)` returns `keep`, `shape`, `fields`, `other` or `hidden`, and `drawOrder(steps, flink)` returns the chain indexes in drawn order.

| Step | Debezium | Flink CDC |
| --- | --- | --- |
| stage-0 filter | `keep` | `keep` ("Keep rows") |
| `unwrap` | `shape` | `hidden` (Flink always writes rows; no shape row is drawn today) |
| `soft_delete` | `shape` (below the unwrap row or the locked line) | `keep`, after the filters |
| stage 2 | `fields` | `fields` |
| stage -1 | `other` | `other` |

- **Order on screen:** sections are drawn keep, shape, fields, other, and steps keep their chain order inside a section.
- **Numbering:** a step's number is its position in `drawOrder`.
- **`moveStep` doesn't change.**
- **No spec is reordered.**

**C4. Debezium with `soft_delete` and no unwrap.**
- When the destination doesn't require the shape, the shape area draws "Row after change · Keep deleted rows needs it; BeziSync adds it". It has no Adjust: an Adjust would insert an unwrap in a different place and with different fields from the compiler's (`withShape` against `sinkcaps.go:96-97`), which changes `config_hash`. To edit that unwrap's options, remove Keep deleted rows, choose "Use the row after change", then add Keep deleted rows back.
- When the destination requires the shape, the existing `shapeRequired` line stays, unchanged.

**C5. What deletes really do.** `effectiveDeletes(steps, ctx)`, with `ctx = { flink, unwrap?: SinkUnwrap | null, options }`, mirrors `withSinkUnwrap` and then `withSoftDelete`, in that order (`compiler.go:303`). It returns `{ mode, by, step? }`, and the first row that matches wins:

| Case | `mode` | `by` |
| --- | --- | --- |
| Flink CDC with `soft_delete` | `op_marked` | `keep_deleted` |
| Flink CDC otherwise | `flink_default` | `shape` |
| Debezium with any `soft_delete` | `rewrite` | `keep_deleted`; `step` = the drawn number of the first one |
| Debezium, unwrap present or required, and `unwrap.delete_handling_from` maps the destination option's value | the mapped value | `destination` |
| Debezium, unwrap present | its `delete_handling` (default `rewrite`) | `shape` |
| Debezium, unwrap required but absent | `unwrap.delete_handling[0]` (default `rewrite`) | `shape` |
| Debezium otherwise | `envelope` | `shape` |

Keep deleted rows wins over the destination's Deletes option because `withSoftDelete` runs after `withSinkUnwrap`. The function reads these places:
- **ShapeEditor:** while `by === 'keep_deleted'`, the Deletes select shows "Keep as an event with __deleted = true", disabled, with "Set by Keep deleted rows (step N). Remove that step to choose." It never writes `delete_handling`.
- **`summarizeShape(steps, ctx)`:** the shape line in the Transform row and in Review's StepsSummary. When there is no unwrap and a `soft_delete` exists, StepsSummary says "Event shape: Row after change, added by BeziSync for Keep deleted rows."
- **`outputImpact(before, after, ctx)`:** compares the effective shape (`envelope` against everything else) and the effective deletes. When only `soft_delete` was added or removed, only its own line shows, which today reads "Deleted rows now stay, marked deleted…".
- **`transformView(spec, ctx)` (Overview):** with any `soft_delete`, it shows the row and "Deletes are kept, flagged `__deleted = true`" on Debezium, or "Deleted rows are kept, marked by their change type (`__op = -D`)" on Flink CDC.

`ctx` comes from the connector types the callers already load (`sinkCapsOf`), plus the spec's `sink.options`.

**C6. Locks and their reasons** (a disabled control always says why):
- **The add menu** offers `soft_delete` once: "Already added: see Event shape" on Debezium, or "see Keep rows" on Flink CDC.
- **Saved duplicates** are all drawn, each with Remove. The server accepts them, so the client adds no error.
- **"Use the full change envelope instead"** is disabled while a `soft_delete` exists: "Keep deleted rows needs the row: remove it first." When field steps exist too: "Remove the field changes and Keep deleted rows first: they need the row."
- **Removing `soft_delete`** removes only that step.

**C7.** The inline `soft_delete` text moves into a `SoftDeleteEditor` in `editors.tsx`. On Flink CDC, its summary says it keeps rows (it is not a filter).

**C8. The Destination step note.** When `sinkCaps.unwrap?.delete_handling_from` is set and the spec has a `soft_delete`, `SinkStep` draws "Deletes: kept, marked deleted. Set by Keep deleted rows in Transform." under the options form. `SchemaForm` and the connector definitions don't change.

### Non-functional requirements

- **API load:** a refused draft sends at most one automatic request until it changes. Classifying in Guided mode adds one debounced, cached request when raw rows exist, the same one Expert mode already sends.
- **Security:** no change. The client shows only verdicts the server already gives the same caller.
- **Compatibility:** saved specs are never rewritten and the compiler is unchanged, so an unedited pipeline keeps its version, its spec and its compiled config (and so its `config_hash`).
- **Accessibility:** the badge is a `button` whose name includes the field. Focus moves only on a click. Field errors keep `role="alert"`.

### Implementation plan

One branch from `preview`, `fix/wizard-truth`, and one PR with three commits in this order. The loop fix comes first because it stops the request loop. Each commit:
- writes its tests first, failing before the change;
- changes the code, its contracts and its docs together;
- passes `make verify-quick` and its `make e2e-quick` specs;
- is then pushed.

This ADR and its index row ship in commit 1. `make verify-quick` runs `tsc`, `vitest`, the Go suite, the licence files and `contracts-check`. No Go code changes; the Go suite guards only the rest of the build.

#### Commit 1: Review never repeats a refused save (Fix A)

| File | Change |
| --- | --- |
| `frontend/src/features/pipelines/wizard/review.ts` (new) | `autosaveDecision` (table A1), `isPaused`, the `useAutosave` hook and `rawLabel`. |
| `frontend/src/lib/settings.ts` | `rawErrors(rows, verdicts, stored)` (A4). It imports `RawRow` as a type only. |
| `frontend/src/features/pipelines/wizard/ReviewStep.tsx` | See the list below the table. |
| `frontend/src/features/pipelines/wizard/__tests__/review.test.ts` (new) | Every `autosaveDecision` row and the precedence between rows. `useAutosave` under `renderHook` and fake timers: a `save` that always resolves false is called once in 10 s, once more after a change of signature, once more after Try again; a draft that becomes valid saves with no edit; `attempt` saves while paused; a clean draft never saves. `rawLabel`. |
| `frontend/src/lib/__tests__/settings.test.ts` | `rawErrors`: a new refused key blocks; a stored refused key with the same value doesn't; a stored refused key with a changed value does; an allowed key doesn't; no verdict doesn't; a duplicate blocks; keys are trimmed. |
| `e2e/tests/33-engine-settings.spec.ts` | Two new tests, below the table. |
| `e2e/tests/32-flink-cdc.spec.ts` | In the test at `:191`, add one refused Flink option, `source.scan.startup.mode` = `x` ("Only jdbc.properties.* and debezium.* source options pass through", `compiler/flinkadvanced.go:61`). Expect the "Advanced Flink CDC options" label and at most one `PATCH` in 4 s. |
| `docs/adr/0001-wizard-truth.md`, `docs/adr/README.md` | This ADR and its index row. |
| `frontend/src/features/pipelines/wizard/README.md` | Interface: `review.ts`. Invariants: the autosave pause, and when the next attempt happens; classification in both modes; a refused key blocks only when new or changed. |
| `frontend/src/lib/README.md` | Interface: `rawErrors`. Invariants: it mirrors `RefuseNewAdvanced`'s new-or-changed rule. |
| `frontend/src/components/README.md` | Note that `rawErrors` compares server verdicts with the stored spec and doesn't judge keys, so the "never judged in the browser" invariant still holds. |
| `.rules/frontend.md` | "Autosave never retries a failed save by itself." |
| `changelogs/unreleased/wizard-fixes.md` (new) | `type: patch`, `### Fixed`: "Review stops re-saving a change the server refused". |

What changes in `ReviewStep.tsx`:
- add `failedSignature`, and route every save through one `attempt(body, signature)`;
- the effect switches on `autosaveDecision`;
- call `reset()` when the draft becomes clean with an error showing;
- pass `onRetry` to `InlineError`;
- the badge follows table A3, without the "fix {fields}" button until commit 2, and `deployWhy` gets a paused branch;
- `rawError` becomes the first message from `rawErrors(rawRows, verdicts, spec.advanced)` (`spec` is `fullSpec(pipeline.spec)`);
- `useClassify` is enabled whenever raw rows exist;
- `rawLabel` replaces both hard-coded labels.

The two new tests in `33-engine-settings.spec.ts`:
- **"a refused advanced property never loops":**
  - count `PATCH`es as `walk.ts:16-21` does;
  - in Review (Expert), add `debezium.format.value` = `json`;
  - expect the row's refused message, a badge starting "Not saved", and at most one `PATCH` in 4 s;
  - remove the row, then expect "All changes saved" and no further `PATCH` for 4 s.
- **"a save the server refuses pauses":**
  - in Review (Expert), set the typed settings "Batch size" = `8192` and "Queue size" = `8192`, the server's batch-below-queue rule (`services/tuning.go:63-80`);
  - expect exactly one `PATCH` in 4 s, "Couldn't save your changes" with the server's message, and no Try again button;
  - set "Queue size" = `16384`, then expect "All changes saved".
- **Guided mode:** create the pipeline through the API with an allowed advanced property (`debezium.source.provide.transaction.metadata` = `false`), open Review in Guided mode, and expect a `settings:classify` request for it.

**Gate:** `make verify-quick`, then `make e2e-quick SPECS="tests/33-engine-settings.spec.ts tests/32-flink-cdc.spec.ts tests/12-self-service.spec.ts"`. The PR says whether `32` ran, which needs `E2E_FLINK_IMAGE`.

#### Commit 2: every blocking error names its field (Fix B)

| File | Change |
| --- | --- |
| `frontend/src/lib/cluster.ts` | `streamingErrors` and `manualErrors` (B2). `streamingError` and `manualError` keep their messages and return the first part. |
| `frontend/src/features/pipelines/wizard/review.ts` | `ReviewProblem`, `reviewProblems`, `customErrors`, `problemsSummary`, `focusReviewField` (B1, B3). |
| `frontend/src/features/pipelines/wizard/ReviewStep.tsx` | `problems = reviewProblems(…)` replaces `customError`, `rawError`, `tuningError` and `clusterError`. Each field takes its own error, and Memory gets one. Add the `data-review-field` wrappers. The badge becomes the button. `deployWhy` names the first label. Draw `BlockingSettings` per B4. |
| `frontend/src/features/pipelines/wizard/FlinkClusterEditor.tsx` | New prop `errors: { streamingCpu?, streamingMemory?, taskManagers?, tmMemory? }`, which replaces the local `manualError`/`streamingError` calls (`:80-81`). Each field gets its error and its `data-review-field` wrapper. The diagram shows when there are no errors, as today. |
| `frontend/src/features/pipelines/wizard/BlockingSettings.tsx` (new) | `BlockingSettings({ problems, onRemove, expert })` (B4). |
| `frontend/src/features/pipelines/wizard/__tests__/review.test.ts` | See the list below the table. |
| `frontend/src/lib/__tests__/cluster.test.ts` | `streamingErrors`: bigger CPU only, bigger memory only, same size (both), manual (`{}`). `manualErrors`: too few slots, too little memory per slot, automatic (`{}`). The wrappers keep the existing cases green. |
| `e2e/tests/33-engine-settings.spec.ts` | Two new tests, below the table. |
| `e2e/tests/32-flink-cdc.spec.ts` | In the test at `:191`: set Streaming reader CPU and memory equal to the reader's CPU and memory. Expect errors on both streaming fields and the badge "Not saved: fix Streaming reader CPU and 1 more". |
| `frontend/src/features/pipelines/wizard/README.md` | Interface: the new functions, `BlockingSettings`, and FlinkClusterEditor's `errors` prop. Invariants: every blocking problem has a drawn field in both modes; the badge, `deployWhy` and field errors read `reviewProblems`. This replaces the `tuningError` sentence. |
| `frontend/src/lib/README.md` | Interface: `streamingErrors`, `manualErrors`. |
| `.rules/frontend.md` | Next to the Engine settings rule (`:120`) and Guided against Expert (`:219-220`): Guided shows only the settings that block saving, with Remove; Expert shows the editor. |
| `docs/README.md` | Review: the "Not saved: fix …" badge names the field and goes there on click (`:219`, `:240`). Guided mode lists settings that block saving. |
| `changelogs/unreleased/wizard-fixes.md` | Add "Review says which field to fix". |

The `review.test.ts` cases:
- `reviewProblems`:
  - page order;
  - Memory over the maximum names Memory and not CPU;
  - both over name both;
  - an unknown tuning key gives `tuning` with the key as its label;
  - a blocking raw row gives `advanced` labelled by `rawLabel`;
  - Placement is never a problem;
  - a manual cluster yields no streaming problem.
- `problemsSummary` with one problem and with three.
- `focusReviewField` in jsdom: it focuses the `aria-invalid` input inside the wrapper, falls back to the first focusable element, and doesn't throw without `scrollIntoView`.

The two new tests in `33-engine-settings.spec.ts`:
- **"the badge names the field":**
  - set the project's `max_resources` to `small` through the policies API;
  - in Review (Expert), choose Size → Custom, then a Memory over the limit with CPU within it;
  - expect the error under Memory and none under CPU, and the badge "Not saved: fix Memory";
  - click the badge and expect focus inside Memory's control;
  - keep `getByLabel('CPU', { exact: true })` (`:69-71`) working.
- **"Guided mode lists settings that block saving":**
  - set a valid tuning value through the API;
  - hide that setting with a project `setting_limits` `hide` rule, so `ReviewStep.tsx:110` treats the stored key as unknown;
  - open Review in Guided mode;
  - expect "Settings that block saving" with the key, and a badge naming it;
  - click Remove, then expect "All changes saved".

**Gate:** `make verify-quick`, then `make e2e-quick SPECS="tests/33-engine-settings.spec.ts tests/32-flink-cdc.spec.ts tests/16-accessibility.spec.ts tests/28-layout.spec.ts tests/12-self-service.spec.ts"`.

#### Commit 3: Keep deleted rows is drawn, and deletes are described as they run (Fix C)

| File | Change |
| --- | --- |
| `frontend/src/features/pipelines/transforms/model.ts` | `shapeIndex`; `stepErrors` and `addStep` per C1 and C2; `sectionOf`, `drawOrder`, `effectiveDeletes`, `summarizeShape`; `outputImpact(before, after, ctx)` per C5; the Flink `soft_delete` summary text (C7). |
| `frontend/src/features/pipelines/transforms/TransformStep.tsx` | See the list below the table. |
| `frontend/src/features/pipelines/transforms/editors.tsx` | ShapeEditor reads `effectiveDeletes(ctx.steps, …)` (C5). Add `SoftDeleteEditor`. |
| `frontend/src/features/pipelines/transforms/Impact.tsx` | `OutputImpact` loads the connector types and passes `ctx` to `outputImpact`. `StepsSummary` takes `ctx` and uses `summarizeShape`. |
| `frontend/src/features/pipelines/wizard/ReviewStep.tsx` | Pass `sinkCaps.unwrap` and the options to `StepsSummary`. |
| `frontend/src/features/pipelines/page/overview.ts`, `page/Overview.tsx` | `transformView(spec, ctx)` (C5). `Overview.tsx:274` builds `ctx` from the connector types. |
| `frontend/src/features/pipelines/wizard/SinkStep.tsx` | The note (C8). |
| `frontend/src/features/pipelines/transforms/__tests__/model.test.ts` | The cases listed below the table. |
| `frontend/src/features/pipelines/__tests__/overview.test.ts` | `transformView` with `[soft_delete]` on Debezium says row plus deletes kept. Flink CDC with `soft_delete` says `__op = -D`. |
| `e2e/tests/27-transforms.spec.ts` | Three changes, below the table. |
| `e2e/tests/32-flink-cdc.spec.ts` | The test at `:89` creates `[soft_delete, route, computed]`. Open its Transform step, expect Keep deleted rows under "Keep rows", add a "Keep only some rows" filter, continue, then expect "All changes saved" and a saved chain starting `[filter, soft_delete, …]`. |
| `frontend/src/features/pipelines/transforms/README.md` | Interface: the new functions and `SoftDeleteEditor`. Invariants: drawing (C3), placement (C2), `shapeIndex` and `effectiveDeletes` under "Mirrors of backend rules", and the removed known-issue sentence. |
| `frontend/src/features/pipelines/wizard/README.md`, `page/README.md` | SinkStep's note. `transformView` reads `effectiveDeletes`. |
| `ARCHITECTURE.md` | Delete Known issue 14, leaving the other numbers unchanged. Add a new row: "With a destination that requires the event shape (Iceberg), a chain with `soft_delete` and a field step but no explicit unwrap compiles the field step before the unwrap: `withSinkUnwrap` appends it at the end, then `withSoftDelete` drops the `soft_delete`. The wizard never builds such a chain; the API accepts it." Paths: `compiler/sinkcaps.go`, `compiler/compiler.go`. |
| `docs/README.md` (`:184`) | Keep deleted rows shows under Event shape on Debezium and under Keep rows on Flink CDC, can be added once, and is noted on a destination's Deletes option. |
| `.rules/frontend.md` (`:185`) | The step list is drawn by `sectionOf`/`drawOrder` and ordered by `shapeIndex`, both mirrors. |
| `changelogs/unreleased/wizard-fixes.md` | Add "Keep deleted rows shows in the Transform step". |

What changes in `TransformStep.tsx`:
- draw by `sectionOf` and number by `drawOrder`;
- the shape area follows C4;
- the `soft_delete` lock and the envelope-button reasons follow C6;
- the summary of the unwrap row comes from `summarizeShape`;
- `SoftDeleteEditor` replaces the inline text;
- remove the stage -1 filter's `t.type !== 'unwrap'` check, which can never be true.

The `model.test.ts` cases, one table:
- **`stepErrors`**, chain and expected error indexes:
  - `[soft_delete, filter]` → `{1}`
  - `[filter, soft_delete]` → `{}`
  - `[soft_delete, rename]` → `{}`
  - `[rename, soft_delete]` → `{0}`
  - `[unwrap, soft_delete, rename]` → `{}`
- **`addStep`**, start chain plus the added step, then the result:
  - `[]` + `soft_delete` → `[soft_delete]`
  - `[unwrap]` + `soft_delete` → `[unwrap, soft_delete]`
  - `[filter, unwrap, rename]` + `soft_delete` → `[filter, unwrap, soft_delete, rename]`
  - `[soft_delete]` + `filter` → `[filter, soft_delete]`
  - `[outbox]` + `soft_delete` → `[outbox, soft_delete]`
  - `[unwrap, soft_delete]` + `rename` → `[unwrap, soft_delete, rename]`
- **`sectionOf`:** every `TransformType` (the keys of `TYPE_TITLE`) maps to one section on each engine.
- **`drawOrder`:** `[rename, unwrap, filter, soft_delete]` on Debezium gives `[2, 1, 3, 0]`.
- **`effectiveDeletes`**, one case per row of the C5 table, including:
  - JDBC `delete_handling_from` with `deletes: delete` → `tombstone/destination`
  - the same with a `soft_delete` → `rewrite/keep_deleted`
  - `[]` with no rule → `envelope`
  - two `soft_delete` steps → `step` is the first one's drawn number
- **`outputImpact`:**
  - adding `soft_delete` to an envelope chain gives the soft-delete line, and no line saying the shape changed or deletes changed;
  - adding it to `unwrap(drop)` gives exactly one deletes line (a regression guard: it passes today).
- **`summarizeShape`** with and without `soft_delete`.

The three changes to `27-transforms.spec.ts`:
- **Extend "an editor keeps deleted rows and cuts long text"** (`:323`):
  - after adding Keep deleted rows, expect its row (region "Keep deleted rows") under the "Event shape" heading;
  - open Row after change, set Deletes to "Drop delete events" (enabled before the add), then expect the select disabled with "Set by Keep deleted rows";
  - expect "Use the full change envelope instead" disabled, and Add transform → Keep deleted rows disabled with "Already added";
  - Remove it, and expect Deletes enabled showing "Drop delete events" and the saved spec to have no `soft_delete`;
  - add it back for the rest of the test.
- **A new test, "a saved Keep deleted rows without a shape is drawn and never re-saved":**
  - create through the API `transforms: [{type:'soft_delete'}]` and `[{type:'soft_delete'},{type:'soft_delete'}]` (two drafts);
  - for each, read `GET …/pipelines/{id}` (version, spec) and `GET …/pipelines/{id}/config`;
  - run `walkWizard`, expecting zero `PATCH` requests;
  - on Transform, expect the locked line and every Keep deleted rows row with Remove;
  - read both endpoints again and expect them deep-equal.
- **A new test, "the Destination step notes Keep deleted rows":** create a JDBC destination connection with the config `08-sinks.spec.ts:149-160` uses, and a draft with `[{type:'unwrap'},{type:'soft_delete'}]`. Open the Destination step and expect the note. It never deploys.

**Gate:** `make verify-quick`, then `make e2e-quick SPECS="tests/27-transforms.spec.ts tests/32-flink-cdc.spec.ts tests/04-pipeline-ui.spec.ts tests/33-engine-settings.spec.ts"`.

### Verification plan

- **Per commit:** the gates above.
- **Before the PR** (`.rules/pre-pr.md`):
  - the full `make verify`;
  - `git diff --name-only origin/preview...HEAD -- backend/` is empty;
  - `TestCompileMatrixUnchanged` passes (part of the Go suite);
  - the e2e specs of every commit together;
  - screenshots of Transform (both engines, with Keep deleted rows) and Review (the badge, the Memory error, the Guided card) at 390, 768 and 1280 px, light and dark;
  - one network capture showing a single `PATCH` after a refused save;
  - the edge-case register below, row by row.
- **Flink CDC rows** run only when `E2E_FLINK_IMAGE` is set. The PR says whether they ran.

#### Edge-case register

| ID | Case | Expected | Commit | Test |
| --- | --- | --- | --- | --- |
| E1 | Refused raw key typed in Review (Expert) | Row red; at most one `PATCH` in 4 s | 1 | `33` "never loops" |
| E2 | Remove the refused row | "All changes saved"; no further `PATCH` | 1 | `33` "never loops" |
| E3 | Server-only refusal (batch = queue) | One `PATCH`; server message; no Try again; fixing it saves | 1 | `33` "pauses" |
| E4 | Network error or 5xx | Try again shown; one attempt per click | 1 | `review.test` (loop test); `InlineError` already gates on `isRetryable` |
| E5 | 412 `version_conflict` | Paused; the server's message | 1 | `review.test` (any failure pauses) |
| E6 | Stored refused key, same value | Not blocking; saves | 1 | `settings.test` |
| E7 | Stored refused key, changed value | Blocking | 1 | `settings.test` |
| E8 | Draft becomes valid with no further edit | Saves (`invalid` is a dependency) | 1 | `review.test` (`blocked` → `schedule`) |
| E9 | Guided mode with raw rows | Classify request sent | 1 | `33` Guided case |
| E10 | Flink CDC refused option | "Advanced Flink CDC options"; at most one `PATCH` | 1 | `32` (`:191`) |
| E11 | Custom Memory over max, CPU within | Error under Memory only; badge "fix Memory"; click focuses Memory | 2 | `review.test`, `33` "names the field" |
| E12 | CPU and Memory both over max | Both errors; "CPU and 1 more" | 2 | `review.test` |
| E13 | Flink streaming size equal to the copy size | Errors on both streaming fields | 2 | `cluster.test`, `32` (`:191`) |
| E14 | Flink manual shape | No streaming problem | 2 | `cluster.test`, `review.test` |
| E15 | Guided mode, hidden tuning key | "Settings that block saving"; Remove saves | 2 | `33` Guided case |
| E16 | Expert, nothing typed to offer, unknown tuning key | Blocking rows inside the card | 2 | `review.test` (problem has `key`); drawn by the `tunable.length === 0` branch |
| E17 | Placement profile missing | Not a problem; preflight reports it | 2 | `review.test` |
| E18 | Badge at 390 px; axe | No overflow; the button has a name | 2 | `28`, `16` (the pipeline in `33` reaches the state; `16` and `28` cover the button in its idle state) |
| E19 | Debezium envelope + Keep deleted rows | Row under Event shape; locked line; envelope button and menu disabled | 3 | `27` extended |
| E20 | `unwrap(drop)` + Keep deleted rows | Select disabled "Set by Keep deleted rows"; stored `drop` kept | 3 | `27` extended, `model.test` |
| E21 | Remove after E20 | Select enabled showing "Drop delete events" | 3 | `27` extended |
| E22 | Flink: Keep deleted rows, then a filter | `[filter, soft_delete]` under Keep rows; saves | 3 | `model.test`, `32` (`:89`) |
| E23 | `[soft_delete, filter]` / `[rename, soft_delete]` | Client error, the same as the server | 3 | `model.test` |
| E24 | `[soft_delete, rename]` saved through the API | No client error | 3 | `model.test` |
| E25 | Saved `[soft_delete]` and two `soft_delete` steps | Drawn with Remove; zero `PATCH`; version, spec and compiled config equal | 3 | `27` "never re-saved" |
| E26 | `[outbox]` + Keep deleted rows | `[outbox, soft_delete]` | 3 | `model.test` |
| E27 | JDBC destination + Keep deleted rows | Note on the Destination step | 3 | `27` "Destination step notes" |
| E28 | JDBC `deletes: delete`, no `soft_delete` | ShapeEditor and summary say tombstone (destination) | 3 | `model.test` |
| E29 | Adding Keep deleted rows to an envelope pipeline: deploy impact | The soft-delete line only | 3 | `model.test` |
| E30 | Overview of a `[soft_delete]` pipeline | Row + deletes kept | 3 | `overview.test` |

## Consequences

- **Easier:**
  - the client flags exactly the chains the server refuses;
  - every Review blocker comes from one list, so the badge, the Deploy reason, the field errors and focus agree;
  - a refused save costs one request instead of one a second.
- **Accepted: three new frontend copies of backend rules.** These are `shapeIndex` (`validateTransforms`), `effectiveDeletes` (`withSinkUnwrap` then `withSoftDelete`) and `rawErrors` (`RefuseNewAdvanced`). A change to those Go functions must update them. Their contracts list them under the backend mirrors.
- **Accepted: a compiler ordering issue stays open.** For a destination that requires the event shape, `[soft_delete, rename]` without an unwrap compiles to `[rename, unwrap]`. The wizard can't build that chain; the API can. It is recorded in `ARCHITECTURE.md` Known issues and needs a backend change.
- **Accepted: older specs keep their order.** A saved chain is drawn by section and numbered in drawn order, but never reordered. Server messages still count steps in chain order.
- **Revert:** reverting the PR restores today's client. It would again flag chains the server accepts, such as `[soft_delete, rename]`, and hide Keep deleted rows. No data changes either way.

### Future improvements

- **Name fields from the server's error locations** (`problem.errors[].loc`), as label-only problems that don't count as client-side invalid. Deferred because folding them into `invalid` defeats the pause.
- **A ReviewStep component test**, the first one for that component.
- **A compiler fix for the ordering issue**, with a `config_hash` migration note.
- **A "put steps in run order" action** for older specs.

## Alternatives considered

Each option below was weighed in the design review (`.egm/`, working files not committed). The reasons are given here so the ADR stands on its own.

### A. A separate `stageOf` value for `soft_delete`
- **Mechanism:** draw it with a stage filter, like the others.
- **Rejected:** `stageOf` mirrors `domain.Stage()`, and a test pins that. Drawing and ordering are separate questions, and `sectionOf` answers drawing without touching the mirror.

### B. Keep deleted rows as a switch on the Row after change row
- **Rejected:** Flink CDC has no such row, and neither does a Debezium spec with `soft_delete` and no unwrap. The stored model is a separate step, which the compiler, the preview, YAML and the API already use. A second saved copy would have nowhere to be removed from.

### C. Insert an explicit unwrap when Keep deleted rows is added, or offer Adjust on the locked line
- **Rejected:** the client would place the unwrap differently and with other fields than the compiler (`withShape` against `sinkcaps.go:96-97`). That changes `config_hash` and restarts deployed pipelines. It also covers only new adds. The locked line draws new and saved specs alike, and changes nothing.

### D. Write `rewrite` into the unwrap's `delete_handling` on add
- **Rejected:** removing the step couldn't bring the user's choice back, and saved specs would still be wrong unless rewritten. `effectiveDeletes` derives the value instead.

### E. Pause only after `managed_property`
- **Rejected:** tuning refusals, 412, network errors and 5xx loop the same way. The loop is in the effect, so the pause has to cover every failure.

### F. Automatic retries with backoff
- **Rejected:** automatic retrying is what caused the loop. Try again, the next edit and leaving the step cover recovery, with less state to test.

### G. Every refused verdict blocks saving
- **Rejected:** the server accepts unchanged refused keys (`RefuseNewAdvanced`), so an older pipeline could no longer save any change. `rawErrors` compares against the stored spec.

### H. Show the full Engine settings card in Guided mode when it has values
- **Rejected:** accepting a Recommendation writes `tuning` in both modes (`ReviewStep.tsx:491-495`), so most Guided users would get the Expert editor. `BlockingSettings` shows only what blocks.

### I. Name the field without focusing it
- **Rejected:** on a long page the user still has to find it. One wrapper attribute and one helper are enough, and `components/ui` is unchanged.

### J. Keep the `??` chain and only change the text
- **Rejected:** the badge, the Deploy reason and the field errors would read different sources, only the first problem would be known, and nothing could be tested without the component.

### K. Merge the server's error locations into the problem list
- **Rejected for now:** that makes a paused draft invalid, so the pause never applies and leaving the step refuses. See Future improvements.

### L. Reorder saved specs on load
- **Rejected:** it makes a draft dirty from opening a step, creates a version, and can change `config_hash`.

### M. Shared Go and TypeScript test vectors for each mirror
- **Rejected:** they add Go test files to a frontend-only fix. Guarding mirrors that way would be a repo-wide change; today all mirrors are guarded by their contracts and review.
