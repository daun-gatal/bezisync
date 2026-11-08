# 0011 — The wizard follows the source and the engine

- **Status:** Accepted
- **Date:** 2026-11-08
- **Scope:** `backend/internal/engine` (`EventSinks`), `backend/internal/api` (`/engines` returns `event_sinks`;
  `openapi.json`), `backend/internal/services` (`ChooseEngine` follows the allowed engines; the Flink CDC Kafka
  format refused at save), `backend/internal/connectors` (`FlinkFormatProblem`, shared by save and compile);
  `frontend/src/lib/engines.ts` (`sinkFit`, `sourceFit`, `engineAllowed`, `pickEngine`, `writesEvents`,
  `enginePitch`, `hiddenLines`), `frontend/src/features/pipelines` (Source, Destination, Transform and Review
  steps, the Flink CDC preview, the Overview's transform view, `specForEngine`),
  `frontend/src/features/connections/ConnectionDialog.tsx`; e2e `08-sinks`, `32-flink-cdc`, `34-flink-sinks`;
  the guide; the contracts of every touched directory.

## Context

The maintainer reported two places where the new-pipeline wizard ignores what was already chosen:

1. **The Destination step lists every destination.** It filtered only by the project's `allowed_sink_types`.
   A MongoDB source was offered StarRocks (only Flink CDC writes it, and Flink CDC can't read MongoDB); a
   project that allows only Debezium was offered every Flink-only destination; a destination whose only
   engine isn't set up looked like any other. The user learned at Continue ("No engine runs…"), at save
   (`policy_violation`) or at preflight (`policy_engine`). "Add a destination" listed every type, and the Source
   step's templates could name a destination the chosen source can't go to.
2. **Flink CDC to Kafka used the table layout in Transform.** Flink CDC writes Kafka as change events
   (`debezium-json`: `before`, `after`, `op` and a short `source`, keyed by `{"TableId": …, <key>}`, one topic
   per table; ADR 0009 D1). The Transform step treated every Flink CDC pipeline as rows written into tables
   ("Which rows and columns reach each table?", "Keep rows", "Primary key", a "Destination table" preview), and
   so did Review and the Overview.

Two destinations, Kafka and Iceberg, are written by both engines, so neither problem is a property of the
destination alone: the layout follows the **engine and destination pair** (Debezium → Kafka and Flink CDC →
Kafka are events; Flink CDC → Iceberg is rows), and a destination is usable when **any** engine the project
allows runs the pair and is on.

The Goldfish review of the concept also found: the backend's default engine (`ChooseEngine`, mirrored by the
UI's `pickEngine`) ignored `allowed_engines`, so PostgreSQL → Kafka in a Flink-only project defaulted to
Debezium and failed at save or preflight, also for CLI and MCP callers; switching a Kafka draft from Debezium
with Avro to Flink CDC kept Avro, refused only at compile; unreadable policies carry the strictest values
(`allowed_engines: []`), which must not hide everything in the UI.

## Decision

### D1. One fit rule for destinations (`sinkFit`)
For a destination type and the pipeline's source, with the project's policies:

| Case | Result |
|---|---|
| No engine runs the source to it | **hidden** |
| Engines run it, but the policy allows none of them | **hidden** |
| An allowed engine runs it, but none of those is on (no Flink CDC image) | **shown, disabled**, the engine's reason as the card's text |
| An allowed, available engine runs it | shown |

Hidden destinations are named under the list, one line per reason, three names then "and N more" ("Not shown
for MongoDB: doris-prod: no engine runs this source to them."). The pipeline's saved destination always shows.
Nothing is judged before `/engines` and the policies answer (a skeleton instead of cards that later vanish), and
unreadable policies judge nothing (a notice says changes are blocked; the server refuses saving). When nothing
fits, the step says so and points at "Add a destination". The same rule drives:
- the engine picker: an engine the policy disallows is disabled ("Not allowed in this project") and never picked
  by default (`pickEngine`: allowed and available first, then available);
- "Add a destination" in the wizard (`fitting` prop): it lists only types that fit, minus `allowed_sink_types`;
  the Connections page passes nothing and lists every type;
- the Source step: a source no allowed engine reads is disabled with the reason (`sourceFit`: a Flink-only
  project and MongoDB), and a template whose destination doesn't fit the chosen source is disabled, blocking
  Continue if already chosen.

Hidden rather than disabled for the first two cases: nothing the user does in this wizard makes them work (the
source is fixed once the draft exists; a policy is an Admin's), and a disabled card per connection is the full
list the maintainer objected to. Disabled for the third: an Admin closes that gap, and the reason says how.

### D2. The server picks an allowed engine by default
`ChooseEngine` takes the project and prefers, among available engines that run the pair (the current one
first), one the policy allows. With none, the old order applies and `enforceEnginePolicy` / preflight refuse with
their own message. A requested engine is unchanged. Existing pipelines are untouched: the choice runs only on
create, on a destination change or on a requested engine.

### D3. The engine table says which destinations are events
`engine.EventSinks(FlinkCDC) = ["kafka"]`; `/engines` returns it as `event_sinks` (Flink CDC only; MCP's
`list_engines` passes it through). The UI's `writesEvents(engine, sinkType)` is true for Debezium and for
Flink CDC's event sinks. No destination type is hard-coded in the layout code.

### D4. Flink CDC to Kafka in event terms
When `writesEvents` holds on Flink CDC, the Transform step uses an event vocabulary (one object, chosen once):
heading "How should events look?", sections "Keep events", "Fields · order matters", "Message key"; step titles
as on Debezium ("Keep only some events", "Rename fields", "Add a field", "Computed field"); the filter's notes
("Events a filter drops never reach the topic"; an update that makes a row match is sent as an insert, one that
stops matching as a delete); the Route editor says "topic"; a keyless table's notice says its events share one
key and so one partition; Keep deleted rows says (in its editor and its one-line summary, `flinkSummary`) a delete arrives as an
insert event whose `after` carries `__op = -D`; Review's run settings say a restart can send an event
again (not "writes are idempotent by key"). The preview panel becomes **Events**: `table → topic`, what the key and the value hold (in words,
not a sample whose details could drift with Flink CDC), then the fields table. Review's "To" line, its steps
summary and the Overview's transform view say events and message key. The Destination step describes Flink CDC
on Kafka without "upserts by key" and says records are Debezium-style JSON without a schema. Event shape, value
formats and Partition by stay hidden (unchanged).

### D5. Kafka on Flink CDC takes only JSON without the schema, at save
`connectors.FlinkFormatProblem` holds the rule; the compiler and `ValidateSinkSpec` both call it, and an engine
switch without a spec validates the stored spec too. The UI's `specForEngine(spec, engine, {events})` sets the
format to JSON without the schema (keeping the stored key order, so autosave settles), on every save path.

### Implementation plan (as built)
1. Backend: `EventSinks`, `event_sinks` on `/engines`, policy-aware `ChooseEngine`, `FlinkFormatProblem` at
   save; unit and API tests; OpenAPI.
2. Frontend fit: `sinkFit` and friends, Destination step, its engine picker, "Add a destination", Source step,
   `specForEngine`; unit tests.
3. Frontend events layout: the vocabulary, Transform, the preview, editors, Review, Overview; unit tests.
4. e2e, guide, contracts, changelog.

### Verification plan
- Unit: `sinkFit`/`sourceFit`/`pickEngine`/`writesEvents` (engines.test.ts), the dialog filter, `specForEngine`,
  the vocabulary; Go: `EventSinks`, `TestEnginePolicyDefaultAndFlinkKafkaFormat`.
- e2e: `08-sinks` (Destination step hides and names what doesn't fit), `32-flink-cdc` (wizard on Flink CDC →
  Kafka: event layout), `34-flink-sinks` (the real Kafka record's shape on Flink CDC), and the specs that walk
  the wizard (`04`, `27`, `33`).

### Edge-case register
| ID | Case | Expected | Test |
|---|---|---|---|
| W-1 | MongoDB source, StarRocks connection | hidden, named in "Not shown" | unit, e2e 08 |
| W-2 | PostgreSQL, Kafka, Flink CDC off | shown, Debezium | unit |
| W-3 | PostgreSQL, StarRocks, Flink CDC off | shown disabled, the engine's reason | unit |
| W-4 | Policy Debezium only, Doris | hidden ("doesn't allow Flink CDC") | unit |
| W-5 | Policy Flink only, Kafka, Flink CDC on | shown; Flink CDC picked (UI and server) | unit, Go API |
| W-6 | Policy Flink only, Kafka, Flink CDC off | shown disabled (Flink CDC's reason) | unit |
| W-7 | Policy Flink only, MongoDB source | source disabled with the reason | unit |
| W-8 | Unreadable policies | nothing hidden on policy grounds; notice | unit |
| W-9 | Saved destination now unfit | still shown, Continue blocked with the reason | code path |
| W-10 | Nothing fits | empty state pointing at "Add a destination" | e2e 08 |
| W-11 | Template whose destination doesn't fit | option disabled; Continue blocked if chosen | unit (sinkFit) |
| W-12 | Debezium → Kafka Transform | unchanged event layout | e2e 27 |
| W-13 | Flink CDC → Iceberg Transform | unchanged table layout | e2e 32 |
| W-14 | Flink CDC → Kafka Transform, preview, Review | event wording, no table wording | unit, e2e 32 |
| W-15 | Kafka draft Debezium + Avro switched to Flink CDC | UI saves JSON; API without spec → 422 | unit, Go API |
| W-16 | Kafka on Flink CDC with Avro or the schema via API | 422 `engine_settings` at save | Go API |
| W-17 | Flink CDC table destination with a stored Avro format | saves (ignored) | Go API |
| W-18 | Existing pipelines | same `config_hash` (no compile change) | matrix |
| W-19 | Flink CDC Kafka record | `before`/`after`/`op`/`source`, key with `TableId` | e2e 34 |

## Consequences
- The Destination step lists only what can run, and says what it left out and why.
- A project that allows only Flink CDC gets Flink CDC by default for Kafka and Iceberg, through the UI, the CLI
  and MCP.
- A Flink CDC Kafka pipeline's screens describe topics and events.
- **Not done:** the Promote dialog (pipeline page) still filters the target project's destinations by type
  only, and Adopt (import) stays Debezium-only; both keep the server's refusals. A server-side list of
  compatible destinations: `/engines` already carries the rule.

## Alternatives considered
- **Disable every unfit destination with a reason.** Rejected: it keeps the full list, and in the no-engine and
  policy cases there is nothing the user can do.
- **Hide destinations whose engine is off.** Rejected: the Admin's fix (register a Flink CDC image) would
  become invisible.
- **`sink.type === 'kafka'` in the UI.** Rejected: capabilities come from the engine tables (engine.go's package
  rule), so the UI, CLI and MCP agree, and a second Flink CDC event sink is one line.
- **A literal sample event in the preview.** Rejected after review: its `source` block and key format would be a
  copy of Flink CDC's serializer that can drift; the panel describes the record in words and e2e checks the
  real one.
- **Refusing the Flink CDC Kafka format only in the UI.** Rejected: API, CLI and MCP saves and older drafts
  would still store it; the server refuses it at save.
