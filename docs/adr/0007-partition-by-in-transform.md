# 0007 — Partition by in the Transform step; no Iceberg change log on Flink CDC

- **Status:** Accepted
- **Date:** 2026-11-07
- **Scope:** `frontend/src/features/pipelines` (`transforms/PartitionBy.tsx`, moved from `wizard/`;
  `transforms/TransformStep.tsx`, `wizard/SinkStep.tsx`, `spec.ts`), `frontend/src/components/SchemaForm.tsx`
  (an option shown unavailable, with its reason), `backend/internal/connectors/iceberg.go`
  (`iceberg_rustfs_upsert`), `backend/internal/checks/catalog.json`, e2e `08-sinks` and `32-flink-cdc`, the
  guide, the wizard and transforms contracts. **Partly supersedes [ADR 0005](0005-iceberg-partitioning.md)**:
  its *Editor* section (the editor's place and who saves it). Its rules (R1–R9) are unchanged.

## Context

ADR 0005 put *Partition by* in the Destination step. Two things in the maintainer's report follow from it.

1. **Partition by depends on choices made after it.** On an upserted table only key columns can be picked
   (R5), and the columns offered are the written ones: renames, computed and added columns and the key are
   all chosen in Transform, the step after Destination. A user who picks the key or adds a computed column
   in Transform must go back to Destination to partition by it. ADR 0005 accepted this with "stale fields"
   (Continue waits; nothing is dropped). The Debezium automatic default has the same problem: Destination
   records each table's key (`options.partitions`, from `spec.keys`) before Transform lets the user change
   the key, so a key chosen in Transform leaves the recorded one stale (E2, blocked by preflight until
   Destination or Review re-records it).
2. **Flink CDC offers no *Every change appended* for Iceberg.** The frontend hides the choice on Flink CDC
   (`optionsSchemaFor`) and the server refuses it (`iceberg_flink.go`). Reading the pinned Flink CDC 3.7.0
   sources (copies in the design notes):
   - `IcebergWriter` builds `RowDataTaskWriterFactory(…, table.schema().identifierFieldIds(), upsert = true)`:
     a table with identifier fields is always upserted by them.
   - The metadata applier creates a table's identifier fields from the event schema's primary keys, and a
     transform can't clear them: `PreTransformOperator` keeps the source's key when `primary-keys` is
     empty, and `PostTransformOperator` copies the key into the projected schema.
   - A table without identifier fields gets Iceberg's insert-only writer, but 3.7.0 writes every delete (and
     the old values of every update) into it as ordinary rows. Upstream fixed this after 3.7.0
     (FLINK-41057, apache/flink-cdc #4588 and its 3.7 backport #4606, 2026-11-02); the 3.7.0 docs still say
     "The source table must have a primary key". BeziSync now blocks writing into such a table on Flink
     CDC (`iceberg_flink_keyless_table`, the change before this one).

   So a change log can't be written correctly on Flink CDC 3.7.0. A per-change `UUID()` key would turn
   every insert into an equality delete plus a row, which slows every read until compaction.

One more problem in the same place: `iceberg_rustfs_upsert` (RustFS S3 Tables refuses the first upsert of a
new table) tells the user to choose *Every change appended*, which a Flink CDC pipeline can't choose.

The invariant: every existing spec compiles to the same `config_hash`. Nothing here changes the spec's shape
or the compilers.

## Decision

### P1. Partition by lives in Transform

- The Transform step gets a **Partition by** section after *Primary key* (Flink CDC) or *Message key*
  (Debezium), for Iceberg destinations on both engines. It is one collapsed row (like the key row) with a
  one-line summary (below). Opening it shows the same
  `PartitionByEditor`, moved to `transforms/PartitionBy.tsx` (the wizard imports `transforms/`, never the
  other way round).
- The editor reads the Transform step's **draft**: renames, computed and added columns and the key the user
  is editing on the same screen are offered or refused as they change, without a save in between.
- One name, *Partition by*, everywhere (the guide, the row, the Destination line, preflight fixes).
- The row's summary uses the editor's own words for a table it doesn't list (`automaticSummary`):
  "Automatic: 16 buckets of id" for one table; otherwise listed tables by name up to two ("orders: Buckets
  of id (16)") or "3 tables partitioned", then "N automatic" and "N not partitioned". On Debezium the
  editor says that the automatic bucket count and turning automatic partitioning off are Destination
  settings.
- The editor refuses tables routed to one destination with different fields, as the server does
  (`icebergPartitionKey`), now that a route can be added on the same screen.

### P2. Transform is the only writer of `partition_by`

- The Transform step saves `sink.options.partition_by` with the transforms and keys (one PATCH, so a
  partition field and the rename it depends on change together). It stores what Destination stored:
  tables without fields left out, the JSONB key order, Debezium's time of the change dropped on Flink CDC
  (`cleanPartitionBy(partitionByFor(…))`). Unchanged, it returns the same options object, and the whole
  draft is put in the server's key order (`inServerOrder`), so opening and leaving the step saves nothing.
  Continue (and the step's save) waits on `partitionError`.
- The Destination step no longer edits it. It **passes the stored `partition_by` through unchanged**
  (`keepPartitionBy`), drops it when the destination is no longer Iceberg (a destination without options
  keeps none of another's), and the engine switch keeps stripping `__source_ts_ms` (`specForEngine`).
- Destination shows one line for Iceberg: "Partition by is chosen in Transform, after the key and renames."
  When the saved Partition by no longer fits (a write-mode or engine change made a field invalid), the line
  becomes a warning that names the table and says it is fixed in Transform. The step list can skip
  Transform, so Review shows the same warning with a button back to Transform. Neither blocks saving: the
  deploy's preflight (`iceberg_partition_column`, `iceberg_partition_key`) blocks the same cases, and a
  save that blocked would trap a pipeline (ADR 0005, *Editor*).
- The Tables step prunes `partition_by` and the recorded keys to the captured tables (`pruneToTables`): the
  server refuses a table the pipeline doesn't capture.

### P3. The recorded automatic key (Debezium): unchanged owners

`options.partitions` (each table's key for the automatic default) keeps the writers it has: the Destination
and Review steps re-record it from the chosen keys with `deriveSinkOptions`, and Review autosaves it before
any deploy. The Transform step doesn't save it; its editor shows the automatic default as it will be
recorded for the key on screen. `deriveSinkOptions` now returns the map in the server's JSONB order, so
an unchanged Destination or Review no longer looks dirty and saves again (a bug found here).

### P4. Every change appended, on Flink CDC: shown, unavailable, with the reason

- Flink CDC pipelines see the Rows choice *Every change appended* **disabled**, with the reason: "Flink
  CDC's Iceberg writer always writes rows by the table's key, so it can't keep every change as its own
  row." When the pipeline isn't deployed and Debezium can run this source to Iceberg, it adds: "Choose
  Debezium above for a change log." The reason names the writer's behaviour, not a version.
- `SchemaForm` gains `ui.unavailable` (enum value → reason), drawn as a disabled option whose second line
  is the reason. Only the frontend sets it (`optionsSchemaFor(schema, engine, { debeziumOffered })`, which
  keeps `append` in the enum on Flink CDC); descriptors don't.
- Switching a pipeline whose Rows is *Every change appended* to Flink CDC keeps the value and blocks
  Continue with "Choose how rows are written: Flink CDC can't append every change."
- The server keeps refusing `write_mode: append` on Flink CDC (422 `engine_settings`), as today.
- **When this can change:** a registered Flink CDC image whose Iceberg connector has FLINK-41057, plus
  BeziSync creating keyless tables with a type mapping proven equal to Flink CDC's (iceberg-go can create
  them; Flink CDC skips tables that exist). That is a later ADR.

### P5. Preflight fixes name the right step and engine

- `iceberg_rustfs_upsert`: on Debezium the fix stays "Choose *Every change appended* in the Destination
  step". On Flink CDC it becomes "Flink CDC can only upsert into Iceberg: use a catalog other than RustFS
  S3 Tables for this pipeline, or run it on Debezium with *Every change appended*."
- `iceberg_partition_column` and `iceberg_partition_key` fixes say "in the Transform step".

### Implementation plan

One PR, one logical commit per step, each passing `make verify-quick` on what it touches and pushed:

1. **Move and save from Transform (P1–P2).** Move `PartitionBy.tsx` and its tests to `transforms/`; add the
   Partition by row to `TransformStep`; save `sink.options` from Transform; pass through in `SinkStep`, with
   the Destination line and warning. Vitest for the summary and the save shapes.
2. **Unavailable options (P4).** `ui.unavailable` in `SchemaForm`, set by `optionsSchemaFor` on Flink CDC.
3. **RustFS fix per engine (P5).** Go test, catalog fix text, `docs/reference/checks.md`.
4. **e2e.** `08-sinks` and `32-flink-cdc` pick Partition by in Transform and assert the Flink CDC Rows
   choice is shown disabled. (Commits 1–3 leave these two specs pointing at the old control; the PR is
   squash-merged, and the e2e runs on its head.)
5. **Review findings.** Key order (`inServerOrder`, JSONB-ordered `partitions`), P3 as above, the Review
   warning, the engine-switch block, `pruneToTables`, the route check, the summary wording, the fix texts,
   and `walkWizard` clicking Continue through a settled pipeline with no save.
6. **Docs and contracts.** The guide, the contracts, ADR 0005's status line, the changelog fragment.

### Verification plan

- Vitest: the save shapes (`withPartitionBy`, `keepPartitionBy`, same object when unchanged), the summary,
  the route check, `inServerOrder`, the JSONB-ordered `partitions`, `pruneToTables`, the unavailable option.
- Go: `iceberg_rustfs_upsert` on each engine. `TestCompileMatrixUnchanged` stays green with no golden
  change (nothing compiles differently).
- e2e: `08-sinks` (Debezium Iceberg Partition by), `32-flink-cdc` (all Flink CDC journeys, with the Flink CDC
  image), and every `walkWizard` caller, which now also clicks Continue through the settled pipeline and
  expects no save; the Transform screen at 390/768/1280 px, light and dark.

**Edge-case register**

| ID | Case | Expected | Commit / test |
|---|---|---|---|
| P-1 | Saved Partition by, Transform opened and left with no change | Same spec; no PATCH | 1, 5 / vitest, e2e `walkWizard` |
| P-2 | Key chosen in Transform, then partitioned by it, one save | One PATCH with keys and `partition_by`; R5 passes | 1 / e2e 32 |
| P-3 | Computed column added and partitioned by in the same visit (Flink CDC) | Offered at once (refused as outside the key), saved together | 4 / e2e 32 |
| P-4 | Destination saved after Transform set Partition by | `partition_by` kept byte for byte | 1 / vitest |
| P-5 | Destination changed to a non-Iceberg sink (with or without options) | `partition_by` dropped | 1, 5 / vitest |
| P-6 | Destination switches the engine to Flink CDC with a `__source_ts_ms` field | Field dropped (`specForEngine`), unchanged | existing vitest |
| P-7 | Write mode changed in Destination so a field breaks R5 | Destination and Review warn and name the table; Transform blocks Continue; preflight blocks deploy | 1, 5 |
| P-8 | Debezium key changed in Transform, automatic table | Editor shows the new automatic key; Review records it | 5 / e2e `walkWizard` |
| P-9 | Destination or Review opened with nothing changed, several tables | No save (JSONB-ordered `partitions`) | 5 / vitest, e2e `walkWizard` |
| P-11 | Flink CDC pipeline, Rows | *Every change appended* disabled with the reason; Debezium named only when offered and not deployed | 2 / vitest, 4 / e2e 32 |
| P-12 | A stored Flink spec with `write_mode: append` (API) | Still 422 `engine_settings` | existing compiler test |
| P-13 | RustFS catalog, upsert, Flink CDC | Fix names another catalog or Debezium, never a Flink choice | 3 / go test |
| P-14 | 30 captured tables, 3 listed | "3 tables partitioned · 27 not partitioned" (Flink CDC) | 1, 5 / vitest |
| P-15 | Destination not Iceberg | No Partition by row in Transform | 1 / code |
| P-16 | Debezium pipeline with Rows *Every change appended* switched to Flink CDC | Value kept, Continue blocked with the reason | 5 |
| P-17 | A partitioned table unticked in Tables | Its `partition_by` and recorded key removed; the save is accepted | 5 / vitest |
| P-18 | Two tables routed together, partitioned differently | Editor refuses, as the server does | 5 / vitest |
| P-19 | Partition source column later renamed or left out in Transform | The field is named "no longer written"; Continue waits | existing vitest (`partitionError`) |

## Consequences

- Partition by is chosen where its inputs are; going back to Destination is no longer needed.
- No spec, compiler or `config_hash` change: existing pipelines don't restart. Draft saves never restart a
  running pipeline; only a deploy does.
- Flink CDC users see why there is no change log, instead of a missing option.
- `SchemaForm` can show an unavailable option with its reason (reusable by later options).
- **Not done:** Iceberg change logs on Flink CDC (P4's conditions); moving the bucket count or
  `partitioning: none` out of Destination (they are destination settings and stay in its form).

## Alternatives considered

- **Partition by in both steps.** Rejected: two writers of one option; a Destination save after a Transform
  edit would revert it.
- **Inside the key row** ("orders → id · bucket[16](id)"). Rejected: on Debezium, appended and keyless tables
  partition by non-key columns (and by the time of the change), which a key row can't express.
- **A fourth writer of the recorded automatic key in Transform** (the first plan: re-record only tables whose
  key changed). Dropped after review: Destination and Review already re-record every table from the chosen
  keys, and Review autosaves before any deploy, so a Transform writer added a save path and nothing else.
- **A Flink CDC change log with a per-change `UUID()` key and SOFT_DELETE.** Rejected: every insert becomes
  an equality delete plus a row, so reads slow down until compaction, and a replay after a failure writes
  new keys (duplicates).
- **BeziSync pre-creating keyless tables now.** Rejected for 3.7.0: the writer would still write deletes and
  update before-images as rows (FLINK-41057 is not in 3.7.0).
