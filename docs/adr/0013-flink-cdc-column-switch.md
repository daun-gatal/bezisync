# 0013 — Flink CDC switches columns at the saved position

- **Status:** Accepted
- **Date:** 2026-11-09
- **Amends:** 0012 (D2 is replaced: a column change on a pipeline with a saved position switches at that position
  instead of being refused; D6 now covers only positions from before a Start over)
- **Scope:** `backend/internal/flinkhelper` (`metadata.go`: removing the schema coordinator's state from a saved
  position; the column generation in `settings.json` and in the state table), `backend/internal/services`
  (`flinklayout.go`: deciding and planning a switch, the `flink_columns_changed` check; `flinkcdc.go`: Restore
  keeps the generation; `pipelines.go`: Deploy records the version the job ran), `backend/internal/compiler`
  (`FlinkInput.Generation`), `backend/internal/checks/catalog.json` and `docs/reference/checks.md`, the guide, the
  threat model, e2e `35-flink-cdc-columns`, the contracts of every touched directory.

## Context

ADR 0012 made BeziSync write the destination's columns in the destination's order, or not start. It left one gap
open on purpose: a deploy that changes the columns a pipeline writes, while the pipeline would resume a saved
position, was refused (`flink_columns_changed`), because Flink CDC 3.7.0 resumes with the columns it started with
and silently drops the change (0012 E5). The only fix was Start over: copy every table again, or read changes
from now and lose what the source wrote while the pipeline was stopped. Adding one computed field to a large
table cost a full copy.

Why a resume drops the change (read in the `release-3.7.0` sources):

| # | Fact | Where |
|---|---|---|
| F1 | The schema operator's coordinator (`SchemaRegistry`) holds every table's schema before and after the steps, and checkpoints it as **coordinator state**. On a restore it skips a `CreateTableEvent` for a table it knows (`SchemaUtils.isSchemaChangeEventRedundant`), so the new columns never reach the sink. | `schema/common/SchemaRegistry.java`, `schema/regular/SchemaCoordinator.java` |
| F2 | The transform operators keep no state: since FLINK-38045 the sources send each table's `CreateTableEvent` again after a restore, before its first row (Postgres, MySQL, Oracle and SQL Server emitters, `alreadySendCreateTableTables` kept in memory). | `transform/PreTransformOperator.java` (`initializeState` is empty), `*PipelineRecordEmitter.java` |
| F3 | Restored without coordinator state, the coordinator starts empty (`resetToCheckpoint(id, null)` → `new SchemaManager()`), learns each table from those `CreateTableEvent`s, sends them to the sink, whose metadata applier creates only missing tables. That is exactly a start without a saved position (0012 D1), but with every other operator's state restored. | `SchemaRegistry.resetToCheckpoint` |
| F4 | BeziSync compiles `operator.uid.prefix: bz-<short id>`, so the schema operator's UID is `bz-<short id>-schema-operator` and its operator ID is Guava's `murmur3_128` (seed 0) of that UID. `schema.operator.uid` can't be changed on its own (Flink CDC refuses it together with the prefix), and changing the prefix changes every operator's ID, the source's included, which loses the source's position. | `FlinkPipelineComposer`, `OperatorUidGenerator`, Flink `StreamGraphHasherV2` |
| F5 | Flink 2.2.1 writes checkpoint and savepoint `_metadata` in format 5 or 6. Each operator entry is: name and UID (Java UTF), operator ID (16 bytes), parallelism and max parallelism (int32), the coordinator state as a stream handle (`0` = none, `1` = bytes: Java UTF name, int32 length, data), then the subtask states. The coordinator state is always inline bytes. | `MetadataV5Serializer`, `MetadataV2V3SerializerBase.serializeStreamStateHandle` |

So the change is dropped by one piece of state, which Flink can do without: the source's exact position, the
sink's pending commits and every other operator's state can be restored as saved.

## Decision

**Invariant.** A deploy that changes the columns a Flink CDC pipeline writes, while the pipeline has a saved position
to resume, resumes that position with the new columns: no row the source wrote is lost, none is copied again, and
every value is written into its own column (0012's invariant, for this start too). Where BeziSync can't do that
safely, the job doesn't start and the reason is shown; the running job keeps running until the new one can start.

### D1 — A column generation

`artifacts.flink_layout.generation` counts the switches (absent = 0). The compiler puts it in `settings.json`
(`generation`, omitted when 0, so existing pipelines keep their `config_hash`); the helper records it with every
position it saves (a `generation` column in `debezium_state.flink_state_<id>`, default 0, added when the helper
opens the table), and Restore copies it with the position.

### D2 — Restoring a position from an older generation

When the helper restores a position whose generation is lower than its own, it removes the schema operator's
coordinator state from the restored copy's `_metadata` before it submits (F5): it finds the entry by the operator
ID (F4) and checks that the UID before it is this one (or empty), that the format version is 5 or 6, that the ID
appears once, that the handle is `0` or `1`, that the lengths stay in the file and that a plausible subtask count
follows; then it writes the handle as `0`. Anything else is an error: the job isn't submitted, and the helper
reports `failed`/`restore` with "saved before the columns changed, and BeziSync can't make it write the new ones".
The stored position isn't changed; only the copy extracted for this restore is. A position from the current
generation is restored as saved.

The same rule covers every way an older position comes back: the switch itself, a restart before the new job
saved anything, a job resubmitted after it used up Flink's restarts, and Positions → Restore of a position from
before the switch.

### D3 — When BeziSync switches

`artifacts.flink_written` is the version whose columns the newest saved position's job writes. Every planned start
records the deployed version there; Deploy records the version it replaces when nothing is recorded yet (pipelines
from before this ADR), so an upgrade alone changes nothing.

`EnsureFlinkStartPlanned` (the controller, once per deployed version and restore floor, before the compile): when a
saved position is left above the restore floor and `flink_written` is another version, it compares the columns
both versions write (`producedChanges` on `compiler.FlinkProduced`, the live source columns for both). Equal:
the job resumes as before. Different: BeziSync plans the destination tables as for a start without a saved
position (0012 D1: read every table, add what it lacks, read it back, pin positional tables to their order with
retired columns as typed NULL), raises the generation, keeps the layout floor, records `flink_written` and the
audit entry `pipeline.columns_switched` (the changes, the columns added, the destinations pinned, the generation
and the newest saved position's id). A problem is a 409 `flink_destination_columns`; the controller doesn't apply
the new version, so the running job keeps running the old one.

At a switch, a source column that is neither in the columns BeziSync recorded at the last planned start
(`flink_columns`) nor in the destination table is left out: the source may have added it after the saved position,
so the restored job hasn't read it, and writing it by name would fail the job (Flink CDC can't find the column).
It waits like any new source column of a pinned table.

### D4 — The deploy check says what will happen

`flink_columns_changed` (configuration section) is now a **pass** that names the changes and says the pipeline
switches columns at its saved position; a **warning** when BeziSync can't read the saved positions. The planning
checks of 0012 (`flink_destination_columns`) run for a switch as for a start without a saved position: a column
that can't be written in its place blocks, Paimon's `ALTER TABLE` statements block until someone runs them, Fluss
is a warning.

### D5 — The running job keeps writing while BeziSync adds columns

BeziSync adds the new columns while the old job still runs (the controller plans before it replaces the job).
Only optional columns at the end are added (Iceberg `AddColumn`, `ALTER TABLE … ADD COLUMN … NULL`), which writers
with the old schema keep writing past: Iceberg data files leave the new field out (read as NULL), and StarRocks,
Doris and OceanBase map by name.

### Implementation plan

One PR, one commit per step, each passing `make verify-quick` on what it touches:

1. `flinkhelper`: `ForgetCoordinatorState`, the generation in settings, the state table and `extract`.
2. `services`, `compiler`: `flink_written`, the switch in `EnsureFlinkStartPlanned`, `flink_columns_changed`,
   Restore keeps the generation; the catalog and the check reference.
3. The guide, the threat model, the changelog, the contracts; e2e `35-flink-cdc-columns`.

### Verification plan

Go tests per commit; `make verify-quick`; `go test -race ./...`; e2e `35-flink-cdc-columns` (Iceberg, Kafka,
Elasticsearch, Paimon, StarRocks, Doris) and `32-flink-cdc` against the real Flink CDC 3.7.0 image.

**Edge-case register**

| ID | Case | Expected | Test |
|---|---|---|---|
| S1 | Deploy a column change, a saved position left | check passes; generation +1, layout planned, `flink_written` updated, audited; the job resumes with the new columns | `TestFlinkColumnSwitchAtTheSavedPosition`; e2e 35 |
| S2 | Deploy that keeps the columns (a value changes) | resumes in the same generation | `TestFlinkColumnSwitchAtTheSavedPosition` |
| S3 | Take a column away again (a rollback) | another switch; the column is retired on positional tables | `TestFlinkColumnSwitchAtTheSavedPosition`; e2e 35 (Start over leg) |
| S4 | Helper restores a position from an older generation (or from before generations) | coordinator state removed; the source's state untouched; new positions saved with the current generation | `TestJobManagerSwitchesColumnsAtAnOlderPosition` |
| S5 | Helper restores a position from the current generation | restored as saved | `TestJobManagerSwitchesColumnsAtAnOlderPosition` |
| S6 | `_metadata` the helper doesn't know (another format version, a file handle, truncated, the ID twice, misplaced) | refused; nothing submitted; `failed`/`restore` | `TestForgetCoordinatorStateRefusesWhatItDoesntKnow`, `TestJobManagerSwitchesColumnsAtAnOlderPosition` |
| S7 | The operator ID is Guava's `murmur3_128` | matches Guava's value for a known input | `TestOperatorIDIsGuavaMurmur3`; e2e 35 (the real savepoint) |
| S8 | Positions → Restore of a position from before the switch | allowed above the layout floor; restored without the coordinator state like the switch | `TestJobManagerSwitchesColumnsAtAnOlderPosition`; e2e 35 (the switch restores the old job's savepoint) |
| S9 | Restore copies a position | the copy keeps its generation | `TestSavedPositionsListRestoreAndStartOver` |
| S10 | Rows the source writes while the job stops and switches | all written, each value in its own column | e2e 35 |
| S11 | A destination problem at a switch (key, NOT NULL, unmapped type) | blocked in preflight; at the start a 409 and the old job keeps running | services path shared with 0012 L8–L10 |
| S12 | Paimon lacks the new columns | blocked in preflight with the statements to run | e2e 35 |
| S13 | Kafka, Elasticsearch (no table) | generation +1 only; events carry the new fields | e2e 35 |
| S14 | Pipelines from before this ADR | compile byte-identical (no generation, no `flink_written` until the next deploy) | `TestCompileMatrixUnchanged` |
| S15 | Saved positions can't be read in preflight | `flink_columns_changed` warns | services path |
| S16 | A source column added after the saved position, `new_columns: send` | left out of the switch; waits | `TestFlinkColumnSwitchAtTheSavedPosition` |
| S17 | Start over after a switch | planned start (0012), generation kept | e2e 35 |

## Consequences

- A column change on a running pipeline costs a restart, not a Start over: the job resumes where it stopped.
- Rows written before the switch keep the columns they had (a new column is empty there, a retired one keeps its
  values); the guide says so, and a copy of the tables is still Start over.
- A positional table is pinned after a switch, as after 0012's planned start: new source columns wait for the
  next deploy there.
- The helper changes a Flink file format it doesn't own. It changes only a copy, checks every byte it relies on,
  and refuses anything else; a Flink version with another format makes a switch fail loudly (the job doesn't
  start), never write wrong columns. The Flink CDC image's Flink version is part of what BeziSync supports
  (3.7.0 on 2.2.1).
- Open, not caused by this ADR: on `new_columns: hold` (the default) and on tables written column by column,
  every deploy refreshes the source columns it writes (`RefreshFlinkColumns`); a column the source added while the
  pipeline was stopped, after its last saved position, is then named before the restored job has read it, and the
  job fails on it. A follow-up decides how to release held columns only once the job has read them.
- Open, not caused by this ADR, found while testing it: restoring an **older** position into Iceberg reuses the
  Iceberg writer's job id from its state, so the restored job's checkpoint ids overlap ones already committed and
  Flink CDC's committer skips them (`Checkpoint id N has been committed …, skipping`); and a Postgres job restored
  from a checkpoint taken while its first copy was finishing stopped reading new changes. The switch restores the
  newest position (the old job's stop savepoint), so it meets neither; Positions → Restore of an older position
  does, with or without a switch. A follow-up handles both.

## Alternatives considered

- **Start from the source's own position, without Flink state** (Postgres `committed-offset`, MySQL
  `specific-offset`, SQL Server and MySQL `timestamp`). Oracle has none; a timestamp is not an offset (a lagging
  job loses rows, a margin copies them twice); the exact offset is in the source's split state, which is a
  per-connector binary format; and a start without state loses the sink's pending commits.
- **A new schema operator UID** (`schema.operator.uid`). Flink CDC refuses it together with `operator.uid.prefix`
  (F4); dropping the prefix or changing it changes the source's operator ID and loses its position.
- **The State Processor API** (rewrite the savepoint in a Java job). A second Flink job per switch, a JVM in the
  helper's path and the same format dependency, for the same result.
- **Keep refusing (0012 D2).** Every column change costs a copy or the changes made while stopped.
- **Wait for Flink CDC to apply a changed `CreateTableEvent` on restore.** Not in 3.7.0; BeziSync can't wait for it.
