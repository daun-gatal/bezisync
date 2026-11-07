# 0005 — Iceberg partitioning that cannot lose deletes, the same on both engines

- **Status:** Accepted; its *Editor* section (where the editor sits and who saves it) is superseded by [ADR 0007](0007-partition-by-in-transform.md)
- **Date:** 2026-11-07
- **Scope:** `backend/internal/connectors` (`iceberg_partition.go`, `iceberg.go`, `iceberg_flink.go`,
  `descriptors.json`), `backend/internal/domain` (a partition transform table), `backend/internal/compiler`
  (compile matrix rows), `backend/internal/checks/catalog.json` and `docs/reference/checks.md`,
  `frontend/src/features/pipelines` (`wizard/PartitionBy.tsx`, `wizard/SinkStep.tsx`, `spec.ts`,
  Review), e2e `08-sinks` and `32-flink-cdc`, the guide (`docs/README.md`, Apache Iceberg), the
  connectors and wizard contracts, `.rules/backend.md`.

## Context

BeziSync writes change-data-capture rows into Apache Iceberg tables with two engines: the Debezium
Server Iceberg build (`memiiso/debezium-server-iceberg` **1.2.1.Final**) and Flink CDC **3.7.0**'s
Iceberg sink (Iceberg **1.10**). Partitioning decides how a table's files are split; it is chosen
when the table is created and kept afterwards.

The two engines diverged:

- **Debezium** partitions automatically and offers no choice: an upserted table by
  `bucket(<recorded key column>, 4|16|64)`, a change log (*every change appended*) or a keyless table
  by `day(__source_ts_ms)`, from the hidden `options.partitions` map the wizard records. Expert
  `partitioning: none` turns it off. It refuses `partition_by`.
- **Flink CDC** lets a user give each table up to four fields (*Partition by*: identity, year, month,
  day, hour, `bucket[N]`, `truncate[N]`) and has no default: tables are unpartitioned.

Reading the pinned sources and the Iceberg spec (copies kept with the design notes) found:

1. **Equality deletes stay inside one partition.** Iceberg applies an equality delete only to
   older data files with the *same partition spec id and partition values* (spec, *Scan
   Planning*; an equality delete with an unpartitioned spec is global). Both engines upsert by
   writing equality deletes on the table's **identifier fields**: Flink CDC's writer passes
   `table.schema().identifierFieldIds()` with `upsert = true`, and the Debezium build does the same
   (with no identifier fields it appends). Iceberg's Flink documentation states the consequence as
   a requirement: in upsert mode, the source columns of partition fields must be among the equality
   fields. Neither engine enforces it. If an upserted table is partitioned by a column outside its
   identifier fields (`day(updated_at)`, `status`), an update that changes that column writes the new
   row and its delete into the new partition. The old row is never deleted, so readers see both.
   **Flink CDC's Partition by allows exactly this today**, and the e2e journey partitions an upserted
   table by `day(created_at)`.
2. **Identifier fields are fixed when the table is created.** Both engines set them only on
   creation (Flink CDC's `applyCreateTable` is guarded by `!catalog.tableExists`, and the Debezium
   build loads an existing table as is). So what protects an *existing* table is its live
   identifier fields, not the spec's key columns.
3. **Changing an existing table's partitioning breaks upserts as well.** Iceberg can evolve a
   partition spec as metadata only (`updateSpec`, which iceberg-go v0.7.0 has), but the delete scope
   rule includes the spec id: deletes written under the new spec never reach rows written under the
   old one.
4. **The Iceberg type rules are stricter than BeziSync's.** `bucket` takes int, long, decimal,
   date, time, timestamps, string, uuid, fixed and binary, so **not boolean, float or double**.
   `truncate` takes int, long, decimal, string and binary. `year`, `month` and `day` take dates and
   timestamps, and `hour` timestamps only. The editor offers bucket on booleans and floats, and the
   server never checks bucket's type. The checks also read the *source* type, but the engine writes
   the type after its own mapping and, on Debezium, after BeziSync's value formats. For example,
   decimals sent as `double` become floats, decimals from CockroachDB, Vitess and Spanner arrive as
   text, binary sent as hex/base64 becomes text, `time` columns become longs, and json/uuid become
   strings.
5. **The Debezium build supports every transform, per table.** `debezium.sink.iceberg.partition-by.<destination>`
   (`IcebergConfig.partitionByForTable`) takes a column, `year|month|day|hour(c)`, `bucket(c, N)` and
   `truncate(c, N)` (column first; the comma escaped `\,` because the property is a list). The
   per-table form is **strict**: a missing column or an unknown transform throws when the table is
   first created, which stops the pipeline. (The build's docs say an invalid transform gives an
   unpartitioned table; the code throws.) `__source_ts_ms` is written as `timestamptz`.
6. **Routed Debezium tables are unpartitioned today.** The automatic default is keyed by the naming
   template's topic (`CompileContext.Destination`), but the build looks the setting up by the event's
   destination after the route's RegexRouter. For a table a Route step sends elsewhere, the key never
   matches.
7. **A catalog error reads as "the table doesn't exist".** Preflight's partition checks treat any
   `LoadTable` error (auth, network) as a table to be created, so the existing-table check passes
   silently.

The hard constraint: **every existing spec compiles to the same `config_hash`**. The controller
replaces a running pipeline whose hash changed, so a hash change restarts real pipelines on upgrade.

## Decision

One *Partition by*, on both engines, governed by one rule set that preflight enforces and the
editor mirrors.

### Terms

- **Equality fields**: the columns an engine's upsert deletes by. For an existing table, its live
  Iceberg identifier fields. For a table the pipeline will create:
  - **Flink CDC**: the table's key columns (the ones chosen in Transform, else the primary key),
    under their written names after renames.
  - **Debezium**: in upsert modes, the key columns (chosen, keeping only the ones the table has,
    else the primary key; Debezium's own fallback) **that the written row still carries under
    their own names**. The build makes a column an identifier field only when the event's key and
    value share its name (`StructSchemaConverter`), and a rename changes the value only
    (`ReplaceField$Value`), so a renamed or left-out key column drops out. In *every change
    appended*, or for a keyless table, there are none, even though the build gives the table
    identifier fields: it never deletes.
- **Upserted table**: one whose equality fields are not empty. A table with none is appended:
  nothing is deleted, so any partitioning is safe.
- **Written columns and kinds**: the columns the destination table receives (source columns minus
  the left-out ones, renamed, plus added and computed columns and the engine's own fields), each
  with the Iceberg type class the engine writes (`domain.PartitionKind`).

### Rules

- **R1. Partition by on both engines.** `sink.options.partition_by` (table → up to four fields)
  compiles:
  - **Flink CDC**: into `partition.key`, as today (`bucket[N](c)`).
  - **Debezium**: into `debezium.sink.iceberg.partition-by.<destination>`, with fields joined by
    `,`: identity `c`, `year(c)`, `month(c)`, `day(c)`, `hour(c)`, `bucket(c\,N)` and
    `truncate(c\,N)`.

  The destination key is the event's destination after routes (`domain.RouteTargets`, else the
  naming template). Tables a route sends to one destination must list the same fields; a conflict
  is refused at compile (422 `engine_settings`), as Flink CDC already does.
- **R2. Precedence on Debezium.** A listed table uses its fields. Otherwise, with `partitioning: none`,
  the table is not partitioned; otherwise the automatic default applies. `buckets` and `partitions`
  only feed the automatic default. On Flink CDC an unlisted table is unpartitioned (no automatic
  default).
  - **Routes (fixes a bug found here).** The automatic default used to be keyed by the table's
    topic *before* its route, but the build looks it up by the event's destination after the
    route, so a routed table was always created unpartitioned. It is now keyed after the route
    (`debeziumDestinationFields`), like Partition by. Tables routed to one destination share its
    partitioning: a listed table's fields if one lists them (the others take them), else the
    automatic default when the tables agree, and none when they don't (refusing would stop those
    pipelines compiling). Unrouted tables compile byte for byte as before; **routed Debezium Iceberg
    pipelines change `config_hash` and restart once on upgrade**, and only tables they create
    afterwards are partitioned (an existing table keeps its spec).
- **R3. Debezium-only column.** On Debezium, `__source_ts_ms` (the source's commit time, UTC,
  `timestamptz`) is a written column, offered as *Time of the change*. By R5 it is only valid on
  appended tables, since it changes with every update.
- **R4. Types.** One table, `domain.PartitionTransforms`, maps each transform to the written kinds it
  accepts. It follows Iceberg's spec, restricted to what BeziSync's kinds can express:

  | Transform | Written kinds |
  |---|---|
  | identity | int, decimal, float, bool, text, uuid, date, time, timestamp, timestamptz, binary |
  | bucket[N] (1–1024) | int, decimal, text, uuid, date, time, timestamp, timestamptz, binary |
  | truncate[W] (1–1,000,000) | int, decimal, text, binary |
  | year, month, day | date, timestamp, timestamptz |
  | hour | timestamp, timestamptz |

  `domain.PartitionKind(engine, sourceType, dbType, values)` gives the written kind:
  - It starts from `domain.ColumnKind`.
  - **On Debezium:**
    - decimals in mode `double` → float and in mode `string` → text;
    - decimals from CockroachDB, Vitess and Spanner → text;
    - binary in modes `hex`/`base64` → text;
    - time → int;
    - json → text, uuid → Iceberg uuid (bucket, not truncate);
    - PostgreSQL arrays, user-defined types and NUMERIC → none (discovery doesn't say whether a
      NUMERIC has a scale, and one without is written as a structure);
    - Spanner `timestamp` → text (it arrives as ISO text without a logical name BeziSync can
      prove).
  - **On Flink CDC:** json and uuid → text; a MySQL TINYINT → bool (TINYINT(1) is a boolean there,
    and discovery doesn't show the width).
  - **Any engine:** document and list have no kind, so they can't be partitioned.
  - Iceberg refuses two time transforms (year, month, day, hour) of one column, so preflight and
    the editor do too.

  The frontend mirrors both functions, and shared vectors (`backend/testdata/partition_kinds.json`,
  read by the Go and the TypeScript tests) keep the two copies equal.
- **R5. Equality fields (the safety rule, new blocking check `iceberg_partition_key`).**
  - **On an upserted table, every partition source column must be an equality field.** This holds
    for listed fields and for the Debezium automatic default (a recorded key that no longer matches
    the table's key, or `day(__source_ts_ms)` on a table that has gained a key, fails it), checked
    on what each table's destination gets after routes (R2).
  - **For a destination table that already exists**, preflight reads its live partition spec and
    identifier fields. Every listed or automatic partition must also hold on the live identifiers.
    A live table whose own spec breaks the rule fails with "may already hold duplicate rows" and the
    fix *Start over, dropping destinations* (or, outside BeziSync, a data engineer fixing the
    identifier fields and removing duplicates, with the query to find them). This holds whether or
    not Partition by lists the table: the writer's deletes are wrong either way.
  - The fix never says "add the column to the key": the key would then carry the same promise
    (that the column never changes) without anything enforcing it, and deletes whose before-image
    carries only the primary key would miss the row.
- **R6. Existing tables (unchanged rule, both engines).** A listed table that exists partitioned
  differently is blocked (`iceberg_partition_existing`), with a fix that gives the table's own fields
  as Partition by would write them. The Debezium automatic default never blocks on it (the table
  keeps its partitioning, as today). BeziSync never evolves a table's spec.
- **R7. Fail closed on the catalog.** Only `catalog.ErrNoSuchTable` means "will be created". Any
  other error, including a catalog that doesn't open, reports `iceberg_partition_existing` and
  `iceberg_partition_key` as Warn, "couldn't read <table>". Without discovery, the type and key
  checks are not marked passed: they report Warn, "couldn't check: the tables couldn't be read".
  A table that exists partitioned as asked passes `iceberg_partition_existing` explicitly.
- **R8. MongoDB.** A MongoDB pipeline can't list tables in Partition by (blocked in preflight; the
  compile doesn't know the source type): a collection has no fixed columns, and the build takes the
  schema from the first event. Its automatic default is unchanged.
- **R8b. Key columns on Debezium (`iceberg_key_columns`).** A key column renamed or left out of the
  row drops out of the table's identity (see Terms). With some key columns left, upserts would
  replace every row that shares the rest of the key, so it is blocked; with none left the table is
  appended, which is a warning. This is not a partitioning rule, but R5 depends on it and the bug
  was found here.
- **R9. Codes.**
  - `iceberg_partition_column` (blocking) reports a missing column, a kind its transform doesn't
    take, or redundant time transforms, per field.
  - `iceberg_partition_key` (blocking; Warn when unverifiable) and `iceberg_partition_existing`
    (blocking; Warn when unverifiable) are reported alongside it.
  - The existing Debezium `iceberg_partitioning` warning (stale recorded keys) skips listed tables.
  - All three codes are now engine-neutral; their catalog *whys* say so.

### Outcome matrix

| ID | Engine | Rows | Table | Partition by | Outcome |
|---|---|---|---|---|---|
| E1 | Debezium | upsert, key | new | unlisted, automatic | `bucket(key\,N)`, hash unchanged; passes R5 |
| E2 | Debezium | upsert, key | new | unlisted, recorded `""` (stale) | `day(__source_ts_ms)` compiled as today; R5 fails |
| E3 | Debezium | append | new | unlisted | `day(__source_ts_ms)`; no key rule |
| E4 | Debezium | upsert, key | new | `bucket[8](id)` | `partition-by.<dest>=bucket(id\,8)`; passes |
| E5 | Debezium | upsert, key `id` | new | `day(created_at)` | R5 fails, blocking |
| E6 | Debezium | append | new | `day(__source_ts_ms)`, `identity(status)` | `day(__source_ts_ms),status`; passes |
| E7 | Debezium | upsert, keyless | new | `day(created_at)` | appended; passes |
| E8 | Debezium | any | new | listed + `partitioning: none` | listed fields compile; others unpartitioned |
| E9 | Debezium | upsert | exists, other spec | listed | R6 blocks, fix shows the table's fields |
| E10 | Debezium | upsert | exists, spec off its identifiers | unlisted | R5 live check blocks: may hold duplicates |
| E11 | either | any | exists, catalog unreachable | listed | R7: Warn, never Pass |
| E12 | Flink CDC | upsert | new | `bucket[4](id)` | `partition.key` unchanged; passes |
| E13 | Flink CDC | upsert | new | `day(created_at)` (non-key) | R5 fails, blocking (behaviour change) |
| E14 | Flink CDC | upsert | exists, `day(created_at)` | same | R6 passes, R5 live check blocks: Start over |
| E15 | either | any | new | bucket on bool/float | R4 fails, blocking |
| E16 | Debezium | any | new | two tables routed together, different fields | 422 at compile |
| E17 | Debezium | any | new | routed table, unlisted | automatic default keyed after the route (restart on upgrade); R5 applies; merged tables that disagree: unpartitioned |
| E18 | Debezium | MongoDB | any | listed | blocked in preflight |
| E19 | Flink CDC | upsert | new | `day(__source_ts_ms)` | R4/R1: no such column, blocking |
| E20 | Debezium | upsert, key renamed | new | `bucket[8](order_id)` (renamed key) | no identity left: appended (R5 passes), R8b warns |
| E21 | either | any | new | discovery failed | column/key checks Warn "couldn't check" |
| E22 | either | upsert, key `(tenant_id, id)` | new | `identity(tenant_id)`, `bucket[16](id)` | passes |

### Editor (wizard, Destination step)

The editor sits in the Destination step on both engines. It is the same component, with the engine
passed in:

- **Columns offered** are the written columns, computed from the whole spec as it stands (renames,
  added and computed columns from Transform, left-out ones removed). On Debezium, *Time of the
  change* (`__source_ts_ms`) is included.
- **Upserted tables.** Each column the rule refuses is shown **disabled, with the reason** (for
  example, "Not part of the table's key: an update that changes it would leave the old row behind").
  The reason states the rule; it never suggests adding the column to the key.
- **Transforms** are filtered by the written kind (R4), so the `PartitionKind` mirror is used, not
  the raw source kind.
- **A Debezium table with no fields** shows what it gets: *Automatic: buckets of id (16)*,
  *Automatic: day of the change*, or *Not partitioned*.
- **Stale fields.** Key columns and renames are chosen later, in Transform. When the Destination
  step is opened again, a field whose column is no longer written (or no longer a key column) is
  shown as such and Continue waits for it; it is never dropped silently. Deploying from Review
  runs preflight, whose `iceberg_partition_column` and `iceberg_partition_key` name the table and
  the field. (A Review badge was weighed and dropped: the live-table half of R5 needs the catalog,
  and a badge that blocks saving would trap a pipeline whose table must be recreated.)
- **Switching a draft to Flink CDC** removes `__source_ts_ms` fields, which don't exist there.
- **truncate's width** may go up to 1,000,000, so `truncate[100000](id)` gives insert-ordered
  ranges of an increasing key safely. The guide shows it as the time-like option for upserted
  tables.

### Implementation plan

One PR, one logical commit per step, each passing `make verify-quick` on its packages and pushed:

1. **Pin today's Iceberg compile output.** Add Iceberg-only matrix shapes and their golden rows,
   against the current code: automatic with a recorded key, a recorded `""`, append,
   `partitioning: none`, a route merging two tables with automatic defaults (Debezium), and
   `partition_by` with a route (Flink CDC).
2. **Kinds and types (R4, R7 for discovery).** Add `domain.PartitionTransforms` and
   `domain.PartitionKind` with the shared vectors. Make `outputColumns` engine-aware (writtenColumns:
   Debezium's `__source_ts_ms`, `__op`, `__deleted`; the soft-delete field per engine; chained
   renames). Use the written kind in `iceberg_partition_column`, and fix bucket/truncate. Raise
   truncate's descriptor limit.
3. **The equality-field rule (R5, R7, R9).** Add `iceberg_partition_key` for new and live tables,
   covering both listed fields and the Debezium automatic default. Read live identifier fields.
   Treat only `ErrNoSuchTable` as absent. Update the catalog entries and `docs/reference/checks.md`.
4. **Partition by on Debezium (R1–R3, R8).** Compile listed tables, route-aware, with conflict
   refusal and the precedence rules (the MongoDB block and `iceberg_key_columns` land with step 3). Update the descriptor help. Add matrix rows
   for the new shapes; no existing row changes.
4b. **Routed automatic defaults (R2, Routes).** Key every Debezium destination after routes
   (`debeziumDestinationFields`), shared by the compile, the key rule and the editor's summary;
   accept the two `ice-route` matrix rows on purpose.
5. **The editor on both engines.** Written columns, kinds, the key rule's disabled reasons, the
   automatic summary, stale fields named, the engine-switch strip, the truncate cap. Vitest.
6. **e2e.** In `08-sinks`, add a Debezium journey: Partition by through the wizard, deploy, the
   table created with that spec (asserted by a re-check through `iceberg_partition_existing`), the
   non-key columns offered disabled, and a later `day(created_at)` blocked against the existing
   table. In `32-flink-cdc`, the wizard journey partitions by key
   columns and asserts the key rule on `day(created_at)`; `walkWizard` runs on both engines.
7. **Docs and contracts.** The guide, the connectors and wizard contracts, `.rules/backend.md` and
   the changelog fragment (with the behaviour change, the duplicate-finding query, the routed
   restart and the rollback note).

### Verification plan

- Go: table tests for `PartitionKind` (every row of R4's kind list) and `PartitionTransforms`
  (vectors shared with TS), the Debezium compile of each transform (escaping included), precedence
  (E1–E8), routes (E16, E17), MongoDB (E18), and the key rule for each equality-field case (E5, E7,
  E10, E13, E14, E20, E22) with a fake catalog returning live specs and identifier fields, not-found
  and other errors (E11).
- `TestCompileMatrixUnchanged`: rows from commit 1 never change; new rows only.
- Vitest: the editor (disabled reasons, kinds, automatic summary), `partitionError` with the
  final spec, the engine-switch strip, the shared vectors.
- e2e: `08-sinks` (Debezium Iceberg journeys), `32-flink-cdc` (all Iceberg journeys, with the Flink
  image), `walkWizard` on both engines.
- Full `make verify` and the affected e2e specs before the PR; the screens at 390/768/1280 px in
  light and dark.

**Edge-case register**: E1–E22 above, plus:

| ID | Case | Expected | Commit / test |
|---|---|---|---|
| X1 | Existing Debezium spec without `partition_by`, every unrouted shape | same `config_hash` (routed: changes, announced) | 1, 4 / matrix |
| X2 | Existing Flink spec with `partition_by` | same `partition.key`, same hash | 1 / matrix |
| X3 | 63-character table name, column names in upper case (Oracle) | compiled verbatim, case kept | 4 / compile test |
| X4 | Two time transforms of one column (`day(ts)`, `hour(ts)`) | refused, as Iceberg refuses them | 3 / unit, 5 / vitest |
| X5 | Same field twice | refused at save (unchanged) | 2 / unit |
| X6 | bucket N 0 or 1025, truncate W 0 | refused at save | 2 / unit |
| X7 | A partition column later renamed in Transform | the editor names the field; preflight fails `iceberg_partition_column` | 5 / vitest, 2 / unit |
| X8 | A key column chosen in Transform after partitioning by `id` | key rule re-evaluated in the editor and preflight | 3, 5 |
| X12 | Debezium change log whose table has identifier fields | appended: no key rule, no live risk | 3 / unit |
| X13 | Debezium key column renamed or left out (some / all) | `iceberg_key_columns` blocks / warns | 3 / unit |
| X14 | Catalog doesn't open | partition checks warn, never pass | 3 / unit |
| X9 | Live table with no identifier fields | appended: R5 skipped, existing warning kept | 3 / unit |
| X10 | Engine switch Debezium → Flink with `__source_ts_ms` | field removed with a notice; server refuses if sent | 4, 5 |
| X11 | Viewer opens the wizard | editor read-only (unchanged) | 5 / vitest |

## Consequences

- **Newly blocked deploys (no running pipeline restarts; no `config_hash` changes):**
  - Flink CDC pipelines whose upserted table is partitioned outside its key (E13, E14).
  - Debezium pipelines whose recorded automatic key is stale on a table still to be created (E2):
    reopening the Destination step records the key again.
  - Debezium pipelines whose key columns are partly renamed or left out (R8b).
  - Any pipeline writing into an existing upserted table partitioned off its identifier fields
    (E10), including tables created outside BeziSync.
- **A Flink CDC pipeline whose upserted table is partitioned outside its key is blocked on its next
  deploy.** Its running workload is not stopped, and its table may already hold duplicate rows. The
  changelog says how to find them (`SELECT <key>, count(*) … GROUP BY <key> HAVING count(*) > 1`)
  and that the remedy is Start over, dropping destinations.
- **Debezium users can choose partitioning.** Strict mode means a field the table doesn't have
  stops the pipeline when the table is created, so preflight is the only guard against it. A health
  rule that names the failing field is a follow-up.
- **Rollback.** An older BeziSync refuses a Debezium spec with `partition_by`, and its controller
  skips a pipeline that doesn't compile. Remove Partition by from Debezium pipelines before rolling
  back.
- **Routed Debezium Iceberg pipelines restart once** on upgrade (their `config_hash` changes,
  the matrix rows `ice-route` say so); the tables they create afterwards are partitioned as the
  automatic default says, and existing ones keep what they have.
- **Not done:**
  - evolving partitions (unsafe for upserts; possible later for change logs only);
  - an automatic default on Flink CDC (it would change running pipelines' hashes);
  - a running-pipeline health finding for unsafe live tables;
  - sort orders, `write.distribution-mode`, `void`;
  - v1 tables;
  - Flink CDC's transform-level `partition-keys`.
- **Start over** drops every destination table of the pipeline, not only the one at risk.
- **Not covered:** a Custom transform step that changes the event's destination disconnects
  Partition by's key from the table (the build looks it up after every step).
- **Format v3:** the rule is the same on v2 and v3 (the build's `upsert-use-dv` path was not
  pinned). Deletion vectors are position deletes scoped to
  one data file in the same partition, and both writers still delete upserts by equality.

## Alternatives considered

- **Warn instead of block on a non-key partition.** Rejected: the result is silent duplicate rows.
- **Advise adding the column to the key, or offer a "never changes" switch.** Rejected: neither
  is enforced by the writer. If the column changes, the composite key no longer matches, and a
  delete whose before-image carries only the primary key misses the row.
- **Evolve an existing table's spec on deploy.** Rejected: deletes written under the new spec never
  reach rows under the old one. It would also change a table the user owns as a side effect of a
  deploy.
- **An automatic bucket-by-key default on Flink CDC, or written into new specs.** Rejected for now:
  the implicit form changes running hashes. The written form creates two generations of specs and
  needs a hook for tables added later.
- **The global `partition-by` on Debezium.** Rejected: it skips a missing column with only a log
  warning, and it can't differ per table.
- **Key and type rules at save time.** Rejected: they need discovery (keys, column types), which
  saving doesn't have. They live in preflight; the editor and Review mirror them.
- **Serving the rule table from the API.** Rejected for this change: a mirrored table with shared
  vectors follows the `ComputedFunctions` pattern and adds no endpoint.
- **Keeping routed automatic defaults as they compiled (unpartitioned), as a known issue.**
  Rejected by the maintainer: the bug is fixed here, and the restart is announced in the changelog.
