# 0010 — Column types in the Destination table, and Flink CDC keys from any written column

- **Status:** Accepted
- **Date:** 2026-11-08
- **Scope:** `backend/internal/domain` (`keys.go`: key resolution and fitness; `findings.go`: engine-aware key
  findings), `backend/internal/expr` (`never.go`), `backend/internal/compiler/flinktransforms.go` (`primary-keys`,
  NOT NULL key columns), `backend/internal/connectors/iceberg_partition.go` (equality fields),
  `backend/internal/controller` and `backend/internal/services` (the engine passed to findings),
  `backend/internal/api` (`expressions:check`: `nullable` in, `key_problem` out) and `openapi.json`,
  `backend/internal/checks/catalog.json` and `docs/reference/checks.md`, `backend/testdata/key_columns.json`;
  `frontend/src/features/pipelines/transforms` (`FlinkPreviewPanel`, `KeysEditor`, `model.ts`, `PartitionBy.tsx`,
  `TransformStep`), `frontend/src/lib/transforms.ts`; e2e `27-transforms` and `32-flink-cdc`; the guide; the
  contracts of every touched directory.

## Context

The maintainer asked for two things in the wizard's Transform step:

1. The **Destination table** panel (Flink CDC) lists each column a table gets and where it comes from, but not
   its type. It should show the type from the source.
2. The **Primary key** picker offers only the source table's own columns. It should offer every column the
   table gets: source columns, and the columns computed or added by steps.

How keys work today. `spec.keys` maps a table to column names. On Debezium it becomes
`message.key.columns`: the source connector builds the message key **before any step runs**, from source
columns only. On Flink CDC it becomes the transform rule's `primary-keys`; the compiler wrote each name through
the table's renames. Nothing stopped a name the table doesn't write: the job failed at runtime. The left-out
check compared the *renamed* name with the left-out list (source names), so it missed the case it was for, and
Debezium's finding ("falls back to the primary key") also ran on Flink CDC pipelines, where it is false.

What Flink CDC 3.7.0 does with a key (read in the pinned sources):

- `PreTransformOperator.transformSchemaMetaData` sets the schema's primary key to the rule's `primary-keys` as
  given; `PostTransformOperator` keeps it on the projected schema. So a key may name any projected column,
  a computed one included.
- `TransformParser` types a computed projection column with `CalciteDataTypeConverter`, which drops
  nullability: **every computed or literal column is nullable**, except an outer `IFNULL(…)`, which keeps
  Calcite's nullability (a special case in the parser). A plain or renamed column keeps the source's type.
- The Iceberg sink builds the table with `new Schema(columns, identifierFieldIds)` from the key, and Iceberg
  refuses an identifier field that is optional.

**Real-engine probe.** Each key shape below ran on the e2e cluster (Flink CDC 3.7.0 image, PostgreSQL with
`REPLICA IDENTITY FULL` → Iceberg REST catalog on RustFS), one pipeline each, through BeziSync's API with
this change's compile: copy two rows, then insert one and update one, and read the table back.

| Key | Generated | Result |
|---|---|---|
| `id, tenant` (added field `'acme'`) | `IFNULL('acme', '') AS tenant` | Streams; one row per key after the update |
| `id, code_up` (*Upper case* of a NOT NULL column) | `IFNULL((CASE … UPPER(code) END), '')` | Streams; upserted (`+U` replaces the row) |
| `id, yr` (*Year* of a NOT NULL date) | `IFNULL(…, 0)` | Streams; `yr` 2026/2027 (the whole-number fallback compiles in Janino) |
| `id, ck` (expression `code \|\| '-'`) | `IFNULL(…, '')` | Streams |
| `note` (a **nullable source column**, no step) | `primary-keys: note` | Streams, but the row whose `note` is NULL is written with `note = ''`: every such row would share one key |

The last row is behaviour BeziSync already allowed: a key column that can be empty silently merges rows. It
is now a preflight warning (K4). A computed key without `IFNULL` was not run on the final code: K3 relies on
the parser rule above (Flink CDC types such a column nullable), and `IFNULL` costs nothing.

## Decision

### K1. Types in the Destination table

Each column shows a type: **the source's type** for a source column (as discovered: `bigint`, `text`,
`timestamp with time zone`, …) and, for a column a step adds, **the kind it writes** in words (text, whole
number, date, …): `COMPUTED_FUNCTIONS[fn].out`, or the kind of its first column for functions whose kind
follows their input; an expression's kind comes from the server's `expressions:check` (the request the
expression's editor makes). An added field is text. The From column also says when a column is cut short.
The destination's own type (Iceberg's, StarRocks', …) is decided by Flink CDC and the sink; BeziSync doesn't
model it and doesn't guess it.

### K2. What a key name means

`spec.keys` keeps its shape. On Flink CDC a name resolves, in order (`domain.FlinkKey`, mirrored by
`flinkKey` in `model.ts`):

1. a **source column the table writes** (not left out): written under its renamed name;
2. else the **new name a rename gives** a source column the table writes: that column (the compile before
   this change wrote such a name as it is, so a spec stored this way through the API keeps its meaning);
3. else a **column a step adds** to the table: an added field that applies to it, or one of its computed
   columns, under its own name;
4. else an error: *left out* (a source column the pipeline leaves out) or *not written*.

With the columns unknown (not read yet), every name is taken as it is, through the renames: exactly the
compile before this change. **Every spec that compiled before compiles to the same `primary-keys`**; a
source column wins over a step column of the same name, so no stored key changes meaning.

On Debezium nothing changes: the key is the source's columns, and the editor shows added fields disabled with
the reason ("Debezium builds the message key at the source, before any step runs"). Rebuilding the key after
the steps (`ValueToKey`) was rejected (below).

### K3. A computed or added key column is written NOT NULL

Because Flink CDC types every computed and literal column as nullable (Context), a computed or added column in
the key is compiled as `IFNULL(<its SQL>, <fallback>)` (`domain.KeyColumnSQL`): `''` for text, `0` for a whole
number. The fallback is never written, because K4 allows only values that can't be empty; it exists so the
column's type is NOT NULL. The compile reads the kind without the table's columns (`expr.Check` with an empty
environment), so a key column whose kind depends on its inputs (a rounded number, the first of some columns)
is refused there too (`transform_expression`). Columns not in the key compile as before.

### K4. Which columns can key a table (Flink CDC)

A computed key column (`domain.ComputedKeyProblem`, the one rule preflight and the expression check apply)
must be:

- **text or a whole number, as the compile reads it** (`domain.KeyKinds`, kind read without the table's
  columns): K3 needs that kind to write the column NOT NULL. A function whose kind follows its inputs (a
  rounded number, the first of some columns, an `IFNULL` of a column) is refused with the fix: `CAST(… AS
  STRING)`. Floating-point and decimal keys compare unreliably anyway;
- **never empty without a stand-in**: every column it reads is NOT NULL in the source or in its primary key,
  and no form in it can give an empty value (`expr.NeverNull`: a division, `TRY_CAST` — *As a whole number*,
  *As a decimal/floating-point number* —, `TO_DATE` — *Date from text* —, `NULLIF`, a `CASE` without
  `ELSE`). An `IFNULL`/`COALESCE` fallback for a column that can be empty, or *Join with a separator* over
  one, doesn't make it fit: every such row would get the same key and replace the others;
- **deterministic**: `NOW()` or `UUID()` would make every change a new row.

A source column in the key that can be empty (NULL in the source, not in its primary key) is a warning,
`transform_key_column_nullable` (the probe: those rows share one key); it stays allowed, as before, so no
deployed pipeline is newly blocked. A source column in the key can't be **cut** (rows sharing their first characters would replace each
other). Added fields are literals. On a table with a primary key, **a key with a column a step adds must keep
the whole primary key**: a constant, a month or a code alone would give many rows one key. (Keys of source
columns only stay the user's choice, as before.)

Preflight blocks a key column the table doesn't write (`transform_key_column_unknown`) and one that can't key
rows (`transform_key_column_unfit`); both run where the transform findings run (preflight and the schema-drift
alert, which now carries each column's nullability). The wizard mirrors the rule: a computed column that
can't be a key is offered disabled with the reason, a chosen one that can't stays ticked so it can be removed,
and Continue waits. For typed functions the mirror is checked against the server by
`backend/testdata/key_columns.json` (each function's compile-time kind, and whether it is never empty with all,
none or only the first of its column inputs NOT NULL); for an expression the wizard asks `expressions:check`,
which now takes each column's `nullable` and answers `key_problem` from `domain.ComputedKeyProblem` itself.
While that answer is pending, or if the check failed, Continue waits. Nullability that isn't known (an older
recorded schema) counts as nullable: the checks fail closed.

### K5. Iceberg partitioning follows the key

`equalityFields` (Go and TypeScript) resolves a Flink CDC key with K2, so a computed key column (a month, a
region code) can partition an upserted Iceberg table (ADR 0005 R5), and a key column the table doesn't write is
left out there (it fails its own check).

### Implementation plan (as built)

One PR, commits in order:

1. Domain: `keys.go` (`FlinkKey`, `StepColumns`, `FlinkRenames`), engine-aware `TransformFindings`
   (`transform_key_column_unknown`), the compiler's `primary-keys` through `FlinkKey` (the left-out fix), Iceberg
   `equalityFields`; catalog entries and the check reference.
2. `expr.NeverNull`, `FlinkKeyProblems` (`transform_key_column_unfit`), `KeyColumnSQL` in the compile,
   `key_columns.json`; `expressions:check` `nullable`/`key_problem`, OpenAPI and the typed client.
3. Frontend: types in the Destination table, the key editor, `flinkKey`/`keyChoices`/`flinkKeyProblem`, the
   Partition by mirror, Continue waiting on the key.
4. e2e journeys, the guide, contracts, this ADR and the changelog fragment.

### Verification plan

- Go: `TestFlinkKey`, `TestFlinkKeyFindings`, `TestKeyColumnVectors` (domain), `TestNeverNull` (expr),
  `TestCompileFlinkKeysFromSteps` (compiler), `TestIcebergPartitionKey` rows (connectors),
  `TestExpressionCheckEndpoint` (api), `TestCompileMatrixUnchanged`.
- Frontend: `model.test.ts`, `keys.test.ts` (shared vectors), `PartitionBy.test.ts`.
- e2e: `32-flink-cdc` *the destination table shows types, and computed and added columns key the table*
  (wizard, real Iceberg rows upserted by the composite key, API refusals) with `walkWizard`; `27-transforms`
  (Debezium: an added field is disabled in Message key).

**Edge-case register**

| ID | Case | Expected | Test |
|---|---|---|---|
| K-1 | Existing specs (source-column keys, renamed or not) | Same `primary-keys`, same `config_hash` | `TestCompileFlinkKeysFromSteps`, matrix |
| K-2 | Key = renamed source column, stored by source name | Written under the new name; picker shows the new name "from" the old | compiler, `model.test.ts` |
| K-3 | Key = the new name of a renamed column (stored through the API) | That source column; same `primary-keys` as before | `TestFlinkKey`, compiler, `model.test.ts` |
| K-4 | Key = added field | `IFNULL('v', '') AS name`, in `primary-keys` | compiler, e2e |
| K-5 | Key = computed text/int from NOT NULL inputs | `IFNULL(…)`, in `primary-keys`; Iceberg rows upserted | compiler, e2e |
| K-6 | Computed key reading a nullable column | `transform_key_column_unfit`; disabled in the wizard | `TestFlinkKeyFindings`, `keys.test.ts`, e2e |
| K-7 | Computed key that can be empty on values (to_integer, parse_date, divide) | Unfit | vectors, `keys.test.ts` |
| K-8 | Computed key of kind float, decimal, date | Unfit; compile `transform_expression` | `keys.test.ts`, domain |
| K-9 | Expression key over a nullable column, with or without an IFNULL/COALESCE fallback | Unfit (rows would share the fallback) | `TestFlinkKeyProblems`, api |
| K-10 | Expression key whose kind follows its columns (`IFNULL(col, 'x')`, `first_set`) | Unfit with the CAST fix, in preflight, the wizard and the compile alike | `TestFlinkKeyProblems`, `keys.test.ts` |
| K-10b | Expression key `code || '-' || CAST(id AS STRING)` over NOT NULL columns | Fit; compiles with `IFNULL` | `TestFlinkKeyProblems`, api |
| K-11 | Non-deterministic expression key (`NOW()`) | Unfit | domain |
| K-12 | Key = left-out source column | `column_unknown` ("left out") at compile; wizard error | compiler, `model.test.ts` |
| K-13 | Left-out source column with a computed column of its name | The computed column | `TestFlinkKey`, `model.test.ts` |
| K-14 | Key column whose step was removed or renamed | Listed "not written"; Continue blocked; `transform_key_column_unknown`; compile 422 | e2e, compiler, `model.test.ts` |
| K-15 | Columns not read yet | Names as they are, through renames | `TestFlinkKey`, `model.test.ts` |
| K-16 | Added field limited to other tables | Not offered; refused for this table | `TestFlinkKey`, `model.test.ts` |
| K-17 | Cut key column | Unfit | `keys.test.ts`, domain |
| K-18 | Debezium: added field | Shown disabled with the reason; key and findings unchanged | e2e 27, `TestFlinkKeyFindings` |
| K-19 | Debezium finding on a Flink CDC pipeline | Not shown | `TestFlinkKeyFindings` |
| K-20 | Iceberg Partition by a computed key column | Allowed (Go and TS) | connectors, `PartitionBy.test.ts` |
| K-21 | Key order | Kept as picked | compiler |
| K-22 | Autosave round-trip | No save loop | `walkWizard` |
| K-23 | Types: source column, function, expression, added | Source type; kind words; server kind; text | `model.test.ts`, e2e |
| K-24 | A step column in the key without the whole primary key (a constant, a month alone) | Unfit | `TestFlinkKeyProblems`, `keys.test.ts`, e2e |
| K-25 | *Join with a separator* over a column that can be empty | Unfit | `TestFlinkKeyProblems`, `keys.test.ts` |
| K-26 | Nullability not recorded (older schema artifact) | Counts as nullable (fails closed) | controller |
| K-27 | Expression check pending or failed | Continue waits; the choice says so | `keys.test.ts` |
| K-28 | Empty expression column | Unfit, never "checking" forever | `keys.test.ts` |
| K-30 | Nullable source column in a Flink CDC key | Warning `transform_key_column_nullable`; a note in the picker | `TestFlinkKeyWarnings` |
| K-31 | The key editor and the typed table at 390/768/1280 px, light and dark | No overflow (the editor stacks each table's name above its key), no serious axe finding | e2e 32 |
| K-29 | Every key column unresolved, Iceberg Partition by | Still keyed (names as they are); the key check blocks | connectors, `PartitionBy.test.ts` |

## Consequences

- A table can be keyed by its primary key plus columns BeziSync computes or adds: a tenant tag, so tables
  routed together get a key that tells their rows apart, or a month, so an upserted Iceberg table can be
  partitioned by it (ADR 0005 R5).
- The fallback in K3 assumes the preflight rule holds. If a source column a computed key reads later becomes
  nullable, the schema-drift alert reports it (the key finding runs there), but rows written after that with an
  empty input would carry the fallback, still alongside the primary key, so no rows merge. Fix: pick other key
  columns, or add `NOT NULL` in the source.
- A key whose column an update can change (any non-primary-key column, computed or not) moves the row: the old
  one stays. The editor says so for computed columns; this was already true for source columns.
- `expressions:check` takes an optional `nullable` per column and answers `key_problem`; older clients are
  unaffected.
- A deployed Flink CDC pipeline whose key column is cut, or keyed by a computed column that no longer fits,
  now shows it in the schema-drift alert and is blocked at its next deploy.
- A spec keyed by an added field through the API before this change (the editor never offered it) now compiles
  the field as `IFNULL('v', '')`: its `config_hash` changes once. On Iceberg such a pipeline could not have
  created its table (Context).

## Alternatives considered

- **Store written (renamed) names in `spec.keys` on Flink CDC.** Rejected: every stored spec with a renamed key
  column would change meaning, and the same name would mean different things on the two engines.
- **Rebuild the Debezium key after the steps (`ValueToKey`).** Rejected: it changes the key's schema for every
  consumer, needs the row shape first, and fails on tombstones (no value to copy from). A different feature.
- **Render key columns without NULL guards** so Calcite types them NOT NULL. Rejected: Flink CDC's converter
  drops nullability anyway (only `IFNULL` keeps it), and an unguarded function crash-loops the job on a NULL
  (ADR 0008).
- **Allow any computed column and let `IFNULL` fill empty values.** Rejected: rows whose value is empty would
  share one key and overwrite each other, silently.
- **Show the destination's own column types.** Rejected for now: BeziSync doesn't model each sink's type
  mapping, and a guessed type would be wrong in places.

## Amendments

- **2026-11-08 (after merge).** `transform_key_column_nullable` warns only when the key doesn't also hold the
  whole primary key (`domain.FlinkKeyWarnings`, `keepsPrimaryKey`): with the primary key in the key, rows can't
  share a key whatever a nullable column holds. A computed date or timestamp key column is still refused (no
  date function is never empty on 3.7.0: `CAST … AS DATE` doesn't compile, `TO_DATE` can give NULL); the
  refusal now says how to partition by a date instead: the timestamp column itself in the key, partitioned by
  its day (ADR 0005). The key editor shows an expression column's kind from the server's check.
