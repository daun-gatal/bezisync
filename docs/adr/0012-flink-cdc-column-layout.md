# 0012 — Flink CDC writes the destination's columns in the destination's order, or doesn't start

- **Status:** Accepted
- **Date:** 2026-11-08
- **Amends:** 0010 (destination types are now modelled, for Iceberg and Paimon only), 0006 (the restore floor gains a layout reason)
- **Scope:** `backend/internal/domain` (`flinklayout.go`: the layout planner and the Flink type names),
  `backend/internal/connectors` (`flinkcolumns.go`: reading and adding destination columns per Flink CDC
  destination; Iceberg, Paimon, StarRocks, Doris, OceanBase, Elasticsearch, Fluss, Kafka; the StarRocks
  soft-delete field), `backend/internal/compiler` (`flinktransforms.go`: the projection from the stored layout),
  `backend/internal/services` (`flinklayout.go`: planning at start points, the `flink_columns_changed` and
  `flink_destination_columns` checks, Restore across a layout), `backend/internal/controller` (planning in
  `prepareIfNeeded`), `backend/internal/flinkhelper` (no unplanned fresh start), `backend/internal/mcpserver`
  (`update_pipeline` merges), `backend/internal/checks/catalog.json` and `docs/reference/checks.md`, the guide,
  e2e `35-flink-cdc-columns` and its fixtures, the contracts of every touched directory.

## Context

A Flink CDC pipeline on the playground (`shop-postgres-cdc`, Postgres → Iceberg) showed `__op` holding the text
of another column (`"shop-postgres-cdc"`) on every new row, while Health said Healthy. A transform step had added
`test_field` before `__op`; a later Start over sent rows `id, order_id, sku, qty, test_field, __op` into a table
that still had `id, order_id, sku, qty, __op`. Nothing failed; existing columns filled with wrong values.

What Flink CDC 3.7.0 does (read in the `release-3.7.0` sources, then probed on the playground with a table whose
values name their own column):

| # | Case | Result |
|---|---|---|
| E1 | Source `ADD COLUMN` while running (`*` projection) | Appended at the **end** of the destination, after `__op` (`AddColumnEvent` LAST). Correct. |
| E2 | Source `RENAME COLUMN` while running | Lenient mode keeps the old column (NULL from then on) and appends the new name. Correct. |
| E3 | Source type widened while running | The same column widened. Correct. |
| E4 | Source `DROP COLUMN` while running | The column stays, NULL from then on. Correct. |
| E5 | A step adds a column, then deploy (resume from a savepoint) | The column is **silently missing**: `SchemaUtils.isSchemaChangeEventRedundant` treats any `CreateTableEvent` for a known table as applied. |
| E5b | A step's column removed, then deploy (resume) | The column stays, NULL. |
| E6 | A start **without** a saved position into an **existing** table whose columns differ | Iceberg and Paimon write **by position**: values shift. `IcebergMetadataApplier.applyCreateTable` is `if (!tableExists) create`; `IcebergWriter` builds rows from the job's schema and writes them with the table's row type; Paimon's writer builds a `GenericRow` the same way. |
| E7 | After E1–E4, a start without a saved position | `*` expands in the source's **current** order (`id, bb, n, f1, __op`) while the table is `id, a, f1, __op, b, n, bb`: every such start after a live source change mis-aligns a positional table, even with no step changed. |
| E8 | StarRocks with "keep deleted rows" | StarRocks refuses a column named `__op` ("a system reserved name": its stream load reads `__op` as the upsert/delete marker, and the 3.7.0 writer puts its own `__op` key, 0 or 1, into every row's JSON): the table is never created and the job fails. Keeping deleted rows has never worked on StarRocks. |

So a **resume never shifts values but ignores column changes**, and a **start without a saved position shifts
values on positional destinations**. Every Start over is such a start, and so are hidden ones the helper makes
on its own: a crash before the first saved position, state too large to save, a store error.

Destination writers (3.7.0): Iceberg and Paimon map **by position**; StarRocks, Doris, OceanBase, Elasticsearch
and Kafka map **by name** from the job's own schema (a column the table lacks is dropped or refused, never
shifted); Fluss maps by name against the real table and silently drops a column the table lacks. BeziSync can read
the columns of Iceberg (its catalog), Paimon (the warehouse's schema files), StarRocks, Doris and OceanBase (SQL),
Elasticsearch (`_mapping`), not Fluss.

A Goldfish review (seven lenses, `.egm/brainstorm_synthesis.md`) rejected the first design (a ledger edited by
mirroring lenient rules, and "column changes start fresh from where they stopped") for three reasons kept here:
a ledger drifts from the table; a deploy cannot make the next start fresh, because the old job's stop savepoint is
written after the deploy, above any floor it sets, and is then resumed; and no source has a proven lossless fresh
start (Start over drops the Postgres slot; a timestamp is the helper's clock, not the source's offset).

## Decision

**Invariant.** A Flink CDC job that starts without a saved position writes, into every destination table that
already exists, exactly that table's columns: on a positional destination in the table's order, on a by-name
destination none the table lacks. Otherwise it does not start, and BeziSync says why and what to do.

### D1 — The destination is the source of truth, read only at start points

`artifacts.flink_layout` holds, per destination table, the columns the job writes in order
(`name`, Flink `type` when known, `retired`), the planning time and the restore floor it was planned at. It is
written **only** where BeziSync plans a start without a saved position: the first deploy, a deploy after Start
over, and Start over itself (the controller plans again when the pipeline next starts: Start over clears
`prepared_version_id`). The compiler reads only the artifact: no network, so `config_hash` doesn't move when a
table gains a column live. A pipeline without the artifact compiles byte-identically to before.

The planner (`domain.PlanFlinkLayout`) takes the columns the spec produces (source columns under their written
names, step columns, the soft-delete column) and the destination's columns:

- **table missing:** nothing to plan; Flink CDC creates it.
- **positional destination:** the table's order. A column the spec still produces → its expression; a column it no
  longer produces → `CAST(NULL AS <type>) AS name` (retired), with the type mapped from the table's type (D4);
  a produced column the table lacks → appended at the end, after BeziSync adds it to the table (D3).
- **by-name destination:** the spec's own order (`*` stays); a produced column the table lacks → added (D3).
- When the planned order equals what the spec compiles to anyway, nothing is stored: the projection keeps `*` and
  live source changes keep flowing (E1–E4). A stored layout pins the order, so new source columns wait for the
  next deploy (the Hold behaviour), and the Flink settings say so.

Refused (blocking `flink_destination_columns`, never forced): a retired column that is a key, a partition source or
`NOT NULL` in the table; a table type BeziSync can't map (D4); a positional table it can't read; a type the table holds that differs from the produced one (other than the widenings Iceberg and Paimon allow).

Fluss: BeziSync can't read its tables (no client), so a planned start into Fluss is a warning (`flink_destination_columns`,
never a pass): Fluss maps columns by name and silently drops a column an existing table lacks.

### D2 — A column change on a pipeline with a saved position is refused

`flink_columns_changed` (blocking, configuration, can't be forced) fires when a deploy would change the columns a
running or paused pipeline writes (the produced names per destination, the soft-delete column included) and its
next start resumes a saved position: resuming would drop the change silently (E5). The fix is Start over (copy
again, changes from now, or from a time where the source allows it), then deploy: that start is planned (D1).
Rolling back to a version with other columns is the same change. A lossless switch (resume the old position with
the new columns) is future work: it needs an exact source position or a rewritten savepoint, and is out of scope.

### D3 — Adding a column to an existing table

Iceberg: `UpdateSchema().AddColumn` (optional, at the end) through the catalog. StarRocks, Doris, OceanBase:
`ALTER TABLE … ADD COLUMN … NULL` over SQL, waiting for an asynchronous schema change to finish. Elasticsearch:
nothing (dynamic mapping; `dynamic: strict` or `false` is refused). Kafka: nothing. Paimon: refused with the
statement to run (Paimon has no writer API in BeziSync). Every add is read back before the start; the audit entry
lists the statements run. The new column's type: an added field and the soft-delete column are `STRING`; a
computed column is wrapped in a `CAST` to the type its expression returns (D4), so the job and the table agree;
a source column takes its mapped type.

### D4 — Types

`domain` names the Flink types BeziSync writes or retires (`VARCHAR`, `BOOLEAN`, `INT`, `BIGINT`, `DOUBLE`,
`FLOAT`, `DECIMAL(p,s)`, `DATE`, `TIMESTAMP(p)`, `TIMESTAMP_LTZ(p)`, `BYTES`), and maps Iceberg and Paimon table
types to them. Anything else (nested, `TIME`, `UUID`, fixed binary) is refused, never guessed. `STRING` is written
`VARCHAR` (ADR 0008: `CAST … AS STRING` doesn't compile on 3.7.0).

### D5 — No hidden fresh start

`settings.json` gains `fresh_start: true` only when the compile planned a start without a saved position (D1). The
helper starts without a saved position only when that is set **and** nothing was ever saved above the restore floor;
otherwise it stops with `no_position` ("Start over, or restore a saved position") instead of starting fresh into
tables it can't see. The field is omitted when false, so existing pipelines keep their `config_hash`.

### D6 — Restore across a layout is refused

A saved position taken before the current layout was planned (its id is at or below `flink_layout.floor`) can't be
restored (`position_before_layout`): its job wrote other columns.

### D7 — StarRocks keeps deleted rows under `__bz_op`

On StarRocks the soft-delete column is `__bz_op` (E8). No working StarRocks pipeline has an `__op` column (the table
was never created), so nothing migrates.

### Also fixed

- `update_pipeline` (MCP) merges the spec it is sent onto the current one, as its description says; it replaced it.
- Fix texts that sent people to Start over for a column mismatch, or to a "dropping destinations" option Start over
  doesn't have, now say what is safe.

### Implementation plan

One PR, one commit per step, each passing `make verify-quick` on the packages it touches:

1. MCP `update_pipeline` merges the spec.
2. `domain`: `PlanFlinkLayout`, Flink type names and the Iceberg/Paimon type maps; table tests for every register row.
3. `connectors`: `FlinkColumns` (read, add, positional) for every Flink CDC destination; live tests where a fixture exists.
4. `compiler`: the projection from `FlinkInput.Layout`; the byte-identical matrix with no layout.
5. `services` + `controller`: planning at start points, `flink_destination_columns`, `flink_columns_changed`,
   `position_before_layout`, the audit entry; catalog and check reference.
6. `flinkhelper`: `fresh_start` and `no_position`.
7. StarRocks soft delete under `__bz_op`.
8. Fix texts, the guide, the frontend copy (Flink settings Hold, Positions Restore), contracts.
9. e2e `35-flink-cdc-columns` with StarRocks, Doris and Fluss fixtures.

### Verification plan

Go tests per commit; `make verify`; e2e `32-flink-cdc`, `34-flink-sinks`, `35-flink-cdc-columns`.

**Edge-case register**

| ID | Case | Expected | Test |
|---|---|---|---|
| L1 | First deploy, table missing | `*`, nothing stored, hash unchanged | compiler matrix; e2e 35 |
| L2 | No layout artifact (every existing pipeline) | byte-identical compile | `TestCompileMatrixUnchanged` |
| L3 | Start over into an Iceberg table after live ADD/RENAME/DROP (E7) | projection in the table's order, retired typed NULL, values read back under their names | planner test; e2e 35 |
| L4 | Start over after a step added a column (the playground case) | the column is added to the table at the end, written by name, `__op` correct | e2e 35 (Iceberg, StarRocks, Doris) |
| L5 | Deploy a column change, pipeline has a saved position | blocked `flink_columns_changed`, can't be forced | services test; e2e 35 |
| L6 | Roll back to a version with fewer columns | blocked the same way | services test |
| L7 | Start over, then deploy the column change | allowed; planned | services test; e2e 35 |
| L8 | Retired column is a key / partition source / NOT NULL | blocked `flink_destination_columns` | planner test |
| L9 | Positional table unreadable (catalog error) | blocked, never a pass | services test |
| L10 | Iceberg/Paimon type BeziSync can't map | blocked, names the column and type | planner test |
| L11 | Fluss at a planned start (BeziSync can't read its tables) | warning: an existing table drops the columns it lacks; never a pass | services test |
| L12 | Routed many-to-one destination | one layout per destination; each member writes it, NULL for what it lacks | planner + compiler test |
| L13 | Live source ADD after a pinned layout | held (not written) until the next deploy; Flink settings say so | e2e 35 |
| L14 | Helper: no position, `fresh_start` unset | `no_position`, doesn't start | flinkhelper test |
| L15 | Helper: `fresh_start`, but something was saved above the floor | `no_position` | flinkhelper test |
| L16 | Restore a position at or below the layout floor | 409 `position_before_layout` | services test |
| L17 | StarRocks soft delete | column `__bz_op` with `+I/-D`; rows kept | compiler test; e2e 35 |
| L19 | MCP `update_pipeline` with one field | the rest of the spec kept; `null` removes | `TestMCPToolsFollowRolesTokensAndSettings` |
| L20 | Kafka / Elasticsearch | nothing to add; order irrelevant; Elasticsearch `strict` refused | connectors test |
| L21 | Computed column added to an existing table | wrapped in `CAST` to its type; the table column has it | planner + compiler test |
| L22 | Column names differing only in case on a case-insensitive destination | matched | planner test |

## Consequences

- No Flink CDC start writes a value into another column, and a column change can't be dropped silently: it is
  refused until the start that can apply it.
- A column change costs a Start over (a copy, or changes from now). This is what applying it cost already; it is
  now stated instead of half-applied.
- After a planned start into a table whose order differs, the pipeline holds new source columns back until the
  next deploy.
- BeziSync reads and alters destination tables at start points, with the destination's credentials and through the
  SSRF guard (ADR 0004).
- The type maps tie BeziSync to Flink CDC 3.7.0's cast rules; the capability is per destination, so a by-name
  writer upstream makes it one change.

## Alternatives considered

- **A ledger mirroring Flink CDC's lenient rules.** Drifts: polling sees a rename as a drop and an add, misses the
  order of adds between polls, and can't see a table changed by hand.
- **Read the destination inside the compile.** The hash would move whenever a column is added live, restarting
  pipelines unannounced.
- **Explicit projections everywhere.** A restart of every Flink CDC pipeline, and "send new columns" lost.
- **A column-change deploy that starts fresh from where it stopped.** The stop savepoint races the floor; Postgres
  Start over drops the slot; timestamps are wall-clock. Future work, with exact positions.
- **Rewrite the savepoint's stored schema** (turn every column change into a resume). Promising, tied to an
  internal state format; future spike.
- **Reorder the destination to follow the job** (Iceberg and Paimon can move columns as metadata). Changes what
  `SELECT *` readers see; kept as an option for the lossless switch.
- **`existing-table.schema-expansion.mode`.** Paimon and Fluss only, matches by name, adds at the end; Iceberg is
  refused. Not relied on until proven on 3.7.0.
