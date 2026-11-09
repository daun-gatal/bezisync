# 0015 — Flink CDC holds back source columns a restored job hasn't read yet

- **Status:** Accepted
- **Date:** 2026-11-09
- **Amends:** 0013 (closes its open item on `new_columns: hold`)
- **Scope:** `backend/internal/flinkhelper` (`column_not_read`, `Status.UnreadColumns`), `backend/internal/services`
  (`HoldUnreadColumns`, `RefreshFlinkColumns`), `backend/internal/controller` (holding them back), the guide, the
  contracts.

## Context

A Flink CDC pipeline that writes columns by name (`new_columns: hold`, the default, or a table that renames,
leaves out or keeps deleted rows) names its source columns in its steps. Every deploy refreshes that list from
the source (`RefreshFlinkColumns`). A column the source added while the pipeline was stopped, after its last
saved position, is then named before the restored job has read the change that added it: Flink CDC learns a
table's columns from its saved state and from the changes it reads. Reproduced on the e2e cluster (hold mode,
`ALTER TABLE … ADD COLUMN n` while paused, any deploy, resume):

`TransformException: Failed to post-transform … CalciteContextException: … Column 'n' not found in any table`

The job failed, reported as `transform_failed` ("A transform can't run on Flink CDC. Change it in the Transform
step"), which is wrong: nothing in the steps is broken, and no change of them would help. BeziSync can't tell in
advance which columns the restored job has read: the source's catalog has no history, and the job's knowledge is
inside its saved state.

## Decision

React to the job's own answer:

- The helper recognises Calcite's `Column '…' not found in any table` (before `transform_failed`), cancels the
  job, reports `column_not_read` and the column names (`unread_columns`).
- The controller then removes those names from the columns the pipeline writes (`flink_columns`,
  `flink_defaults`), records them in `flink_held_unread` and audits `pipeline.columns_held`. The next compile
  leaves them out, the config hash moves, and the job starts again from the same saved position, holding them like
  any new column in hold mode.
- The next deploy's refresh writes them again: by then the job has read the change. That deploy is a column
  change for the job, which never wrote them, although both versions name them (the comparison of 0013 reads the
  live columns for both), so its planned start counts the held columns as changes and switches columns at the
  saved position (0013); `flink_held_unread` is cleared when the start is planned. Without the switch Flink CDC
  would skip the columns as already applied (found on the e2e cluster: the released column never arrived). If
  the job still hasn't read the change (no row of that table changed since), the same happens once more.

A switch (0013) already leaves such columns out on positional tables it pins; this covers every table written by
name.

**Edge-case register**

| ID | Case | Expected | Test |
|---|---|---|---|
| U1 | The job fails on a column it hasn't read | `column_not_read` with the column names; cancelled | `TestJobManagerStopsOnFailuresThatDontHeal` |
| U2 | The controller sees it | the columns held, `pipeline.columns_held`, the job started again without them | `TestFlinkHoldsColumnsTheJobHasntRead` |
| U3 | The next deploy | preflight names them; the columns written again by a column switch (generation + 1), `flink_held_unread` cleared | `TestFlinkHoldsColumnsTheJobHasntRead` |
| U4 | On the real engine: hold mode, a column added while paused, a deploy, resume | streams; the column held, then written after a deploy | e2e 35 |

## Consequences

- A column added while a pipeline was stopped reaches the destination one deploy later than the others, instead
  of failing the job.
- The job fails once before BeziSync holds the column; the status says why in the meantime.

## Alternatives considered

- **Refresh only columns the job has read.** BeziSync can't see that (it is in the saved state).
- **Never release held columns on a resume.** New columns would wait for a Start over.
