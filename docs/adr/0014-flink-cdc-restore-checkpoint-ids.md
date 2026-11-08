# 0014 — A restored Flink CDC job numbers its checkpoints past every one the pipeline reached

- **Status:** Accepted
- **Date:** 2026-11-09
- **Amends:** 0013 (closes its first open item for Iceberg)
- **Scope:** `backend/internal/flinkhelper` (`metadata.go`: reading and rewriting a restored copy's checkpoint
  id; `jobmanager.go`: renumbering a restore of an older position, storing a savepoint with its checkpoint id;
  `store.go`: `MaxFlinkID`), the guide, the threat model, e2e `35-flink-cdc-columns`, the helper's contract.

## Context

Testing ADR 0013 found that Positions → Restore of an **older** position into Iceberg drops rows. The restored
job's Iceberg committer logged `Checkpoint id 3 has been committed to table …, skipping` for every commit until its
checkpoint ids passed the ones the newer job had committed.

Read in the `release-3.7.0` and Flink 2.2.1 sources:

| # | Fact | Where |
|---|---|---|
| F1 | Flink numbers a restored job's checkpoints from the restored checkpoint's id (`CheckpointIDCounter` set to it + 1; sinks get it as `getRestoredCheckpointId`). | Flink `CheckpointCoordinator` |
| F2 | Flink CDC's Iceberg writer starts its own counter at the restored checkpoint id and takes its job id and operator id from its state. | `IcebergSink.restoreWriter`, `IcebergWriter.prepareCommit` |
| F3 | The committer reads the highest checkpoint id committed for that job id and operator id from the table's snapshot summaries and skips any commit at or below it: what it holds is dropped, not retried. | `IcebergCommitter.commit`, `getMaxCommittedCheckpointId` |
| F4 | A checkpoint's or savepoint's `_metadata` (format 5 or 6) holds its checkpoint id as the int64 right after the magic number and the version. | `MetadataV2V3SerializerBase.serializeMetadata` |

So a restore of an older position replays changes the newer job had already committed (harmless: the table has
them), then commits new changes under ids the committer has seen, and drops them.

## Decision

**When the helper restores a position whose checkpoint id is below the highest checkpoint id the pipeline stored,
it rewrites the restored copy's checkpoint id to that highest id + 1000** before it submits. The restored job, its
Iceberg writer included, then numbers every commit above anything committed. The stored position isn't changed.

- **Highest id.** `MaxFlinkID` reads the largest `flink_id` among every stored position, set aside or not (a
  restore sets the newer ones aside; they still say how far the job got). A savepoint is now stored with its own
  checkpoint id, read from its `_metadata` (before, `0`).
- **The gap of 1000.** A job can complete checkpoints between two polls, or stop without a savepoint, and commit
  them unseen; checkpoint ids are 64-bit, so a gap costs nothing.
- **Only older positions.** The newest position (the usual restart after a stop) is restored as saved, so sinks
  that probe transactions by checkpoint id after a crash (Kafka exactly-once, Doris two-phase commit) keep their
  numbering.
- **A format the helper doesn't know.** The restore goes on unchanged when the stored id shows the position is the
  newest; otherwise it fails (`failed`/`restore`) instead of dropping rows.

The rewrite applies to every destination: it only raises numbers.

### Verification plan

Go tests; `make verify-quick`; `go test -race ./...`; e2e `35-flink-cdc-columns`, whose journeys restore an older
position of the switched job and check that a row written after the restore arrives.

**Edge-case register**

| ID | Case | Expected | Test |
|---|---|---|---|
| R1 | Restore a position older than the newest stored checkpoint | checkpoint id rewritten to the highest + 1000; rows written after the restore committed | `TestJobManagerRenumbersAnOlderPosition`; e2e 35 |
| R2 | Restore the newest position | restored as saved | `TestJobManagerRenumbersAnOlderPosition` |
| R3 | An older position whose `_metadata` the helper can't read | refused, nothing submitted | `TestJobManagerRenumbersAnOlderPosition` |
| R4 | Only the 8 bytes of the id change; another format is refused | as stated | `TestCheckpointIDReadAndRewrite` |
| R5 | A stop savepoint | stored with its own checkpoint id | `TestJobManagerRunsStoresAndStopsWithSavepoint` |
| R6 | The highest id counts positions set aside by a restore | as stated | `TestStoreRoundTrip` (`MaxFlinkID`) |

## Consequences

- Restoring an older position into Iceberg no longer drops what the restored job writes; it re-applies
  the changes since that position (upserts on keyed tables, duplicates on tables without a key, as any
  at-least-once replay).
- Checkpoint ids jump by about 1000 after such a restore. Nothing reads them as a count.
- Still open (0013): a Postgres job restored from a checkpoint taken while its first copy was finishing stopped
  reading new changes.

## Alternatives considered

- **Remove the Iceberg writer's state** so it draws a new job id. Subtask state has many handle formats; the
  committer's pending commits would go with it.
- **Refuse restoring an older position into Iceberg.** Takes away a recovery tool to hide a numbering problem.
- **Renumber every restore.** Breaks sinks that look for their own lingering transactions by checkpoint id after a
  crash.
