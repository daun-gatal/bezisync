# 0003 — Reclaiming jobs a crashed controller left running

- **Status:** Accepted
- **Date:** 2026-11-07
- **Scope:** `backend/internal/controller/jobs.go` (`RunJobs`, `runJob`, `reclaimJobs`, `ReclaimGrace`), the controller and API contracts, Known issue 5. No migration.

## Context

Postgres is BeziSync's job queue (`jobs`: `state`, `attempts`, `max_attempts`, `run_after`,
`locked_by`, `locked_at`). The leader controller claims a due job in one transaction (`queued` →
`running`, `attempts + 1`, `locked_at` from the controller's clock), commits, then runs it in a
second transaction (`runJob`): the handler runs inside a savepoint and the job's bookkeeping
(`done`, `failed`, or back to `queued` with backoff) is written at the end.

If the controller dies between those two commits (a crash, an OOM kill, a node drain) the row
stays `running` forever. Nothing reclaimed it (Known issue 5). That blocks project deletion (which
waits for cleanup jobs) and the runtime's pending-jobs check, and the work is never done.

| Kind | What it does | Safe to run again? |
|---|---|---|
| `restart` | Restart a pipeline's workload | Yes: the end state is the same |
| `cleanup` | Remove a deleted pipeline's slot, artifacts and workload | Yes: each step tolerates "already gone" |
| `notify` | Send an alert to its channels | Mostly: a re-run may send a notification twice |
| `snapshot` | Send Debezium an execute-snapshot signal | No: a second signal copies the tables again |

A timed lease (refreshing `locked_at` while the handler runs) doesn't work: the refresh would be
written inside `runJob`'s transaction, invisible to other sessions until the job ends, so a slow
job would look dead and run twice.

## Decision

**The Postgres row lock is the lease.**

1. `runJob` reads its job with `SELECT … FOR UPDATE`. The lock lasts as long as the handler, and
   Postgres releases it when the transaction ends or the connection is gone.
2. Each `RunJobs` pass first runs `reclaimJobs`: `SELECT … WHERE state = 'running' AND locked_at
   < $1 FOR UPDATE SKIP LOCKED`, where `$1` is the controller's own clock minus `ReclaimGrace`
   (1 minute). `locked_at` was written by a controller clock too, so database clock skew doesn't
   matter. A running row that can be locked has no live owner. The grace covers the instant
   between the claim's commit and `runJob`'s lock.
3. A reclaimed job's lost run is its attempt:
   - `snapshot` → `failed`: "Interrupted: the controller stopped while sending this snapshot.
     Check the pipeline, then request it again." It is never re-sent on its own.
   - other kinds → `queued` now, or `failed` when `attempts >= max_attempts`, with an
     "Interrupted" error.
   - An audit event `job.reclaimed` (actor `controller`, the job's project) names the job, its
     kind, the owner and the new state; the controller logs a warning.
4. A `runJob` whose own transaction fails (the database restarts mid-job) leaves the row
   `running`, and a later pass reclaims it the same way.

**What this guarantees: at least once, as the job queue always promised.** Two limits are stated,
not solved:

- **Recovery time** is how long Postgres takes to notice the dead owner. When the controller
  process dies, the kernel closes its connections and recovery is the next tick. When a whole node
  vanishes, it is the server's TCP keepalive (`tcp_keepalives_*`; Linux defaults are about two
  hours). Operators who want faster recovery lower those settings.
- **A server-side `idle_in_transaction_session_timeout`** shorter than a job's handler ends the
  session while the handler still runs, and the job may run twice. BeziSync's handlers are short
  (cleanup, the longest, is about a minute); keep such a timeout above that.

Also accepted: a snapshot job that superseded queued snapshots of the same pipeline (it marks them
`done` in its own transaction) and then lost its controller rolls those marks back; they run
again and may copy their tables a second time. The rows are re-sent as reads, which consumers of
at-least-once CDC already handle.

### Implementation plan

One commit in `controller/jobs.go` with its test, the controller/API contract lines, and the
removal of Known issue 5.

### Verification plan

| ID | Case | Expected | Test |
|---|---|---|---|
| J1 | A `restart` left `running` 5 minutes ago, row free | Reclaimed, then runs to `done` in the same pass (attempt 2) | `api` `TestStuckJobsAreReclaimed` |
| J2 | A `snapshot` left `running` | `failed` with the Interrupted error; no signal row written | same |
| J3 | A `running` job whose row another transaction holds | Not reclaimed; reclaimed once that transaction ends | same |
| J4 | A `running` job claimed 10 seconds ago | Not reclaimed (grace) | same |
| J5 | A reclaimed job at `max_attempts` | `failed`, not re-queued | same |
| J6 | Audit | One `job.reclaimed` per reclaimed job, in its project | same |
| J7 | Ordinary jobs | Unchanged (the existing job, cleanup and project-deletion tests) | existing `api` tests |

## Consequences

- A crashed controller's jobs are picked up on the next leader's tick once Postgres has dropped
  the dead connections, and project deletion and runtime removal stop being blocked forever.
- An interrupted snapshot needs a person to request it again; BeziSync never risks a second full
  copy on its own.
- An interrupted notification may be delivered twice; a duplicate alert is better than a lost one.
- `runJob` holds one row lock per job for its length, inside the transaction it already held.

## Alternatives considered

- **Timed lease with a heartbeat.** Invisible inside the running transaction, so live jobs would be
  reclaimed and run twice.
- **Reclaim every `running` job when a controller takes leadership.** A job still running on a
  partitioned old leader would run twice, and a database restart under a steady leader would never
  be recovered.
- **A per-handler deadline.** It would cancel the shared transaction too, so the job's bookkeeping
  couldn't land and the job would be reclaimed as "interrupted" for the wrong reason. Hung handlers
  are bounded by their own client timeouts.
- **A `lease_until` column with a separate heartbeat connection.** A migration and a second
  connection per job for what the row lock already gives.
