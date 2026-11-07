package api_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

// stuckJob enqueues a job as a crashed controller would have left it: running, claimed long ago.
func stuckJob(t *testing.T, pipelineID, kind string, lockedAgo time.Duration, attempts int) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	p, err := db.Find[models.Pipeline](ctx, testdb.Pool(t), `SELECT * FROM pipelines WHERE id = $1`, pipelineID)
	if err != nil || p == nil {
		t.Fatal(p, err)
	}
	j, err := services.Enqueue(ctx, testdb.Pool(t), kind, p, map[string]any{"tables": []any{"shop.orders"}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	exec(t, `UPDATE jobs SET state = 'running', locked_by = 'gone-1', attempts = `+itoa(attempts)+`, locked_at = now() - interval '`+itoa(int(lockedAgo.Seconds()))+` seconds' WHERE id = '`+j.ID.String()+`'`)
	return j.ID
}

func itoa(n int) string { return strconv.Itoa(n) }

func jobRow(t *testing.T, id uuid.UUID) *models.Job {
	t.Helper()
	j, err := db.Find[models.Job](context.Background(), testdb.Pool(t), `SELECT * FROM jobs WHERE id = $1`, id)
	if err != nil || j == nil {
		t.Fatal(j, err)
	}
	return j
}

// A job a crashed controller left running is taken back (ADR 0003, Known issue 5).
func TestStuckJobsAreReclaimed(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	pid := pipe.Str("id")
	e.admin.post("/api/v1/projects/"+e.pid+"/pipelines/"+pid+"/deploy", map[string]any{}).expect(200)
	e.tick()

	restart := stuckJob(t, pid, "restart", 5*time.Minute, 1)   // J1: re-queued and run
	snapshot := stuckJob(t, pid, "snapshot", 5*time.Minute, 1) // J2: failed, never re-sent
	fresh := stuckJob(t, pid, "restart", 10*time.Second, 1)    // J4: inside the grace
	spent := stuckJob(t, pid, "restart", 5*time.Minute, 5)     // J5: no attempts left
	held := stuckJob(t, pid, "restart", 5*time.Minute, 1)      // J3: its owner is alive
	ctx := context.Background()
	owner, err := testdb.Pool(t).Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Exec(ctx, `SELECT 1 FROM jobs WHERE id = $1 FOR UPDATE`, held); err != nil {
		t.Fatal(err)
	}
	e.runJobs()

	if j := jobRow(t, restart); j.State != "done" || j.Attempts != 2 {
		t.Fatalf("restart: %s attempts %d (%v)", j.State, j.Attempts, j.Error)
	}
	if j := jobRow(t, snapshot); j.State != "failed" || j.Error == nil || j.Attempts != 1 {
		t.Fatalf("snapshot: %+v", j)
	}
	n, err := db.Scalar[int64](ctx, testdb.Pool(t), `SELECT count(*) FROM bezisync.signal_`+pid[:8]+pid[9:13])
	if err != nil || n != 0 {
		t.Fatal("a reclaimed snapshot sent a signal:", n, err)
	}
	if j := jobRow(t, fresh); j.State != "running" {
		t.Fatalf("within the grace: %s", j.State)
	}
	if j := jobRow(t, spent); j.State != "failed" || j.FinishedAt == nil {
		t.Fatalf("no attempts left: %s", j.State)
	}
	if j := jobRow(t, held); j.State != "running" {
		t.Fatalf("a job whose owner holds its row was reclaimed: %s", j.State)
	}
	_ = owner.Rollback(ctx)
	audits, err := db.Scalar[int64](ctx, testdb.Pool(t), `SELECT count(*) FROM events WHERE kind = 'job.reclaimed' AND project_id = $1`, e.pid)
	if err != nil || audits != 3 {
		t.Fatal("audit events:", audits, err)
	}
	// Once its owner is gone, the held one is taken back too.
	e.runJobs()
	if j := jobRow(t, held); j.State != "done" {
		t.Fatalf("held, after its owner went: %s", j.State)
	}
}
