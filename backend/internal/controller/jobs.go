package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
	"github.com/daun-gatal/bezisync/backend/internal/services"
)

// Job handlers. Each is idempotent: jobs are delivered at least once.

// Defer runs the job at Until instead (e.g. a maintenance window); not an attempt.
type Defer struct {
	Until  time.Time
	Reason string
}

func (d *Defer) Error() string { return d.Reason }

func jobPipeline(ctx context.Context, q db.Querier, j *models.Job) (*models.Pipeline, error) {
	if j.ResourceID == nil {
		return nil, errors.New("pipeline vanished")
	}
	p, err := db.Find[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE id = $1`, *j.ResourceID)
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, errors.New("pipeline vanished")
	}
	return p, nil
}

// RunJob dispatches a job to its handler.
func RunJob(ctx context.Context, q db.Querier, j *models.Job) (map[string]any, error) {
	switch j.Kind {
	case "restart":
		return jobRestart(ctx, q, j)
	case "cleanup":
		return jobCleanup(ctx, q, j)
	case "snapshot":
		return jobSnapshot(ctx, q, j)
	case "notify":
		return jobNotify(ctx, q, j)
	}
	return nil, fmt.Errorf("unknown job kind %s", j.Kind)
}

func pipelineDriver(ctx context.Context, q db.Querier, p *models.Pipeline) (runtime.Driver, error) {
	if p.RuntimeID == nil {
		return nil, errors.New("pipeline has no runtime")
	}
	rt, err := services.GetRuntime(ctx, q, *p.RuntimeID)
	if err != nil {
		return nil, err
	}
	if rt == nil {
		return nil, errors.New("runtime vanished")
	}
	return services.DriverFor(ctx, q, rt)
}

func jobRestart(ctx context.Context, q db.Querier, j *models.Job) (map[string]any, error) {
	p, err := jobPipeline(ctx, q, j)
	if err != nil {
		return nil, err
	}
	d, err := pipelineDriver(ctx, q, p)
	if err != nil {
		return nil, err
	}
	if err := d.Delete(ctx, p.ShortID()); err != nil {
		return nil, err
	}
	return map[string]any{"restarted": true}, nil
}

func artifactMaps(rs []connectors.ArtifactResult) []map[string]any {
	out := make([]map[string]any, len(rs))
	for i, r := range rs {
		out[i] = map[string]any{"kind": r.Kind, "name": r.Name, "outcome": r.Outcome, "detail": r.Detail}
	}
	return out
}

func jobCleanup(ctx context.Context, q db.Querier, j *models.Job) (map[string]any, error) {
	p, err := jobPipeline(ctx, q, j)
	if err != nil {
		return nil, err
	}
	d, err := pipelineDriver(ctx, q, p)
	if err != nil {
		return nil, err
	}
	if err := d.Delete(ctx, p.ShortID()); err != nil {
		return nil, err
	}
	for range 30 {
		obs, err := d.Observe(ctx, p.ShortID())
		if err != nil {
			return nil, err
		}
		if obs.Phase == runtime.PhaseMissing {
			break
		}
		sleep(ctx, 2*time.Second)
	}
	vid := p.DeployedVersionID
	if vid == nil {
		vid = p.CurrentVersionID
	}
	spec, err := services.SpecOf(ctx, q, vid)
	if err != nil {
		return nil, err
	}
	results := []map[string]any{{"kind": "workload", "name": "bz-" + p.ShortID(), "outcome": "removed"}}
	if spec != nil && p.DeployedVersionID != nil {
		cctx := compileCtx(p, *spec)
		source, err := services.GetConnection(ctx, q, p.SourceID)
		if err != nil {
			return nil, err
		}
		func() {
			src, err := connectors.GetSource(source.Type)
			var rs []connectors.ArtifactResult
			if err == nil {
				var resolved connectors.Resolved
				if resolved, err = services.Resolve(ctx, q, source); err == nil {
					rs, err = src.Cleanup(ctx, resolved, cctx)
				}
			}
			if err != nil {
				results = append(results, map[string]any{"kind": "source", "name": source.Name, "outcome": "failed", "detail": domain.Truncate(errMsg(err), 300)})
				return
			}
			results = append(results, artifactMaps(rs)...)
		}()
		if truthy(j.Payload["drop_destinations"]) && p.SinkID != nil {
			destinations := map[string]string{}
			for k, v := range mapOf(p.Artifacts["destinations"]) {
				destinations[k] = strOf(v)
			}
			sinkConn, err := services.GetConnection(ctx, q, *p.SinkID)
			if err != nil {
				return nil, err
			}
			func() {
				snk, err := connectors.GetSink(sinkConn.Type)
				var rs []connectors.ArtifactResult
				if err == nil {
					var resolved connectors.Resolved
					if resolved, err = services.Resolve(ctx, q, sinkConn); err == nil {
						rs, err = snk.CleanupSink(ctx, resolved, cctx, destinations)
					}
				}
				if err != nil {
					results = append(results, map[string]any{"kind": "destination", "name": sinkConn.Name, "outcome": "failed", "detail": domain.Truncate(errMsg(err), 300)})
					return
				}
				results = append(results, artifactMaps(rs)...)
			}()
		}
		if p.SinkID != nil {
			// Helpers a sink keeps next to its destinations (the SNS preview queue) go with the
			// pipeline, dropped destinations or not.
			destinations := map[string]string{}
			for k, v := range mapOf(p.Artifacts["destinations"]) {
				destinations[k] = strOf(v)
			}
			sinkConn, err := services.GetConnection(ctx, q, *p.SinkID)
			if err != nil {
				return nil, err
			}
			if snk, err := connectors.GetSink(sinkConn.Type); err == nil {
				if releaser, ok := snk.(connectors.SinkReleaser); ok {
					resolved, err := services.Resolve(ctx, q, sinkConn)
					var rs []connectors.ArtifactResult
					if err == nil {
						rs, err = releaser.ReleaseSink(ctx, resolved, cctx, destinations)
					}
					if err != nil {
						results = append(results, map[string]any{"kind": "destination", "name": sinkConn.Name, "outcome": "failed", "detail": domain.Truncate(errMsg(err), 300)})
					} else {
						results = append(results, artifactMaps(rs)...)
					}
				}
			}
		}
		if err := services.ClearOffsets(ctx, p.ShortID()); err != nil {
			return nil, err
		}
		results = append(results, map[string]any{"kind": "offsets", "name": "offsets_" + p.ShortID(), "outcome": "removed"})
		if services.EngineOf(p) == engine.FlinkCDC {
			if err := services.DropSavedPositions(ctx, p.ShortID()); err != nil {
				return nil, err
			}
			results = append(results, map[string]any{"kind": "saved_positions", "name": "flink_state_" + p.ShortID(), "outcome": "removed"})
		}
	}
	var failed []map[string]any
	for _, r := range results {
		if r["outcome"] == "failed" {
			failed = append(failed, r)
		}
	}
	list := make([]any, len(results))
	for i, r := range results {
		list[i] = r
	}
	artifacts := db.Map{}
	for k, v := range p.Artifacts {
		artifacts[k] = v
	}
	artifacts["cleanup"] = list
	p.Artifacts = artifacts
	p.Lifecycle = "deleted"
	summary := "Removed."
	if len(failed) > 0 {
		p.Lifecycle = "cleanup_failed"
		summary = fmt.Sprintf("Cleanup incomplete: %s — %s", strOf(failed[0]["name"]), strOf(failed[0]["detail"]))
	}
	st := statusCopy(p)
	st["lifecycle"], st["summary"] = p.Lifecycle, summary
	p.Status = st
	if err := services.SavePipeline(ctx, q, p); err != nil {
		return nil, err
	}
	var parts []string
	for _, r := range results {
		parts = append(parts, fmt.Sprintf("%s %s %s", strOf(r["kind"]), strOf(r["name"]), strOf(r["outcome"])))
	}
	pid := p.ProjectID
	if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "pipeline.cleanup", ProjectID: &pid, ResourceType: "pipeline", ResourceID: &p.ID,
		Summary: fmt.Sprintf("Cleanup of %s: %s", p.Name, strings.Join(parts, ", ")), Details: map[string]any{"results": list}, NotAudit: true}); err != nil {
		return nil, err
	}
	return map[string]any{"results": list}, nil
}

func errMsg(err error) string {
	type detail interface{ Error() string }
	return detail(err).Error()
}

func stringList(v any, def []string) []string {
	l, ok := v.([]any)
	if !ok || len(l) == 0 {
		return def
	}
	out := make([]string, len(l))
	for i, x := range l {
		out[i] = strOf(x)
	}
	return out
}

func jobSnapshot(ctx context.Context, q db.Querier, j *models.Job) (map[string]any, error) {
	p, err := jobPipeline(ctx, q, j)
	if err != nil {
		return nil, err
	}
	if p.Lifecycle != domain.LifecycleStreaming && p.Lifecycle != domain.LifecycleSnapshotting {
		return nil, &services.Retry{Msg: fmt.Sprintf("waiting for the pipeline to stream (now %s)", p.Lifecycle)}
	}
	spec, err := services.SpecOf(ctx, q, p.DeployedVersionID)
	if err != nil || spec == nil {
		return nil, errors.Join(err, errors.New("pipeline has no deployed spec"))
	}
	tables := stringList(j.Payload["tables"], spec.Source.Tables.Include)
	status := statusCopy(p)
	window := spec.Snapshot.Window
	if window != nil && *window != "" && !truthy(j.Payload["ignore_window"]) {
		opens, err := domain.WindowNextStart(*window, db.Now())
		if err != nil {
			return nil, err
		}
		if opens != nil {
			tl := make([]any, len(tables))
			for i, t := range tables {
				tl[i] = t
			}
			status["scheduled_snapshot"] = map[string]any{"tables": tl, "at": pytime.ISOFormat(*opens), "window": *window}
			p.Status = status
			if err := services.SavePipeline(ctx, q, p); err != nil {
				return nil, err
			}
			return nil, &Defer{Until: *opens, Reason: fmt.Sprintf("waiting for the maintenance window %s UTC", *window)}
		}
	}
	// Queued snapshots of tables this run covers are done too.
	waiting, err := db.Select[models.Job](ctx, q, `SELECT * FROM jobs WHERE kind = 'snapshot' AND resource_id = $1 AND state = 'queued' AND id <> $2`, p.ID, j.ID)
	if err != nil {
		return nil, err
	}
	covered := map[string]bool{}
	for _, t := range tables {
		covered[t] = true
	}
	remaining := false
	for i := range waiting {
		other := &waiting[i]
		subset := true
		for _, t := range stringList(other.Payload["tables"], spec.Source.Tables.Include) {
			if !covered[t] {
				subset = false
			}
		}
		if subset {
			now := db.Now()
			other.State, other.FinishedAt, other.Result = "done", &now, db.Map{"superseded_by": j.ID.String()}
			if err := db.Update(ctx, q, "jobs", other); err != nil {
				return nil, err
			}
		} else {
			remaining = true
		}
	}
	if !remaining {
		delete(status, "scheduled_snapshot")
	}
	p.Status = status
	if err := services.SavePipeline(ctx, q, p); err != nil {
		return nil, err
	}
	source, err := services.GetConnection(ctx, q, p.SourceID)
	if err != nil {
		return nil, err
	}
	src, err := connectors.GetSource(source.Type)
	if err != nil {
		return nil, err
	}
	resolved, err := services.Resolve(ctx, q, source)
	if err != nil {
		return nil, err
	}
	mode := "incremental"
	if m, ok := j.Payload["mode"]; ok && m != nil {
		mode = strOf(m)
	}
	tl := make([]any, len(tables))
	for i, t := range tables {
		tl[i] = t
	}
	signalID, err := src.Signal(ctx, resolved, compileCtx(p, *spec), "execute-snapshot", map[string]any{"data-collections": tl, "type": mode})
	if err != nil {
		return nil, err
	}
	pid := p.ProjectID
	if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "pipeline.snapshot", ProjectID: &pid, ResourceType: "pipeline", ResourceID: &p.ID,
		Summary: "Started an incremental snapshot of " + strings.Join(tables, ", "), NotAudit: true,
		Details: map[string]any{"tables": tl, "signal_id": signalID, "reason": j.Payload["reason"]}}); err != nil {
		return nil, err
	}
	return map[string]any{"signal_id": signalID, "tables": tl}, nil
}

func jobNotify(ctx context.Context, q db.Querier, j *models.Job) (map[string]any, error) {
	if j.ResourceID == nil {
		return nil, errors.New("notify job without an alert")
	}
	return services.RunNotifyJob(ctx, q, *j.ResourceID, stringList(j.Payload["targets"], []string{}))
}

// RunJobs claims and runs up to limit due jobs.
func (c *Controller) RunJobs(ctx context.Context, limit int) {
	for range limit {
		var id uuid.UUID
		err := db.Tx(ctx, func(tx pgx.Tx) error {
			j, err := db.Find[models.Job](ctx, tx, `SELECT * FROM jobs WHERE state = 'queued' AND run_after <= $1 ORDER BY run_after LIMIT 1 FOR UPDATE SKIP LOCKED`, db.Now())
			if err != nil || j == nil {
				return err
			}
			now := db.Now()
			ident := Identity
			j.State, j.LockedBy, j.LockedAt = "running", &ident, &now
			j.Attempts++
			id = j.ID
			return db.Update(ctx, tx, "jobs", j)
		})
		if err != nil {
			log.Error("cannot claim a job", "exc", err.Error())
			return
		}
		if id == uuid.Nil {
			return
		}
		c.runJob(ctx, id)
	}
}

func backoff(attempts int) time.Duration {
	s := 5 << attempts
	if attempts > 10 || s > 300 {
		s = 300
	}
	return time.Duration(s) * time.Second
}

func (c *Controller) runJob(ctx context.Context, id uuid.UUID) {
	err := db.Tx(ctx, func(tx pgx.Tx) error {
		j, err := db.Find[models.Job](ctx, tx, `SELECT * FROM jobs WHERE id = $1`, id)
		if err != nil || j == nil {
			return err
		}
		// Like the Python implementation, a handler's writes are kept even when it then
		// fails (e.g. deliveries recorded before a retry). The savepoint only rolls back a
		// handler whose SQL error aborted the transaction, so the job's bookkeeping still lands.
		sp, err := tx.Begin(ctx)
		if err != nil {
			return err
		}
		result, runErr := RunJob(ctx, sp, j)
		if cerr := sp.Commit(ctx); cerr != nil {
			_ = sp.Rollback(ctx)
			if runErr == nil {
				runErr = cerr
			}
		}
		now := db.Now()
		var deferred *Defer
		var retry *services.Retry
		switch {
		case errors.As(runErr, &deferred):
			msg := deferred.Reason
			j.State, j.RunAfter, j.Error = "queued", deferred.Until, &msg
			j.Attempts-- // waiting for a window is not a failed attempt
		case runErr == nil:
			j.Result = result
			j.State, j.FinishedAt, j.Error = "done", &now, nil
		default:
			msg := domain.Truncate(runErr.Error(), 1000)
			j.Error = &msg
			if !errors.As(runErr, &retry) {
				log.Error(fmt.Sprintf("job %s failed", j.Kind), "exc", runErr.Error())
			}
			if j.Attempts >= j.MaxAttempts {
				j.State, j.FinishedAt = "failed", &now
			} else {
				j.State, j.RunAfter = "queued", now.Add(backoff(j.Attempts))
			}
		}
		return db.Update(ctx, tx, "jobs", j)
	})
	if err != nil {
		log.Error("job bookkeeping failed", "job_id", id.String(), "exc", err.Error())
	}
}
