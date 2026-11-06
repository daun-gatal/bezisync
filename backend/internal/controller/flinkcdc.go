package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/flinkhelper"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
	"github.com/daun-gatal/bezisync/backend/internal/services"
)

// A Flink CDC pipeline's evidence comes from the helper beside its JobManager: /q/health (UP once
// the job runs on every TaskManager) and /bz/status (the job, its saves and its progress). They
// map onto the same verdict Debezium pipelines get.

// observeFlink reads the helper of a running cluster into o.
func observeFlink(ctx context.Context, d runtime.Driver, shortID string, port int, o *observed) {
	code, body, err := d.HTTPGet(ctx, shortID, port, "/q/health", 4*time.Second)
	switch {
	case err != nil:
		o.healthError = domain.Truncate(err.Error(), 300)
	default:
		if o.healthUp = ParseHealth(code, body); o.healthUp == nil {
			o.healthError = fmt.Sprintf("The Flink CDC helper answered HTTP %d on port %d.", code, port)
		}
	}
	code, body, err = d.HTTPGet(ctx, shortID, port, "/bz/status", 4*time.Second)
	if err != nil || code != 200 {
		return
	}
	var st flinkhelper.Status
	if json.Unmarshal([]byte(body), &st) != nil {
		return
	}
	o.flink = &st
	o.metrics = flinkMetrics(st)
	if o.healthUp != nil && !*o.healthUp {
		o.engineDown = flinkDown(st)
	}
}

// flinkMetrics is the helper's status as the metrics the verdict reads. Nothing is reported that
// the helper did not see: the copy is running until the source's log split exists.
func flinkMetrics(st flinkhelper.Status) *domain.Metrics {
	m := &domain.Metrics{}
	running := st.Phase == flinkhelper.PhaseRunning && st.JobState == "RUNNING"
	switch {
	case running:
		t := true
		m.Connected = &t
		copying, done := !st.Streaming, st.Streaming
		m.SnapshotRunning, m.SnapshotCompleted = &copying, &done
	case st.Phase == flinkhelper.PhaseFailed || st.Phase == flinkhelper.PhaseRestarting || st.Phase == flinkhelper.PhaseConfigError:
		f := false
		m.Connected = &f
	}
	if st.LagMS != nil {
		lag := *st.LagMS
		m.MSBehindSource = &lag
	}
	read := float64(st.RecordsRead)
	if st.Streaming {
		m.EventsSeenStreaming = &read
	} else if running {
		m.EventsSeenSnapshot = &read
	}
	return m
}

// flinkDown says why the helper reports DOWN. Starting up is "health_down" (a young pipeline is
// still deploying); a failed or restarting job says what went wrong.
func flinkDown(st flinkhelper.Status) *domain.Reason {
	switch st.Phase {
	case flinkhelper.PhaseFailed, flinkhelper.PhaseConfigError:
		code := st.ErrorCode
		if code == "" {
			code = "job_failed"
		}
		msg := st.Error
		if msg == "" {
			msg = "The Flink job failed."
		}
		return &domain.Reason{Code: code, Message: msg, Severity: "error"}
	case flinkhelper.PhaseRestarting:
		msg := "The Flink job is restarting after a failure."
		if len(st.Exceptions) > 0 {
			msg = "The Flink job is restarting after a failure: " + domain.Truncate(st.Exceptions[len(st.Exceptions)-1], 300)
		}
		return &domain.Reason{Code: "job_restarting", Message: msg, Severity: "error"}
	}
	msg := "Flink CDC is starting."
	switch {
	case st.TaskManagersWanted > 0 && st.TaskManagers < st.TaskManagersWanted:
		msg = fmt.Sprintf("Waiting for TaskManagers: %d of %d connected.", st.TaskManagers, st.TaskManagersWanted)
	case st.Phase == flinkhelper.PhaseSubmitting || st.Phase == flinkhelper.PhaseResubmitting:
		msg = "Submitting the pipeline to Flink."
	case st.Phase == flinkhelper.PhaseStopping:
		msg = "Saving the position and stopping."
	}
	return &domain.Reason{Code: "health_down", Message: msg, Severity: "error"}
}

// flinkStatus is what the pipeline's status keeps of the helper's: the job, its saves and how far
// each table's copy got (chunks reached, by spec table).
func flinkStatus(st *flinkhelper.Status, obs *runtime.Observation, tables []string) map[string]any {
	out := map[string]any{"phase": st.Phase, "job_state": st.JobState, "streaming": st.Streaming, "taskmanagers": st.TaskManagers,
		"taskmanagers_wanted": st.TaskManagersWanted, "checkpoints_completed": st.CheckpointsDone, "checkpoints_failed": st.CheckpointsFailed,
		"restarts": st.Restarts}
	if st.LastCheckpointAt != nil {
		out["last_checkpoint_at"] = pytime.ISOFormat(*st.LastCheckpointAt)
	}
	saved := func(s *flinkhelper.Saved) map[string]any {
		return map[string]any{"id": s.ID, "kind": s.Kind, "reason": s.Reason, "taken_at": pytime.ISOFormat(s.TakenAt), "size": s.Size}
	}
	if st.LastStored != nil {
		out["last_saved"] = saved(st.LastStored)
	}
	if st.RestoredFrom != nil {
		out["restored_from"] = saved(st.RestoredFrom)
	}
	if st.StoreError != "" {
		out["store_error"] = domain.Truncate(st.StoreError, 300)
	}
	if len(st.Chunks) > 0 {
		chunks := map[string]any{}
		for _, t := range tables {
			// Flink names splits by its table id with dots as underscores; SQL Server's id
			// starts with the database ("shop_dbo_orders" for dbo.orders).
			key := strings.ReplaceAll(t, ".", "_")
			for k, n := range st.Chunks {
				if k == key || strings.HasSuffix(k, "_"+key) {
					chunks[t] = n
				}
			}
		}
		out["chunks"] = chunks
	}
	if obs != nil {
		if c, ok := obs.Details["components"]; ok {
			out["components"] = c
		}
	}
	return out
}

// flinkPhase shrinks a custom Flink CDC cluster after its first copy, or puts it back on its copy
// shape when its streaming shape failed (services.FlinkPhaseStep), auditing each resize as the
// controller. The new shape takes effect on the next tick through its config hash.
func (c *Controller) flinkPhase(ctx context.Context, tx pgx.Tx, p *models.Pipeline, spec domain.PipelineSpec, st *flinkhelper.Status, failed bool) error {
	if services.EngineOf(p) != engine.FlinkCDC || p.DesiredState != "running" || p.DeployedVersionID == nil {
		return nil
	}
	pol, err := services.PoliciesOf(ctx, tx, p.ProjectID)
	if err != nil {
		return err
	}
	ev := services.FlinkPhaseStep(p, spec, st, failed, pol.ShrinkAuto(), time.Now())
	if ev == nil {
		return nil
	}
	pid := p.ProjectID
	_, err = services.Record(ctx, tx, services.AuditEvent{Kind: ev.Kind, Summary: ev.Summary, ProjectID: &pid, ResourceType: "pipeline", ResourceID: &p.ID,
		Details: ev.Details, Actor: "controller"})
	return err
}
