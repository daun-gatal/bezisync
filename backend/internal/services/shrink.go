package services

import (
	"context"
	"fmt"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/problem"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/flinkhelper"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
)

// Shrinking a custom Flink CDC cluster after its first copy. The controller calls FlinkPhaseStep
// on each tick it has the helper's status for a running pipeline. Only the flink_shape artifact
// changes: the next compile then yields the streaming shape, and its new config hash restarts the
// cluster from its saved position once (the drivers' stop-save-recreate). A controller that takes
// over mid-way sees the artifact already changed and does nothing twice.

// ResizeEvent is what a phase step did, for the audit log (actor: the controller).
type ResizeEvent struct {
	Kind, Summary string
	Details       map[string]any
}

// FlinkPhaseStep moves a cluster that has a streaming slot size between its copy and streaming
// shapes on evidence (R25–R34): it shrinks once every chunk is copied (the helper streams) and a
// position was saved after streaming began, never on missing evidence; under the "suggest" policy
// it only marks the cluster ready (Shrink now). A shrink whose cluster then fails before saving a
// position goes back to the copy shape; after MaxShrinkFailures BeziSync stops trying. It changes
// p's artifacts (the caller saves p) and returns what to audit, if anything.
func FlinkPhaseStep(p *models.Pipeline, spec domain.PipelineSpec, st *flinkhelper.Status, failed bool, auto bool, now time.Time) *ResizeEvent {
	stored := storedShape(p, spec)
	if stored == nil || stored["streaming_size"] == nil {
		return nil
	}
	next := map[string]any{}
	for k, v := range stored {
		next[k] = v
	}
	commit := func() {
		artifacts := cloneArtifacts(p)
		artifacts[ArtifactFlinkShape] = next
		p.Artifacts = artifacts
	}
	running := st != nil && st.Phase == flinkhelper.PhaseRunning && st.JobState == "RUNNING"
	copySize, _ := sizeFromArtifact(stored["size"])
	streamSize, _ := sizeFromArtifact(stored["streaming_size"])
	switch stored["phase"] {
	case PhaseStreaming:
		shrunk, _ := pytime.Parse(fmt.Sprint(stored["shrunk_at"]))
		// Pods that never start leave no helper status: failed still counts.
		savedSince := st != nil && st.LastStored != nil && st.LastStored.TakenAt.After(shrunk)
		if !failed || savedSince {
			return nil
		}
		// R31: the streaming shape failed before it saved a position: back to the copy shape.
		failures := int(toFloat(stored["shrink_failures"])) + 1
		next["phase"], next["shrink_failures"] = PhaseCopy, failures
		delete(next, "shrunk_at")
		delete(next, "streaming_since")
		msg := fmt.Sprintf("%s failed on its streaming shape, so it went back to its copy shape", p.Name)
		if failures >= MaxShrinkFailures {
			next["shrink_off"] = true
			msg += fmt.Sprintf("; after %d attempts BeziSync stopped shrinking it", failures)
		}
		commit()
		return &ResizeEvent{Kind: "pipeline.resize_failed", Summary: msg + ".", Details: map[string]any{"attempt": failures, "to": sizeMap(copySize)}}
	default:
		if next["shrink_off"] == true || !running || !st.Streaming {
			return nil
		}
		since, seen := stored["streaming_since"].(string)
		if !seen {
			next["streaming_since"] = pytime.ISOFormat(now)
			commit()
			return nil
		}
		began, err := pytime.Parse(since)
		if err != nil || st.LastStored == nil || !st.LastStored.TakenAt.After(began) {
			return nil
		}
		if !auto {
			if next["shrink_ready"] == true {
				return nil
			}
			next["shrink_ready"], next["copy_done_save"] = true, st.LastStored.ID
			commit()
			return nil
		}
		shrink(next, st.LastStored.ID, now)
		commit()
		return &ResizeEvent{Kind: "pipeline.resized", Summary: fmt.Sprintf("Shrank %s after its first copy: %s", p.Name, shapeWords(streamSize)),
			Details: map[string]any{"from": sizeMap(copySize), "to": sizeMap(streamSize), "after_save": st.LastStored.ID, "reason": "first copy done"}}
	}
}

func shrink(next map[string]any, save int64, now time.Time) {
	next["phase"], next["copy_done_save"], next["shrunk_at"] = PhaseStreaming, save, pytime.ISOFormat(now)
	delete(next, "shrink_ready")
}

// ShrinkNow shrinks a cluster the "suggest" policy marked ready (someone chose Shrink now).
func ShrinkNow(p *models.Pipeline, spec domain.PipelineSpec, now time.Time) (*ResizeEvent, bool) {
	stored := storedShape(p, spec)
	if stored == nil || stored["shrink_ready"] != true || stored["phase"] == PhaseStreaming {
		return nil, false
	}
	next := map[string]any{}
	for k, v := range stored {
		next[k] = v
	}
	copySize, _ := sizeFromArtifact(stored["size"])
	streamSize, _ := sizeFromArtifact(stored["streaming_size"])
	shrink(next, int64(toFloat(stored["copy_done_save"])), now)
	artifacts := cloneArtifacts(p)
	artifacts[ArtifactFlinkShape] = next
	p.Artifacts = artifacts
	return &ResizeEvent{Kind: "pipeline.resized", Summary: fmt.Sprintf("Shrank %s after its first copy: %s", p.Name, shapeWords(streamSize)),
		Details: map[string]any{"from": sizeMap(copySize), "to": sizeMap(streamSize), "reason": "shrink now"}}, true
}

func shapeWords(s engine.FlinkSize) string {
	return fmt.Sprintf("%d TaskManager%s × %d slots, %s CPU and %s each", s.TaskManagers, plural(s.TaskManagers, "", "s"), s.SlotsPerTaskManager(),
		s.TaskManagerCPU, s.TaskManagerMemory)
}

// ShrinkCluster is Shrink now: a running pipeline whose cluster finished its first copy under the
// "suggest" policy moves to its streaming shape (a restart from its saved position).
func ShrinkCluster(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline) error {
	if p.DeployedVersionID == nil || p.DesiredState != "running" {
		return problem.Conflict("not_running", "Only a running pipeline can be shrunk.", nil)
	}
	spec, err := SpecOf(ctx, q, p.DeployedVersionID)
	if err != nil {
		return err
	}
	ev, ok := ShrinkNow(p, *spec, time.Now())
	if !ok {
		return problem.Conflict("not_ready", "This cluster has nothing to shrink to yet: it shrinks once its first copy is done and a position was saved after it.", nil)
	}
	if err := SavePipeline(ctx, q, p); err != nil {
		return err
	}
	_, err = Record(ctx, q, AuditEvent{Kind: ev.Kind, Summary: ev.Summary, ProjectID: &a.Project.ID, Principal: a.Principal,
		ResourceType: "pipeline", ResourceID: &p.ID, Details: ev.Details})
	return err
}
