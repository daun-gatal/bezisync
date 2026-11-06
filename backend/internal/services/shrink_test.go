package services

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/flinkhelper"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

// R25: no helper status, or a job not running, never shrinks; nor does a manual shape (R32), which
// stores no shape at all.
func TestFlinkPhaseStepNeedsEvidence(t *testing.T) {
	spec := customFlink(4, "1", "2Gi")
	spec.FlinkCDC.Cluster.StreamingSlot = &spec.FlinkCDC.Cluster.JobManager // 1 CPU, 1 GiB: smaller in memory
	p := &models.Pipeline{Name: "lake", Artifacts: db.Map{}}
	applyFlinkShape(p, spec, nodes(8, 32), false)
	now := time.Now()
	for _, st := range []*flinkhelper.Status{nil, {Phase: "submitting"}, {Phase: "running", JobState: "RUNNING"}} {
		if ev := FlinkPhaseStep(p, spec, st, false, true, now); ev != nil || storedShape(p, spec)["streaming_since"] != nil {
			t.Fatal(st, ev)
		}
	}
	// A failure while still copying is the pipeline's own trouble, not the shrink's.
	if ev := FlinkPhaseStep(p, spec, nil, true, true, now); ev != nil || storedShape(p, spec)["phase"] != PhaseCopy {
		t.Fatal(ev)
	}
	manual := customFlink(4, "1", "1Gi")
	manual.FlinkCDC.Cluster.Shape = "manual"
	q := &models.Pipeline{Artifacts: db.Map{}}
	applyFlinkShape(q, manual, nil, false)
	if ev := FlinkPhaseStep(q, manual, &flinkhelper.Status{Phase: "running", JobState: "RUNNING", Streaming: true}, false, true, now); ev != nil {
		t.Fatal(ev)
	}
}

func TestClusterShrinkFailedAlert(t *testing.T) {
	v := uuid.New()
	p := &models.Pipeline{Name: "lake", DeployedVersionID: &v, Artifacts: db.Map{ArtifactFlinkShape: map[string]any{"shrink_off": true, "shrink_failures": 3.0}}}
	c := Evaluate("cluster_shrink_failed", p, map[string]any{}, db.Now())
	if !c.Active || c.Title != "lake's cluster stopped shrinking" {
		t.Fatal(c)
	}
	p.Artifacts = db.Map{ArtifactFlinkShape: map[string]any{"shrink_failures": 1.0}}
	if Evaluate("cluster_shrink_failed", p, map[string]any{}, db.Now()).Active {
		t.Fatal("fires before it stopped trying")
	}
}
