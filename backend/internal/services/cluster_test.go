package services

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

func customFlink(parallelism int, slotCPU, slotMem string) domain.PipelineSpec {
	s := domain.DefaultSpec()
	s.Policies.Resources = "custom"
	s.FlinkCDC.Cluster = &domain.FlinkCluster{Parallelism: parallelism, Shape: "auto", Slot: domain.SlotSize{CPU: slotCPU, Memory: slotMem},
		JobManager: domain.SlotSize{CPU: "1", Memory: "1Gi"}}
	return s
}

func nodes(cpu float64, memGiB int64) map[string]any {
	return map[string]any{"node_cpu_max": cpu, "node_memory_max": memGiB << 30}
}

func TestFlinkSizeOf(t *testing.T) {
	p := &models.Pipeline{Artifacts: db.Map{}}
	// Presets resolve as before.
	preset := domain.DefaultSpec()
	preset.Policies.Resources = "large"
	if got := FlinkSizeOf(p, preset, nil); got.Size != engine.FlinkSizes["large"] || !got.Fits {
		t.Fatal(got)
	}
	if n := NodeOf(nil); n.Known() {
		t.Fatal(n)
	}
	if n := NodeOf(nodes(4, 8)); n != (engine.Node{CPUMilli: 4000, MemoryMiB: 8192}) {
		t.Fatal(n)
	}
	// Custom: packed for the nodes.
	spec := customFlink(6, "1", "2Gi")
	got := FlinkSizeOf(p, spec, nodes(4, 8))
	if got.Size.TaskManagers != 2 || got.Size.Slots != 3 || got.Size.Parallelism() != 6 || got.Size.TaskManagerMemory != "6656Mi" {
		t.Fatalf("%+v", got)
	}
	// Manual: as written, whatever the nodes.
	manual := customFlink(4, "1", "1Gi")
	manual.FlinkCDC.Cluster.Shape = "manual"
	manual.FlinkCDC.Cluster.TaskManagers = &domain.ManualTaskManagers{Count: 2, Slots: 2, CPU: "2", Memory: "3Gi"}
	m := FlinkSizeOf(p, manual, nodes(64, 256))
	if m.Size.TaskManagers != 2 || m.Size.Slots != 2 || m.Size.Readers != 4 || m.Size.TaskManagerMemory != "3Gi" {
		t.Fatalf("%+v", m)
	}
	applyFlinkShape(p, manual, nil, false)
	if _, ok := p.Artifacts[ArtifactFlinkShape]; ok {
		t.Fatal("a manual shape stores nothing")
	}
}

// I3: a deployed custom cluster keeps the shape it was packed into, whatever the nodes do later,
// until its spec changes (then the next deploy packs again).
func TestFlinkShapeStoredAtDeploy(t *testing.T) {
	spec := customFlink(6, "1", "2Gi")
	p := &models.Pipeline{Artifacts: db.Map{"other": "kept"}}
	applyFlinkShape(p, spec, nodes(4, 8), false)
	// As the database returns it: JSON numbers.
	raw, _ := json.Marshal(p.Artifacts)
	p.Artifacts = db.Map{}
	_ = json.Unmarshal(raw, &p.Artifacts)
	if p.Artifacts["other"] != "kept" {
		t.Fatal(p.Artifacts)
	}
	deployed := FlinkSizeOf(p, spec, nodes(4, 8))
	for _, later := range []map[string]any{nodes(64, 256), nodes(1, 2), nil} {
		if got := FlinkSizeOf(p, spec, later); got.Size != deployed.Size {
			t.Fatalf("%v: %+v, deployed %+v", later, got.Size, deployed.Size)
		}
	}
	// A changed spec packs fresh.
	edited := customFlink(8, "1", "2Gi")
	if got := FlinkSizeOf(p, edited, nodes(64, 256)); got.Size.TaskManagers != 1 || got.Size.Slots != 8 {
		t.Fatalf("%+v", got.Size)
	}
	// Redeploying repacks for today's nodes; going back to a preset forgets the shape.
	applyFlinkShape(p, spec, nodes(64, 256), false)
	if got := FlinkSizeOf(p, spec, nil); got.Size.TaskManagers != 1 || got.Size.Slots != 6 {
		t.Fatalf("%+v", got.Size)
	}
	applyFlinkShape(p, domain.DefaultSpec(), nil, false)
	if _, ok := p.Artifacts[ArtifactFlinkShape]; ok {
		t.Fatal(p.Artifacts)
	}
	// A damaged artifact is ignored, never trusted.
	p.Artifacts[ArtifactFlinkShape] = map[string]any{"spec_hash": domain.SpecHash(spec), "size": map[string]any{"task_managers": 0}}
	if got := FlinkSizeOf(p, spec, nodes(4, 8)); got.Size != deployed.Size {
		t.Fatalf("%+v", got.Size)
	}
}

func TestFlinkClusterFitCheck(t *testing.T) {
	p := &models.Pipeline{}
	if c := flinkClusterFitCheck(domain.DefaultSpec(), engine.Packed{}, nil); c != nil {
		t.Fatal("presets have no fit check")
	}
	spec := customFlink(4, "1", "2Gi")
	check := func(s domain.PipelineSpec, capacity map[string]any) *checks.Check {
		return flinkClusterFitCheck(s, FlinkSizeOf(p, s, capacity), capacity)
	}
	if c := check(spec, nodes(8, 16)); c.Status != checks.Pass || !strings.Contains(c.Message, "1 TaskManager × 4 slots") {
		t.Fatal(c)
	}
	// R11: unknown nodes are never a pass.
	if c := check(spec, nil); c.Status != checks.Unknown || c.Blocking {
		t.Fatal(c)
	}
	// R9: one slot bigger than the largest node blocks.
	big := customFlink(2, "4", "2Gi")
	if c := check(big, nodes(2, 16)); c.Status != checks.Fail || !c.Blocking || !strings.Contains(c.Message, "One slot needs more") {
		t.Fatal(c)
	}
	// A manual TaskManager bigger than any node blocks.
	manual := customFlink(4, "1", "1Gi")
	manual.FlinkCDC.Cluster.Shape = "manual"
	manual.FlinkCDC.Cluster.TaskManagers = &domain.ManualTaskManagers{Count: 1, Slots: 4, CPU: "8", Memory: "8Gi"}
	if c := check(manual, nodes(4, 16)); c.Status != checks.Fail || !c.Blocking || !strings.Contains(c.Message, "largest node has 4 CPU") {
		t.Fatal(c)
	}
	if c := check(manual, nodes(8, 16)); c.Status != checks.Pass {
		t.Fatal(c)
	}
}
