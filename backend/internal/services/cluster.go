package services

import (
	"context"
	"fmt"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

// ArtifactFlinkShape is the shape a custom Flink CDC cluster was packed into at deploy, with the
// hash of the spec it was packed for: compiling that spec again uses it, so the nodes changing
// never restarts a running pipeline. The next deploy packs again.
const ArtifactFlinkShape = "flink_shape"

// NodeOf is the largest node of a runtime's last capacity report (unknown when it has none).
func NodeOf(capacity map[string]any) engine.Node {
	return engine.Node{CPUMilli: int(toFloat(capacity["node_cpu_max"]) * 1000), MemoryMiB: int(toFloat(capacity["node_memory_max"]) / (1 << 20))}
}

// FlinkSizeOf is a Flink CDC pipeline's size: a preset, or its custom cluster as stored at deploy
// for this spec, or packed now for the runtime's nodes.
func FlinkSizeOf(p *models.Pipeline, spec domain.PipelineSpec, capacity map[string]any) engine.Packed {
	c := spec.FlinkCDC.Cluster
	if spec.Policies.Resources != "custom" || c == nil {
		size, ok := engine.FlinkSizes[spec.Policies.Resources]
		if !ok {
			size = engine.FlinkSizes["small"]
		}
		return engine.Packed{Size: size, Fits: true}
	}
	if c.Shape == "manual" && c.TaskManagers != nil {
		tm := c.TaskManagers
		return engine.Packed{Fits: true, Why: "Manual shape.", Size: engine.FlinkSize{JobManagerCPU: c.JobManager.CPU, JobManagerMemory: c.JobManager.Memory,
			TaskManagers: tm.Count, Slots: tm.Slots, Readers: c.Parallelism, TaskManagerCPU: tm.CPU, TaskManagerMemory: tm.Memory}}
	}
	if stored, ok := p.Artifacts[ArtifactFlinkShape].(map[string]any); ok && stored["spec_hash"] == domain.SpecHash(spec) {
		if size, ok := sizeFromArtifact(stored["size"]); ok {
			return engine.Packed{Size: size, Fits: true, Why: "Packed at deploy."}
		}
	}
	cpu, _ := domain.ParseCPUAny(c.Slot.CPU)
	mem, _ := domain.ParseMemoryAny(c.Slot.Memory)
	return engine.Pack(c.Parallelism, cpu, mem, c.JobManager.CPU, c.JobManager.Memory, NodeOf(capacity))
}

// applyFlinkShape records in p's artifacts the shape a custom cluster is deployed with, packed now
// (nothing for presets and manual shapes, which never depend on the nodes). The caller saves p.
func applyFlinkShape(p *models.Pipeline, spec domain.PipelineSpec, capacity map[string]any) {
	c := spec.FlinkCDC.Cluster
	if spec.Policies.Resources != "custom" || c == nil || c.Shape == "manual" {
		delete(p.Artifacts, ArtifactFlinkShape)
		return
	}
	fresh := *p
	fresh.Artifacts = db.Map{} // pack now, not from an older store
	packed := FlinkSizeOf(&fresh, spec, capacity)
	if p.Artifacts == nil {
		p.Artifacts = db.Map{}
	}
	s := packed.Size
	p.Artifacts[ArtifactFlinkShape] = map[string]any{"spec_hash": domain.SpecHash(spec), "size": map[string]any{
		"job_manager_cpu": s.JobManagerCPU, "job_manager_memory": s.JobManagerMemory, "task_managers": s.TaskManagers,
		"task_manager_cpu": s.TaskManagerCPU, "task_manager_memory": s.TaskManagerMemory, "slots": s.Slots, "readers": s.Readers}}
}

func sizeFromArtifact(raw any) (engine.FlinkSize, bool) {
	m, ok := raw.(map[string]any)
	if !ok {
		return engine.FlinkSize{}, false
	}
	str := func(k string) string { s, _ := m[k].(string); return s }
	num := func(k string) int { return int(toFloat(m[k])) }
	s := engine.FlinkSize{JobManagerCPU: str("job_manager_cpu"), JobManagerMemory: str("job_manager_memory"), TaskManagers: num("task_managers"),
		TaskManagerCPU: str("task_manager_cpu"), TaskManagerMemory: str("task_manager_memory"), Slots: num("slots"), Readers: num("readers")}
	return s, s.TaskManagers > 0 && s.Readers > 0
}

// flinkReadersCheck blocks a deploy that changes a running Flink CDC pipeline's readers while it
// would restore a saved position: the position is split by reader, so it would not restore.
// After Start over (nothing left to restore) the readers can change. In the config section, so a
// forced deploy can't skip it.
func flinkReadersCheck(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec, capacity map[string]any) (*checks.Check, error) {
	if p.DeployedVersionID == nil {
		return nil, nil
	}
	deployed, err := SpecOf(ctx, q, p.DeployedVersionID)
	if err != nil {
		return nil, err
	}
	was, now := FlinkSizeOf(p, *deployed, capacity).Size.Parallelism(), FlinkSizeOf(p, spec, capacity).Size.Parallelism()
	if was == now {
		return nil, nil
	}
	check := &checks.Check{Code: "flink_readers_changed", Title: "Readers", Category: checks.CatResilience, Status: checks.Fail, Blocking: true,
		Message: fmt.Sprintf("This pipeline runs %d readers and this version asks for %d: its saved position holds what each reader had read, so it can't be restored with another number.", was, now),
		Fix:     &checks.Fix{Kind: "text", Who: "you", Code: fmt.Sprintf("Keep %d readers, or pause the pipeline and use Positions → Start over, then deploy.", was)}}
	left, known := positionLeft(ctx, p)
	if !known {
		check.Message += " BeziSync couldn't read the saved positions to see whether one is left."
		return check, nil
	}
	if !left {
		return &checks.Check{Code: "flink_readers_changed", Title: "Readers", Category: checks.CatResilience, Status: checks.Pass,
			Message: fmt.Sprintf("%d readers instead of %d: no saved position is left to restore, so the pipeline starts without one.", now, was)}, nil
	}
	return check, nil
}

// positionLeft reports whether p has a saved position above its Start over floor (known false
// when the state database can't be read: then nobody can say).
func positionLeft(ctx context.Context, p *models.Pipeline) (left, known bool) {
	conn, err := stateConn(ctx)
	if err != nil {
		return false, false
	}
	defer func() { _ = conn.Close(ctx) }()
	latest, err := latestSavedID(ctx, conn, p.ShortID())
	if err != nil {
		return false, false
	}
	return latest > restoreFloor(p), true
}

// ClusterPreview is how a custom cluster would be packed on a pipeline's runtime now: the shape,
// why, what it asks for in all and whether it fits the largest node (the fit check).
type ClusterPreview struct {
	Size  engine.FlinkSize
	Why   string
	Need  Need
	Node  engine.Node
	Check *checks.Check
}

// PreviewCluster packs c for p's runtime as a deploy would now (pure: nothing is stored).
func PreviewCluster(ctx context.Context, q db.Querier, p *models.Pipeline, c *domain.FlinkCluster) (*ClusterPreview, error) {
	var capacity map[string]any
	if p.RuntimeID != nil {
		rt, err := GetRuntime(ctx, q, *p.RuntimeID)
		if err != nil {
			return nil, err
		}
		capacity = rt.Capacity
	}
	spec := domain.DefaultSpec()
	spec.Policies.Resources, spec.FlinkCDC.Cluster = "custom", c
	packed := FlinkSizeOf(&models.Pipeline{Artifacts: db.Map{}}, spec, capacity)
	return &ClusterPreview{Size: packed.Size, Why: packed.Why, Need: FlinkNeed(packed.Size), Node: NodeOf(capacity),
		Check: flinkClusterFitCheck(spec, packed, capacity)}, nil
}
