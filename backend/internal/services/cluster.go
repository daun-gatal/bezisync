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
// for this spec (the streaming shape once it shrank after its first copy), or packed now for the
// first copy on the runtime's nodes.
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
	if stored := storedShape(p, spec); stored != nil {
		if stored["phase"] == PhaseStreaming {
			if size, ok := sizeFromArtifact(stored["streaming_size"]); ok {
				return engine.Packed{Size: size, Fits: true, Why: "Shrunk after the first copy."}
			}
		}
		if size, ok := sizeFromArtifact(stored["size"]); ok {
			return engine.Packed{Size: size, Fits: true, Why: "Packed at deploy."}
		}
	}
	return packSlot(c, c.Slot, capacity)
}

// StreamingSizeOf is the shape a custom cluster shrinks to after its first copy, packed now
// (false when it has no streaming slot size).
func StreamingSizeOf(spec domain.PipelineSpec, capacity map[string]any) (engine.Packed, bool) {
	c := spec.FlinkCDC.Cluster
	if spec.Policies.Resources != "custom" || c == nil || c.Shape == "manual" || c.StreamingSlot == nil {
		return engine.Packed{}, false
	}
	return packSlot(c, *c.StreamingSlot, capacity), true
}

func packSlot(c *domain.FlinkCluster, slot domain.SlotSize, capacity map[string]any) engine.Packed {
	cpu, _ := domain.ParseCPUAny(slot.CPU)
	mem, _ := domain.ParseMemoryAny(slot.Memory)
	return engine.Pack(c.Parallelism, cpu, mem, c.JobManager.CPU, c.JobManager.Memory, NodeOf(capacity))
}

// Phases of a custom Flink CDC cluster with a streaming slot size.
const (
	PhaseCopy      = "copy"
	PhaseStreaming = "streaming"
	// MaxShrinkFailures: after this many shrinks that failed, BeziSync stops trying (R31).
	MaxShrinkFailures = 3
)

// storedShape is the flink_shape artifact when it was packed for this spec (nil otherwise).
func storedShape(p *models.Pipeline, spec domain.PipelineSpec) map[string]any {
	stored, ok := p.Artifacts[ArtifactFlinkShape].(map[string]any)
	if !ok || stored["spec_hash"] != domain.SpecHash(spec) {
		return nil
	}
	return stored
}

// applyFlinkShape records in p's artifacts the shapes a custom cluster is deployed with, packed now:
// for its first copy and, with a streaming slot size, for once it has copied. A redeploy that
// copies nothing new keeps a cluster that already shrank on its streaming shape; one that adds
// tables (copied while it streams) grows back first. Presets and manual shapes store nothing.
// The caller saves p.
func applyFlinkShape(p *models.Pipeline, spec domain.PipelineSpec, capacity map[string]any, keepStreaming bool) {
	c := spec.FlinkCDC.Cluster
	previous, _ := p.Artifacts[ArtifactFlinkShape].(map[string]any)
	if spec.Policies.Resources != "custom" || c == nil || c.Shape == "manual" {
		delete(p.Artifacts, ArtifactFlinkShape)
		return
	}
	if p.Artifacts == nil {
		p.Artifacts = db.Map{}
	}
	shape := map[string]any{"spec_hash": domain.SpecHash(spec), "size": sizeMap(packSlot(c, c.Slot, capacity).Size), "phase": PhaseCopy}
	if streaming, ok := StreamingSizeOf(spec, capacity); ok {
		shape["streaming_size"] = sizeMap(streaming.Size)
		if keepStreaming && previous != nil && previous["phase"] == PhaseStreaming {
			for _, k := range []string{"phase", "copy_done_save", "shrunk_at"} {
				if v, ok := previous[k]; ok {
					shape[k] = v
				}
			}
		}
	}
	p.Artifacts[ArtifactFlinkShape] = shape
}

// GrowBack puts a custom cluster that shrank back on its copy shape before a later copy (Start
// over with a copy, restoring a position saved during the copy). Reports whether it changed.
func GrowBack(p *models.Pipeline) bool {
	stored, ok := p.Artifacts[ArtifactFlinkShape].(map[string]any)
	if !ok || stored["streaming_size"] == nil {
		return false
	}
	changed := stored["phase"] != PhaseCopy || stored["streaming_since"] != nil
	next := map[string]any{}
	for k, v := range stored {
		next[k] = v
	}
	next["phase"] = PhaseCopy
	for _, k := range []string{"streaming_since", "copy_done_save", "shrunk_at", "shrink_ready"} {
		delete(next, k)
	}
	artifacts := cloneArtifacts(p)
	artifacts[ArtifactFlinkShape] = next
	p.Artifacts = artifacts
	return changed
}

// CopyDoneSave is the saved position whose save let a cluster shrink (0 when it hasn't): a
// position before it was saved during the copy.
func CopyDoneSave(p *models.Pipeline) int64 {
	stored, _ := p.Artifacts[ArtifactFlinkShape].(map[string]any)
	return int64(toFloat(stored["copy_done_save"]))
}

func sizeMap(s engine.FlinkSize) map[string]any {
	return map[string]any{"job_manager_cpu": s.JobManagerCPU, "job_manager_memory": s.JobManagerMemory, "task_managers": s.TaskManagers,
		"task_manager_cpu": s.TaskManagerCPU, "task_manager_memory": s.TaskManagerMemory, "slots": s.Slots, "readers": s.Readers}
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
	// Streaming is the shape once the first copy is done (nil without a streaming slot size).
	Streaming *engine.Packed
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
	out := &ClusterPreview{Size: packed.Size, Why: packed.Why, Need: FlinkNeed(packed.Size), Node: NodeOf(capacity),
		Check: flinkClusterFitCheck(spec, packed, capacity)}
	if s, ok := StreamingSizeOf(spec, capacity); ok {
		out.Streaming = &s
	}
	return out, nil
}
