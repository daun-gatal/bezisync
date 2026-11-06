package services

import (
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
