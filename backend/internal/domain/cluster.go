package domain

import (
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// A Flink CDC pipeline at a custom size (policies.resources "custom") describes its cluster in
// flink_cdc.cluster: the parallelism (readers while copying, writers always), what each slot
// needs, the JobManager, and either an automatic shape (BeziSync packs the slots into the fewest
// TaskManagers a node can hold) or a manual one.

// FlinkCluster is a Flink CDC pipeline's custom cluster.
type FlinkCluster struct {
	Parallelism int      `json:"parallelism"`
	Slot        SlotSize `json:"slot"`
	JobManager  SlotSize `json:"job_manager"`
	// Shape is "auto" or "manual" (TaskManagers then set).
	Shape        string              `json:"shape"`
	TaskManagers *ManualTaskManagers `json:"task_managers"`
}

// SlotSize is CPU and memory, in the canonical forms of CustomResources.
type SlotSize struct {
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
}

// ManualTaskManagers is a manual shape.
type ManualTaskManagers struct {
	Count  int    `json:"count"`
	Slots  int    `json:"slots"`
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
}

// Cluster bounds.
const (
	MaxParallelism    = 128 // pipeline.max-parallelism: what saved state can be split into
	MaxSlotsPerTM     = 8
	MinSlotMemory     = 512
	MaxSlotMemory     = 8192
	MinSlotCPU        = 250
	MaxSlotCPU        = 4000
	MinJobManagerMem  = 768
	MaxJobManagerMem  = 8192
	MaxJobManagerCPU  = 2000
	MaxTaskManagers   = 16
	MinTaskManagerMem = 1024
	MaxTaskManagerMem = 65536
	MaxTaskManagerCPU = 16000
)

func (c *FlinkCluster) clone() *FlinkCluster {
	if c == nil {
		return nil
	}
	out := *c
	if c.TaskManagers != nil {
		tm := *c.TaskManagers
		out.TaskManagers = &tm
	}
	return &out
}

// sizeIn reads a SlotSize within bounds (CPU thousandths, memory MiB).
func sizeIn(v *val.V, loc val.Loc, raw any, name string, cpuMin, cpuMax, memMin, memMax int) SlotSize {
	o := v.Model(loc, raw, name)
	if o.Failed {
		return SlotSize{}
	}
	out := sizeFields(v, o, cpuMin, cpuMax, memMin, memMax)
	o.Forbid()
	return out
}

// sizeFields reads an object's cpu and memory within bounds; the caller forbids other keys.
func sizeFields(v *val.V, o *val.Object, cpuMin, cpuMax, memMin, memMax int) SlotSize {
	var out SlotSize
	if f := o.Get("cpu"); !f.Present {
		v.Fail(f.Loc, "missing", "Field required")
	} else if s, ok := v.Str(f.Loc, f.Raw); ok {
		if n, good := ParseCPUAny(s); !good || n < cpuMin || n > cpuMax {
			v.ValueError(f.Loc, "CPU is "+FormatCPU(cpuMin)+" to "+FormatCPU(cpuMax)+" in quarters, like 1.5")
		}
		out.CPU = s
	}
	if f := o.Get("memory"); !f.Present {
		v.Fail(f.Loc, "missing", "Field required")
	} else if s, ok := v.Str(f.Loc, f.Raw); ok {
		if n, good := ParseMemoryAny(s); !good || n < memMin || n > memMax {
			v.ValueError(f.Loc, "memory is "+FormatMemory(memMin)+" to "+FormatMemory(memMax)+" in steps of 256Mi, whole GiB written as Gi")
		}
		out.Memory = s
	}
	return out
}

func decodeCluster(v *val.V, loc val.Loc, raw any) *FlinkCluster {
	o := v.Model(loc, raw, "FlinkCluster")
	if o.Failed {
		return nil
	}
	out := &FlinkCluster{Shape: "auto"}
	if f := o.Get("parallelism"); !f.Present {
		v.Fail(f.Loc, "missing", "Field required")
	} else if n, ok := v.IntIn(f.Loc, f.Raw, val.Num{GE: val.F(1), LE: val.F(MaxParallelism)}); ok {
		out.Parallelism = n
	}
	if f := o.Get("slot"); !f.Present {
		v.Fail(f.Loc, "missing", "Field required")
	} else {
		out.Slot = sizeIn(v, f.Loc, f.Raw, "SlotSize", MinSlotCPU, MaxSlotCPU, MinSlotMemory, MaxSlotMemory)
	}
	if f := o.Get("job_manager"); !f.Present {
		v.Fail(f.Loc, "missing", "Field required")
	} else {
		out.JobManager = sizeIn(v, f.Loc, f.Raw, "SlotSize", MinSlotCPU, MaxJobManagerCPU, MinJobManagerMem, MaxJobManagerMem)
	}
	if f := o.Get("shape"); f.Present {
		if s, ok := v.Literal(f.Loc, f.Raw, "auto", "manual"); ok {
			out.Shape = s
		}
	}
	if f := o.Get("task_managers"); f.Present && !f.IsNull() {
		t := v.Model(f.Loc, f.Raw, "ManualTaskManagers")
		if !t.Failed {
			tm := &ManualTaskManagers{}
			if g := t.Get("count"); g.Present {
				if n, ok := v.IntIn(g.Loc, g.Raw, val.Num{GE: val.F(1), LE: val.F(MaxTaskManagers)}); ok {
					tm.Count = n
				}
			} else {
				v.Fail(g.Loc, "missing", "Field required")
			}
			if g := t.Get("slots"); g.Present {
				if n, ok := v.IntIn(g.Loc, g.Raw, val.Num{GE: val.F(1), LE: val.F(MaxSlotsPerTM)}); ok {
					tm.Slots = n
				}
			} else {
				v.Fail(g.Loc, "missing", "Field required")
			}
			size := sizeFields(v, t, 500, MaxTaskManagerCPU, MinTaskManagerMem, MaxTaskManagerMem)
			tm.CPU, tm.Memory = size.CPU, size.Memory
			t.Forbid()
			out.TaskManagers = tm
		}
	}
	o.Forbid()
	switch {
	case out.Shape == "manual" && out.TaskManagers == nil:
		v.ValueError(o.Loc().Add("task_managers"), "a manual shape needs task_managers")
	case out.Shape == "auto" && out.TaskManagers != nil:
		v.ValueError(o.Loc().Add("task_managers"), "task_managers is for a manual shape only")
	case out.TaskManagers != nil && out.TaskManagers.Count > 0 && out.TaskManagers.Slots > 0:
		tm := out.TaskManagers
		if tm.Count*tm.Slots < out.Parallelism {
			v.ValueError(o.Loc().Add("task_managers"), "parallelism needs that many slots: add TaskManagers or slots")
		}
		if mem, ok := ParseMemoryAny(tm.Memory); ok && mem/tm.Slots < MinSlotMemory {
			v.ValueError(o.Loc().Add("task_managers", "memory"), "each slot needs at least 512Mi: add memory or remove slots")
		}
	}
	return out
}

func dumpCluster(c *FlinkCluster) *om.Map {
	size := func(s SlotSize) *om.Map { return om.New().Set("cpu", s.CPU).Set("memory", s.Memory) }
	m := om.New().Set("parallelism", c.Parallelism).Set("slot", size(c.Slot)).Set("job_manager", size(c.JobManager)).Set("shape", c.Shape)
	if c.TaskManagers != nil {
		tm := c.TaskManagers
		m.Set("task_managers", om.New().Set("count", tm.Count).Set("slots", tm.Slots).Set("cpu", tm.CPU).Set("memory", tm.Memory))
	}
	return m
}
