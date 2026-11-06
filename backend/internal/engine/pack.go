package engine

import (
	"fmt"
	"strconv"
)

// Packing a custom Flink CDC cluster: the person picks the parallelism and what each slot needs;
// BeziSync puts the slots into the fewest TaskManagers the largest node can hold, at most
// MaxSlotsPerTaskManager each. Pure: the services resolve a shape once, at deploy, and store it,
// so a node joining or leaving never changes what a running pipeline compiles to.

// TaskManagerOverheadMiB is what a TaskManager needs besides its slots (framework, JVM).
const TaskManagerOverheadMiB = 512

// MaxSlotsPerTaskManager bounds a packed TaskManager.
const MaxSlotsPerTaskManager = 8

// Node is the largest node a cluster's containers can land on (0, 0: unknown).
type Node struct {
	CPUMilli  int
	MemoryMiB int
}

// Known reports whether the node's size is known.
func (n Node) Known() bool { return n.CPUMilli > 0 && n.MemoryMiB > 0 }

// Packed is a packed shape and why it has that many TaskManagers.
type Packed struct {
	Size FlinkSize
	// Why explains the count in words ("Fits on one node.").
	Why string
	// Fits is false when one slot alone needs more than the node offers.
	Fits bool
}

// Pack places parallelism slots of slotCPU (thousandths) and slotMiB onto TaskManagers, next to a
// JobManager of jmCPU and jmMemory (quantities as the spec writes them).
func Pack(parallelism, slotCPU, slotMiB int, jmCPU, jmMemory string, node Node) Packed {
	perTM, limit := MaxSlotsPerTaskManager, fmt.Sprintf("%d slots per TaskManager", MaxSlotsPerTaskManager)
	if node.Known() {
		byMem := (node.MemoryMiB - TaskManagerOverheadMiB) / slotMiB
		byCPU := node.CPUMilli / slotCPU
		if byMem < perTM {
			perTM, limit = byMem, "memory ("+quantityMiB(node.MemoryMiB)+")"
		}
		if byCPU < perTM {
			perTM, limit = byCPU, "CPU ("+quantityCPU(node.CPUMilli)+")"
		}
	}
	if perTM < 1 {
		return Packed{Size: FlinkSize{JobManagerCPU: jmCPU, JobManagerMemory: jmMemory, TaskManagers: parallelism, Slots: 1, Readers: parallelism,
			TaskManagerCPU: quantityCPU(slotCPU), TaskManagerMemory: quantityMiB(slotMiB + TaskManagerOverheadMiB)},
			Why: "One slot needs more than the largest node offers (" + quantityCPU(node.CPUMilli) + ", " + quantityMiB(node.MemoryMiB) + ")."}
	}
	tms := (parallelism + perTM - 1) / perTM
	slots := (parallelism + tms - 1) / tms
	why := "Fits on one node."
	if tms > 1 {
		why = fmt.Sprintf("Split across %d: one node holds at most %d such slots (limited by %s).", tms, perTM, limit)
	}
	if !node.Known() {
		why = "The nodes' size is unknown: packed up to " + strconv.Itoa(MaxSlotsPerTaskManager) + " slots per TaskManager."
	}
	return Packed{Fits: true, Why: why, Size: FlinkSize{JobManagerCPU: jmCPU, JobManagerMemory: jmMemory, TaskManagers: tms, Slots: slots, Readers: parallelism,
		TaskManagerCPU: quantityCPU(slots * slotCPU), TaskManagerMemory: quantityMiB(slots*slotMiB + TaskManagerOverheadMiB)}}
}

// cpu writes thousandths as a Kubernetes quantity ("1500m" → "1.5", "2000" → "2").
func quantityCPU(milli int) string {
	if milli%1000 == 0 {
		return strconv.Itoa(milli / 1000)
	}
	return strconv.FormatFloat(float64(milli)/1000, 'f', -1, 64)
}

// mib writes MiB as a Kubernetes quantity ("2Gi", "1536Mi").
func quantityMiB(n int) string {
	if n%1024 == 0 {
		return strconv.Itoa(n/1024) + "Gi"
	}
	return strconv.Itoa(n) + "Mi"
}
