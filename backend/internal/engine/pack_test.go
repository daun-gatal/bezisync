package engine

import (
	"strings"
	"testing"
)

func TestPack(t *testing.T) {
	big := Node{CPUMilli: 16000, MemoryMiB: 65536}
	cases := []struct {
		name                 string
		parallelism, cpu, mi int
		node                 Node
		tms, slots           int
		tmCPU, tmMem, why    string
		fits                 bool
	}{
		{"one node holds all", 4, 1000, 1024, big, 1, 4, "4", "4608Mi", "Fits on one node.", true},
		{"capped at 8 slots", 12, 500, 512, big, 2, 6, "3", "3584Mi", "at most 8 such slots (limited by 8 slots per TaskManager)", true},
		{"split by memory", 4, 500, 2048, Node{CPUMilli: 8000, MemoryMiB: 4608}, 2, 2, "1", "4608Mi", "limited by memory (4608Mi)", true},
		{"split by CPU", 6, 1500, 1024, Node{CPUMilli: 4000, MemoryMiB: 65536}, 3, 2, "3", "2560Mi", "limited by CPU (4)", true},
		{"uneven split stays even", 5, 1000, 1024, Node{CPUMilli: 2000, MemoryMiB: 65536}, 3, 2, "2", "2560Mi", "Split across 3", true},
		{"unknown node", 3, 250, 512, Node{}, 1, 3, "0.75", "2Gi", "size is unknown", true},
		{"a slot bigger than any node", 2, 4000, 1024, Node{CPUMilli: 2000, MemoryMiB: 65536}, 2, 1, "4", "1536Mi", "One slot needs more", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := Pack(c.parallelism, c.cpu, c.mi, "1", "1Gi", c.node)
			s := p.Size
			if p.Fits != c.fits || s.TaskManagers != c.tms || s.Slots != c.slots || s.TaskManagerCPU != c.tmCPU || s.TaskManagerMemory != c.tmMem || !strings.Contains(p.Why, c.why) {
				t.Fatalf("%+v %q", s, p.Why)
			}
			// Never fewer slots than readers, and the readers are the parallelism asked for.
			if s.Readers != c.parallelism || s.Parallelism() != c.parallelism || s.TaskManagers*s.SlotsPerTaskManager() < c.parallelism {
				t.Fatalf("%+v", s)
			}
			if s.JobManagerCPU != "1" || s.JobManagerMemory != "1Gi" {
				t.Fatalf("%+v", s)
			}
		})
	}
}

// The presets keep their shape: no slots or readers of their own (their config hash is pinned).
func TestPresetsHaveNoCustomShape(t *testing.T) {
	for name, s := range FlinkSizes {
		if s.Slots != 0 || s.Readers != 0 || s.SlotsPerTaskManager() != FlinkSlots || s.Parallelism() != s.TaskManagers*FlinkSlots {
			t.Fatal(name, s)
		}
	}
}
