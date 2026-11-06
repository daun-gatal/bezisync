package services

import (
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
)

func TestCapacityCheck(t *testing.T) {
	need := Need{CPU: 1, MiB: 2048}
	room := map[string]any{"cpu_allocatable": 8.0, "cpu_requested": 2.0, "memory_allocatable": float64(16 << 30), "memory_requested": float64(4 << 30)}
	with := func(base map[string]any, kv ...any) map[string]any {
		out := map[string]any{}
		for k, v := range base {
			out[k] = v
		}
		for i := 0; i < len(kv); i += 2 {
			out[kv[i].(string)] = kv[i+1]
		}
		return out
	}
	for name, c := range map[string]struct {
		capacity      map[string]any
		need, runs    Need
		status        string
		blocking      bool
		wantInMessage string
	}{
		// R23: no measurement is never a pass.
		"nothing reported":  {nil, need, Need{}, checks.Unknown, false, "can't tell"},
		"no CPU figure":     {map[string]any{"workloads": 2}, need, Need{}, checks.Unknown, false, "can't tell"},
		"room":              {room, need, Need{}, checks.Pass, false, "6.0 CPU and 12288 MiB free; this pipeline asks for 1 CPU and 2048 MiB."},
		"short of CPU":      {with(room, "cpu_requested", 7.5), need, Need{}, checks.Warn, false, "has 0.5 CPU and 12288 MiB free"},
		"short of memory":   {with(room, "memory_requested", float64(15<<30)), need, Need{}, checks.Warn, false, "1024 MiB free"},
		"docker: no memory": {map[string]any{"cpu_allocatable": 4.0}, need, Need{}, checks.Pass, false, "4.0 CPU free"},
		// R21: no quota passes, and says so.
		"no quota":     {with(room, "quota", false), need, Need{}, checks.Pass, false, "no ResourceQuota"},
		"within quota": {with(room, "quota", true, "quota_cpu_free", 2.0, "quota_memory_free", float64(4<<30)), need, Need{}, checks.Pass, false, "Within the namespace's ResourceQuota"},
		// R22: over the quota blocks, whatever the nodes have.
		"over quota CPU":    {with(room, "quota", true, "quota_cpu_free", 0.5), need, Need{}, checks.Fail, true, "leaves 0.50 CPU"},
		"over quota memory": {with(room, "quota", true, "quota_memory_free", float64(1<<30)), need, Need{}, checks.Fail, true, "leaves 1024 MiB"},
		// A redeploy needs only what it adds.
		"redeploy adds less": {with(room, "quota", true, "quota_cpu_free", 0.5, "quota_memory_free", float64(1<<30)), Need{CPU: 1.5, MiB: 3072}, need, checks.Pass, false, "0.5 CPU and 1024 MiB more than it runs now"},
		"redeploy shrinks":   {with(room, "quota", true, "quota_cpu_free", 0.0, "quota_memory_free", 0.0), Need{CPU: 0.5, MiB: 768}, need, checks.Pass, false, "0 CPU and 0 MiB more"},
	} {
		got := capacityCheck(c.capacity, c.need, c.runs, "")
		if got == nil || got.Status != c.status || got.Blocking != c.blocking || !strings.Contains(got.Message, c.wantInMessage) {
			t.Errorf("%s: %+v", name, got)
		}
	}
}
