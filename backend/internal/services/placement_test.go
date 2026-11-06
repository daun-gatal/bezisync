package services

import (
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

func TestPlacementCheckAndCapacity(t *testing.T) {
	profile := map[string]any{"name": "cdc", "node_selector": map[string]any{"pool": "cdc"}}
	nodes := []any{map[string]any{"labels": map[string]any{"pool": "web"}, "cpu": 32.0, "memory": float64(128 << 30)},
		map[string]any{"labels": map[string]any{"pool": "cdc"}, "cpu": 4.0, "memory": float64(8 << 30)}}
	rt := &models.Runtime{Name: "k8s", Kind: "kubernetes", Mode: "agent", PlacementProfiles: db.List{profile},
		AgentState: db.Map{"version": "0.3.0", "features": []any{"health_port"}},
		Capacity:   db.Map{"node_cpu_max": 32.0, "node_memory_max": float64(128 << 30), "nodes": nodes}}
	spec := domain.DefaultSpec()
	spec.Placement = "cdc"
	p := &models.Pipeline{Artifacts: db.Map{}}
	// R20: an agent that predates placement would put the pods anywhere.
	if c := placementCheck(p, spec, rt); c == nil || c.Status != checks.Fail || !strings.Contains(c.Message, "doesn't apply placement profiles") {
		t.Fatal(c)
	}
	rt.AgentState["features"] = []any{"health_port", "placement"}
	if c := placementCheck(p, spec, rt); c == nil || c.Status != checks.Pass {
		t.Fatal(c)
	}
	// R12: a profile packs for the largest node it can land on, not the runtime's largest.
	got := RuntimeCapacityFor(p, spec, rt)
	if got["node_cpu_max"] != 4.0 || got["node_memory_max"] != int64(8<<30) || rt.Capacity["node_cpu_max"] != 32.0 {
		t.Fatal(got)
	}
	if n := NodeOf(got); n.CPUMilli != 4000 || n.MemoryMiB != 8192 {
		t.Fatal(n)
	}
	// Without a placement: the runtime's own capacity.
	if got := RuntimeCapacityFor(p, domain.DefaultSpec(), rt); got["node_cpu_max"] != 32.0 {
		t.Fatal(got)
	}
	// Docker runtimes have no profiles.
	if c := placementCheck(p, spec, &models.Runtime{Name: "dock", Kind: "docker"}); c == nil || c.Status != checks.Fail || !strings.Contains(c.Message, "Kubernetes runtimes") {
		t.Fatal(c)
	}
	// The profile a pipeline deployed with is the one it keeps for that spec.
	applyPlacement(p, spec, rt)
	rt.PlacementProfiles = db.List{map[string]any{"name": "cdc", "node_selector": map[string]any{"pool": "other"}}}
	if pl := PlacementOf(p, spec, rt); pl == nil || pl.NodeSelector["pool"] != "cdc" {
		t.Fatal(pl)
	}
	edited := spec
	edited.Policies.LatencyTargetSeconds = 99
	if pl := PlacementOf(p, edited, rt); pl == nil || pl.NodeSelector["pool"] != "other" {
		t.Fatal("a new version takes the profile as it is now", pl)
	}
}
