package services

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
)

// Placement profiles: a Kubernetes runtime's Admins define where pipelines may run (node selector,
// tolerations, priority class, labels); a pipeline names one in spec.placement. The profile is
// resolved when the pipeline deploys and stored (ArtifactPlacement), so editing a profile changes
// a running pipeline only at its next deploy, like a node joining never repacks one (I3).

// ArtifactPlacement is the placement profile a pipeline was deployed with, for its spec's hash.
const ArtifactPlacement = "placement"

// ProfilesOf reads a runtime's placement profiles.
func ProfilesOf(rt *models.Runtime) []runtime.Placement {
	if rt == nil || len(rt.PlacementProfiles) == 0 {
		return nil
	}
	raw, err := json.Marshal([]any(rt.PlacementProfiles))
	if err != nil {
		return nil
	}
	var out []runtime.Placement
	if json.Unmarshal(raw, &out) != nil {
		return nil
	}
	return out
}

func profileNamed(profiles []runtime.Placement, name string) *runtime.Placement {
	for i := range profiles {
		if profiles[i].Name == name {
			return &profiles[i]
		}
	}
	return nil
}

// PlacementOf is where p's pods run for spec: the profile stored when this spec deployed, or the
// runtime's profile of that name now (nil: none, or a profile that no longer exists).
func PlacementOf(p *models.Pipeline, spec domain.PipelineSpec, rt *models.Runtime) *runtime.Placement {
	if spec.Placement == "" {
		return nil
	}
	if stored, ok := p.Artifacts[ArtifactPlacement].(map[string]any); ok && stored["spec_hash"] == domain.SpecHash(spec) {
		raw, _ := json.Marshal(stored["profile"])
		var pl runtime.Placement
		if json.Unmarshal(raw, &pl) == nil && pl.Name == spec.Placement {
			return &pl
		}
	}
	return profileNamed(ProfilesOf(rt), spec.Placement)
}

// applyPlacement records the profile p deploys with (the caller saves p).
func applyPlacement(p *models.Pipeline, spec domain.PipelineSpec, rt *models.Runtime) {
	fresh := *p
	fresh.Artifacts = db.Map{}
	pl := PlacementOf(&fresh, spec, rt)
	if pl == nil {
		delete(p.Artifacts, ArtifactPlacement)
		return
	}
	if p.Artifacts == nil {
		p.Artifacts = db.Map{}
	}
	var profile map[string]any
	raw, _ := json.Marshal(pl)
	_ = json.Unmarshal(raw, &profile)
	p.Artifacts[ArtifactPlacement] = map[string]any{"spec_hash": domain.SpecHash(spec), "profile": profile}
}

// CapacityFor is the runtime's capacity as a pipeline with this placement sees it: the largest
// node it can land on (its node selector and tolerations) instead of the runtime's largest.
// Unknown (no node size) when the runtime lists no nodes; the runtime's own without a placement.
func CapacityFor(capacity map[string]any, pl *runtime.Placement) map[string]any {
	if pl == nil || capacity == nil {
		return capacity
	}
	out := map[string]any{}
	for k, v := range capacity {
		out[k] = v
	}
	delete(out, "node_cpu_max")
	delete(out, "node_memory_max")
	if _, listed := capacity["nodes"]; !listed {
		return out
	}
	var cpu float64
	var mem int64
	for _, n := range runtime.NodesOf(capacity) {
		if pl.Accepts(n) {
			cpu, mem = max(cpu, n.CPU), max(mem, n.Memory)
		}
	}
	if cpu > 0 {
		out["node_cpu_max"], out["node_memory_max"] = cpu, mem
	}
	out["placement_nodes"] = cpu > 0
	return out
}

// RuntimeCapacityFor is CapacityFor with the placement p would run spec with on rt.
func RuntimeCapacityFor(p *models.Pipeline, spec domain.PipelineSpec, rt *models.Runtime) map[string]any {
	if rt == nil {
		return nil
	}
	return CapacityFor(rt.Capacity, PlacementOf(p, spec, rt))
}

// placementCheck: a pipeline's placement must name one of its runtime's profiles, on a Kubernetes
// runtime, with nodes it can land on (R36), applied by an agent that knows placement (R20).
func placementCheck(p *models.Pipeline, spec domain.PipelineSpec, rt *models.Runtime) *checks.Check {
	if spec.Placement == "" {
		return nil
	}
	title := "Placement"
	fail := func(msg, fix string) *checks.Check {
		return &checks.Check{Code: "placement", Title: title, Category: checks.CatCapacity, Status: checks.Fail, Blocking: true, Message: msg,
			Fix: &checks.Fix{Kind: "text", Who: "you", Code: fix}}
	}
	if rt.Kind != "kubernetes" {
		return fail("Placement profiles are for Kubernetes runtimes; "+rt.Name+" isn't one.", "Remove the placement, or choose a Kubernetes runtime.")
	}
	pl := PlacementOf(p, spec, rt)
	if pl == nil {
		names := []string{}
		for _, x := range ProfilesOf(rt) {
			names = append(names, x.Name)
		}
		have := "it has none"
		if len(names) > 0 {
			have = "it has " + strings.Join(names, ", ")
		}
		return fail(fmt.Sprintf("Runtime %s has no placement profile %q (%s).", rt.Name, spec.Placement, have), "Choose one of the runtime's profiles in Review.")
	}
	if rt.Mode == "agent" && rt.AgentState != nil && rt.AgentState["version"] != nil && !hasFeature(rt.AgentState, runtime.FeaturePlacement) {
		return &checks.Check{Code: "placement", Title: title, Category: checks.CatCapacity, Status: checks.Fail, Blocking: true,
			Message: fmt.Sprintf("The agent on %s (%v) doesn't apply placement profiles: the pods would run anywhere.", rt.Name, rt.AgentState["version"]),
			Fix:     &checks.Fix{Kind: "text", Who: "runtime admin", Code: "Upgrade the agent (Settings → Runtimes shows the install command)."}}
	}
	capacity := CapacityFor(rt.Capacity, pl)
	switch fits, known := capacity["placement_nodes"].(bool); {
	case !known:
		return &checks.Check{Code: "placement", Title: title, Category: checks.CatCapacity, Status: checks.Unknown,
			Message: fmt.Sprintf("Profile %s. The runtime doesn't list its nodes, so BeziSync can't check any node takes these pods.", pl.Name)}
	case !fits:
		return fail(fmt.Sprintf("No schedulable node of %s matches profile %s (its node selector and tolerations): the pods would never start.", rt.Name, pl.Name),
			"Ask the runtime's Admin to fix the profile, or label or untaint nodes for it.")
	}
	return &checks.Check{Code: "placement", Title: title, Category: checks.CatCapacity, Status: checks.Pass,
		Message: fmt.Sprintf("Profile %s: its largest node has %s CPU and %d MiB.", pl.Name, cpuWords(toFloat(capacity["node_cpu_max"])), int(toFloat(capacity["node_memory_max"])/(1<<20)))}
}

// ProfilesInUse lists the pipelines (current or deployed version) on rt that name each profile.
func ProfilesInUse(ctx context.Context, q db.Querier, rt *models.Runtime) (map[string][]string, error) {
	rows, err := q.Query(ctx, `SELECT p.name, v.spec->>'placement' FROM pipelines p
		JOIN pipeline_versions v ON v.id IN (p.current_version_id, p.deployed_version_id)
		WHERE p.runtime_id = $1 AND p.deleted_at IS NULL AND v.spec ? 'placement'`, rt.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var name, profile string
		if err := rows.Scan(&name, &profile); err != nil {
			return nil, err
		}
		if !slices.Contains(out[profile], name) {
			out[profile] = append(out[profile], name)
		}
	}
	return out, rows.Err()
}

// ValidateProfiles checks a runtime's new placement profiles: well-formed (R38), only on
// Kubernetes (R37), and none that pipelines use removed (R35).
func ValidateProfiles(ctx context.Context, q db.Querier, rt *models.Runtime, kind string, profiles []runtime.Placement) error {
	if len(profiles) > 0 && kind != "kubernetes" {
		return problem.New(422, "placement_profiles", "Placement profiles are for Kubernetes runtimes.")
	}
	if bad := runtime.ValidatePlacement(profiles); len(bad) > 0 {
		errs := make([]map[string]any, len(bad))
		for i, b := range bad {
			loc, msg, _ := strings.Cut(b, ": ")
			errs[i] = map[string]any{"loc": append([]string{"placement_profiles"}, strings.Split(loc, ".")...), "message": msg}
		}
		return problem.New(422, "placement_profiles", "Some placement profiles can't be used.").WithErrors(errs)
	}
	if rt == nil {
		return nil
	}
	used, err := ProfilesInUse(ctx, q, rt)
	if err != nil {
		return err
	}
	var gone []string
	for name, pipelines := range used {
		if profileNamed(profiles, name) == nil {
			slices.Sort(pipelines)
			gone = append(gone, fmt.Sprintf("%s (used by %s)", name, strings.Join(pipelines, ", ")))
		}
	}
	if len(gone) > 0 {
		slices.Sort(gone)
		return problem.Conflict("placement_in_use", "These placement profiles are in use: "+strings.Join(gone, "; ")+". Move the pipelines to another profile first.", nil)
	}
	return nil
}
