package domain

import (
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Project policies: guardrails Admins set once so anyone can create pipelines safely.

var sizes = []string{"small", "medium", "large"}

// AskAdmin is the default fix of a policy failure.
const AskAdmin = "Ask an Admin to change the policy under Settings → Policies."

// ProjectPolicies mirrors the Python model; nil lists mean "any".
type ProjectPolicies struct {
	AllowedRuntimeIDs     []uuid.UUID `json:"allowed_runtime_ids"`
	AllowedSourceTypes    []string    `json:"allowed_source_types"`
	AllowedSinkTypes      []string    `json:"allowed_sink_types"`
	AllowedSnapshotModes  []string    `json:"allowed_snapshot_modes"`
	MaxResources          *string     `json:"max_resources"`
	AllowForceDeploy      bool        `json:"allow_force_deploy"`
	MaxPipelinesPerSource *int        `json:"max_pipelines_per_source"`
	RequireApproval       bool        `json:"require_approval"`
	// Added after the Python releases: dumped only when set, so stored policies and their
	// recorded dumps stay byte-identical.
	AllowScriptedTransforms bool     `json:"allow_scripted_transforms"`
	AllowCustomTransforms   bool     `json:"allow_custom_transforms"`
	AllowedTransforms       []string `json:"allowed_transforms"` // nil = every built-in type
	AllowedEngines          []string `json:"allowed_engines"`    // nil = every engine
	// AllowAdvancedProperties lets Experts pass raw Debezium properties (refused by preflight
	// otherwise). Dumped only when set.
	AllowAdvancedProperties bool `json:"allow_advanced_properties"`
}

// DefaultPolicies is ProjectPolicies().
func DefaultPolicies() ProjectPolicies {
	return ProjectPolicies{AllowForceDeploy: true}
}

// Dump is model_dump(mode="json") in declaration order.
func (p ProjectPolicies) Dump() *om.Map {
	var runtimes any
	if p.AllowedRuntimeIDs != nil {
		l := make([]any, len(p.AllowedRuntimeIDs))
		for i, id := range p.AllowedRuntimeIDs {
			l[i] = id.String()
		}
		runtimes = l
	}
	list := func(s []string) any {
		if s == nil {
			return nil
		}
		return strList(s)
	}
	var maxRes any
	if p.MaxResources != nil {
		maxRes = *p.MaxResources
	}
	var maxPipes any
	if p.MaxPipelinesPerSource != nil {
		maxPipes = *p.MaxPipelinesPerSource
	}
	out := om.New().
		Set("allowed_runtime_ids", runtimes).
		Set("allowed_source_types", list(p.AllowedSourceTypes)).
		Set("allowed_sink_types", list(p.AllowedSinkTypes)).
		Set("allowed_snapshot_modes", list(p.AllowedSnapshotModes)).
		Set("max_resources", maxRes).
		Set("allow_force_deploy", p.AllowForceDeploy).
		Set("max_pipelines_per_source", maxPipes).
		Set("require_approval", p.RequireApproval)
	if p.AllowScriptedTransforms {
		out.Set("allow_scripted_transforms", true)
	}
	if p.AllowCustomTransforms {
		out.Set("allow_custom_transforms", true)
	}
	if p.AllowedTransforms != nil {
		out.Set("allowed_transforms", strList(p.AllowedTransforms))
	}
	if p.AllowedEngines != nil {
		out.Set("allowed_engines", strList(p.AllowedEngines))
	}
	if p.AllowAdvancedProperties {
		out.Set("allow_advanced_properties", true)
	}
	return out
}

// MarshalJSON renders the policies in declaration order.
func (p ProjectPolicies) MarshalJSON() ([]byte, error) { return p.Dump().MarshalJSON() }

// ParsePolicies validates policies (API body or stored JSON) at loc.
func ParsePolicies(v *val.V, loc val.Loc, raw any) (ProjectPolicies, bool) {
	before := v.Count()
	p := DefaultPolicies()
	o := v.Model(loc, raw, "ProjectPolicies")
	if o.Failed {
		return p, false
	}
	optList := func(key string) []string {
		f := o.Get(key)
		if !f.Present || f.IsNull() {
			return nil
		}
		l, ok := v.StrList(f.Loc, f.Raw)
		if !ok {
			return nil
		}
		return l
	}
	if f := o.Get("allowed_runtime_ids"); f.Present && !f.IsNull() {
		if items, ok := v.List(f.Loc, f.Raw); ok {
			ids := make([]uuid.UUID, 0, len(items))
			for i, item := range items {
				if id, ok := v.UUID(f.Loc.Add(i), item); ok {
					ids = append(ids, id)
				}
			}
			p.AllowedRuntimeIDs = ids
		}
	}
	p.AllowedSourceTypes = optList("allowed_source_types")
	p.AllowedSinkTypes = optList("allowed_sink_types")
	p.AllowedSnapshotModes = optList("allowed_snapshot_modes")
	if f := o.Get("max_resources"); f.Present && !f.IsNull() {
		if s, ok := v.Literal(f.Loc, f.Raw, sizes...); ok {
			p.MaxResources = &s
		}
	}
	// Retired: personal data is protected per pipeline (Tables step), not by a project policy.
	// Stored policies and older clients may still send it; it is accepted and ignored.
	if f := o.Get("require_pii_masking"); f.Present {
		v.Bool(f.Loc, f.Raw)
	}
	if f := o.Get("allow_force_deploy"); f.Present {
		if b, ok := v.Bool(f.Loc, f.Raw); ok {
			p.AllowForceDeploy = b
		}
	}
	if f := o.Get("max_pipelines_per_source"); f.Present && !f.IsNull() {
		if n, ok := v.IntIn(f.Loc, f.Raw, val.Num{GE: val.F(1), LE: val.F(100)}); ok {
			p.MaxPipelinesPerSource = &n
		}
	}
	if f := o.Get("require_approval"); f.Present {
		if b, ok := v.Bool(f.Loc, f.Raw); ok {
			p.RequireApproval = b
		}
	}
	if f := o.Get("allow_scripted_transforms"); f.Present {
		if b, ok := v.Bool(f.Loc, f.Raw); ok {
			p.AllowScriptedTransforms = b
		}
	}
	if f := o.Get("allow_custom_transforms"); f.Present {
		if b, ok := v.Bool(f.Loc, f.Raw); ok {
			p.AllowCustomTransforms = b
		}
	}
	if f := o.Get("allow_advanced_properties"); f.Present {
		if b, ok := v.Bool(f.Loc, f.Raw); ok {
			p.AllowAdvancedProperties = b
		}
	}
	if f := o.Get("allowed_transforms"); f.Present && !f.IsNull() {
		if items, ok := v.List(f.Loc, f.Raw); ok {
			types := make([]string, 0, len(items))
			for i, item := range items {
				if t, ok := v.Literal(f.Loc.Add(i), item, BuiltInTransformTypes()...); ok {
					types = append(types, t)
				}
			}
			p.AllowedTransforms = types
		}
	}
	if f := o.Get("allowed_engines"); f.Present && !f.IsNull() {
		if items, ok := v.List(f.Loc, f.Raw); ok {
			ids := make([]string, 0, len(items))
			for i, item := range items {
				if id, ok := v.Literal(f.Loc.Add(i), item, engineIDs()...); ok {
					ids = append(ids, id)
				}
			}
			p.AllowedEngines = ids
		}
	}
	o.Forbid()
	return p, v.Count() == before
}

func engineIDs() []string {
	out := make([]string, len(engine.All))
	for i, id := range engine.All {
		out[i] = string(id)
	}
	return out
}

// EngineViolation is the policy failure of a pipeline on an engine the project does not allow
// (nil when allowed).
func EngineViolation(p ProjectPolicies, id string) *checks.Check {
	if id == "" || p.AllowedEngines == nil || slices.Contains(p.AllowedEngines, id) {
		return nil
	}
	c := policyFail("policy_engine", "Engine", fmt.Sprintf("This project does not allow pipelines on %s.", engine.ID(id).Title()), AskAdmin)
	return &c
}

// LoadPolicies reads stored policies; invalid stored data falls back to defaults, as
// Python's model_validate would have raised (callers there never stored invalid data).
func LoadPolicies(raw map[string]any) ProjectPolicies {
	if raw == nil {
		return DefaultPolicies()
	}
	p, ok := ParsePolicies(&val.V{}, nil, raw)
	if !ok {
		return DefaultPolicies()
	}
	return p
}

func policyFail(code, title, message, fix string) checks.Check {
	c := checks.Check{
		Code: code, Title: title, Status: checks.Fail, Category: checks.CatPolicy, Blocking: true, Message: message,
	}
	if fix != "" {
		who := "You"
		if fix == AskAdmin {
			who = "Admin"
		}
		c.Fix = &checks.Fix{Kind: "text", Code: fix, Who: who}
	}
	return c
}

// SaveViolations is what may not even be saved: the building blocks themselves.
func SaveViolations(p ProjectPolicies, runtimeID *uuid.UUID, sourceType, sinkType string) []checks.Check {
	var out []checks.Check
	if runtimeID != nil && p.AllowedRuntimeIDs != nil && !slices.Contains(p.AllowedRuntimeIDs, *runtimeID) {
		out = append(out, policyFail("policy_runtime", "Runtime", "This project does not allow pipelines on that runtime.", AskAdmin))
	}
	if sourceType != "" && p.AllowedSourceTypes != nil && !slices.Contains(p.AllowedSourceTypes, sourceType) {
		out = append(out, policyFail("policy_source_type", "Source type", fmt.Sprintf("This project does not allow %s sources.", sourceType), AskAdmin))
	}
	if sinkType != "" && p.AllowedSinkTypes != nil && !slices.Contains(p.AllowedSinkTypes, sinkType) {
		out = append(out, policyFail("policy_sink_type", "Destination type", fmt.Sprintf("This project does not allow %s destinations.", sinkType), AskAdmin))
	}
	return out
}

// EvaluatePolicies returns the policy preflight section of a Debezium pipeline.
func EvaluatePolicies(p ProjectPolicies, spec PipelineSpec, runtimeID *uuid.UUID, sourceType, sinkType string,
	otherPipelinesOnSource int) []checks.Check {
	return EvaluatePoliciesOn(p, spec, runtimeID, sourceType, sinkType, string(engine.Debezium), otherPipelinesOnSource)
}

// EvaluatePoliciesOn returns the policy preflight section of a pipeline on an engine.
func EvaluatePoliciesOn(p ProjectPolicies, spec PipelineSpec, runtimeID *uuid.UUID, sourceType, sinkType, engineID string,
	otherPipelinesOnSource int) []checks.Check {
	out := SaveViolations(p, runtimeID, sourceType, sinkType)
	if c := EngineViolation(p, engineID); c != nil {
		out = append(out, *c)
	}
	mode := spec.Snapshot.Mode
	if p.AllowedSnapshotModes != nil && !slices.Contains(p.AllowedSnapshotModes, mode) {
		out = append(out, policyFail("policy_snapshot_mode", "Snapshot mode",
			fmt.Sprintf("Snapshot mode '%s' is not allowed here. Allowed: %s.", mode, strings.Join(p.AllowedSnapshotModes, ", ")),
			"Pick an allowed snapshot mode in the pipeline settings."))
	}
	size := spec.Policies.Resources
	if p.MaxResources != nil && *p.MaxResources != "" && slices.Index(sizes, size) > slices.Index(sizes, *p.MaxResources) {
		out = append(out, policyFail("policy_resources", "Resources",
			fmt.Sprintf("The '%s' preset exceeds this project's limit (%s).", size, *p.MaxResources),
			fmt.Sprintf("Choose %s or smaller.", *p.MaxResources)))
	}
	out = append(out, transformPolicyChecks(p, spec)...)
	if !p.AllowAdvancedProperties && len(spec.Advanced) > 0 {
		keys := make([]string, 0, len(spec.Advanced))
		for k := range spec.Advanced {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		out = append(out, policyFail("policy_advanced_properties", "Advanced properties",
			fmt.Sprintf("This pipeline sets %d advanced Debezium propert%s (%s); this project does not allow them.",
				len(keys), map[bool]string{true: "y", false: "ies"}[len(keys) == 1], strings.Join(keys, ", ")), AskAdmin))
	}
	if !p.AllowScriptedTransforms {
		if steps := ScriptedSteps(spec); len(steps) > 0 {
			out = append(out, policyFail("policy_scripted_transform", "Scripted transforms",
				fmt.Sprintf("%s run(s) a script you wrote (%s); this project does not allow scripted transforms.",
					pluralSteps(len(steps)), strings.Join(steps, ", ")),
				AskAdmin))
		}
	}
	if limit := p.MaxPipelinesPerSource; limit != nil && otherPipelinesOnSource >= *limit {
		out = append(out, policyFail("policy_source_budget", "Pipelines per source",
			fmt.Sprintf("The source already feeds %d pipeline(s); this project allows %d. "+
				"Each one holds a replication slot or log reader on the database.", otherPipelinesOnSource, *limit),
			"Reuse an existing pipeline (add tables to it) or ask an admin to raise the limit."))
	}
	if len(out) == 0 {
		out = append(out, checks.Check{
			Code: "policy_ok", Title: "Project policies", Status: checks.Pass, Category: checks.CatPolicy,
			Message: "Meets every project policy.",
		})
	}
	return out
}
