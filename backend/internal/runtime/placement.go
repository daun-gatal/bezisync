package runtime

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// Placement is where a pipeline's pods may run on a Kubernetes runtime: a placement profile the
// runtime's Admins define and a pipeline picks by name.
type Placement struct {
	Name          string            `json:"name"`
	NodeSelector  map[string]string `json:"node_selector,omitempty"`
	Tolerations   []Toleration      `json:"tolerations,omitempty"`
	PriorityClass string            `json:"priority_class,omitempty"`
	// Labels are added to the pods (never over BeziSync's own).
	Labels map[string]string `json:"labels,omitempty"`
}

// Toleration lets pods land on nodes with a matching taint.
type Toleration struct {
	Key      string `json:"key"`
	Operator string `json:"operator"` // Equal | Exists
	Value    string `json:"value,omitempty"`
	Effect   string `json:"effect,omitempty"` // NoSchedule | PreferNoSchedule | NoExecute; "": any
}

// FeaturePlacement is what an agent that applies placement reports in its features.
const FeaturePlacement = "placement"

// MaxPlacementProfiles bounds a runtime's profiles; each map holds at most 20 entries.
const MaxPlacementProfiles = 20

var (
	labelNameRe  = regexp.MustCompile(`^([A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?)$`)
	dnsSubRe     = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$`)
	labelValueRe = regexp.MustCompile(`^(([A-Za-z0-9][-A-Za-z0-9_.]{0,61})?[A-Za-z0-9])?$`)
	profileRe    = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
)

// validLabelKey is a Kubernetes qualified name: an optional DNS-subdomain prefix and a name.
func validLabelKey(k string) bool {
	prefix, name, has := strings.Cut(k, "/")
	if !has {
		name, prefix = prefix, ""
	}
	return labelNameRe.MatchString(name) && (prefix == "" || (len(prefix) <= 253 && dnsSubRe.MatchString(prefix)))
}

// templated: placement values are taken as they are, never expanded (R38).
func templated(s string) bool { return strings.Contains(s, "${") || strings.Contains(s, "{{") }

// ownLabel: BeziSync's own pod labels, which a profile can't set.
func ownLabel(k string) bool {
	return strings.HasPrefix(k, "app.bezisync.io/") || k == Label || k == ManagedBy || k == "app"
}

// ValidatePlacement lists what is wrong with the profiles (empty: none), each as "profiles[i].field: why".
func ValidatePlacement(profiles []Placement) []string {
	var out []string
	bad := func(i int, field, why string) { out = append(out, fmt.Sprintf("profiles[%d].%s: %s", i, field, why)) }
	if len(profiles) > MaxPlacementProfiles {
		out = append(out, fmt.Sprintf("profiles: at most %d", MaxPlacementProfiles))
	}
	seen := map[string]bool{}
	for i, p := range profiles {
		switch {
		case !profileRe.MatchString(p.Name):
			bad(i, "name", "lowercase letters, digits and dashes, at most 63")
		case seen[p.Name]:
			bad(i, "name", "another profile has this name")
		}
		seen[p.Name] = true
		if len(p.NodeSelector) > 20 || len(p.Labels) > 20 || len(p.Tolerations) > 20 {
			bad(i, "", "at most 20 node selector entries, labels and tolerations each")
		}
		for k, v := range p.NodeSelector {
			if !validLabelKey(k) || !labelValueRe.MatchString(v) || templated(k+v) {
				bad(i, "node_selector."+k, "a Kubernetes label name and value (no ${…} or {{…}})")
			}
		}
		for k, v := range p.Labels {
			switch {
			case !validLabelKey(k) || !labelValueRe.MatchString(v) || templated(k+v):
				bad(i, "labels."+k, "a Kubernetes label name and value (no ${…} or {{…}})")
			case ownLabel(k):
				bad(i, "labels."+k, "BeziSync sets this label itself")
			}
		}
		for j, t := range p.Tolerations {
			field := fmt.Sprintf("tolerations[%d]", j)
			switch {
			case t.Key != "" && (!validLabelKey(t.Key) || templated(t.Key)):
				bad(i, field+".key", "a Kubernetes label name")
			case !slices.Contains([]string{"Equal", "Exists"}, t.Operator):
				bad(i, field+".operator", "Equal or Exists")
			case t.Operator == "Exists" && t.Value != "":
				bad(i, field+".value", "Exists takes no value")
			case t.Operator == "Equal" && t.Key == "":
				bad(i, field+".key", "Equal needs a key")
			case !labelValueRe.MatchString(t.Value) || templated(t.Value):
				bad(i, field+".value", "a Kubernetes label value")
			case !slices.Contains([]string{"", "NoSchedule", "PreferNoSchedule", "NoExecute"}, t.Effect):
				bad(i, field+".effect", "NoSchedule, PreferNoSchedule, NoExecute or empty (any)")
			}
		}
		if p.PriorityClass != "" && (!dnsSubRe.MatchString(p.PriorityClass) || templated(p.PriorityClass)) {
			bad(i, "priority_class", "a Kubernetes PriorityClass name")
		}
	}
	slices.Sort(out)
	return out
}

// NodeInfo is one schedulable node as a Kubernetes runtime reports it (capacity "nodes").
type NodeInfo struct {
	Labels map[string]string
	Taints []Taint
	CPU    float64 // CPUs allocatable
	Memory int64   // bytes allocatable
}

// Taint keeps pods off a node unless they tolerate it.
type Taint struct{ Key, Value, Effect string }

// Accepts reports whether a pod placed by p may land on n: n has every node selector label and
// p tolerates every taint that keeps pods off (NoSchedule, NoExecute).
func (p *Placement) Accepts(n NodeInfo) bool {
	for k, v := range p.NodeSelector {
		if n.Labels[k] != v {
			return false
		}
	}
	for _, t := range n.Taints {
		if t.Effect == "PreferNoSchedule" {
			continue
		}
		if !slices.ContainsFunc(p.Tolerations, func(tol Toleration) bool { return tol.tolerates(t) }) {
			return false
		}
	}
	return true
}

func (tol Toleration) tolerates(t Taint) bool {
	if tol.Effect != "" && tol.Effect != t.Effect {
		return false
	}
	if tol.Operator == "Exists" {
		return tol.Key == "" || tol.Key == t.Key
	}
	return tol.Key == t.Key && tol.Value == t.Value
}

// applyPlacement puts a pod template on the profile's nodes: its node selector, tolerations and
// priority class, and its labels beside BeziSync's own (never over them, never in the selector).
func applyPlacement(dep *appsv1.Deployment, p *Placement) {
	if p == nil {
		return
	}
	spec := &dep.Spec.Template.Spec
	if len(p.NodeSelector) > 0 {
		spec.NodeSelector = map[string]string{}
		for k, v := range p.NodeSelector {
			spec.NodeSelector[k] = v
		}
	}
	for _, t := range p.Tolerations {
		spec.Tolerations = append(spec.Tolerations, corev1.Toleration{Key: t.Key, Operator: corev1.TolerationOperator(t.Operator), Value: t.Value,
			Effect: corev1.TaintEffect(t.Effect)})
	}
	spec.PriorityClassName = p.PriorityClass
	labels := map[string]string{}
	for k, v := range dep.Spec.Template.Labels {
		labels[k] = v
	}
	for k, v := range p.Labels {
		if _, own := labels[k]; !own && !ownLabel(k) {
			labels[k] = v
		}
	}
	dep.Spec.Template.Labels = labels
}

// MaxReportedNodes bounds the nodes a Kubernetes runtime lists in its capacity.
const MaxReportedNodes = 500

// NodesOf reads the nodes of a capacity report (nil when it has none).
func NodesOf(capacity map[string]any) []NodeInfo {
	raw, ok := capacity["nodes"].([]any)
	if !ok {
		return nil
	}
	out := make([]NodeInfo, 0, len(raw))
	for _, r := range raw {
		m, _ := r.(map[string]any)
		n := NodeInfo{Labels: map[string]string{}, CPU: num(m["cpu"]), Memory: int64(num(m["memory"]))}
		if l, ok := m["labels"].(map[string]any); ok {
			for k, v := range l {
				n.Labels[k] = fmt.Sprint(v)
			}
		}
		if ts, ok := m["taints"].([]any); ok {
			for _, t := range ts {
				tm, _ := t.(map[string]any)
				n.Taints = append(n.Taints, Taint{Key: fmt.Sprint(tm["key"]), Value: fmt.Sprint(tm["value"]), Effect: fmt.Sprint(tm["effect"])})
			}
		}
		out = append(out, n)
	}
	return out
}

func num(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case interface{ Float64() (float64, error) }:
		f, _ := x.Float64()
		return f
	}
	return 0
}
