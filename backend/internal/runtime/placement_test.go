package runtime

import (
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestValidatePlacement(t *testing.T) {
	good := []Placement{{Name: "batch", NodeSelector: map[string]string{"node.kubernetes.io/pool": "cdc", "tier": "batch"},
		Tolerations:   []Toleration{{Key: "dedicated", Operator: "Equal", Value: "cdc", Effect: "NoSchedule"}, {Operator: "Exists"}},
		PriorityClass: "cdc-high", Labels: map[string]string{"team": "payments"}}}
	if bad := ValidatePlacement(good); len(bad) > 0 {
		t.Fatal(bad)
	}
	for name, c := range map[string]struct {
		p    Placement
		want string
	}{
		"bad name":           {Placement{Name: "Batch_1"}, "name"},
		"template in value":  {Placement{Name: "a", NodeSelector: map[string]string{"pool": "${POOL}"}}, "node_selector.pool"},
		"go template":        {Placement{Name: "a", Labels: map[string]string{"team": "{{ .team }}"}}, "labels.team"},
		"bad label key":      {Placement{Name: "a", NodeSelector: map[string]string{"-x": "y"}}, "node_selector.-x"},
		"own label":          {Placement{Name: "a", Labels: map[string]string{"app.bezisync.io/short-id": "x"}}, "BeziSync sets"},
		"bad operator":       {Placement{Name: "a", Tolerations: []Toleration{{Key: "k", Operator: "In"}}}, "operator"},
		"exists with value":  {Placement{Name: "a", Tolerations: []Toleration{{Key: "k", Operator: "Exists", Value: "v"}}}, "Exists takes no value"},
		"equal without key":  {Placement{Name: "a", Tolerations: []Toleration{{Operator: "Equal", Value: "v"}}}, "Equal needs a key"},
		"bad effect":         {Placement{Name: "a", Tolerations: []Toleration{{Key: "k", Operator: "Exists", Effect: "Never"}}}, "effect"},
		"bad priority class": {Placement{Name: "a", PriorityClass: "High Priority"}, "priority_class"},
		"templated priority": {Placement{Name: "a", PriorityClass: "${PC}"}, "priority_class"},
	} {
		if bad := ValidatePlacement([]Placement{c.p}); !strings.Contains(strings.Join(bad, "|"), c.want) {
			t.Errorf("%s: %v", name, bad)
		}
	}
	if bad := ValidatePlacement([]Placement{{Name: "a"}, {Name: "a"}}); !strings.Contains(strings.Join(bad, "|"), "another profile") {
		t.Fatal(bad)
	}
}

func TestPlacementAcceptsNodes(t *testing.T) {
	p := Placement{NodeSelector: map[string]string{"pool": "cdc"}, Tolerations: []Toleration{{Key: "dedicated", Operator: "Equal", Value: "cdc", Effect: "NoSchedule"}}}
	cases := []struct {
		n    NodeInfo
		want bool
	}{
		{NodeInfo{Labels: map[string]string{"pool": "cdc"}}, true},
		{NodeInfo{Labels: map[string]string{"pool": "web"}}, false},
		{NodeInfo{Labels: map[string]string{"pool": "cdc"}, Taints: []Taint{{Key: "dedicated", Value: "cdc", Effect: "NoSchedule"}}}, true},
		{NodeInfo{Labels: map[string]string{"pool": "cdc"}, Taints: []Taint{{Key: "dedicated", Value: "gpu", Effect: "NoSchedule"}}}, false},
		{NodeInfo{Labels: map[string]string{"pool": "cdc"}, Taints: []Taint{{Key: "spot", Effect: "PreferNoSchedule"}}}, true},
		{NodeInfo{Labels: map[string]string{"pool": "cdc"}, Taints: []Taint{{Key: "spot", Effect: "NoExecute"}}}, false},
	}
	for i, c := range cases {
		if got := p.Accepts(c.n); got != c.want {
			t.Errorf("%d: %v", i, got)
		}
	}
	anything := Placement{Tolerations: []Toleration{{Operator: "Exists"}}}
	if !anything.Accepts(NodeInfo{Taints: []Taint{{Key: "x", Effect: "NoExecute"}}}) {
		t.Fatal("Exists without a key tolerates every taint")
	}
}

func TestApplyPlacementKeepsBeziSyncLabels(t *testing.T) {
	dep := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{Label: "abc"}},
		Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{Label: "abc", ManagedBy: "bezisync"}}}}}
	applyPlacement(dep, nil)
	if dep.Spec.Template.Spec.NodeSelector != nil {
		t.Fatal("nothing without a profile")
	}
	applyPlacement(dep, &Placement{NodeSelector: map[string]string{"pool": "cdc"}, PriorityClass: "cdc-high",
		Tolerations: []Toleration{{Key: "dedicated", Operator: "Equal", Value: "cdc", Effect: "NoSchedule"}}, Labels: map[string]string{"team": "payments", Label: "evil"}})
	s := dep.Spec.Template.Spec
	if s.NodeSelector["pool"] != "cdc" || s.PriorityClassName != "cdc-high" || len(s.Tolerations) != 1 || s.Tolerations[0].Effect != corev1.TaintEffectNoSchedule {
		t.Fatalf("%+v", s)
	}
	if l := dep.Spec.Template.Labels; l["team"] != "payments" || l[Label] != "abc" || len(dep.Spec.Selector.MatchLabels) != 1 {
		t.Fatal(l, dep.Spec.Selector.MatchLabels)
	}
}
