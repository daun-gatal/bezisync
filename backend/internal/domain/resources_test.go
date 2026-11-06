package domain_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

func TestCustomResources(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int
		ok   bool
	}{
		{"0.25", 250, true}, {"1", 1000, true}, {"1.5", 1500, true}, {"8", 8000, true},
		{"0", 0, false}, {"0.2", 0, false}, {"0.3", 0, false}, {"8.25", 0, false}, {"08", 0, false}, {"1.50", 0, false}, {"500m", 0, false}, {" 1", 0, false},
	} {
		if got, ok := domain.ParseCPU(c.in); got != c.want || ok != c.ok {
			t.Errorf("cpu %q: %d %v", c.in, got, ok)
		}
		if c.ok && domain.FormatCPU(c.want) != c.in {
			t.Errorf("cpu %d formats as %q", c.want, domain.FormatCPU(c.want))
		}
	}
	for _, c := range []struct {
		in   string
		want int
		ok   bool
	}{
		{"512Mi", 512, true}, {"1536Mi", 1536, true}, {"2Gi", 2048, true}, {"16Gi", 16384, true},
		{"2048Mi", 0, false}, {"256Mi", 0, false}, {"600Mi", 0, false}, {"17Gi", 0, false}, {"2G", 0, false}, {"0Gi", 0, false}, {"02Gi", 0, false},
	} {
		if got, ok := domain.ParseMemory(c.in); got != c.want || ok != c.ok {
			t.Errorf("memory %q: %d %v", c.in, got, ok)
		}
		if c.ok && domain.FormatMemory(c.want) != c.in {
			t.Errorf("memory %d formats as %q", c.want, domain.FormatMemory(c.want))
		}
	}
	// R1: the heap always leaves at least 256 MiB, and 512 MiB still gets a 256 MiB heap.
	for mem, heap := range map[int]int{512: 256, 768: 512, 2048: 1433, 16384: 11468} {
		if got := domain.HeapMiB(mem); got != heap || mem-got < 256 {
			t.Errorf("heap of %d: %d", mem, got)
		}
	}
	parse := func(policies map[string]any) (domain.PipelineSpec, []string) {
		v := &val.V{}
		s, _ := domain.ParseSpec(v, nil, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"a.b"}}}, "policies": policies})
		var msgs []string
		for _, e := range v.Errors {
			msgs = append(msgs, strings.Join(e.Loc, ".")+": "+e.Message)
		}
		return s, msgs
	}
	s, errs := parse(map[string]any{"resources": "custom", "custom_resources": map[string]any{"cpu": "1.5", "memory": "2Gi"}})
	if len(errs) > 0 || s.Policies.Amount() != (domain.ResourceAmount{CPUMilli: 1500, MemoryMiB: 2048}) {
		t.Fatal(errs, s.Policies.Amount())
	}
	b, _ := json.Marshal(s)
	if !strings.Contains(string(b), `"delivery_canary":false,"custom_resources":{"cpu":"1.5","memory":"2Gi"}}`) {
		t.Fatal(string(b))
	}
	if c := s.Clone(); c.Policies.Custom == s.Policies.Custom {
		t.Fatal("clone shares the custom size")
	}
	// Presets dump as before.
	p, _ := parse(map[string]any{"resources": "medium"})
	if b, _ := json.Marshal(p); strings.Contains(string(b), "custom_resources") {
		t.Fatal(string(b))
	}
	for _, bad := range []map[string]any{
		{"resources": "custom"},
		{"resources": "medium", "custom_resources": map[string]any{"cpu": "1", "memory": "1Gi"}},
		{"resources": "custom", "custom_resources": map[string]any{"cpu": "1"}},
		{"resources": "custom", "custom_resources": map[string]any{"cpu": "1", "memory": "1Gi", "heap": "1g"}},
		{"resources": "custom", "custom_resources": map[string]any{"cpu": "0.1", "memory": "1Gi"}},
	} {
		if _, errs := parse(bad); len(errs) == 0 {
			t.Errorf("%v accepted", bad)
		}
	}
}

func TestCustomSizeWithinProjectLimit(t *testing.T) {
	medium := "medium"
	pol := domain.DefaultPolicies()
	pol.MaxResources = &medium
	spec := domain.DefaultSpec()
	spec.Source.Tables.Include = []string{"a.b"}
	spec.Policies.Resources, spec.Policies.Custom = "custom", &domain.CustomResources{CPU: "1", Memory: "1536Mi"}
	codes := func() string {
		var out []string
		for _, c := range domain.EvaluatePolicies(pol, spec, nil, "postgresql", "kafka", 0) {
			out = append(out, c.Code+":"+c.Message)
		}
		return strings.Join(out, " ")
	}
	if got := codes(); !strings.HasPrefix(got, "policy_ok") {
		t.Fatal(got)
	}
	spec.Policies.Custom.Memory = "2Gi"
	if got := codes(); !strings.Contains(got, "policy_resources:This custom size (1 CPU · 2 GiB) exceeds this project's limit (medium: 1 CPU · 1536 MiB).") {
		t.Fatal(got)
	}
}
