package domain_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

func parseCluster(policies map[string]any, cluster any) (domain.PipelineSpec, []string) {
	v := &val.V{}
	raw := map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"a.b"}}}, "policies": policies}
	if cluster != nil {
		raw["flink_cdc"] = map[string]any{"cluster": cluster}
	}
	s, _ := domain.ParseSpec(v, nil, raw)
	var msgs []string
	for _, e := range v.Errors {
		msgs = append(msgs, strings.Join(e.Loc, ".")+": "+e.Message)
	}
	return s, msgs
}

func TestFlinkCluster(t *testing.T) {
	custom := map[string]any{"resources": "custom"}
	slot := map[string]any{"cpu": "1", "memory": "2Gi"}
	jm := map[string]any{"cpu": "1", "memory": "1Gi"}
	auto := map[string]any{"parallelism": 4, "slot": slot, "job_manager": jm}
	with := func(base map[string]any, k string, v any) map[string]any {
		out := map[string]any{}
		for kk, vv := range base {
			out[kk] = vv
		}
		out[k] = v
		return out
	}
	s, errs := parseCluster(custom, auto)
	if len(errs) > 0 || s.FlinkCDC.Cluster == nil || s.FlinkCDC.Cluster.Shape != "auto" || s.FlinkCDC.Cluster.Parallelism != 4 {
		t.Fatal(errs, s.FlinkCDC.Cluster)
	}
	b, _ := json.Marshal(s)
	if !strings.Contains(string(b), `"cluster":{"parallelism":4,"slot":{"cpu":"1","memory":"2Gi"},"job_manager":{"cpu":"1","memory":"1Gi"},"shape":"auto"}`) {
		t.Fatal(string(b))
	}
	if c := s.Clone(); c.FlinkCDC.Cluster == s.FlinkCDC.Cluster {
		t.Fatal("clone shares the cluster")
	}
	// A streaming slot size: dumped after slot, cloned.
	st, errs := parseCluster(custom, with(auto, "streaming_slot", map[string]any{"cpu": "0.5", "memory": "2Gi"}))
	if len(errs) > 0 || st.FlinkCDC.Cluster.StreamingSlot == nil || *st.FlinkCDC.Cluster.StreamingSlot != (domain.SlotSize{CPU: "0.5", Memory: "2Gi"}) {
		t.Fatal(errs)
	}
	if b, _ := json.Marshal(st); !strings.Contains(string(b), `"slot":{"cpu":"1","memory":"2Gi"},"streaming_slot":{"cpu":"0.5","memory":"2Gi"},"job_manager"`) {
		t.Fatal(string(b))
	}
	if c := st.Clone(); c.FlinkCDC.Cluster.StreamingSlot == st.FlinkCDC.Cluster.StreamingSlot {
		t.Fatal("clone shares the streaming slot")
	}
	// A manual shape round-trips (its count and slots are fields of the same object as cpu/memory).
	manual := map[string]any{"parallelism": 6, "slot": slot, "job_manager": jm, "shape": "manual",
		"task_managers": map[string]any{"count": 3, "slots": 2, "cpu": "2", "memory": "4Gi"}}
	m, errs := parseCluster(custom, manual)
	if len(errs) > 0 || m.FlinkCDC.Cluster.TaskManagers == nil || *m.FlinkCDC.Cluster.TaskManagers != (domain.ManualTaskManagers{Count: 3, Slots: 2, CPU: "2", Memory: "4Gi"}) {
		t.Fatal(errs)
	}
	b, _ = json.Marshal(m)
	var back map[string]any
	_ = json.Unmarshal(b, &back)
	if r, errs := parseCluster(custom, back["flink_cdc"].(map[string]any)["cluster"]); len(errs) > 0 || *r.FlinkCDC.Cluster.TaskManagers != *m.FlinkCDC.Cluster.TaskManagers {
		t.Fatal(errs)
	}
	tm := func(count, slots int, mem string) map[string]any {
		return map[string]any{"count": count, "slots": slots, "cpu": "2", "memory": mem}
	}
	for name, bad := range map[string]struct {
		policies map[string]any
		cluster  any
		want     string
	}{
		"no parallelism":          {custom, map[string]any{"slot": slot, "job_manager": jm}, "parallelism: Field required"},
		"parallelism 0":           {custom, with(auto, "parallelism", 0), "parallelism"},
		"parallelism over 128":    {custom, with(auto, "parallelism", 129), "parallelism"},
		"slot too small":          {custom, with(auto, "slot", map[string]any{"cpu": "0.25", "memory": "256Mi"}), "slot.memory"},
		"slot too much CPU":       {custom, with(auto, "slot", map[string]any{"cpu": "4.25", "memory": "1Gi"}), "slot.cpu"},
		"JobManager too small":    {custom, with(auto, "job_manager", map[string]any{"cpu": "1", "memory": "512Mi"}), "job_manager.memory"},
		"unknown key":             {custom, with(auto, "replicas", 2), "replicas: Extra inputs"},
		"unknown slot key":        {custom, with(auto, "slot", map[string]any{"cpu": "1", "memory": "1Gi", "gpu": "1"}), "slot.gpu"},
		"unknown TaskManager key": {custom, with(with(auto, "shape", "manual"), "task_managers", with(tm(2, 2, "4Gi"), "heap", "1g")), "task_managers.heap"},
		"manual without TMs":      {custom, with(auto, "shape", "manual"), "a manual shape needs task_managers"},
		"auto with TMs":           {custom, with(auto, "task_managers", tm(2, 2, "4Gi")), "for a manual shape only"},
		"too few slots":           {custom, with(with(auto, "shape", "manual"), "task_managers", tm(1, 2, "4Gi")), "parallelism needs that many slots"},
		"slots too small":         {custom, with(with(auto, "shape", "manual"), "task_managers", tm(1, 8, "2Gi")), "each slot needs at least 512Mi"},
		"too many TMs":            {custom, with(with(auto, "shape", "manual"), "task_managers", tm(17, 1, "2Gi")), "task_managers.count"},
		"cluster without custom":  {map[string]any{"resources": "medium"}, auto, "a custom size needs resources: custom"},
		"both custom kinds":       {map[string]any{"resources": "custom", "custom_resources": map[string]any{"cpu": "1", "memory": "1Gi"}}, auto, "use one"},
		"custom without either":   {custom, nil, "custom_resources (Debezium) or flink_cdc.cluster"},
		"streaming bigger":        {custom, with(auto, "streaming_slot", map[string]any{"cpu": "2", "memory": "1Gi"}), "never bigger"},
		"streaming the same":      {custom, with(auto, "streaming_slot", map[string]any{"cpu": "1", "memory": "2Gi"}), "smaller than slot"},
		"streaming out of bounds": {custom, with(auto, "streaming_slot", map[string]any{"cpu": "0.1", "memory": "1Gi"}), "streaming_slot.cpu"},
		"streaming on manual":     {custom, with(with(with(auto, "shape", "manual"), "task_managers", tm(2, 2, "4Gi")), "streaming_slot", map[string]any{"cpu": "0.5", "memory": "1Gi"}), "never resized"},
	} {
		if _, errs := parseCluster(bad.policies, bad.cluster); !strings.Contains(strings.Join(errs, "|"), bad.want) {
			t.Errorf("%s: %v", name, errs)
		}
	}
}

func TestShrinkAfterCopyPolicy(t *testing.T) {
	parse := func(raw map[string]any) (domain.ProjectPolicies, int) {
		v := &val.V{}
		p, _ := domain.ParsePolicies(v, nil, raw)
		return p, len(v.Errors)
	}
	p, errs := parse(map[string]any{})
	if errs > 0 || !p.ShrinkAuto() {
		t.Fatal(errs)
	}
	if b, _ := json.Marshal(p); strings.Contains(string(b), "shrink_after_copy") {
		t.Fatal("the default is dumped", string(b))
	}
	p, _ = parse(map[string]any{"shrink_after_copy": "suggest"})
	if b, _ := json.Marshal(p); p.ShrinkAuto() || !strings.HasSuffix(string(b), `"shrink_after_copy":"suggest"}`) {
		t.Fatal(string(b))
	}
	if p, _ := parse(map[string]any{"shrink_after_copy": "auto"}); !p.ShrinkAuto() {
		t.Fatal(p)
	}
	if _, errs := parse(map[string]any{"shrink_after_copy": "never"}); errs == 0 {
		t.Fatal("accepted")
	}
}
