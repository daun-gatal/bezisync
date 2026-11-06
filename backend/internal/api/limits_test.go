package api_test

import (
	"strings"
	"testing"
)

// P1–P5: resource limits are counted against each pipeline's peak footprint, on both engines; a
// limit below a running pipeline leaves it running and blocks its next deploy; only Admins set
// them.
func TestResourceLimits(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	codes := func(u string) map[string]map[string]any {
		out := map[string]map[string]any{}
		for _, s := range admin.post(u+"/preflight", nil).expect(200).List("sections") {
			for _, c := range at(t, s, "report", "checks").([]any) {
				out[at(t, c, "code").(string)] = c.(map[string]any)
			}
		}
		return out
	}
	// A running Debezium pipeline at Medium (1 CPU, 1536 MiB), with its footprint.
	p := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := base + "/pipelines/" + p.Str("id")
	cur := admin.get(u).expect(200)
	medium := map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}, "policies": map[string]any{"resources": "medium"}}
	cur = admin.patch(u, map[string]any{"spec": medium}, ifMatch(cur.At("version"))...).expect(200)
	if cur.At("footprint", "cpu") != float64(1) || cur.At("footprint", "memory_mib") != float64(1536) || cur.At("footprint", "readers") != float64(1) {
		t.Fatal(string(cur.Raw))
	}
	admin.post(u+"/deploy", map[string]any{}).expect(200)
	e.tick()
	// The list carries it too (Policies counts it before a save), without the spec.
	for _, l := range admin.get(base + "/pipelines").expect(200).List() {
		if at(t, l, "id") == p.Str("id") && (at(t, l, "footprint", "memory_mib") != float64(1536) || at(t, l, "footprint", "custom") != false) {
			t.Fatal(l)
		}
	}
	// P4: viewers can't set them.
	viewer := member(t, e.srv, admin, e.pid, "viewer")
	viewer.put(base+"/policies", map[string]any{"resource_limits": map[string]any{"max_cpu": "0.5"}}).expect(403)
	// P2: a limit below it: it keeps running, and its next deploy is blocked (even forced).
	admin.put(base+"/policies", map[string]any{"resource_limits": map[string]any{"max_memory": "1Gi"}, "allow_force_deploy": true}).expect(200)
	e.tick()
	if got := admin.get(u).expect(200); got.Str("desired_state") != "running" {
		t.Fatal(string(got.Raw))
	}
	c := codes(u)["policy_resource_limits"]
	if c == nil || c["status"] != "fail" || c["blocking"] != true || !strings.Contains(c["message"].(string), "1536 MiB (at most 1Gi)") || codes(u)["policy_ok"] != nil {
		t.Fatal(c)
	}
	if r := admin.post(u+"/deploy", map[string]any{"force_reason": "we know what we are doing"}).expect(409); !strings.Contains(string(r.Raw), "cannot be skipped") {
		t.Fatal(string(r.Raw))
	}
	admin.put(base+"/policies", map[string]any{"resource_limits": map[string]any{"max_memory": "2Gi", "max_cpu": "1"}}).expect(200)
	if c := codes(u); c["policy_resource_limits"] != nil || c["policy_ok"] == nil {
		t.Fatal(c["policy_resource_limits"])
	}
	// Flink CDC: readers count, and a custom cluster is held to max_resources by the Flink
	// preset's totals (it used to pass whatever its size).
	enableFlink(t, admin)
	lake := icebergSink(t, e)
	cluster := map[string]any{"parallelism": 8, "slot": map[string]any{"cpu": "1", "memory": "2Gi"}, "job_manager": map[string]any{"cpu": "1", "memory": "1Gi"}}
	f := admin.post(base+"/pipelines", map[string]any{"name": "lake-big", "source_id": e.ids.Source, "sink_id": lake, "runtime_id": e.ids.Runtime, "engine": "flink-cdc",
		"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}, "policies": map[string]any{"resources": "custom"},
			"flink_cdc": map[string]any{"cluster": cluster}}}).expect(201)
	fu := base + "/pipelines/" + f.Str("id")
	if f.At("footprint", "readers") != float64(8) || f.At("footprint", "memory_mib") != float64(1024+8*2048+512) {
		t.Fatal(string(f.Raw))
	}
	admin.put(base+"/policies", map[string]any{"resource_limits": map[string]any{"max_parallelism": 4}, "max_resources": "large"}).expect(200)
	fc := codes(fu)
	if !strings.Contains(fc["policy_resource_limits"]["message"].(string), "8 readers (at most 4)") {
		t.Fatal(fc["policy_resource_limits"])
	}
	if fc["policy_resources"] == nil || !strings.Contains(fc["policy_resources"]["message"].(string), "this project allows up to large") {
		t.Fatal(fc["policy_resources"])
	}
	// P5: unset limits are not returned.
	if got := admin.put(base+"/policies", map[string]any{}).expect(200); strings.Contains(string(got.Raw), "resource_limits") {
		t.Fatal(string(got.Raw))
	}
}
