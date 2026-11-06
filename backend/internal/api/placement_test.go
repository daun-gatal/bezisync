package api_test

import (
	"strings"
	"testing"
)

// R35–R38: a Kubernetes runtime's placement profiles, picked by a pipeline by name; applied to
// its pods from its deploy on; checked against the nodes; never removed while in use.
func TestPlacementProfiles(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	rtURL := base + "/runtimes/" + e.ids.Runtime
	profile := map[string]any{"name": "cdc", "node_selector": map[string]any{"pool": "cdc"},
		"tolerations":    []any{map[string]any{"key": "dedicated", "operator": "Equal", "value": "cdc", "effect": "NoSchedule"}},
		"priority_class": "cdc-high", "labels": map[string]any{"team": "payments"}}
	rt := admin.get(rtURL).expect(200)
	if l := rt.List("placement_profiles"); len(l) != 0 {
		t.Fatal(l)
	}
	// R38: templates and bad names are refused; unknown keys too.
	bad := map[string]any{"name": "cdc", "node_selector": map[string]any{"pool": "${POOL}"}}
	if r := admin.patch(rtURL, map[string]any{"placement_profiles": []any{bad}}, ifMatch(rt.At("version"))...).expect(422); r.Code() != "placement_profiles" {
		t.Fatal(string(r.Raw))
	}
	admin.patch(rtURL, map[string]any{"placement_profiles": []any{map[string]any{"name": "cdc", "affinity": "x"}}}, ifMatch(rt.At("version"))...).expect(422)
	// Profiles change without pausing anything.
	rt = admin.patch(rtURL, map[string]any{"placement_profiles": []any{profile}}, ifMatch(rt.At("version"))...).expect(200)
	if rt.At("placement_profiles").([]any)[0].(map[string]any)["priority_class"] != "cdc-high" {
		t.Fatal(string(rt.Raw))
	}
	// R37: Docker runtimes have none.
	docker := admin.post(base+"/runtimes", map[string]any{"name": "dock", "kind": "docker", "config": map[string]any{"host": "unix:///var/run/docker.sock"},
		"placement_profiles": []any{profile}}).expect(422)
	if !strings.Contains(docker.Str("detail"), "Kubernetes") {
		t.Fatal(string(docker.Raw))
	}

	p := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := base + "/pipelines/" + p.Str("id")
	check := func() map[string]any {
		for _, s := range admin.post(u+"/preflight", nil).expect(200).List("sections") {
			for _, c := range at(t, s, "report", "checks").([]any) {
				if at(t, c, "code") == "placement" {
					if at(t, s, "key") != "runtime" {
						t.Fatal("not in the runtime section")
					}
					return c.(map[string]any)
				}
			}
		}
		return nil
	}
	spec := func(placement string) map[string]any {
		return map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}, "placement": placement}
	}
	cur := admin.get(u).expect(200)
	admin.patch(u, map[string]any{"spec": spec("nope")}, ifMatch(cur.At("version"))...).expect(200)
	if c := check(); c == nil || c["status"] != "fail" || !strings.Contains(c["message"].(string), `no placement profile "nope" (it has cdc)`) {
		t.Fatal(c)
	}
	cur = admin.get(u).expect(200)
	admin.patch(u, map[string]any{"spec": spec("cdc")}, ifMatch(cur.At("version"))...).expect(200)
	// Without a node list: unknown. R36: no node takes these pods: blocking.
	if c := check(); c == nil || c["status"] != "unknown" {
		t.Fatal(c)
	}
	web := map[string]any{"labels": map[string]any{"pool": "web"}, "taints": []any{}, "cpu": 16, "memory": 64 << 30}
	cdc := map[string]any{"labels": map[string]any{"pool": "cdc"}, "taints": []any{map[string]any{"key": "dedicated", "value": "cdc", "effect": "NoSchedule"}}, "cpu": 4, "memory": 8 << 30}
	e.driver.Cap = map[string]any{"cpu_allocatable": 20, "memory_allocatable": 72 << 30, "node_cpu_max": 16, "node_memory_max": 64 << 30, "nodes": []any{web}}
	if c := check(); c == nil || c["status"] != "fail" || c["blocking"] != true || !strings.Contains(c["message"].(string), "No schedulable node") {
		t.Fatal(c)
	}
	e.driver.Cap["nodes"] = []any{web, cdc}
	if c := check(); c == nil || c["status"] != "pass" || !strings.Contains(c["message"].(string), "largest node has 4 CPU and 8192 MiB") {
		t.Fatal(c)
	}
	// Deployed: the pods carry the profile, and the profile is kept with the deploy.
	admin.post(u+"/deploy", map[string]any{}).expect(200)
	e.tick()
	var w map[string]any
	for _, wl := range e.driver.Workloads {
		if wl.Placement != nil {
			w = map[string]any{"selector": wl.Placement.NodeSelector["pool"], "pc": wl.Placement.PriorityClass, "hash": wl.ConfigHash}
		}
	}
	if w == nil || w["selector"] != "cdc" || w["pc"] != "cdc-high" {
		t.Fatal(e.driver.Workloads)
	}
	// Editing the profile changes nothing that runs until the next deploy.
	edited := map[string]any{}
	for k, v := range profile {
		edited[k] = v
	}
	edited["priority_class"] = "cdc-low"
	rt = admin.get(rtURL).expect(200)
	admin.patch(rtURL, map[string]any{"placement_profiles": []any{edited}}, ifMatch(rt.At("version"))...).expect(200)
	e.tick()
	for _, wl := range e.driver.Workloads {
		if wl.Placement != nil && (wl.Placement.PriorityClass != "cdc-high" || wl.ConfigHash != w["hash"]) {
			t.Fatal("a profile edit restarted a running pipeline", wl.Placement)
		}
	}
	// R35: a profile in use can't be removed; the error names its pipelines.
	rt = admin.get(rtURL).expect(200)
	r := admin.patch(rtURL, map[string]any{"placement_profiles": []any{}}, ifMatch(rt.At("version"))...).expect(409)
	if r.Code() != "placement_in_use" || !strings.Contains(r.Str("detail"), "cdc (used by "+p.Str("name")+")") {
		t.Fatal(string(r.Raw))
	}
}
