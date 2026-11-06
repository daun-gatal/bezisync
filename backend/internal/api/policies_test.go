package api_test

import (
	"strings"
	"testing"
)

func TestPoliciesEnforcedAtSaveAndPreflight(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	role := strings.TrimPrefix(at(t, admin.get(base+"/connections/"+e.ids.Source).JSON(), "config", "user").(string), "")
	exec(t, `CREATE TABLE IF NOT EXISTS shop.customers (id serial primary key, email text, total int)`)
	exec(t, `GRANT SELECT ON shop.customers TO `+role)
	viewer := member(t, e.srv, admin, e.pid, "viewer")
	if viewer.get(base+"/policies").expect(200).At("require_approval") != false {
		t.Fatal("approval on by default")
	}
	viewer.put(base+"/policies", map[string]any{}).expect(403)
	admin.put(base+"/policies", map[string]any{"allowed_sink_types": []string{"http"}}).expect(200)
	body := e.ids.body()
	body["name"], body["spec"] = "p1", map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}}
	blocked := admin.post(base+"/pipelines", body).expect(422)
	if blocked.Code() != "policy_violation" || !strings.Contains(blocked.Str("detail"), "kafka destinations") {
		t.Fatal(string(blocked.Raw))
	}
	admin.put(base+"/policies", map[string]any{"max_resources": "small"}).expect(200)
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.customers")
	u := base + "/pipelines/" + pipe.Str("id")
	admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.customers"}}},
		"policies": map[string]any{"resources": "large"}}}, ifMatch(pipe.At("version"))...).expect(200)
	pre := admin.post(u+"/preflight", nil).expect(200)
	codes := map[any]bool{}
	for _, s := range pre.List("sections") {
		if at(t, s, "key") == "policy" {
			for _, c := range at(t, s, "report", "checks").([]any) {
				codes[at(t, c, "code")] = true
			}
		}
	}
	if len(codes) != 1 || !codes["policy_resources"] || pre.At("ok") != false {
		t.Fatal(string(pre.Raw))
	}
	forced := admin.post(u+"/deploy", map[string]any{"force_reason": "we know what we are doing"}).expect(409)
	if !strings.Contains(forced.Str("detail"), "policy") {
		t.Fatal(string(forced.Raw))
	}
	admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.customers"}},
		"columns": map[string]any{"shop.customers": map[string]any{"mask": []string{"email"}}}}, "policies": map[string]any{"resources": "small"}}},
		ifMatch(admin.get(u).At("version"))...).expect(200)
	if admin.post(u+"/preflight", nil).At("ok") != true {
		t.Fatal("preflight still failing")
	}
	admin.post(u+"/deploy", map[string]any{}).expect(200)
	admin.put(base+"/policies", map[string]any{"allow_force_deploy": false}).expect(200)
	admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.customers", "shop.missing"}}}}},
		ifMatch(admin.get(u).At("version"))...).expect(200)
	res := admin.post(u+"/deploy", map[string]any{"force_reason": "table is created later today"}).expect(403)
	if res.Code() != "force_not_allowed" {
		t.Fatal(string(res.Raw))
	}
	admin.put(base+"/policies", map[string]any{"max_resources": "huge"}).expect(422)
}

func TestApprovalFlow(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := base + "/pipelines/" + pipe.Str("id")
	admin.put(base+"/policies", map[string]any{"require_approval": true}).expect(200)
	editor := member(t, e.srv, admin, e.pid, "editor")
	res := editor.post(u+"/deploy", map[string]any{}).expect(202)
	if res.At("approval_required") != true || res.At("deployed_version") != nil {
		t.Fatal(string(res.Raw))
	}
	if res.Str("pending_change", "state") != "pending" || res.At("pending_change", "version") != float64(1) || res.At("pending_change", "preflight_ok") != true {
		t.Fatal(string(res.Raw))
	}
	cr := res.Str("pending_change", "id")
	editor.post(base+"/change-requests/"+cr+"/approve", map[string]any{}).expect(403)
	found := false
	for _, n := range admin.get(base + "/notifications").List("items") {
		found = found || strings.Contains(at(t, n, "title").(string), "Approve deploying")
	}
	if !found {
		t.Fatal("no approval notification")
	}
	admin.post(base+"/change-requests/"+cr+"/reject", map[string]any{"comment": ""}).expect(422)
	if s := editor.post(base+"/change-requests/"+cr+"/cancel", nil).expect(200).Str("state"); s != "cancelled" {
		t.Fatal(s)
	}
	cr2 := editor.post(u+"/deploy", map[string]any{}).expect(202).Str("pending_change", "id")
	approved := admin.post(base+"/change-requests/"+cr2+"/approve", map[string]any{"comment": "looks good"}).expect(200)
	if approved.Str("state") != "approved" {
		t.Fatal(string(approved.Raw))
	}
	after := admin.get(u)
	if after.At("deployed_version") != float64(1) || after.At("pending_change") != nil {
		t.Fatal(string(after.Raw))
	}
	admin.post(base+"/change-requests/"+cr2+"/approve", map[string]any{}).expect(409)
	own := admin.post(u+"/deploy", map[string]any{"version": 1}).expect(202).Str("pending_change", "id")
	if c := admin.post(base+"/change-requests/"+own+"/approve", map[string]any{}).expect(403).Code(); c != "own_change_request" {
		t.Fatal(c)
	}
	pending := admin.get(base + "/change-requests?state=pending").expect(200).List()
	if len(pending) != 1 || at(t, pending[0], "state") != "pending" {
		t.Fatal(pending)
	}
	admin.get(base + "/change-requests?state=nope").expect(422)
}

func TestScriptedTransformsNeedThePolicy(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := base + "/pipelines/" + pipe.Str("id")
	admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
		"transforms": []any{map[string]any{"type": "filter", "condition": "value.op == 'c'"}, map[string]any{"type": "unwrap"}}}},
		ifMatch(pipe.At("version"))...).expect(200)
	policyCodes := func() map[any]bool {
		out := map[any]bool{}
		for _, s := range admin.post(u+"/preflight", nil).expect(200).List("sections") {
			if at(t, s, "key") == "policy" {
				for _, c := range at(t, s, "report", "checks").([]any) {
					out[at(t, c, "code")] = true
				}
			}
		}
		return out
	}
	if c := policyCodes(); !c["policy_scripted_transform"] {
		t.Fatal(c)
	}
	forced := admin.post(u+"/deploy", map[string]any{"force_reason": "it is only a filter"}).expect(409)
	if !strings.Contains(forced.Str("detail"), "policy") {
		t.Fatal(string(forced.Raw))
	}
	got := admin.put(base+"/policies", map[string]any{"allow_scripted_transforms": true}).expect(200)
	if got.At("allow_scripted_transforms") != true {
		t.Fatal(string(got.Raw))
	}
	if c := policyCodes(); c["policy_scripted_transform"] || !c["policy_ok"] {
		t.Fatal(c)
	}
	// Switching it off again is returned without the key and recorded in the audit log.
	off := admin.put(base+"/policies", map[string]any{}).expect(200)
	if _, present := off.JSON().(map[string]any)["allow_scripted_transforms"]; present {
		t.Fatal(string(off.Raw))
	}
	found := false
	for _, ev := range admin.get(base + "/events?audit_only=true&limit=200").List("items") {
		found = found || strings.Contains(at(t, ev, "summary").(string), "allow_scripted_transforms")
	}
	if !found {
		t.Fatal("switching scripted transforms off was not audited")
	}
	// Advanced properties cannot rebuild the chain instead: refused when saved.
	cfg := admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
		"advanced": map[string]any{"debezium.transforms": "unwrap"}}}, ifMatch(admin.get(u).At("version"))...).expect(422)
	if cfg.Code() != "managed_property" || !strings.Contains(string(cfg.Raw), "Transform step") {
		t.Fatal(string(cfg.Raw))
	}
}
