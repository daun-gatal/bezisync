package api_test

import (
	"strings"
	"testing"
)

func policyCodes(t *testing.T, pre *resp) map[any]bool {
	t.Helper()
	codes := map[any]bool{}
	for _, s := range pre.List("sections") {
		if at(t, s, "key") == "policy" {
			for _, c := range at(t, s, "report", "checks").([]any) {
				codes[at(t, c, "code")] = true
			}
		}
	}
	return codes
}

// Stored policies that can't be read fail closed (ADR 0002, Known issue 3): changes are refused
// with one reason, running pipelines keep running, and an Admin repairs them in place.
func TestUnreadablePoliciesFailClosed(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := base + "/pipelines/" + pipe.Str("id")
	admin.post(u+"/deploy", map[string]any{}).expect(200)
	e.tick()
	// What a later release could find stored: a value this one refuses, among valid fields.
	exec(t, `UPDATE projects SET policies = '{"allowed_source_types": ["postgresql"], "max_resources": "huge", "allow_force_deploy": true}' WHERE id = '`+e.pid+`'`)

	// P7: GET says so, keeps what parsed, and holds the strictest value where it didn't.
	got := admin.get(base + "/policies").expect(200)
	if got.At("unreadable") != true || got.At("max_resources") != "small" || got.At("require_approval") != false ||
		got.Str("allowed_source_types", 0) != "postgresql" || got.At("allow_force_deploy") != true {
		t.Fatal(string(got.Raw))
	}
	if p := got.List("problems"); len(p) != 1 || !strings.HasPrefix(p[0].(string), "max_resources: ") || strings.Contains(p[0].(string), "huge") {
		t.Fatal(string(got.Raw))
	}

	// P3: preflight has the one blocking reason; deploys are refused, forced ones too.
	pre := admin.post(u+"/preflight", nil).expect(200)
	if codes := policyCodes(t, pre); pre.At("ok") != false || len(codes) != 1 || !codes["policy_unreadable"] {
		t.Fatal(string(pre.Raw))
	}
	admin.post(u+"/deploy", map[string]any{}).expect(409)
	// Forcing never skips the policy section: refused although allow_force_deploy parsed as true.
	if r := admin.post(u+"/deploy", map[string]any{"force_reason": "ship it anyway, please"}); r.Status != 409 || r.Code() != "preflight_failed" {
		t.Fatal("a forced deploy:", r.Status, string(r.Raw))
	}

	// P4: spec saves and new pipelines are refused.
	cur := admin.get(u).expect(200)
	edit := admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders", "shop.refunds"}}}}},
		ifMatch(cur.At("version"))...)
	if edit.Status != 409 || edit.Code() != "policy_unreadable" {
		t.Fatal(edit.Status, string(edit.Raw))
	}
	body := e.ids.body()
	body["name"], body["spec"] = "p2", map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}}
	if c := admin.post(base+"/pipelines", body).Code(); c != "policy_unreadable" {
		t.Fatal(c)
	}

	// P5, P6: the running pipeline is still observed, and pause and resume still work.
	e.tick()
	if l := admin.get(u).Str("lifecycle"); l != "streaming" {
		t.Fatal(l)
	}
	admin.post(u+"/pause", nil).expect(200)
	admin.post(u+"/resume", nil).expect(200)

	// P8: an Admin saves the form as loaded (read-only keys included) and the project is readable again.
	fixed := got.JSON().(map[string]any)
	admin.put(base+"/policies", fixed).expect(200)
	after := admin.get(base + "/policies").expect(200)
	if after.At("unreadable") != nil || after.At("problems") != nil || after.At("max_resources") != "small" {
		t.Fatal(string(after.Raw))
	}
	if pre := admin.post(u+"/preflight", nil).expect(200); policyCodes(t, pre)["policy_unreadable"] {
		t.Fatal(string(pre.Raw))
	}
}
