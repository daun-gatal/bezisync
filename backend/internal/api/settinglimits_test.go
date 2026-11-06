package api_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/services"
)

// P6–P12: setting limits are checked when Admins save them, refuse values when pipelines are
// saved, block deploys of versions saved before them, write pins at deploy, and are counted
// before they are saved.
func TestSettingLimits(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	// P6: a cap below the setting's minimum is refused, naming it.
	r := admin.put(base+"/policies", map[string]any{"setting_limits": map[string]any{"debezium.source.snapshot.max.threads": map[string]any{"max": "0"}}}).expect(422)
	if r.Code() != "setting_limits" || !strings.Contains(string(r.Raw), "1 to 16") {
		t.Fatal(string(r.Raw))
	}
	viewer := member(t, e.srv, admin, e.pid, "viewer")
	viewer.post(base+"/policies/setting-limits:impact", map[string]any{"setting_limits": map[string]any{}}).expect(403)

	// Three pipelines; two tune snapshot threads above 4. Counted in pages of two (P12).
	defer services.SetImpactPage(2)()
	var ids []string
	for i, threads := range []string{"8", "6", "2"} {
		body := e.ids.body()
		body["name"] = fmt.Sprintf("tuned-%d", i)
		body["spec"] = map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
			"tuning": map[string]any{"debezium.source.snapshot.max.threads": threads}}
		ids = append(ids, admin.post(base+"/pipelines", body).expect(201).Str("id"))
	}
	limits := map[string]any{"debezium.source.snapshot.max.threads": map[string]any{"max": "4"}, "debezium.source.poll.interval.ms": map[string]any{"pin": "500"}}
	imp := admin.post(base+"/policies/setting-limits:impact", map[string]any{"setting_limits": limits}).expect(200).List("items")
	if len(imp) != 1 || at(t, imp[0], "pipelines") != float64(2) || len(at(t, imp[0], "names").([]any)) != 2 {
		t.Fatal(imp)
	}
	admin.put(base+"/policies", map[string]any{"setting_limits": limits}).expect(200)
	got := admin.get(base + "/policies").expect(200)
	if got.At("setting_limits", "debezium.source.poll.interval.ms", "pin") != "500" {
		t.Fatal(string(got.Raw))
	}
	// Saving a value over the limit is refused; the pipeline saved before keeps saving other things.
	u := base + "/pipelines/" + ids[0]
	cur := admin.get(u).expect(200)
	r = admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
		"tuning": map[string]any{"debezium.source.snapshot.max.threads": "9"}}}, ifMatch(cur.At("version"))...).expect(422)
	if r.Code() != "policy_violation" || !strings.Contains(string(r.Raw), "at most 4") {
		t.Fatal(string(r.Raw))
	}
	// P8: its deploy is blocked by preflight's policy check, even forced.
	pre := admin.post(u+"/preflight", nil).expect(200)
	if !strings.Contains(string(pre.Raw), `"policy_setting_limit"`) || pre.At("ok") != false {
		t.Fatal(string(pre.Raw))
	}
	admin.post(u+"/deploy", map[string]any{"force_reason": "we know what we are doing"}).expect(409)
	// P7: the third, within the limit, deploys with the pin written into its version.
	u3 := base + "/pipelines/" + ids[2]
	if pre := admin.post(u3+"/preflight", nil).expect(200); !strings.Contains(pre.Str("properties"), "debezium.source.poll.interval.ms=500") {
		t.Fatal(pre.Str("properties"))
	}
	admin.post(u3+"/deploy", map[string]any{}).expect(200)
	if v := admin.get(u3).expect(200); v.At("spec", "tuning", "debezium.source.poll.interval.ms") != "500" {
		t.Fatal(string(v.Raw))
	}
	// A pipeline can't set another value than the pin.
	cur = admin.get(u3).expect(200)
	r = admin.patch(u3, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
		"tuning": map[string]any{"debezium.source.snapshot.max.threads": "2", "debezium.source.poll.interval.ms": "250"}}}, ifMatch(cur.At("version"))...).expect(422)
	if !strings.Contains(string(r.Raw), "sets it to 500") {
		t.Fatal(string(r.Raw))
	}
	// Promote's plan names what the target project's limits refuse.
	other := admin.post("/api/v1/projects", map[string]any{"slug": "strict", "name": "Strict"}).expect(201).Str("id")
	admin.put("/api/v1/projects/"+other+"/policies", map[string]any{"setting_limits": map[string]any{"debezium.source.snapshot.max.threads": map[string]any{"max": "1"}}}).expect(200)
	src := admin.post("/api/v1/projects/"+other+"/connections", map[string]any{"kind": "source", "type": "postgresql", "name": "src",
		"config": admin.get(base + "/connections/" + e.ids.Source).At("config"), "secrets": map[string]any{"password": "x"}}).expect(201).Str("id")
	plan := admin.post(u3+"/promote", map[string]any{"target_project_id": other, "source_id": src, "dry_run": true}).expect(200)
	if !strings.Contains(string(plan.Raw), "Refused by this project's setting limits: debezium.source.snapshot.max.threads (this project allows at most 1)") {
		t.Fatal(string(plan.Raw))
	}
}
