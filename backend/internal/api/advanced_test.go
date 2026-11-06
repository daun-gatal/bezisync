package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/db"
)

func advancedErrors(t *testing.T, r *resp) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, e := range r.List("errors") {
		m := e.(map[string]any)
		loc := m["loc"].([]any)
		out[loc[len(loc)-1].(string)] = m["message"].(string)
	}
	return out
}

func TestAdvancedPropertiesRefusedWhenSaved(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid + "/pipelines"
	spec := func(advanced map[string]any) map[string]any {
		return map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}, "advanced": advanced}
	}
	// A new pipeline can't carry them.
	body := e.ids.body()
	body["name"], body["owner"] = "raw-cdc", "payments"
	body["spec"] = spec(map[string]any{
		"debezium.sink.kafka.producer.bootstrap.servers": "evil:9092",
		"debezium.source.database.sslpassword":           "pw",
		"debezium.source.slot.drop.on.stop":              "true",
		"debezium.sink.kafka.producer.linger.ms":         "${BZ_STATE_PASSWORD}",
	})
	r := admin.post(base, body).expect(422)
	if r.Code() != "managed_property" {
		t.Fatal(string(r.Raw))
	}
	errs := advancedErrors(t, r)
	if len(errs) != 4 || !strings.Contains(errs["debezium.source.database.sslpassword"], "connection") ||
		!strings.Contains(errs["debezium.sink.kafka.producer.linger.ms"], "${") {
		t.Fatal(errs)
	}
	// I5: nothing echoes a value, and nothing was stored.
	if strings.Contains(string(r.Raw), "evil:9092") || strings.Contains(string(r.Raw), `"pw"`) {
		t.Fatal(string(r.Raw))
	}
	// Allowed ones save, and an edit can't add a refused one.
	body["spec"] = spec(map[string]any{"debezium.source.poll.interval.ms": "250"})
	p := admin.post(base, body).expect(201)
	u := base + "/" + p.Str("id")
	bad := admin.patch(u, map[string]any{"spec": spec(map[string]any{"debezium.source.poll.interval.ms": "250", "debezium.source.converters": "geo"})},
		ifMatch(p.At("version"))...).expect(422)
	if errs := advancedErrors(t, bad); len(errs) != 1 || errs["debezium.source.converters"] == "" {
		t.Fatal(string(bad.Raw))
	}
	// Changing a refused key's value counts as adding it.
	if got := admin.get(u).Str("spec", "advanced", "debezium.source.poll.interval.ms"); got != "250" {
		t.Fatal(got)
	}
}

func TestAdvancedPropertiesFromBeforeKeepSavingButCantDeploy(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	p := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := "/api/v1/projects/" + e.pid + "/pipelines/" + p.Str("id")
	// A spec saved by an older release, with keys this one refuses.
	old := map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}}},
		"advanced": map[string]any{"debezium.source.max.queue.size": "65536", "debezium.source.heartbeat.action.query": "INSERT INTO x VALUES (1)",
			"debezium.source.poll.interval.ms": "250"}}
	raw, _ := json.Marshal(old)
	ctx := context.Background()
	if _, err := db.MustPool(ctx).Exec(ctx, `UPDATE pipeline_versions SET spec = $1 WHERE pipeline_id = $2`, raw, p.Str("id")); err != nil {
		t.Fatal(err)
	}
	// Editing something else still saves: the keys it already had are unchanged.
	cur := admin.get(u).expect(200)
	edited := map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders", "shop.refunds"}}}, "advanced": old["advanced"]}
	admin.patch(u, map[string]any{"spec": edited}, ifMatch(cur.At("version"))...).expect(200)
	// Preflight blocks them, naming each one, including a key BeziSync compiled for this pipeline.
	pre := admin.post(u+"/preflight", nil).expect(200)
	if pre.At("ok") != false {
		t.Fatal(string(pre.Raw))
	}
	var check map[string]any
	for _, s := range pre.List("sections") {
		for _, c := range at(t, s, "report", "checks").([]any) {
			if at(t, c, "code") == "advanced_properties" {
				check = c.(map[string]any)
			}
		}
	}
	if check == nil || check["status"] != "fail" || check["blocking"] != true {
		t.Fatal(string(pre.Raw))
	}
	msg := check["message"].(string)
	if !strings.Contains(msg, "debezium.source.heartbeat.action.query") || !strings.Contains(msg, "debezium.source.max.queue.size") ||
		strings.Contains(msg, "poll.interval") || strings.Contains(msg+fmt.Sprint(check["technical"]), "INSERT INTO x") {
		t.Fatal(msg)
	}
	// A forced deploy can't skip it.
	forced := admin.post(u+"/deploy", map[string]any{"force_reason": "we know what we are doing"}).expect(409)
	if !strings.Contains(string(forced.Raw), "cannot be skipped") {
		t.Fatal(string(forced.Raw))
	}
	// Removing them leaves the allowed one, which still needs the project policy.
	cur = admin.get(u).expect(200)
	clean := map[string]any{"source": edited["source"], "advanced": map[string]any{"debezium.source.poll.interval.ms": "250"}}
	admin.patch(u, map[string]any{"spec": clean}, ifMatch(cur.At("version"))...).expect(200)
	codes := func() map[string]map[string]any {
		out := map[string]map[string]any{}
		for _, s := range admin.post(u+"/preflight", nil).expect(200).List("sections") {
			for _, c := range at(t, s, "report", "checks").([]any) {
				out[at(t, c, "code").(string)] = c.(map[string]any)
			}
		}
		return out
	}
	off := codes()
	if off["policy_advanced_properties"]["status"] != "fail" || !strings.Contains(off["policy_advanced_properties"]["message"].(string), "debezium.source.poll.interval.ms") {
		t.Fatal(off["policy_advanced_properties"])
	}
	// Allowed by policy: preflight passes, with the property shown as not verified (unknown).
	admin.put("/api/v1/projects/"+e.pid+"/policies", map[string]any{"allow_advanced_properties": true}).expect(200)
	pre = admin.post(u+"/preflight", nil).expect(200)
	if pre.At("ok") != true {
		t.Fatal(string(pre.Raw))
	}
	on := codes()
	if on["advanced_unverified"]["status"] != "unknown" || on["advanced_unverified"]["blocking"] != false || on["policy_advanced_properties"] != nil {
		t.Fatal(on["advanced_unverified"])
	}
	for _, s := range pre.List("sections") {
		if at(t, s, "key") == "config" && at(t, s, "report", "summary") != "Ready · 1 not verified" {
			t.Fatal(at(t, s, "report", "summary"))
		}
	}
	// Turning the policy off again is returned without the key (stored dumps stay unchanged).
	got := admin.put("/api/v1/projects/"+e.pid+"/policies", map[string]any{}).expect(200)
	if _, present := got.JSON().(map[string]any)["allow_advanced_properties"]; present {
		t.Fatal(string(got.Raw))
	}
}

// I5: an older pipeline's credential-looking advanced property is never repeated in what a
// deploy would change.
func TestSettingsChangeHidesCredentials(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	p := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := "/api/v1/projects/" + e.pid + "/pipelines/" + p.Str("id")
	old := map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}}},
		"advanced": map[string]any{"debezium.source.database.sslpassword": "legacy-s3cret"}}
	raw, _ := json.Marshal(old)
	ctx := context.Background()
	if _, err := db.MustPool(ctx).Exec(ctx, `UPDATE pipeline_versions SET spec = $1 WHERE pipeline_id = $2`, raw, p.Str("id")); err != nil {
		t.Fatal(err)
	}
	pre := admin.post(u+"/preflight", nil).expect(200)
	if strings.Contains(string(pre.Raw), "legacy-s3cret") {
		t.Fatal(string(pre.Raw))
	}
	if at(t, pre.JSON(), "settings_change", "settings").([]any)[0].(map[string]any)["to"] != "(hidden)" ||
		!strings.Contains(pre.Str("properties"), "debezium.source.database.sslpassword=(hidden)") {
		t.Fatal(string(pre.Raw))
	}
	cfg := admin.get(u + "/config").expect(200)
	if strings.Contains(string(cfg.Raw), "legacy-s3cret") {
		t.Fatal(string(cfg.Raw))
	}
}
