package api_test

import (
	"strings"
	"testing"
)

func TestTuningSavedCompiledAndRefused(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid + "/pipelines"
	body := e.ids.body()
	body["name"], body["owner"] = "tuned-cdc", "payments"
	body["spec"] = map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
		"tuning": map[string]any{"debezium.source.snapshot.max.threads": "4"}}
	p := admin.post(base, body).expect(201)
	u := base + "/" + p.Str("id")
	if got := p.Str("spec", "tuning", "debezium.source.snapshot.max.threads"); got != "4" {
		t.Fatal(string(p.Raw))
	}
	pre := admin.post(u+"/preflight", nil).expect(200)
	if !strings.Contains(pre.Str("properties"), "debezium.source.snapshot.max.threads=4") {
		t.Fatal(pre.Str("properties"))
	}
	// Refused when saved, each key with why; nothing changes.
	bad := admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
		"tuning": map[string]any{"debezium.source.snapshot.max.threads": "04", "source.connection.pool.size": "5", "debezium.source.max.batch.size": "9"}}},
		ifMatch(p.At("version"))...).expect(422)
	if bad.Code() != "tuning" {
		t.Fatal(string(bad.Raw))
	}
	msgs := map[string]string{}
	for _, e := range bad.List("errors") {
		m := e.(map[string]any)
		loc := m["loc"].([]any)
		msgs[loc[len(loc)-1].(string)] = m["message"].(string)
	}
	if msgs["debezium.source.snapshot.max.threads"] != "write it as 4" || !strings.Contains(msgs["source.connection.pool.size"], "Flink CDC") ||
		msgs["debezium.source.max.batch.size"] != "is not a setting BeziSync offers" {
		t.Fatal(msgs)
	}
	if got := admin.get(u).Str("spec", "tuning", "debezium.source.snapshot.max.threads"); got != "4" {
		t.Fatal(got)
	}
	// Removing every setting drops the block from the spec.
	cur := admin.get(u)
	admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}, "tuning": map[string]any{}}},
		ifMatch(cur.At("version"))...).expect(200)
	if strings.Contains(string(admin.get(u).Raw), `"tuning"`) {
		t.Fatal("empty tuning is dumped")
	}
}

// Preflight and the version diff say what a deploy changes in the settings, and how heavily.
func TestSettingsChangeInPreflightAndDiff(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	p := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := "/api/v1/projects/" + e.pid + "/pipelines/" + p.Str("id")
	pre := admin.post(u+"/preflight", nil).expect(200)
	if pre.At("settings_change") != nil {
		t.Fatal("nothing set, nothing changes:", string(pre.Raw))
	}
	admin.post(u+"/deploy", map[string]any{}).expect(200)
	e.tick()
	admin.put("/api/v1/projects/"+e.pid+"/policies", map[string]any{"allow_advanced_properties": true}).expect(200)
	cur := admin.get(u)
	admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
		"tuning":   map[string]any{"debezium.source.snapshot.max.threads": "4"},
		"advanced": map[string]any{"debezium.source.driver.loginTimeout": "30"}}}, ifMatch(cur.At("version"))...).expect(200)
	pre = admin.post(u+"/preflight", nil).expect(200)
	sc := pre.At("settings_change")
	if sc == nil || at(t, sc, "impact") != "first_copy" {
		t.Fatal(string(pre.Raw))
	}
	settings := at(t, sc, "settings").([]any)
	if len(settings) != 2 || at(t, settings[0], "title") != "Snapshot threads" || at(t, settings[0], "from") != nil || at(t, settings[0], "to") != "4" ||
		at(t, settings[1], "unverified") != true || at(t, settings[1], "impact") != "restart" {
		t.Fatal(settings)
	}
	diff := admin.get(u + "/diff?from=1&to=2").expect(200)
	if at(t, diff.JSON(), "settings_change", "impact") != "first_copy" {
		t.Fatal(string(diff.Raw))
	}
}
