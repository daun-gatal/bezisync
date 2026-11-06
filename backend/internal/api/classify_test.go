package api_test

import (
	"strings"
	"testing"
)

func TestClassifyAdvancedProperties(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	p := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := "/api/v1/projects/" + e.pid + "/pipelines/" + p.Str("id") + "/settings:classify"
	res := admin.post(u, map[string]any{"items": []any{
		map[string]any{"key": "debezium.source.driver.loginTimeout", "value": "30"},
		map[string]any{"key": "debezium.source.database.sslpassword", "value": "s3cret-value"},
		map[string]any{"key": "debezium.sink.kafka.producer.bootstrap.servers", "value": "evil:9092"},
		map[string]any{"key": "debezium.source.snapshot.max.threads", "value": "4"},
		map[string]any{"key": "debezium.source.poll.interval.ms", "value": "${BZ_STATE_PASSWORD}"},
	}}).expect(200)
	items := res.List("items")
	class := func(i int) string { return at(t, items[i], "class").(string) }
	if class(0) != "allowed" || class(1) != "credential" || class(2) != "modelled" || at(t, items[2], "fix") != "sink" || class(4) != "invalid" {
		t.Fatal(string(res.Raw))
	}
	// A key that is a typed setting comes back with it, so the wizard can offer it typed.
	if class(3) != "allowed" || at(t, items[3], "setting", "title") != "Snapshot threads" || at(t, items[0], "setting") != nil {
		t.Fatal(string(res.Raw))
	}
	// D11: values never come back.
	if strings.Contains(string(res.Raw), "s3cret-value") || strings.Contains(string(res.Raw), "evil:9092") {
		t.Fatal(string(res.Raw))
	}
	// D10: at most 200 keys.
	many := make([]any, 201)
	for i := range many {
		many[i] = map[string]any{"key": "debezium.source.a.b"}
	}
	admin.post(u, map[string]any{"items": many}).expect(422)
	// A22: another project's pipeline, or nonsense in the body for it, is a 404.
	other := admin.post("/api/v1/projects", map[string]any{"slug": "other", "name": "Other"}).expect(201).Str("id")
	wrong := "/api/v1/projects/" + other + "/pipelines/" + p.Str("id") + "/settings:classify"
	admin.post(wrong, map[string]any{"items": []any{}}).expect(404)
	admin.post(wrong, map[string]any{"nonsense": true}).expect(404)
	// The engines list carries each engine's settings.
	for _, eng := range admin.get("/api/v1/engines").expect(200).List() {
		for _, s := range at(t, eng, "settings").([]any) {
			if at(t, s, "engine") != at(t, eng, "id") {
				t.Fatal(s)
			}
		}
	}
}
