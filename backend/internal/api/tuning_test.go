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
		"tuning": map[string]any{"debezium.source.snapshot.max.threads": "04", "source.connection.pool.size": "5", "debezium.source.made.up.key": "9"}}},
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
		msgs["debezium.source.made.up.key"] != "is not a setting BeziSync offers" {
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

func TestCustomSizeReachesTheWorkload(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	body := e.ids.body()
	body["name"], body["owner"] = "custom-size", "payments"
	body["spec"] = map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
		"policies": map[string]any{"resources": "custom", "custom_resources": map[string]any{"cpu": "1.5", "memory": "2Gi"}}}
	p := admin.post("/api/v1/projects/"+e.pid+"/pipelines", body).expect(201)
	u := "/api/v1/projects/" + e.pid + "/pipelines/" + p.Str("id")
	if p.Str("spec", "policies", "custom_resources", "memory") != "2Gi" {
		t.Fatal(string(p.Raw))
	}
	admin.post(u+"/deploy", map[string]any{}).expect(200)
	e.tick()
	var found bool
	for _, w := range e.driver.Workloads {
		found = found || (w.CPU == "1.5" && w.Memory == "2Gi" && w.Heap == "1433m")
	}
	if !found {
		t.Fatal(e.driver.Workloads)
	}
	// A non-canonical size is refused, saying how to write it.
	bad := admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
		"policies": map[string]any{"resources": "custom", "custom_resources": map[string]any{"cpu": "1.5", "memory": "2048Mi"}}}},
		ifMatch(admin.get(u).At("version"))...).expect(422)
	if !strings.Contains(string(bad.Raw), "whole GiB written as Gi") {
		t.Fatal(string(bad.Raw))
	}
}

// The settings checks and recommendations read this run's evidence: the source's connections, the
// tables, the image's release.
func TestSettingChecksAndRecommendations(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	p := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := "/api/v1/projects/" + e.pid + "/pipelines/" + p.Str("id")
	src := map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}
	// A batch as big as the queue never fills: refused, naming both.
	cur := admin.get(u).expect(200)
	r := admin.patch(u, map[string]any{"spec": map[string]any{"source": src, "tuning": map[string]any{"debezium.source.max.batch.size": "8192"}}},
		ifMatch(cur.At("version"))...).expect(422)
	if !strings.Contains(string(r.Raw), "the batch size (8192) must stay below the queue size (8192)") {
		t.Fatal(string(r.Raw))
	}
	// Threads are counted against the source's free connections (read from PostgreSQL itself).
	withCols := map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}, "columns": map[string]any{"shop.orders": map[string]any{"exclude": []string{"total"}}}}
	admin.patch(u, map[string]any{"spec": map[string]any{"source": withCols, "tuning": map[string]any{"debezium.source.snapshot.max.threads": "4"}}},
		ifMatch(cur.At("version"))...).expect(200)
	pre := admin.post(u+"/preflight", nil).expect(200)
	var threads map[string]any
	for _, s := range pre.List("sections") {
		for _, c := range at(t, s, "report", "checks").([]any) {
			if at(t, c, "code") == "setting_threads" {
				threads = c.(map[string]any)
			}
		}
	}
	if threads == nil || threads["status"] != "pass" || !strings.Contains(threads["message"].(string), "4 snapshot threads need 5 connections") {
		t.Fatal(threads)
	}
	// Columns are left out: skipping unchanged updates is recommended, with when its evidence was read.
	recs := pre.List("recommendations")
	if len(recs) != 1 || at(t, recs[0], "key") != "debezium.source.skip.messages.without.change" || at(t, recs[0], "value") != "true" || at(t, recs[0], "evidence_at") == nil {
		t.Fatal(recs)
	}
}

// C19: a connector's own setting is refused on another connector; C21: MongoDB (no JDBC) refuses
// driver.* properties, when classified and when saved.
func TestPerConnectorSettings(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	p := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := base + "/pipelines/" + p.Str("id")
	src := map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}
	cur := admin.get(u).expect(200)
	r := admin.patch(u, map[string]any{"spec": map[string]any{"source": src, "tuning": map[string]any{"debezium.source.connect.keep.alive.interval.ms": "30000"}}},
		ifMatch(cur.At("version"))...).expect(422)
	if !strings.Contains(string(r.Raw), "doesn't apply to this source or destination") {
		t.Fatal(string(r.Raw))
	}
	ok := admin.patch(u, map[string]any{"spec": map[string]any{"source": src, "tuning": map[string]any{"debezium.source.slot.max.retries": "10",
		"debezium.sink.kafka.producer.linger.ms": "20"}}}, ifMatch(cur.At("version"))...).expect(200)
	pre := admin.post(u+"/preflight", nil).expect(200).Str("properties")
	if !strings.Contains(pre, "debezium.source.slot.max.retries=10") || !strings.Contains(pre, "debezium.sink.kafka.producer.linger.ms=20") {
		t.Fatal(pre)
	}
	_ = ok
	// MongoDB has no JDBC driver.
	admin.put(base+"/policies", map[string]any{"allow_advanced_properties": true}).expect(200)
	mongo := admin.post(base+"/connections", map[string]any{"kind": "source", "type": "mongodb", "name": "docs",
		"config": map[string]any{"hosts": "mongo.internal:27017", "user": "cdc"}, "secrets": map[string]any{"password": "pw"}}).expect(201)
	body := e.ids.body()
	body["name"], body["source_id"] = "docs-cdc", mongo.Str("id")
	body["spec"] = map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
		"advanced": map[string]any{"debezium.source.driver.loginTimeout": "30"}}
	if r := admin.post(base+"/pipelines", body).expect(422); !strings.Contains(string(r.Raw), "no JDBC driver") {
		t.Fatal(string(r.Raw))
	}
	body["spec"] = map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}}
	m := admin.post(base+"/pipelines", body).expect(201)
	c := admin.post(base+"/pipelines/"+m.Str("id")+"/settings:classify", map[string]any{"items": []any{map[string]any{"key": "debezium.source.driver.ssl", "value": "x"},
		map[string]any{"key": "debezium.source.mongodb.poll.interval.ms", "value": "5000"}}}).expect(200)
	items := c.List("items")
	if at(t, items[0], "class") != "invalid" || !strings.Contains(at(t, items[0], "message").(string), "no JDBC driver") || at(t, items[1], "setting", "key") != "debezium.source.mongodb.poll.interval.ms" {
		t.Fatal(string(c.Raw))
	}
}

// Import says what becomes of each key in five buckets, and a version change what it does to the
// settings (refused when a tuned setting isn't in that release).
func TestImportPlanAndVersionEffects(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	r := admin.post(base+"/pipelines:adopt", map[string]any{"properties": "debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector\n" +
		"debezium.source.table.include.list=public.orders\ndebezium.source.poll.interval.ms=250\ndebezium.source.column.exclude.list=public.orders.notes\n"}).expect(200)
	if r.Str("source_type") != "postgresql" || r.At("plan", "spec", "tuning", "debezium.source.poll.interval.ms") != "250" ||
		len(r.At("plan", "tunable").([]any)) != 1 || r.At("plan", "spec", "source", "columns", "public.orders", "exclude").([]any)[0] != "notes" {
		t.Fatal(string(r.Raw))
	}
	p := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := base + "/pipelines/" + p.Str("id")
	cur := admin.get(u).expect(200)
	admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
		"tuning": map[string]any{"debezium.source.skip.messages.without.change": "true"}}}, ifMatch(cur.At("version"))...).expect(200)
	old := "quay.io/debezium/server:1.9.7.Final"
	fx := admin.post(base+"/debezium-images:effects", map[string]any{"pipeline_ids": []string{p.Str("id")}, "image": old}).expect(200).List()
	if len(fx) != 1 || at(t, fx[0], "image") != old || at(t, fx[0], "effects").([]any)[0].(map[string]any)["effect"] != "missing" {
		t.Fatal(fx)
	}
	// The change itself refuses it, naming the setting; the pipeline keeps its version.
	res := admin.post(base+"/debezium-images:apply", map[string]any{"pipeline_ids": []string{p.Str("id")}, "image": old, "deploy": false}).expect(200).List()
	if at(t, res[0], "ok") != false || !strings.Contains(at(t, res[0], "message").(string), "debezium.source.skip.messages.without.change") {
		t.Fatal(res)
	}
	if admin.get(u).At("current_version") != cur.At("current_version") {
		t.Fatal("a refused change saved a version")
	}
	// A release that has it changes as before.
	ok := admin.post(base+"/debezium-images:apply", map[string]any{"pipeline_ids": []string{p.Str("id")}, "image": "quay.io/debezium/server:3.7.0.Final", "deploy": false}).expect(200).List()
	if at(t, ok[0], "ok") != true {
		t.Fatal(ok)
	}
	admin.post(base+"/debezium-images:effects", map[string]any{"pipeline_ids": []string{p.Str("id")}, "extra": 1}).expect(422)
}
