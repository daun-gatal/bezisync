package api_test

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func apply(c *client, projectID, manifest string, extra map[string]any) *resp {
	body := map[string]any{"manifest": manifest}
	for k, v := range extra {
		body[k] = v
	}
	return c.post("/api/v1/projects/"+projectID+"/pipelines:apply", body)
}

func yamlDump(t *testing.T, docs ...any) string {
	t.Helper()
	var parts []string
	for _, d := range docs {
		b, err := yaml.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, string(b))
	}
	return strings.Join(parts, "---\n")
}

func clone(t *testing.T, v any) map[string]any {
	t.Helper()
	b, _ := yaml.Marshal(v)
	var out map[string]any
	if err := yaml.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestExportApplyRoundTrip(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	base := "/api/v1/projects/" + e.pid
	exported := admin.get(base + "/pipelines/" + pipe.Str("id") + "/manifest").expect(200)
	doc := exported.At("documents", 0).(map[string]any)
	if doc["apiVersion"] != "bezisync.io/v1" || at(t, doc, "spec", "source", "connection") != "shop-pg" ||
		at(t, doc, "spec", "sink", "connection") != "events" || at(t, doc, "spec", "runtime") != "k8s" || strings.Contains(exported.Str("yaml"), "Canary") {
		t.Fatal(string(exported.Raw))
	}
	same := apply(admin, e.pid, exported.Str("yaml"), nil).expect(200)
	if same.Str(0, "action") != "unchanged" || len(same.List(0, "diff")) != 0 {
		t.Fatal(string(same.Raw))
	}
	at(t, doc, "spec", "sink").(map[string]any)["naming"] = "ci.{table}"
	doc["metadata"].(map[string]any)["owner"] = "data-platform"
	changed := yamlDump(t, doc)
	dry := apply(admin, e.pid, changed, map[string]any{"dry_run": true}).expect(200)
	plus := false
	for _, l := range dry.List(0, "diff") {
		plus = plus || (strings.HasPrefix(l.(string), "+") && strings.Contains(l.(string), "ci.{table}"))
	}
	if dry.Str(0, "action") != "would_update" || !plus {
		t.Fatal(string(dry.Raw))
	}
	if v := admin.get(base + "/pipelines/" + pipe.Str("id")).At("current_version"); v != float64(1) {
		t.Fatal("dry run wrote", v)
	}
	real := apply(admin, e.pid, changed, nil).expect(200)
	// The undeployed draft is updated in place: one change, one version.
	if real.Str(0, "action") != "updated" || real.At(0, "version") != float64(1) {
		t.Fatal(string(real.Raw))
	}
	if o := admin.get(base + "/pipelines/" + pipe.Str("id")).Str("owner"); o != "data-platform" {
		t.Fatal(o)
	}
	newDoc := clone(t, doc)
	newDoc["metadata"] = map[string]any{"name": "refunds-cdc"}
	newDoc["spec"].(map[string]any)["source"] = map[string]any{"connection": "shop-pg", "tables": map[string]any{"include": []string{"shop.refunds"}}}
	broken := clone(t, newDoc)
	broken["metadata"] = map[string]any{"name": "broken"}
	broken["spec"].(map[string]any)["sink"] = map[string]any{"connection": "nope"}
	res := apply(admin, e.pid, yamlDump(t, newDoc, broken), nil).expect(422)
	if res.Code() != "unknown_connection" {
		t.Fatal(string(res.Raw))
	}
	for _, p := range admin.get(base + "/pipelines").List() {
		if at(t, p, "name") == "refunds-cdc" {
			t.Fatal("partial apply")
		}
	}
	created := apply(admin, e.pid, yamlDump(t, newDoc), map[string]any{"deploy": true}).expect(200)
	if created.Str(0, "action") != "created" || created.Str(0, "deploy") != "deploying" {
		t.Fatal(string(created.Raw))
	}
	bad := apply(admin, e.pid, "apiVersion: v0\nkind: Pipeline\nmetadata: {name: x}\nspec: {}\n", nil).expect(422)
	if bad.Code() != "invalid_manifest" {
		t.Fatal(string(bad.Raw))
	}
	apply(admin, e.pid, yamlDump(t, newDoc, newDoc), nil).expect(422)
	apply(member(t, e.srv, admin, e.pid, "viewer"), e.pid, yamlDump(t, newDoc), nil).expect(403)
	all := admin.get(base + "/pipelines:export").expect(200)
	if len(all.List("documents")) != 2 || !strings.Contains(all.Str("yaml"), "refunds-cdc") {
		t.Fatal(string(all.Raw))
	}
}

func TestAdoptApplicationProperties(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	props := `
# from an existing deployment
debezium.sink.type=kafka
debezium.sink.kafka.producer.bootstrap.servers=kafka:9092
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.database.hostname=orders-db.internal
debezium.source.database.port=5432
debezium.source.database.user=debezium
debezium.source.database.password=hunter2
debezium.source.database.dbname=orders
debezium.source.topic.prefix=orders
debezium.source.slot.name=debezium_orders
debezium.source.table.include.list=public\\.orders,public\\.order_.*
debezium.source.snapshot.mode=no_data
debezium.source.decimal.handling.mode=string
debezium.source.offset.storage.file.filename=/data/offsets.dat
debezium.transforms=unwrap
debezium.transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
debezium.format.value=json
`
	r := admin.post("/api/v1/projects/"+pid+"/pipelines:adopt", map[string]any{"properties": props}).expect(200)
	if r.Str("source_type") != "postgresql" || r.Str("sink_type") != "kafka" {
		t.Fatal(string(r.Raw))
	}
	hint := r.At("source_hint").(map[string]any)
	if len(hint) != 4 || hint["host"] != "orders-db.internal" || hint["port"] != float64(5432) || hint["database"] != "orders" || hint["user"] != "debezium" {
		t.Fatal(hint)
	}
	if inc := r.List("spec", "source", "tables", "include"); len(inc) != 1 || inc[0] != "public.orders" {
		t.Fatal(inc)
	}
	if r.Str("spec", "snapshot", "mode") != "no_data" || r.Str("spec", "sink", "naming") != "orders.{schema}.{table}" || r.Str("spec", "transforms", 0, "type") != "unwrap" {
		t.Fatal(string(r.Raw))
	}
	// Value formats BeziSync offers are mapped to the spec, not kept as advanced properties.
	if r.Str("spec", "values", "decimals") != "string" || r.At("spec", "advanced") != nil {
		t.Fatal(string(r.Raw))
	}
	managed := strings.Join(toStrings(r.List("managed")), " ")
	warnings := strings.Join(toStrings(r.List("warnings")), " ")
	if !strings.Contains(managed, "debezium.source.slot.name") || !strings.Contains(managed, "debezium.source.database.password") ||
		!strings.Contains(warnings, "plain-text database password") || !strings.Contains(warnings, "debezium_orders") ||
		!strings.Contains(warnings, "regular expressions") || strings.Contains(string(r.Raw), "hunter2") {
		t.Fatal(string(r.Raw))
	}
	admin.post("/api/v1/projects/"+pid+"/pipelines:adopt", map[string]any{"properties": ""}).expect(422)
}

func toStrings(l []any) []string {
	out := make([]string, len(l))
	for i, v := range l {
		out[i], _ = v.(string)
	}
	return out
}

func TestPromoteBetweenProjects(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	target := admin.post("/api/v1/projects", map[string]any{"slug": "prod", "name": "Prod"}).expect(201).Str("id")
	role := at(t, admin.get("/api/v1/projects/"+e.pid+"/connections/"+e.ids.Source).JSON(), "config", "user").(string)
	tids := setupProject(t, admin, target, role, kafkaCluster(t))
	u := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id") + "/promote"
	body := map[string]any{"target_project_id": target, "source_id": tids.Source, "sink_id": tids.Sink, "runtime_id": tids.Runtime}
	with := func(extra map[string]any) map[string]any {
		out := map[string]any{}
		for k, v := range body {
			out[k] = v
		}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	if a := admin.post(u, with(map[string]any{"dry_run": true})).expect(200).Str("action"); a != "would_create" {
		t.Fatal(a)
	}
	if l := admin.get("/api/v1/projects/" + target + "/pipelines").List(); len(l) != 0 {
		t.Fatal(l)
	}
	admin.put("/api/v1/projects/"+target+"/policies", map[string]any{"require_approval": true}).expect(200)
	done := admin.post(u, with(map[string]any{"deploy": true})).expect(200)
	if done.Str("action") != "created" || done.Str("deploy") != "waiting_for_approval" {
		t.Fatal(string(done.Raw))
	}
	promoted := admin.get("/api/v1/projects/" + target + "/pipelines/" + done.Str("pipeline_id")).expect(200)
	if promoted.Str("source", "id") != tids.Source || promoted.Str("pending_change", "state") != "pending" {
		t.Fatal(string(promoted.Raw))
	}
	admin.post(u, with(map[string]any{"source_id": tids.Sink})).expect(422)
	admin.post(u, with(map[string]any{"target_project_id": e.pid})).expect(422)
	bare := admin.post("/api/v1/projects", map[string]any{"slug": "staging", "name": "Staging"}).expect(201).Str("id")
	src := admin.post("/api/v1/projects/"+bare+"/connections", map[string]any{"kind": "source", "type": "postgresql", "name": "other-pg",
		"config":  map[string]any{"host": "db", "port": 5432, "database": "x", "user": "u", "ssl_mode": "disable"},
		"secrets": map[string]any{"password": "pw"}}).expect(201).Str("id")
	drafted := admin.post(u, map[string]any{"target_project_id": bare, "source_id": src}).expect(200)
	if drafted.Str("action") != "created" {
		t.Fatal(string(drafted.Raw))
	}
	got := admin.get("/api/v1/projects/" + bare + "/pipelines/" + drafted.Str("pipeline_id")).expect(200)
	if got.At("sink") != nil || got.At("runtime") != nil {
		t.Fatal(string(got.Raw))
	}
	member(t, e.srv, admin, e.pid, "editor").post(u, body).expect(404)
}

func TestAPITokens(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	anon := newClient(t, srv)
	created := admin.post("/api/v1/auth/tokens", map[string]any{"name": "ci", "project_id": pid}).expect(201)
	token := created.Str("token")
	if !strings.HasPrefix(token, "bzt_") {
		t.Fatal(token)
	}
	listed := admin.get("/api/v1/auth/tokens").expect(200)
	if listed.Str(0, "prefix") != token[:10] || listed.At(0, "token") != nil {
		t.Fatal(string(listed.Raw))
	}
	bearer := []string{"authorization", "Bearer " + token}
	anon.get("/api/v1/projects/"+pid+"/pipelines", bearer...).expect(200)
	other := admin.post("/api/v1/projects", map[string]any{"slug": "other", "name": "Other"}).expect(201).Str("id")
	anon.get("/api/v1/projects/"+other+"/pipelines", bearer...).expect(404)
	anon.post("/api/v1/auth/tokens", map[string]any{"name": "x"}, bearer...).expect(403)
	ro := admin.post("/api/v1/auth/tokens", map[string]any{"name": "ro", "read_only": true}).expect(201).Str("token")
	anon.post("/api/v1/projects/"+pid+"/pipelines:adopt", map[string]any{"properties": "a=b"}, "authorization", "Bearer "+ro).expect(403)
	admin.del("/api/v1/auth/tokens/" + created.Str("id")).expect(204)
	anon.get("/api/v1/projects/"+pid+"/pipelines", bearer...).expect(401)
}
