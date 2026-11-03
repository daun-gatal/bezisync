package api_test

import (
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/config"
)

func TestTypedTransformsThroughTheAPI(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := base + "/pipelines/" + pipe.Str("id")
	tables := map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}
	steps := []any{
		map[string]any{"type": "filter", "id": "keep", "operations": []string{"insert", "update"},
			"rules": []any{map[string]any{"field": "total", "op": "gte", "value": 10}}},
		map[string]any{"type": "unwrap", "id": "shape"},
		map[string]any{"type": "rename", "id": "ren1", "renames": []any{map[string]any{"from": "total", "to": "amount"}}},
		map[string]any{"type": "add_field", "id": "add1", "name": "source_system", "value": map[string]any{"kind": "static", "text": "orders-eu"}},
	}
	saved := admin.patch(u, map[string]any{"spec": map[string]any{"source": tables, "transforms": steps,
		"values": map[string]any{"decimals": "string"}, "keys": map[string]any{"shop.orders": []string{"id"}}}},
		ifMatch(pipe.At("version"))...).expect(200)
	if !strings.Contains(string(saved.Raw), `{"type":"rename","id":"ren1","renames":[{"from":"total","to":"amount"}]}`) ||
		saved.Str("spec", "values", "decimals") != "string" {
		t.Fatal(string(saved.Raw))
	}
	pre := admin.post(u+"/preflight", nil).expect(200)
	props := pre.Str("properties")
	for _, want := range []string{
		"debezium.transforms=bzmeta,t_keep,t_shape,t_ren1,t_add1,bzroute",
		"debezium.transforms.t_ren1.renames=total:amount",
		"debezium.source.decimal.handling.mode=string",
		`debezium.source.message.key.columns=shop\\.orders:id`,
	} {
		if !strings.Contains(props, want) {
			t.Errorf("properties lack %s:\n%s", want, props)
		}
	}
	// The same spec saved again is not a new version (stable dump and hash).
	again := admin.patch(u, map[string]any{"spec": saved.At("spec")}, ifMatch(admin.get(u).At("version"))...).expect(200)
	if again.At("current_version") != saved.At("current_version") {
		t.Errorf("unchanged spec made a new version: %v → %v", saved.At("current_version"), again.At("current_version"))
	}
	// Invalid steps are refused with their location.
	bad := admin.patch(u, map[string]any{"spec": map[string]any{"source": tables,
		"transforms": []any{map[string]any{"type": "rename", "renames": []any{map[string]any{"from": "total", "to": "amount"}}}}}},
		ifMatch(admin.get(u).At("version"))...).expect(422)
	if bad.Code() != "validation_failed" || !strings.Contains(string(bad.Raw), "needs the row-after-change event shape") ||
		!strings.Contains(string(bad.Raw), `"loc":["body","spec","transforms","0"]`) {
		t.Fatal(string(bad.Raw))
	}
	leak := admin.patch(u, map[string]any{"spec": map[string]any{"source": tables, "transforms": []any{
		map[string]any{"type": "unwrap"},
		map[string]any{"type": "add_field", "name": "x", "value": map[string]any{"kind": "static", "text": "${BZ_SRC_PASSWORD}"}}}}},
		ifMatch(admin.get(u).At("version"))...).expect(422)
	if !strings.Contains(string(leak.Raw), "Debezium Server would read it as a variable") {
		t.Fatal(string(leak.Raw))
	}
	// A custom step needs its policy.
	admin.patch(u, map[string]any{"spec": map[string]any{"source": tables, "transforms": []any{
		map[string]any{"type": "unwrap"}, map[string]any{"type": "custom", "class": "com.acme.Smt"}}}},
		ifMatch(admin.get(u).At("version"))...).expect(200)
	if !strings.Contains(string(admin.post(u+"/preflight", nil).expect(200).Raw), "policy_custom_transform") {
		t.Fatal("custom step passed without the policy")
	}
	admin.put(base+"/policies", map[string]any{"allow_custom_transforms": true}).expect(200)
	if strings.Contains(string(admin.post(u+"/preflight", nil).expect(200).Raw), "policy_custom_transform") {
		t.Fatal("custom step still refused with the policy on")
	}
}

func TestTransformPreviewEndpoint(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := base + "/pipelines/" + pipe.Str("id") + "/transforms:preview"
	cols := []any{map[string]any{"name": "id", "type": "integer"}, map[string]any{"name": "total", "type": "integer"}}
	// Default: the latest version (row shape, deletes rewritten).
	res := admin.post(u, map[string]any{"table": "shop.orders", "operation": "insert", "columns": cols, "primary_key": []string{"id"}}).expect(200)
	if res.Str("outcome") != "delivered" || res.Str("destination") != "cdc.shop.orders" ||
		!strings.Contains(string(res.Raw), `"output":{"key":{"id":1042},"value":{"id":1042,"total":7,"__deleted":"false","__op":"c","__source_ts_ms":1793351642118}}`) {
		t.Fatal(string(res.Raw))
	}
	// A draft spec drops the event.
	draft := map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
		"transforms": []any{map[string]any{"type": "filter", "operations": []string{"delete"}}, map[string]any{"type": "unwrap"}}}
	res = admin.post(u, map[string]any{"spec": draft, "table": "shop.orders", "columns": cols}).expect(200)
	if res.Str("outcome") != "dropped" || res.At("output") != nil || res.At("fields") != nil {
		t.Fatal(string(res.Raw))
	}
	if c := admin.post(u, map[string]any{"table": "shop.nope", "columns": cols}).expect(422).Code(); c != "table_not_captured" {
		t.Fatal(c)
	}
	admin.post(u, map[string]any{"table": "shop.orders", "operation": "upsert"}).expect(422)
	admin.post(u, map[string]any{"table": "shop.orders", "columns": []any{map[string]any{"name": "id", "extra": true}}}).expect(422)
	// Viewers may preview; other projects see nothing.
	viewer := member(t, e.srv, admin, e.pid, "viewer")
	viewer.post(u, map[string]any{"table": "shop.orders", "columns": cols}).expect(200)
	other := admin.post("/api/v1/projects", map[string]any{"slug": "other-preview", "name": "Other"}).expect(201).Str("id")
	admin.post("/api/v1/projects/"+other+"/pipelines/"+pipe.Str("id")+"/transforms:preview", map[string]any{"table": "shop.orders"}).expect(404)
}

func TestTransformDriftAfterASchemaChange(t *testing.T) {
	withConfig(t, func(s *config.Settings) { s.SchemaCheckSeconds = 0 })
	e := newPipelineEnv(t)
	role := at(t, e.admin.get("/api/v1/projects/"+e.pid+"/connections/"+e.ids.Source).JSON(), "config", "user").(string)
	exec(t, `DROP TABLE IF EXISTS shop.drift`)
	exec(t, `CREATE TABLE shop.drift (id serial primary key, cust_id int, status text)`)
	exec(t, `GRANT SELECT ON shop.drift TO `+role)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.drift")
	u := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	tables := map[string]any{"tables": map[string]any{"include": []string{"shop.drift"}}}
	e.admin.patch(u, map[string]any{"spec": map[string]any{"source": tables, "transforms": []any{
		map[string]any{"type": "filter", "rules": []any{map[string]any{"field": "status", "op": "is_not_null"}}},
		map[string]any{"type": "unwrap"},
		map[string]any{"type": "rename", "renames": []any{map[string]any{"from": "cust_id", "to": "customer_id"}}}}}},
		ifMatch(pipe.At("version"))...).expect(200)
	pre := e.admin.post(u+"/preflight", nil).expect(200)
	if pre.At("ok") != true || strings.Contains(string(pre.Raw), `"code":"transform_`) {
		t.Fatal(string(pre.Raw))
	}
	e.admin.post(u+"/deploy", map[string]any{}).expect(200)
	e.tick()
	e.tick()
	if d := e.admin.get(u).List("status", "transform_drift"); len(d) != 0 {
		t.Fatal(d)
	}
	if conditionActive(t, pipe.Str("id"), "transform_drift") {
		t.Fatal("drift alert without drift")
	}
	exec(t, `ALTER TABLE shop.drift RENAME COLUMN cust_id TO customer_ref`)
	e.tick()
	drift := e.admin.get(u).List("status", "transform_drift")
	if len(drift) != 1 || at(t, drift[0], "code") != "transform_column_missing" ||
		!strings.Contains(at(t, drift[0], "message").(string), "No field cust_id") {
		t.Fatal(drift)
	}
	if !conditionActive(t, pipe.Str("id"), "transform_drift") {
		t.Fatal("no drift alert")
	}
	// Status is carried by later ticks, and preflight says the same.
	e.tick()
	if d := e.admin.get(u).List("status", "transform_drift"); len(d) != 1 {
		t.Fatal(d)
	}
	pre = e.admin.post(u+"/preflight", nil).expect(200)
	if !strings.Contains(string(pre.Raw), `"code":"transform_column_missing"`) || pre.At("ok") != true {
		t.Fatal(string(pre.Raw))
	}
	// A rename onto an existing column blocks the deploy: Debezium would stop.
	e.admin.patch(u, map[string]any{"spec": map[string]any{"source": tables, "transforms": []any{
		map[string]any{"type": "unwrap"},
		map[string]any{"type": "rename", "renames": []any{map[string]any{"from": "customer_ref", "to": "status"}}}}}},
		ifMatch(e.admin.get(u).At("version"))...).expect(200)
	pre = e.admin.post(u+"/preflight", nil).expect(200)
	if pre.At("ok") != false || !strings.Contains(string(pre.Raw), "Two fields would be called status") {
		t.Fatal(string(pre.Raw))
	}
}

func TestTemplatesKeepOnlyStepsForEveryTable(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	admin.patch(base+"/pipelines/"+pipe.Str("id"), map[string]any{"spec": map[string]any{
		"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
		"transforms": []any{
			map[string]any{"type": "unwrap", "id": "shape"},
			map[string]any{"type": "rename", "id": "ren1", "tables": []string{"shop.orders"}, "renames": []any{map[string]any{"from": "total", "to": "amount"}}},
			map[string]any{"type": "add_field", "id": "add1", "name": "env", "value": map[string]any{"kind": "static", "text": "prod"}},
		},
		"keys": map[string]any{"shop.orders": []string{"id"}},
	}}, ifMatch(pipe.At("version"))...).expect(200)
	tpl := admin.post(base+"/templates", map[string]any{"name": "scoped", "title": "Scoped", "from_pipeline_id": pipe.Str("id")}).expect(201)
	if s := string(tpl.Raw); strings.Contains(s, "ren1") || strings.Contains(s, `"keys"`) || !strings.Contains(s, `"add1"`) {
		t.Fatal(s)
	}
	// Reading and using it works (a stored spec must always parse).
	listed := admin.get(base + "/templates").expect(200).List()
	if len(listed) != 1 {
		t.Fatal(listed)
	}
	admin.post(base+"/pipelines", map[string]any{"name": "from-template", "source_id": e.ids.Source, "template_id": at(t, listed[0], "id"),
		"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}}}).expect(201)
}
