package api_test

import (
	"strings"
	"testing"
)

func names(t *testing.T, l []any, key string) []string {
	t.Helper()
	out := []string{}
	for _, v := range l {
		out = append(out, at(t, v, key).(string))
	}
	return out
}

func TestCopyConnectionsToAnotherProject(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	vault := newFakeVault(t)
	base := "/api/v1/projects/" + pid
	pg := func(host string) map[string]any {
		return map[string]any{"host": host, "port": 5432, "database": "shop", "user": "cdc", "ssl_mode": "disable"}
	}
	admin.post(base+"/secret-stores", map[string]any{"name": "vault", "kind": "vault", "config": map[string]any{"address": vault.URL},
		"secrets": map[string]any{"token": "root-token"}}).expect(201)
	orders := admin.post(base+"/connections", map[string]any{"kind": "source", "type": "postgresql", "name": "orders-db", "config": pg("db-dev"),
		"secrets": map[string]any{"password": "pw-secret-dev"}}).expect(201).Str("id")
	billing := admin.post(base+"/connections", map[string]any{"kind": "source", "type": "postgresql", "name": "billing-db", "config": pg("billing-dev"),
		"secrets": map[string]any{"password": "ref+vault://cdc/orders#password"}}).expect(201).Str("id")

	prod := admin.post("/api/v1/projects", map[string]any{"slug": "prod", "name": "Prod"}).expect(201).Str("id")
	pbase := "/api/v1/projects/" + prod
	prodOrders := admin.post(pbase+"/connections", map[string]any{"kind": "source", "type": "postgresql", "name": "orders-db", "config": pg("db-prod"),
		"secrets": map[string]any{"password": "pw-secret-prod"}}).expect(201)
	copyURL := base + "/connections:copy"
	clean := func(r *resp) *resp {
		t.Helper()
		for _, s := range []string{"pw-secret-dev", "pw-secret-prod", "root-token"} {
			if strings.Contains(string(r.Raw), s) {
				t.Fatalf("a secret leaked: %s", r.Raw)
			}
		}
		return r
	}

	// The plan says what stops the copy and writes nothing.
	plan := clean(admin.post(copyURL, map[string]any{"target_project_id": prod, "dry_run": true,
		"connections": []any{map[string]any{"id": orders}, map[string]any{"id": billing}}}).expect(200))
	if plan.At("ready") != false || plan.Str("items", 0, "suggested_name") != "orders-db-2" || plan.At("items", 0, "exists") != true {
		t.Fatal(string(plan.Raw))
	}
	if p := strings.Join(names(t, plan.List("stores"), "name"), ","); p != "vault" {
		t.Fatal(p)
	}
	if !strings.Contains(string(plan.Raw), "comes from the secret store 'vault'") || !strings.Contains(string(plan.Raw), "already has a connection named 'orders-db'") {
		t.Fatal(string(plan.Raw))
	}
	if r := admin.post(copyURL, map[string]any{"target_project_id": prod, "connections": []any{map[string]any{"id": orders}}}).expect(422); r.Code() != "copy_blocked" {
		t.Fatal(string(r.Raw))
	}
	if l := admin.get(pbase + "/connections").List(); len(l) != 1 {
		t.Fatal(l)
	}

	// Copy under a free name with the store along; saved secrets stay behind.
	done := clean(admin.post(copyURL, map[string]any{"target_project_id": prod,
		"connections": []any{map[string]any{"id": orders, "name": "orders-db-2"}, map[string]any{"id": billing}},
		"stores":      map[string]any{"vault": map[string]any{"action": "copy"}}}).expect(200))
	if done.At("ready") != true || done.Str("items", 0, "action") != "create" {
		t.Fatal(string(done.Raw))
	}
	if m := done.List("items", 0, "missing_secrets"); len(m) != 1 || m[0] != "password" {
		t.Fatal(m)
	}
	if m := done.List("items", 1, "missing_secrets"); len(m) != 0 {
		t.Fatal(m)
	}
	copied := clean(admin.get(pbase + "/connections/" + done.Str("items", 0, "id")).expect(200))
	if copied.Str("name") != "orders-db-2" || copied.Str("config", "host") != "db-dev" || len(copied.List("secret_fields_set")) != 0 {
		t.Fatal(string(copied.Raw))
	}
	if m := copied.List("missing_secrets"); len(m) != 1 {
		t.Fatal(string(copied.Raw))
	}
	ref := admin.get(pbase + "/connections/" + done.Str("items", 1, "id")).expect(200)
	if ref.Str("secret_refs", "password") != "ref+vault://cdc/orders#password" {
		t.Fatal(string(ref.Raw))
	}
	stores := admin.get(pbase + "/secret-stores").expect(200)
	if s := stores.List(); len(s) != 1 || at(t, s[0], "name") != "vault" || len(at(t, s[0], "secret_fields_set").([]any)) != 0 || at(t, s[0], "secrets_updated_at") != nil {
		t.Fatal(string(stores.Raw))
	}
	// A connection without its required secret can't be used: a blocking check says so.
	check := admin.post(pbase+"/connections/"+done.Str("items", 0, "id")+"/check", map[string]any{}).expect(200)
	if !strings.Contains(string(check.Raw), "secret_missing") || check.At("ok") != false {
		t.Fatal(string(check.Raw))
	}
	// Entering it makes the connection whole.
	fixed := admin.patch(pbase+"/connections/"+done.Str("items", 0, "id"), map[string]any{"secrets": map[string]any{"password": "pw-new"}}, ifMatch(1)...).expect(200)
	if len(fixed.List("missing_secrets")) != 0 {
		t.Fatal(string(fixed.Raw))
	}

	// Updating the target's connection replaces its settings and keeps its own password.
	upd := clean(admin.post(copyURL, map[string]any{"target_project_id": prod, "dry_run": true,
		"connections": []any{map[string]any{"id": orders, "update": true}}}).expect(200))
	if upd.Str("items", 0, "action") != "update" || upd.Str("items", 0, "id") != prodOrders.Str("id") || upd.Str("items", 0, "changes", 0, "field") != "host" {
		t.Fatal(string(upd.Raw))
	}
	admin.post(copyURL, map[string]any{"target_project_id": prod, "connections": []any{map[string]any{"id": orders, "update": true}}}).expect(200)
	after := admin.get(pbase + "/connections/" + prodOrders.Str("id")).expect(200)
	if after.Str("config", "host") != "db-dev" || len(after.List("secret_fields_set")) != 1 || after.At("version") != 2.0 {
		t.Fatal(string(after.Raw))
	}

	// Admins may copy saved secrets: on the server, never shown.
	withSecrets := clean(admin.post(copyURL, map[string]any{"target_project_id": prod, "secrets": "copy",
		"connections": []any{map[string]any{"id": orders, "name": "orders-db-3"}}}).expect(200))
	if len(withSecrets.List("items", 0, "missing_secrets")) != 0 {
		t.Fatal(string(withSecrets.Raw))
	}
	if got := admin.get(pbase + "/connections/" + withSecrets.Str("items", 0, "id")).List("secret_fields_set"); len(got) != 1 {
		t.Fatal(got)
	}

	// The target's policies apply.
	admin.put(pbase+"/policies", map[string]any{"allowed_source_types": []string{"mysql"}}).expect(200)
	if p := admin.post(copyURL, map[string]any{"target_project_id": prod, "dry_run": true, "connections": []any{map[string]any{"id": orders, "name": "x-db"}}}).expect(200); !strings.Contains(string(p.Raw), "don't allow postgresql sources") {
		t.Fatal(string(p.Raw))
	}
	admin.put(pbase+"/policies", map[string]any{"allowed_source_types": nil}).expect(200)

	// Both audit logs record the copy.
	if ev := admin.get(base + "/events?audit_only=true"); !strings.Contains(string(ev.Raw), "Copied source orders-db to Prod as orders-db-2, without its saved secrets") {
		t.Fatal(string(ev.Raw))
	}
	if ev := admin.get(pbase + "/events?audit_only=true"); !strings.Contains(string(ev.Raw), "Copied source orders-db-3 from Default, with its saved secrets") {
		t.Fatal(string(ev.Raw))
	}

	// Editors need connection.manage in both projects, and never copy secrets or stores.
	editor := member(t, srv, admin, pid, "editor", "copier@example.com")
	editor.post(copyURL, map[string]any{"target_project_id": prod, "dry_run": true, "connections": []any{map[string]any{"id": orders, "name": "e-db"}}}).expect(404)
	admin.put(pbase+"/members", map[string]any{"email": "copier@example.com", "role": "viewer"}).expect(200)
	editor.post(copyURL, map[string]any{"target_project_id": prod, "dry_run": true, "connections": []any{map[string]any{"id": orders, "name": "e-db"}}}).expect(403)
	admin.put(pbase+"/members", map[string]any{"email": "copier@example.com", "role": "editor"}).expect(200)
	editor.post(copyURL, map[string]any{"target_project_id": prod, "connections": []any{map[string]any{"id": orders, "name": "e-db"}}}).expect(200)
	if r := editor.post(copyURL, map[string]any{"target_project_id": prod, "secrets": "copy", "connections": []any{map[string]any{"id": orders, "name": "e2-db"}}}).expect(403); r.Code() != "admins_only" {
		t.Fatal(string(r.Raw))
	}
	editor.post(copyURL, map[string]any{"target_project_id": prod, "connections": []any{map[string]any{"id": billing, "name": "b2-db"}},
		"stores": map[string]any{"vault": map[string]any{"action": "copy"}}}).expect(403)
	admin.post(copyURL, map[string]any{"target_project_id": pid, "connections": []any{map[string]any{"id": orders}}}).expect(422)
}

func TestCopySecretStoreAndTestFirst(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	vault := newFakeVault(t)
	base := "/api/v1/projects/" + pid
	store := admin.post(base+"/secret-stores", map[string]any{"name": "vault", "kind": "vault", "config": map[string]any{"address": vault.URL},
		"secrets": map[string]any{"token": "root-token"}}).expect(201)
	if store.At("secrets_updated_at") == nil {
		t.Fatal(string(store.Raw))
	}
	sid := store.Str("id")

	// Test first: a failing test saves nothing and says why.
	bad := admin.patch(base+"/secret-stores/"+sid, map[string]any{"config": map[string]any{"address": "http://127.0.0.1:1"}, "test_first": true}, ifMatch(1)...).expect(422)
	if bad.Code() != "store_test_failed" || !strings.Contains(bad.Str("detail"), "nothing was saved") {
		t.Fatal(string(bad.Raw))
	}
	if v := admin.get(base+"/secret-stores").At(0, "version"); v != 1.0 {
		t.Fatal(v)
	}
	ok := admin.patch(base+"/secret-stores/"+sid, map[string]any{"config": map[string]any{"address": vault.URL}, "test_first": true}, ifMatch(1)...).expect(200)
	if ok.At("last_test", "ok") != true || ok.At("version") != 2.0 {
		t.Fatal(string(ok.Raw))
	}

	staging := admin.post("/api/v1/projects", map[string]any{"slug": "staging", "name": "Staging"}).expect(201).Str("id")
	u := base + "/secret-stores/" + sid + "/copy"
	dry := admin.post(u, map[string]any{"target_project_id": staging, "dry_run": true}).expect(200)
	if dry.Str("action") != "create" || len(admin.get("/api/v1/projects/"+staging+"/secret-stores").List()) != 0 {
		t.Fatal(string(dry.Raw))
	}
	made := admin.post(u, map[string]any{"target_project_id": staging}).expect(200)
	if made.Str("action") != "create" || len(made.List("store", "secret_fields_set")) != 0 || made.At("store", "secrets_updated_at") != nil {
		t.Fatal(string(made.Raw))
	}
	again := admin.post(u, map[string]any{"target_project_id": staging, "secrets": "copy"}).expect(200)
	if again.Str("action") != "update" || len(again.List("store", "secret_fields_set")) != 1 || strings.Contains(string(again.Raw), "root-token") {
		t.Fatal(string(again.Raw))
	}
	editor := member(t, srv, admin, pid, "editor")
	editor.post(u, map[string]any{"target_project_id": staging}).expect(403)
}

func TestCopiedConnectionWithoutItsPasswordCannotDeploy(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	dev := admin.post("/api/v1/projects", map[string]any{"slug": "dev", "name": "Dev"}).expect(201).Str("id")
	src := admin.get("/api/v1/projects/" + e.pid + "/connections/" + e.ids.Source).expect(200)
	cfg := src.At("config").(map[string]any)
	devSrc := admin.post("/api/v1/projects/"+dev+"/connections", map[string]any{"kind": "source", "type": "postgresql", "name": "shop-dev",
		"config": cfg, "secrets": map[string]any{"password": "whatever"}}).expect(201).Str("id")
	plan := admin.post("/api/v1/projects/"+dev+"/connections:copy", map[string]any{"target_project_id": e.pid,
		"connections": []any{map[string]any{"id": devSrc}}}).expect(200)
	copied := plan.Str("items", 0, "id")
	pipe := createPipeline(t, admin, e.pid, ids{Source: copied, Sink: e.ids.Sink, Runtime: e.ids.Runtime}, "shop.orders")
	r := admin.post("/api/v1/projects/"+e.pid+"/pipelines/"+pipe.Str("id")+"/preflight", map[string]any{}).expect(422)
	if r.Code() != "secret_missing" || !strings.Contains(r.Str("detail"), "has no password yet") {
		t.Fatal(string(r.Raw))
	}
}
