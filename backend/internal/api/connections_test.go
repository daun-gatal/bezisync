package api_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

const canary = "Canary-Secret-7f3a9c"

func pgTarget(t *testing.T) (map[string]any, string) {
	t.Helper()
	u, err := url.Parse(testdb.URL())
	if err != nil {
		t.Fatal(err)
	}
	port := 5432
	if p := u.Port(); p != "" {
		port, _ = strconv.Atoi(p)
	}
	pw, _ := u.User.Password()
	return map[string]any{"host": u.Hostname(), "port": port, "database": strings.TrimPrefix(u.Path, "/"), "user": u.User.Username(),
		"ssl_mode": "disable"}, pw
}

func sourceBody(t *testing.T, name string) map[string]any {
	cfg, pw := pgTarget(t)
	return map[string]any{"kind": "source", "type": "postgresql", "name": name, "config": cfg, "secrets": map[string]any{"password": pw}}
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func exec(t *testing.T, sql string) {
	t.Helper()
	if _, err := testdb.Pool(t).Exec(context.Background(), sql); err != nil {
		t.Fatal(err)
	}
}

// canaryRole is a login role whose password is the canary value.
func canaryRole(t *testing.T) string {
	t.Helper()
	role := "canary_" + randHex(4)
	cfg, _ := pgTarget(t)
	exec(t, fmt.Sprintf(`CREATE ROLE %s LOGIN REPLICATION PASSWORD '%s'`, role, canary))
	exec(t, fmt.Sprintf(`GRANT CREATE ON DATABASE "%s" TO %s`, cfg["database"], role))
	t.Cleanup(func() {
		exec(t, "DROP OWNED BY "+role)
		exec(t, "DROP ROLE "+role)
	})
	return role
}

func sampleTables(t *testing.T) {
	t.Helper()
	exec(t, `CREATE SCHEMA IF NOT EXISTS shop`)
	exec(t, `CREATE TABLE IF NOT EXISTS shop.orders (id serial primary key, email text, total numeric)`)
	exec(t, `CREATE TABLE IF NOT EXISTS shop.events (payload text)`)
	exec(t, `INSERT INTO shop.orders (email, total) VALUES ('a@x.io', 1)`)
	t.Cleanup(func() { exec(t, `DROP SCHEMA shop CASCADE`) })
}

func TestConnectionAPINeverReturnsSecrets(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	base := "/api/v1/projects/" + defaultProject(t, admin)
	payload := sourceBody(t, "orders-pg")
	payload["config"].(map[string]any)["user"] = canaryRole(t)
	payload["secrets"] = map[string]any{"password": canary}
	res := admin.post(base+"/connections", payload).expect(201)
	if l := res.List("secret_fields_set"); len(l) != 1 || l[0] != "password" {
		t.Fatal(string(res.Raw))
	}
	id := res.Str("id")
	for _, u := range []string{base + "/connections", base + "/connections/" + id} {
		if strings.Contains(string(admin.get(u).Raw), canary) {
			t.Fatalf("secret in %s", u)
		}
	}
	check := admin.post(base+"/connections/"+id+"/check", nil).expect(200)
	if check.At("ok") != true {
		t.Fatal(string(check.Raw))
	}
	if admin.get(base+"/connections/"+id).At("last_check", "ok") != true {
		t.Fatal("last check not stored")
	}
	if strings.Contains(string(admin.get(base+"/events").Raw), canary) {
		t.Fatal("secret in events")
	}
}

func TestConnectionUpdateRequiresVersionAndKeepsSecret(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	base := "/api/v1/projects/" + defaultProject(t, admin) + "/connections"
	u := base + "/" + admin.post(base, sourceBody(t, "orders-pg")).expect(201).Str("id")
	admin.patch(u, map[string]any{"description": "x"}).expect(428)
	res := admin.patch(u, map[string]any{"description": "x"}, "if-match", `"99"`).expect(412)
	if res.At("current_version") != float64(1) {
		t.Fatal(string(res.Raw))
	}
	admin.patch(u, map[string]any{"description": "x"}, "if-match", `"abc"`).expect(400)
	res = admin.patch(u, map[string]any{"description": "primary"}, "if-match", `"1"`).expect(200)
	if res.At("version") != float64(2) || len(res.List("secret_fields_set")) != 1 {
		t.Fatal(string(res.Raw))
	}
	if admin.post(u+"/check", nil).At("ok") != true {
		t.Fatal("check failed after update")
	}
	// Removing a required secret fails validation; the stored one is untouched.
	admin.patch(u, map[string]any{"secrets": map[string]any{"password": nil}}, "if-match", "W/\"2\"").expect(422)
}

func TestConnectionValidationAndDuplicates(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	base := "/api/v1/projects/" + defaultProject(t, admin) + "/connections"
	body := sourceBody(t, "orders-pg")
	body["config"] = map[string]any{"host": ""}
	bad := admin.post(base, body).expect(422)
	found := false
	for _, e := range bad.List("errors") {
		found = found || at(t, e, "loc", 0) == "config"
	}
	if !found {
		t.Fatal(string(bad.Raw))
	}
	admin.post(base, sourceBody(t, "orders-pg")).expect(201)
	admin.post(base, sourceBody(t, "orders-pg")).expect(409)
	unknown := sourceBody(t, "x")
	unknown["type"] = "db2"
	if c := admin.post(base, unknown).Code(); c != "unknown_connector" {
		t.Fatal(c)
	}
	missing := admin.post(base, map[string]any{"kind": "nope"}).expect(422)
	if len(missing.List("errors")) < 3 {
		t.Fatal(string(missing.Raw))
	}
}

func TestDraftCheckAndDiscoveryViaAPI(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	sampleTables(t)
	base := "/api/v1/projects/" + defaultProject(t, admin)
	draft := sourceBody(t, "orders-pg")
	draft["tables"] = []string{"shop.orders"}
	if res := admin.post(base+"/connections:check", draft).expect(200); res.At("ok") != true {
		t.Fatal(string(res.Raw))
	}
	id := admin.post(base+"/connections", sourceBody(t, "orders-pg")).expect(201).Str("id")
	found := false
	for _, tb := range admin.post(base+"/connections/"+id+"/discover", nil).expect(200).List("tables") {
		found = found || (at(t, tb, "schema") == "shop" && at(t, tb, "name") == "orders")
	}
	if !found {
		t.Fatal("shop.orders not discovered")
	}
	var pg any
	for _, ct := range admin.get(base + "/connector-types").expect(200).List() {
		if at(t, ct, "type") == "postgresql" {
			pg = ct
		}
	}
	if at(t, pg, "secret_schema", "properties", "password") == nil {
		t.Fatal("no password field")
	}
	rds := false
	for _, p := range at(t, pg, "presets").([]any) {
		rds = rds || at(t, p, "id") == "aws_rds"
	}
	if !rds {
		t.Fatal("no aws_rds preset")
	}
	if u := admin.get(base + "/connections/" + id + "/usage").expect(200).List(); len(u) != 0 {
		t.Fatal(u)
	}
}

func TestViewerCannotManageConnectionsAndManagedOnesNeedAdmin(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	base := "/api/v1/projects/" + pid + "/connections"
	viewer := member(t, srv, admin, pid, "viewer")
	viewer.post(base, sourceBody(t, "orders-pg")).expect(403)
	managed := sourceBody(t, "managed-pg")
	managed["managed"] = true
	id := admin.post(base, managed).expect(201).Str("id")
	editor := member(t, srv, admin, pid, "editor")
	if c := editor.del(base + "/" + id).expect(403).Code(); c != "managed_connection" {
		t.Fatal(c)
	}
	editor.get(base + "/" + id).expect(200)
	admin.del(base + "/" + id).expect(204)
	admin.get(base + "/" + id).expect(404)
}
