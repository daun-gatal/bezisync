package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

type fakeVault struct {
	*httptest.Server
	calls atomic.Int64
}

func newFakeVault(t *testing.T) *fakeVault {
	t.Helper()
	data := map[string]map[string]any{"cdc/orders": {"password": canary, "user": "app"}}
	v := &fakeVault{}
	v.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v.calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("X-Vault-Token") != "root-token" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"errors": ["permission denied"]}`))
			return
		}
		if r.Method == "LIST" {
			switch r.URL.Path {
			case "/v1/secret/metadata/":
				_, _ = w.Write([]byte(`{"data": {"keys": ["cdc/"]}}`))
			case "/v1/secret/metadata/cdc/":
				_, _ = w.Write([]byte(`{"data": {"keys": ["orders"]}}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
			return
		}
		if strings.HasSuffix(r.URL.Path, "/v1/auth/token/lookup-self") {
			_, _ = w.Write([]byte(`{"data": {"policies": ["cdc-read"]}}`))
			return
		}
		if d, ok := data[strings.TrimPrefix(r.URL.Path, "/v1/secret/data/")]; ok {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": d}})
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"errors": []}`))
	}))
	t.Cleanup(v.Close)
	return v
}

func TestConnectionSecretFromVault(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	vault := newFakeVault(t)
	role := canaryRole(t)
	base := "/api/v1/projects/" + pid
	// Tested first, a store that can't sign in is not saved at all.
	refused := admin.post(base+"/secret-stores", map[string]any{"name": "vault", "kind": "vault", "config": map[string]any{"address": vault.URL},
		"secrets": map[string]any{"token": "wrong"}, "test_first": true}).expect(422)
	if refused.Code() != "store_test_failed" || !strings.Contains(refused.Str("detail"), "nothing was saved: Vault denied access") {
		t.Fatal(string(refused.Raw))
	}
	if l := admin.get(base + "/secret-stores").expect(200).List(); len(l) != 0 {
		t.Fatal(l)
	}
	bad := admin.post(base+"/secret-stores", map[string]any{"name": "vault", "kind": "vault", "config": map[string]any{"address": vault.URL},
		"secrets": map[string]any{"token": "wrong"}}).expect(201)
	if bad.At("last_test", "ok") != false {
		t.Fatal(string(bad.Raw))
	}
	store := admin.patch(base+"/secret-stores/"+bad.Str("id"), map[string]any{"secrets": map[string]any{"token": "root-token"}}, ifMatch(1)...).expect(200)
	if store.At("last_test", "ok") != true || strings.Contains(string(store.Raw), "root-token") {
		t.Fatal(string(store.Raw))
	}
	if l := store.List("secret_fields_set"); len(l) != 1 || l[0] != "token" {
		t.Fatal(l)
	}
	probe := admin.post(base+"/secret-stores/"+store.Str("id")+"/check-ref", map[string]any{"path": "cdc/orders", "key": "password"}).expect(200)
	if probe.At("ok") != true || strings.Contains(string(probe.Raw), canary) {
		t.Fatal(string(probe.Raw))
	}
	// The picker: secret paths and key names, never a value.
	listed := admin.get(base + "/secret-stores/" + store.Str("id") + "/secrets").expect(200)
	if listed.At("ok") != true || listed.At("truncated") != false || len(listed.List("secrets")) != 1 || listed.Str("secrets", 0) != "cdc/orders" {
		t.Fatal(string(listed.Raw))
	}
	keys := admin.post(base+"/secret-stores/"+store.Str("id")+"/keys", map[string]any{"path": "cdc/orders"}).expect(200)
	if keys.At("ok") != true || keys.At("whole_value") != false || strings.Join([]string{keys.Str("keys", 0), keys.Str("keys", 1)}, ",") != "password,user" ||
		strings.Contains(string(keys.Raw), canary) {
		t.Fatal(string(keys.Raw))
	}
	if gone := admin.post(base+"/secret-stores/"+store.Str("id")+"/keys", map[string]any{"path": "cdc/gone"}).expect(200); gone.At("ok") != false ||
		gone.Str("message") != "Vault has no secret at this path." {
		t.Fatal(string(gone.Raw))
	}
	missingRef := admin.post(base+"/secret-stores/"+store.Str("id")+"/check-ref", map[string]any{"path": "cdc/gone"}).expect(200)
	if missingRef.At("ok") != false {
		t.Fatal(string(missingRef.Raw))
	}
	target, _ := pgTarget(t)
	target["user"] = role
	conn := map[string]any{"kind": "source", "type": "postgresql", "name": "orders-db", "config": target, "secrets": map[string]any{"password": "ref+nope://x#y"}}
	if r := admin.post(base+"/connections", conn).expect(422); !strings.Contains(string(r.Raw), "no secret store named 'nope'") {
		t.Fatal(string(r.Raw))
	}
	conn["secrets"] = map[string]any{"password": "ref+vault://cdc/orders#password"}
	created := admin.post(base+"/connections", conn).expect(201)
	if created.Str("secret_refs", "password") != "ref+vault://cdc/orders#password" {
		t.Fatal(string(created.Raw))
	}
	report := admin.post(base+"/connections/"+created.Str("id")+"/check", nil).expect(200)
	if strings.Contains(string(report.Raw), canary) {
		t.Fatal("canary leaked")
	}
	codes := map[any]any{}
	for _, c := range report.List("checks") {
		codes[at(t, c, "code")] = at(t, c, "status")
	}
	if codes["pg_login"] != "pass" || codes["secret_ref_unresolved"] != nil {
		t.Fatal(codes)
	}
	admin.patch(base+"/connections/"+created.Str("id"), map[string]any{"secrets": map[string]any{"password": "ref+vault://cdc/gone#password"}},
		ifMatch(created.At("version"))...).expect(200)
	broken := admin.post(base+"/connections/"+created.Str("id")+"/check", nil).expect(200)
	if broken.At("ok") != false || broken.Str("checks", 0, "code") != "secret_ref_unresolved" || !strings.Contains(broken.Str("checks", 0, "message"), "no secret at this path") {
		t.Fatal(string(broken.Raw))
	}
	admin.del(base + "/secret-stores/" + store.Str("id")).expect(409)
	viewer := member(t, srv, admin, pid, "viewer")
	if u := viewer.get(base+"/secret-stores").expect(200).List(0, "used_by"); len(u) != 1 || u[0] != "orders-db" {
		t.Fatal(u)
	}
	viewer.post(base+"/secret-stores", map[string]any{"name": "x", "kind": "vault", "config": map[string]any{"address": "http://v"}}).expect(403)
	viewer.get(base + "/secret-stores/" + store.Str("id") + "/secrets").expect(403)
	viewer.post(base+"/secret-stores/"+store.Str("id")+"/keys", map[string]any{"path": "cdc/orders"}).expect(403)
	if k := admin.get(base + "/secret-store-kinds").expect(200).List(); len(k) != 4 {
		t.Fatal(k)
	}
}

func TestDraftCheckResolvesAndCacheIsBounded(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	vault := newFakeVault(t)
	role := canaryRole(t)
	base := "/api/v1/projects/" + pid
	admin.post(base+"/secret-stores", map[string]any{"name": "vault", "kind": "vault", "config": map[string]any{"address": vault.URL},
		"secrets": map[string]any{"token": "root-token"}}).expect(201)
	target, _ := pgTarget(t)
	target["user"] = role
	body := map[string]any{"kind": "source", "type": "postgresql", "config": target, "secrets": map[string]any{"password": "ref+vault://cdc/orders#password"}}
	first := admin.post(base+"/connections:check", body).expect(200)
	if strings.Contains(string(first.Raw), canary) || first.At("ok") != true {
		t.Fatal(string(first.Raw))
	}
	calls := vault.calls.Load()
	admin.post(base+"/connections:check", body).expect(200)
	if vault.calls.Load() != calls {
		t.Fatal("not served from the cache")
	}
}
