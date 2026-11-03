package secrets_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/secrets"
)

func allowLoopback(t *testing.T) {
	t.Helper()
	s, err := config.Load(func(k string) (string, bool) {
		if k == "BEZISYNC_SSRF_ALLOW_CIDRS" {
			return `["127.0.0.0/8"]`, true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	config.Set(s)
	t.Cleanup(config.Reset)
}

func TestParseRef(t *testing.T) {
	r, err := secrets.ParseRef("ref+vault://cdc/orders#password")
	if err != nil || r == nil || r.Store != "vault" || r.Path != "cdc/orders" || *r.Key != "password" || r.String() != "ref+vault://cdc/orders#password" {
		t.Fatalf("got %+v %v", r, err)
	}
	if r, err := secrets.ParseRef("plain value"); r != nil || err != nil {
		t.Fatal("plain value parsed as a reference")
	}
	if _, err := secrets.ParseRef("ref+Bad://x"); err == nil {
		t.Fatal("malformed reference accepted")
	}
	if r, _ := secrets.ParseRef(5); r != nil {
		t.Fatal("non-string parsed")
	}
}

func TestVaultReadAndKeys(t *testing.T) {
	allowLoopback(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Vault-Token") != "tok" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		switch r.URL.Path {
		case "/v1/secret/data/cdc/orders":
			_, _ = w.Write([]byte(`{"data":{"data":{"zeta":"z","password":"p","nested":{"b":1,"a":2}}}}`))
		case "/v1/auth/token/lookup-self":
			_, _ = w.Write([]byte(`{"data":{"policies":["default","cdc"]}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	ctx := context.Background()
	c, err := secrets.Build(ctx, "vault", map[string]any{"address": srv.URL, "mount": "secret", "namespace": "", "auth_method": "token", "approle_mount": "approle"},
		map[string]any{"token": "tok"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := secrets.Get(ctx, c, "cdc/orders", nil)
	if err != nil || raw != `{"zeta": "z", "password": "p", "nested": {"b": 1, "a": 2}}` {
		t.Fatalf("raw %q %v", raw, err)
	}
	key := "password"
	if v, err := secrets.Get(ctx, c, "cdc/orders", &key); err != nil || v != "p" {
		t.Fatalf("key %q %v", v, err)
	}
	nested := "nested"
	if v, _ := secrets.Get(ctx, c, "cdc/orders", &nested); v != `{"b": 1, "a": 2}` {
		t.Fatalf("nested %q", v)
	}
	missing := "nope"
	if _, err := secrets.Get(ctx, c, "cdc/orders", &missing); err == nil || err.Error() != "'cdc/orders' has no key 'nope'." {
		t.Fatalf("missing key: %v", err)
	}
	if _, err := secrets.Get(ctx, c, "other", nil); err == nil || err.Error() != "Vault has no secret at this path." {
		t.Fatalf("404: %v", err)
	}
	if msg, err := c.Test(ctx); err != nil || msg != "Signed in to Vault (policies: default, cdc)." {
		t.Fatalf("test %q %v", msg, err)
	}
}

func TestVaultGuardBlocksLoopbackByDefault(t *testing.T) {
	config.Reset()
	t.Cleanup(config.Reset)
	c, _ := secrets.Build(context.Background(), "vault", map[string]any{"address": "http://127.0.0.1:1", "mount": "secret", "auth_method": "token"},
		map[string]any{"token": "t"})
	if _, err := c.Read(context.Background(), "x"); err == nil {
		t.Fatal("loopback Vault address allowed")
	}
}

func kubeconfig(server, user string) string {
	return `apiVersion: v1
kind: Config
current-context: c
clusters: [{name: k, cluster: {server: "` + server + `", insecure-skip-tls-verify: true}}]
contexts: [{name: c, context: {cluster: k, user: u, namespace: default}}]
users: [{name: u, user: ` + user + `}]
`
}

func TestKubernetesKubeconfig(t *testing.T) {
	allowLoopback(t)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/namespaces/cdc/secrets":
			_, _ = w.Write([]byte(`{"kind":"SecretList","apiVersion":"v1","items":[{"metadata":{"name":"orders"},"type":"Opaque"},` +
				`{"metadata":{"name":"default-token"},"type":"kubernetes.io/service-account-token"},{"metadata":{"name":"sh.helm.release.v1.x.v1"},"type":"helm.sh/release.v1"},` +
				`{"metadata":{"name":"basic"},"type":"kubernetes.io/basic-auth"}]}`))
		case "/api/v1/namespaces/cdc/secrets/orders":
			_, _ = w.Write([]byte(`{"kind":"Secret","apiVersion":"v1","metadata":{"name":"orders"},"data":{"password":"cA==","user":"dQ=="}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","code":404}`))
		}
	}))
	defer srv.Close()
	ctx := context.Background()
	c, err := secrets.Build(ctx, "kubernetes", map[string]any{"namespace": "cdc", "in_cluster": false},
		map[string]any{"kubeconfig": kubeconfig(srv.URL, "{token: tok}")})
	if err != nil {
		t.Fatal(err)
	}
	if msg, err := c.Test(ctx); err != nil || msg != "Can read Secrets in namespace cdc." {
		t.Fatalf("test %q %v", msg, err)
	}
	if v, err := c.Read(ctx, "orders"); err != nil || v != `{"password": "p", "user": "u"}` {
		t.Fatalf("read %q %v", v, err)
	}
	if paths, truncated, err := c.List(ctx, secrets.ListLimit); err != nil || truncated || strings.Join(paths, ",") != "basic,orders" {
		t.Fatalf("list %v %v %v", paths, truncated, err)
	}
	if paths, truncated, _ := c.List(ctx, 1); !truncated || len(paths) != 1 {
		t.Fatalf("limit %v %v", paths, truncated)
	}
	if keys, err := secrets.Keys(ctx, c, "orders"); err != nil || strings.Join(keys, ",") != "password,user" {
		t.Fatalf("keys %v %v", keys, err)
	}
}

func TestKubernetesKubeconfigRefusals(t *testing.T) {
	config.Reset()
	t.Cleanup(config.Reset)
	ctx := context.Background()
	cfg := map[string]any{"namespace": "cdc", "in_cluster": false}
	for name, tc := range map[string]struct{ kubeconfig, want string }{
		"missing":  {"", "A kubeconfig is required when not using the in-cluster account."},
		"exec":     {kubeconfig("https://10.0.0.1", "{exec: {apiVersion: client.authentication.k8s.io/v1beta1, command: gke-gcloud-auth-plugin}}"), "The kubeconfig signs in by running 'gke-gcloud-auth-plugin', which BeziSync can't do. Use a ServiceAccount token instead."},
		"provider": {kubeconfig("https://10.0.0.1", "{auth-provider: {name: oidc}}"), "The kubeconfig signs in with the 'oidc' auth provider, which BeziSync can't use. Use a ServiceAccount token instead."},
		"context":  {"apiVersion: v1\nkind: Config\nclusters: []\n", "The kubeconfig has no current-context."},
	} {
		_, err := secrets.Build(ctx, "kubernetes", cfg, map[string]any{"kubeconfig": tc.kubeconfig})
		if err == nil || err.Error() != tc.want {
			t.Errorf("%s: %v", name, err)
		}
	}
	// The API server is user-supplied: metadata and loopback addresses are never dialled.
	for _, server := range []string{"http://169.254.169.254", "https://127.0.0.1:6443"} {
		c, err := secrets.Build(ctx, "kubernetes", cfg, map[string]any{"kubeconfig": kubeconfig(server, "{token: t}")})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Read(ctx, "orders"); err == nil || !strings.Contains(err.Error(), "not allowed") && !strings.Contains(err.Error(), "blocked") {
			t.Errorf("%s: %v", server, err)
		}
	}
}

func TestGoogleEndpointWithoutKeySendsNoSignIn(t *testing.T) {
	allowLoopback(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/projects/demo-project/secrets/orders/versions/latest:access":
			_, _ = w.Write([]byte(`{"payload":{"data":"eyJwYXNzd29yZCI6InAifQ=="}}`))
		case "/v1/projects/demo-project/secrets":
			if r.URL.Query().Get("pageToken") == "" {
				_, _ = w.Write([]byte(`{"secrets":[{"name":"projects/demo-project/secrets/orders"}],"nextPageToken":"2"}`))
				return
			}
			_, _ = w.Write([]byte(`{"secrets":[{"name":"projects/demo-project/secrets/legacy"}]}`))
		case "/v1/projects/demo-project/secrets/plain/versions/latest:access":
			_, _ = w.Write([]byte(`{"payload":{"data":"aHVudGVyMg=="}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	ctx := context.Background()
	c, err := secrets.Build(ctx, "gcp", map[string]any{"project_id": "demo-project", "endpoint": srv.URL + "/"}, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	key := "password"
	if v, err := secrets.Get(ctx, c, "orders", &key); err != nil || v != "p" {
		t.Fatalf("read %q %v", v, err)
	}
	if msg, err := c.Test(ctx); err != nil || msg != "Connected to Secret Manager in demo-project." {
		t.Fatalf("test %q %v", msg, err)
	}
	if paths, truncated, err := c.List(ctx, secrets.ListLimit); err != nil || truncated || strings.Join(paths, ",") != "legacy,orders" {
		t.Fatalf("list %v %v %v", paths, truncated, err)
	}
	// A plain-text secret has no keys: a reference uses the whole value.
	if keys, err := secrets.Keys(ctx, c, "plain"); err != nil || keys != nil {
		t.Fatalf("plain keys %v %v", keys, err)
	}
	config.Reset()
	if _, err := secrets.Build(ctx, "gcp", map[string]any{"project_id": "demo-project", "endpoint": "http://169.254.169.254"}, map[string]any{}); err == nil {
		t.Fatal("metadata endpoint allowed")
	}
}
