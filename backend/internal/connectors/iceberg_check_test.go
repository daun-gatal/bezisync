package connectors

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/config"
)

// allowLoopback lets connector code reach an httptest server through the SSRF guard.
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

// ADR 0016 E1/E2: the Iceberg check only reads. Whether the namespace exists or not, it sends
// no create (no table, no namespace, no commit) and reports table create and commit as Unknown.
func TestIcebergCheckCreatesNothing(t *testing.T) {
	allowLoopback(t)
	for _, nsExists := range []bool{true, false} {
		var writes []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				writes = append(writes, r.Method+" "+r.URL.Path)
				w.WriteHeader(http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/v1/config":
				_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
			case "/v1/namespaces":
				_, _ = w.Write([]byte(`{"namespaces":[["cdc"]]}`))
			case "/v1/namespaces/cdc":
				if !nsExists {
					w.WriteHeader(http.StatusNotFound)
					_, _ = w.Write([]byte(`{"error":{"message":"no namespace","type":"NoSuchNamespaceException","code":404}}`))
					return
				}
				_, _ = w.Write([]byte(`{"namespace":["cdc"],"properties":{"location":"s3://lakehouse/cdc/"}}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))

		report, err := (&icebergSink{}).Check(context.Background(), Resolved{Type: "iceberg", Config: map[string]any{
			"catalog_type": "rest", "uri": srv.URL, "warehouse": "lakehouse", "namespace": "cdc", "auth": "none", "storage": "s3"},
			Secrets: map[string]any{}}, nil)
		srv.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(writes) > 0 {
			t.Errorf("namespace exists=%v: the check sent %v", nsExists, writes)
		}
		got := map[string]string{}
		for _, c := range report.Checks {
			got[c.Code] = c.Status
		}
		if got["iceberg_catalog"] != checks.Pass || got["iceberg_namespace"] != checks.Pass || got["iceberg_write"] != checks.Unknown {
			t.Errorf("namespace exists=%v: %v", nsExists, got)
		}
		for _, c := range report.Checks {
			if c.Blocking {
				t.Errorf("namespace exists=%v: %s blocks: %s", nsExists, c.Code, c.Message)
			}
		}
	}
}
