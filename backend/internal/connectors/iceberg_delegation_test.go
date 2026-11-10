package connectors

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/apache/iceberg-go/table"
)

// A connection that brings its own storage keys must not ask the REST catalog for vended
// credentials. iceberg-go asks on every request by default, and a catalog that can't vend
// (Polaris over a store without STS) then refuses table creation with HTTP 400 — the
// iceberg_write check failed although the connector itself (Java Iceberg) writes fine.
func TestIcebergAccessDelegation(t *testing.T) {
	allowLoopback(t)

	for _, c := range []struct {
		storage string
		want    string // the X-Iceberg-Access-Delegation value every request must carry ("" = none asked)
	}{
		{"s3", ""},
		{"vended", "vended-credentials"},
	} {
		t.Run(c.storage, func(t *testing.T) {
			var mu sync.Mutex
			var requests []string // "<method> <path> -> <delegation header>"
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				requests = append(requests, r.Method+" "+r.URL.Path+" -> "+r.Header.Get("X-Iceberg-Access-Delegation"))
				mu.Unlock()
				switch r.URL.Path {
				case "/v1/config":
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
				case "/v1/namespaces/cdc":
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"namespace":["cdc"],"properties":{}}`))
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer srv.Close()

			cfg, err := icebergCfg(Resolved{Type: "iceberg", Config: map[string]any{"catalog_type": "rest", "uri": srv.URL,
				"warehouse": "wh", "namespace": "cdc", "auth": "none", "storage": c.storage}, Secrets: map[string]any{}})
			if err != nil {
				t.Fatal(err)
			}
			cat, ctx, err := cfg.open(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := cat.CheckNamespaceExists(ctx, table.Identifier{"cdc"}); err != nil {
				t.Fatal(err)
			}

			mu.Lock()
			defer mu.Unlock()
			if len(requests) < 2 {
				t.Fatalf("want the config and namespace requests, got %v", requests)
			}
			for _, req := range requests {
				if got := req[strings.LastIndex(req, " -> ")+4:]; got != c.want {
					t.Errorf("storage %s: %s, want delegation %q", c.storage, req, c.want)
				}
			}
		})
	}
}
