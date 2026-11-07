package ssrf_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/ssrf"
)

// An operator's egress proxy is used, and the target is still checked first: a public address
// goes through the proxy, a metadata address is refused before the proxy sees it (ADR 0004).
func TestHTTPClientUsesTheOperatorsProxy(t *testing.T) {
	s, err := config.Load(func(string) (string, bool) { return "", false }) // loopback refused
	if err != nil {
		t.Fatal(err)
	}
	config.Set(s)
	t.Cleanup(config.Reset)
	var seen atomic.Value
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Store(r.URL.String())
		w.WriteHeader(http.StatusTeapot)
	}))
	defer proxy.Close()
	t.Setenv("HTTP_PROXY", proxy.URL) // on loopback: the operator's own address, dialled as set
	t.Setenv("NO_PROXY", "")
	c := ssrf.HTTPClient(5e9)
	resp, err := c.Get("http://203.0.113.7/ping")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot || seen.Load() != "http://203.0.113.7/ping" {
		t.Fatalf("status %d via %v", resp.StatusCode, seen.Load())
	}
	seen.Store("")
	if _, err := c.Get("http://169.254.169.254/latest/meta-data/"); err == nil || seen.Load() != "" {
		t.Fatalf("metadata through the proxy: %v (proxy saw %q)", err, seen.Load())
	}
}
