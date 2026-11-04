package api

import (
	"encoding/json"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

// Routes the Python app served outside the OpenAPI document (include_in_schema=False).
var unlisted = map[string]bool{
	"GET /healthz": true, "GET /readyz": true, "GET /metrics": true, "GET /api/v1/projects/{}/stream": true,
	"GET /api/v1/openapi.json": true, "GET /api/docs": true, "GET /api/v1/system/licenses": true,
	// OAuth discovery for MCP agents (RFC 8414, RFC 9728) lives at the site root, outside the API.
	"GET /.well-known/oauth-authorization-server": true, "GET /.well-known/oauth-protected-resource": true,
	"GET /.well-known/oauth-protected-resource/api/v1/mcp": true,
}

var param = regexp.MustCompile(`\{[^}]+\}`)

func TestRoutesMatchOpenAPI(t *testing.T) {
	var doc struct {
		Paths map[string]map[string]json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(OpenAPI(), &doc); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for path, ops := range doc.Paths {
		for method := range ops {
			want[strings.ToUpper(method)+" "+param.ReplaceAllString(path, "{}")] = true
		}
	}
	got := map[string]bool{}
	router, ok := Router().(chi.Routes)
	if !ok {
		t.Fatal("router is not a chi router")
	}
	err := chi.Walk(router, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		route = strings.TrimSuffix(strings.ReplaceAll(route, "/*/", "/"), "/")
		got[method+" "+param.ReplaceAllString(route, "{}")] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var missing, extra []string
	for k := range want {
		if !got[k] {
			missing = append(missing, k)
		}
	}
	for k := range got {
		if !want[k] && !unlisted[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("routes differ from openapi.json\nmissing: %v\nextra: %v", missing, extra)
	}
	t.Logf("%d documented routes", len(want))
}
