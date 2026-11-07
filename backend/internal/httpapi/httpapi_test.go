package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
)

func corsHeaders(origins []string, origin, method string) http.Header {
	h := httpapi.CORS(origins)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	req := httptest.NewRequest(method, "/api/v1/x", nil)
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	if method == http.MethodOptions {
		req.Header.Set("Access-Control-Request-Method", "POST")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Header()
}

// A listed origin gets credentialed CORS; "*" never reflects an Origin with credentials (Known
// issue 2), so other sites can read without the session cookie.
func TestCORS(t *testing.T) {
	for _, c := range []struct {
		name, origin string
		origins      []string
		allow, creds string
	}{
		{"listed", "https://ui.example", []string{"https://ui.example"}, "https://ui.example", "true"},
		{"listed beside *", "https://ui.example", []string{"*", "https://ui.example"}, "https://ui.example", "true"},
		{"wildcard", "https://evil.example", []string{"*"}, "*", ""},
		{"wildcard beside a listed one", "https://evil.example", []string{"https://ui.example", "*"}, "*", ""},
		{"not listed", "https://evil.example", []string{"https://ui.example"}, "", ""},
		{"no Origin", "", []string{"*"}, "", ""},
	} {
		for _, method := range []string{http.MethodGet, http.MethodOptions} {
			h := corsHeaders(c.origins, c.origin, method)
			if got := h.Get("Access-Control-Allow-Origin"); got != c.allow {
				t.Errorf("%s %s: Allow-Origin %q, want %q", c.name, method, got, c.allow)
			}
			if got := h.Get("Access-Control-Allow-Credentials"); got != c.creds {
				t.Errorf("%s %s: Allow-Credentials %q, want %q", c.name, method, got, c.creds)
			}
		}
	}
}
