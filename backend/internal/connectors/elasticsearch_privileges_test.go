package connectors

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
)

// fakeES answers like Elasticsearch: GET / (version), HEAD /<index> (existing indexes), and
// _has_privileges at the version's path with the given rights. Any other request (an index
// created, a document written, an index deleted) is recorded as a write.
type fakeES struct {
	version  string
	existing map[string]bool
	rights   map[string]map[string]bool
	privCode int    // _has_privileges status (0 = 200)
	privBody string // its body when privCode is set
	mu       sync.Mutex
	writes   []string
	asked    []string // privileges in the _has_privileges request
	path     string   // the _has_privileges path used
}

func (f *fakeES) server(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/":
			_, _ = w.Write([]byte(`{"version":{"number":"` + f.version + `"}}`))
		case r.Method == http.MethodHead:
			if f.existing[strings.TrimPrefix(r.URL.Path, "/")] {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/user/_has_privileges"):
			f.path = r.URL.Path
			var req struct {
				Index []struct {
					Names      []string `json:"names"`
					Privileges []string `json:"privileges"`
				} `json:"index"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			if f.privCode != 0 {
				w.WriteHeader(f.privCode)
				_, _ = w.Write([]byte(f.privBody))
				return
			}
			out := map[string]map[string]bool{}
			for _, ix := range req.Index {
				f.asked = ix.Privileges
				for _, n := range ix.Names {
					out[n] = map[string]bool{}
					for _, p := range ix.Privileges {
						out[n][p] = f.rights[n][p]
					}
				}
			}
			b, _ := json.Marshal(map[string]any{"index": out})
			_, _ = w.Write(b)
		default:
			f.writes = append(f.writes, r.Method+" "+r.URL.Path)
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func all(extra ...string) map[string]bool {
	m := map[string]bool{"index": true, "delete": true, "create_index": true, "delete_index": true, "auto_configure": true}
	for _, e := range extra {
		m[e] = false
	}
	return m
}

// ADR 0016 D5, E7-E13: the pipeline's indexes, their rights, nothing created.
func TestElasticsearchPipelinePrivileges(t *testing.T) {
	allowLoopback(t)
	dests := map[string]string{"shop.orders": "cdc.Shop_Orders", "shop.items": "cdc.shop_items"}
	idx := []string{"cdc_shop_items", "cdc_shop_orders"}
	for _, c := range []struct {
		name     string
		f        *fakeES
		status   string
		blocking bool
		inMsg    string
	}{
		{name: "E7 all rights, indexes missing", f: &fakeES{version: "8.15.3", rights: map[string]map[string]bool{idx[0]: all(), idx[1]: all()}},
			status: checks.Pass, inMsg: "may write, delete documents in and create cdc_shop_items, cdc_shop_orders"},
		{name: "E8 auto_configure instead of create_index", f: &fakeES{version: "8.15.3",
			rights: map[string]map[string]bool{idx[0]: all("create_index"), idx[1]: all("create_index")}}, status: checks.Pass},
		{name: "E9 no delete", f: &fakeES{version: "8.15.3", rights: map[string]map[string]bool{idx[0]: all(), idx[1]: all("delete")}},
			status: checks.Fail, blocking: true, inMsg: "can't write or delete documents in cdc_shop_orders"},
		{name: "E9 no index", f: &fakeES{version: "8.15.3", rights: map[string]map[string]bool{idx[0]: all("index"), idx[1]: all()}},
			status: checks.Fail, blocking: true, inMsg: "cdc_shop_items"},
		{name: "E9 can't create a missing index", f: &fakeES{version: "8.15.3", existing: map[string]bool{idx[0]: true},
			rights: map[string]map[string]bool{idx[0]: all("create_index", "auto_configure"), idx[1]: all("create_index", "auto_configure")}},
			status: checks.Fail, blocking: true, inMsg: "can't create cdc_shop_orders"},
		{name: "E10 no create rights, every index exists", f: &fakeES{version: "8.15.3", existing: map[string]bool{idx[0]: true, idx[1]: true},
			rights: map[string]map[string]bool{idx[0]: all("create_index", "auto_configure"), idx[1]: all("create_index", "auto_configure")}},
			status: checks.Pass},
		{name: "E11 no delete_index", f: &fakeES{version: "8.15.3", rights: map[string]map[string]bool{idx[0]: all("delete_index"), idx[1]: all()}},
			status: checks.Warn, inMsg: "\"drop destinations\" would fail"},
		{name: "E12 6.x uses the _xpack path, no auto_configure", f: &fakeES{version: "6.8.23",
			rights: map[string]map[string]bool{idx[0]: all(), idx[1]: all()}}, status: checks.Pass},
		{name: "E12 7.8 asks no auto_configure", f: &fakeES{version: "7.8.1",
			rights: map[string]map[string]bool{idx[0]: all(), idx[1]: all()}}, status: checks.Pass},
		{name: "E12 below 6.4", f: &fakeES{version: "6.3.2"}, status: checks.Unknown, inMsg: "answers that from 6.4"},
		{name: "E13 security disabled", f: &fakeES{version: "8.15.3", privCode: 500,
			privBody: `{"error":{"type":"security_exception","reason":"Security must be explicitly enabled when using a [basic] license"}}`},
			status: checks.Unknown, inMsg: "security is disabled"},
		{name: "E13 refused", f: &fakeES{version: "8.15.3", privCode: 403, privBody: `{"error":{"type":"security_exception"}}`},
			status: checks.Unknown},
		{name: "E13 unauthenticated", f: &fakeES{version: "8.15.3", privCode: 401, privBody: `{"error":{"type":"security_exception"}}`},
			status: checks.Unknown},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := c.f
			srv := f.server(t)
			got := (&elasticsearchSink{}).CheckPipeline(context.Background(), Resolved{Type: "elasticsearch",
				Config: map[string]any{"hosts": srv.URL, "version": strings.SplitN(f.version, ".", 2)[0]}, Secrets: map[string]any{}},
				CompileContext{}, PipelineFacts{Destinations: dests, FlinkCDC: true})
			if len(got) != 1 {
				t.Fatalf("%+v", got)
			}
			r := got[0]
			if r.Code != "elasticsearch_create_index" || r.Status != c.status || r.Blocking != c.blocking || !strings.Contains(r.Message, c.inMsg) {
				t.Fatalf("%s blocking=%v: %s", r.Status, r.Blocking, r.Message)
			}
			if len(f.writes) > 0 {
				t.Fatalf("the check wrote: %v", f.writes)
			}
			switch f.version {
			case "6.8.23":
				if f.path != "/_xpack/security/user/_has_privileges" || strings.Contains(strings.Join(f.asked, ","), "auto_configure") {
					t.Fatal(f.path, f.asked)
				}
			case "7.8.1":
				if strings.Contains(strings.Join(f.asked, ","), "auto_configure") {
					t.Fatal(f.asked)
				}
			case "6.3.2":
				if f.path != "" {
					t.Fatal("asked a 6.3 server for privileges")
				}
			default:
				if f.path != "" && f.path != "/_security/user/_has_privileges" {
					t.Fatal(f.path)
				}
			}
		})
	}
}

// E14: the connection check signs in and reads the version; it writes no document and creates or
// deletes no index.
func TestElasticsearchCheckCreatesNothing(t *testing.T) {
	allowLoopback(t)
	f := &fakeES{version: "8.15.3"}
	srv := f.server(t)
	report, err := (&elasticsearchSink{}).Check(context.Background(), Resolved{Type: "elasticsearch",
		Config: map[string]any{"hosts": srv.URL, "version": "8"}, Secrets: map[string]any{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.writes) > 0 {
		t.Fatalf("the connection check wrote: %v", f.writes)
	}
	for _, c := range report.Checks {
		if c.Status != checks.Pass {
			t.Errorf("%s: %s %s", c.Code, c.Status, c.Message)
		}
	}
}
