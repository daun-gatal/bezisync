package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/api"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/ssrf"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

const (
	adminEmail    = testdb.AdminEmail
	adminPassword = testdb.AdminPassword
)

func TestMain(m *testing.M) {
	// Set before any server goroutine starts. Hostnames never resolve: every test uses IP
	// addresses, and services that only exist in a cluster (the playground's) fail at once
	// instead of waiting for DNS and client timeouts.
	ssrf.Resolver = &net.Resolver{PreferGo: true, Dial: func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("no DNS in tests")
	}}
	connectors.PubSubCheckTimeout = 500 * time.Millisecond
	os.Exit(testdb.Main(m, map[string]string{"BEZISYNC_SSRF_ALLOW_CIDRS": `["127.0.0.0/8", "172.16.0.0/12", "10.0.0.0/8"]`, "BEZISYNC_LOG_LEVEL": "WARNING"}))
}

// --- HTTP client ---

type client struct {
	t   *testing.T
	srv *httptest.Server
	hc  *http.Client
}

type resp struct {
	t      *testing.T
	Status int
	Header http.Header
	Raw    []byte
}

// server starts the API on a fresh, empty database.
func server(t *testing.T) *httptest.Server {
	t.Helper()
	testdb.Reset(t)
	services.ResetDrivers()
	srv := httptest.NewServer(api.Router())
	t.Cleanup(srv.Close)
	return srv
}

func newClient(t *testing.T, srv *httptest.Server) *client {
	jar, _ := cookiejar.New(nil)
	return &client{t: t, srv: srv, hc: &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func (c *client) csrf() string {
	u, _ := url.Parse(c.srv.URL)
	for _, ck := range c.hc.Jar.Cookies(u) {
		if ck.Name == "bz_csrf" {
			return ck.Value
		}
	}
	return ""
}

func (c *client) do(method, path string, body any, headers ...string) *resp {
	c.t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			c.t.Fatal(err)
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, c.srv.URL+path, rd)
	if err != nil {
		c.t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if method != http.MethodGet {
		if tok := c.csrf(); tok != "" {
			req.Header.Set("x-csrf-token", tok)
		}
	}
	for i := 0; i+1 < len(headers); i += 2 {
		if headers[i+1] == "" {
			req.Header.Del(headers[i])
		} else {
			req.Header.Set(headers[i], headers[i+1])
		}
	}
	res, err := c.hc.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	for _, field := range storedSecretFields {
		if bytes.Contains(raw, []byte(`"`+field+`":`)) {
			c.t.Errorf("%s %s returned the stored-secret field %q", method, path, field)
		}
	}
	return &resp{t: c.t, Status: res.StatusCode, Header: res.Header, Raw: raw}
}

// Columns that hold secrets or their hashes: no response may ever carry them.
var storedSecretFields = []string{"secrets_enc", "spec_enc", "password_hash", "token_hash", "agent_token_hash", "csrf_token", "args_enc", "client_secret_enc"}

func (c *client) get(path string, headers ...string) *resp { return c.do("GET", path, nil, headers...) }
func (c *client) post(path string, body any, headers ...string) *resp {
	return c.do("POST", path, body, headers...)
}
func (c *client) put(path string, body any, headers ...string) *resp {
	return c.do("PUT", path, body, headers...)
}
func (c *client) patch(path string, body any, headers ...string) *resp {
	return c.do("PATCH", path, body, headers...)
}
func (c *client) del(path string, headers ...string) *resp {
	return c.do("DELETE", path, nil, headers...)
}

// JSON decodes the body (numbers as float64).
func (r *resp) JSON() any {
	r.t.Helper()
	var v any
	if err := json.Unmarshal(r.Raw, &v); err != nil {
		r.t.Fatalf("invalid JSON (%d): %s", r.Status, r.Raw)
	}
	return v
}

// At walks the JSON body: keys for objects, ints for arrays.
func (r *resp) At(path ...any) any {
	r.t.Helper()
	return at(r.t, r.JSON(), path...)
}

func at(t *testing.T, v any, path ...any) any {
	t.Helper()
	for _, p := range path {
		switch k := p.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				t.Fatalf("not an object at %q: %v", k, v)
			}
			v = m[k]
		case int:
			l, ok := v.([]any)
			if !ok || k >= len(l) {
				t.Fatalf("no item %d in %v", k, v)
			}
			v = l[k]
		}
	}
	return v
}

func (r *resp) Str(path ...any) string {
	r.t.Helper()
	s, _ := r.At(path...).(string)
	return s
}

func (r *resp) List(path ...any) []any {
	r.t.Helper()
	l, _ := r.At(path...).([]any)
	return l
}

func (r *resp) Code() string { return r.Str("code") }

// expect fails unless the status matches.
func (r *resp) expect(status int) *resp {
	r.t.Helper()
	if r.Status != status {
		r.t.Fatalf("status %d, want %d: %s", r.Status, status, r.Raw)
	}
	return r
}

// --- users ---

func login(t *testing.T, srv *httptest.Server, email, password string) *client {
	t.Helper()
	c := newClient(t, srv)
	c.post("/api/v1/auth/login", map[string]any{"email": email, "password": password}).expect(200)
	return c
}

// adminClient bootstraps the instance and signs the admin in.
func adminClient(t *testing.T, srv *httptest.Server) *client {
	t.Helper()
	ctx := context.Background()
	if err := db.Tx(ctx, func(tx pgx.Tx) error { _, err := services.Bootstrap(ctx, tx); return err }); err != nil {
		t.Fatal(err)
	}
	return login(t, srv, adminEmail, adminPassword)
}

func defaultProject(t *testing.T, admin *client) string {
	t.Helper()
	return admin.get("/api/v1/auth/me").Str("projects", 0, "id")
}

var memberSeq int
var memberMu sync.Mutex

func member(t *testing.T, srv *httptest.Server, admin *client, projectID, role string, email ...string) *client {
	t.Helper()
	e := ""
	if len(email) > 0 {
		e = email[0]
	} else {
		memberMu.Lock()
		memberSeq++
		e = fmt.Sprintf("%s-%d-%d@example.com", role, memberSeq, time.Now().UnixNano()%1_000_000)
		memberMu.Unlock()
	}
	password := "a-long-enough-password"
	admin.put("/api/v1/projects/"+projectID+"/members", map[string]any{"email": e, "role": role, "initial_password": password}).expect(200)
	return login(t, srv, e, password)
}

// --- fake runtime ---

const healthyMetrics = `debezium_metrics_Connected{context="streaming",name="x",plugin="postgres"} 1.0
debezium_metrics_SnapshotRunning{context="snapshot",name="x",plugin="postgres"} 0.0
debezium_metrics_MilliSecondsBehindSource{context="streaming",name="x",plugin="postgres"} 500.0
`

type fakeDriver struct {
	mu        sync.Mutex
	Workloads map[string]runtime.Workload
	Applied   []string
	Deleted   []string
	Phase     string
	LogsText  string
	Metrics   string
	Health    [2]any
	OK        bool
	ImagePull string
}

func installFake(t *testing.T) *fakeDriver {
	t.Helper()
	d := &fakeDriver{Workloads: map[string]runtime.Workload{}, Phase: "running", Metrics: healthyMetrics, Health: [2]any{200, `{"status": "UP"}`}, OK: true}
	prev := services.DriverFactory
	services.DriverFactory = func(context.Context, string, map[string]any, map[string]any) (runtime.Driver, error) { return d, nil }
	services.ResetDrivers()
	t.Cleanup(func() { services.DriverFactory = prev; services.ResetDrivers() })
	return d
}

func (d *fakeDriver) Apply(_ context.Context, w runtime.Workload) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Workloads[w.ShortID] = w
	d.Applied = append(d.Applied, w.ConfigHash)
	return nil
}

func (d *fakeDriver) Delete(_ context.Context, shortID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.Workloads, shortID)
	d.Deleted = append(d.Deleted, shortID)
	return nil
}

func (d *fakeDriver) Observe(_ context.Context, shortID string) (runtime.Observation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	w, ok := d.Workloads[shortID]
	if !ok {
		return runtime.Observation{Phase: "missing"}, nil
	}
	h := w.ConfigHash
	if !w.Running {
		return runtime.Observation{Phase: "stopped", ConfigHash: &h}, nil
	}
	return runtime.Observation{Phase: d.Phase, Ready: d.Phase == "running", ConfigHash: &h, ImagePull: d.ImagePull}, nil
}

func (d *fakeDriver) Logs(context.Context, string, int, bool) (string, error) { return d.LogsText, nil }

func (d *fakeDriver) HTTPGet(_ context.Context, _ string, port int, _ string, _ time.Duration) (int, string, error) {
	if port == 9404 {
		return 200, d.Metrics, nil
	}
	return d.Health[0].(int), d.Health[1].(string), nil
}

func (d *fakeDriver) Test(context.Context) runtime.TestResult {
	msg := "fake runtime ready"
	if !d.OK {
		msg = "fake runtime broken"
	}
	return runtime.TestResult{OK: d.OK, Message: msg, Details: map[string]any{"cpu_allocatable": 4}}
}

func (d *fakeDriver) ListWorkloads(_ context.Context, owner string) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []string
	for k, w := range d.Workloads {
		if owner == "" || w.Owner == owner {
			out = append(out, k)
		}
	}
	return out, nil
}

func (d *fakeDriver) Capacity(context.Context) map[string]any {
	return map[string]any{"cpu_allocatable": 4}
}
