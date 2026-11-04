package api_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/cli"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
	"github.com/daun-gatal/bezisync/backend/internal/version"

	"gopkg.in/yaml.v3"
)

func runCLI(t *testing.T, stdin string, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := cli.Execute(args, strings.NewReader(stdin), &out, &errb)
	return code, out.String(), errb.String()
}

func TestCLIRemote(t *testing.T) {
	e := newPipelineEnv(t)
	createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	token := e.admin.post("/api/v1/auth/tokens", map[string]any{"name": "cli"}).expect(201).Str("token")
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	t.Setenv("BEZISYNC_URL", "")
	t.Setenv("BEZISYNC_TOKEN", "")
	if code, _, stderr := runCLI(t, "", "remote", "get"); code != 2 || !strings.Contains(stderr, "Not logged in") {
		t.Fatal(code, stderr)
	}
	t.Setenv("BEZISYNC_URL", e.srv.URL)
	t.Setenv("BEZISYNC_TOKEN", token)
	code, out, stderr := runCLI(t, "", "remote", "get")
	if code != 0 || !strings.Contains(out, "orders-cdc") || !strings.HasPrefix(out, "NAME ") || !strings.Contains(out, "shop-pg → events") {
		t.Fatal(code, out, stderr)
	}
	code, exported, _ := runCLI(t, "", "remote", "export", "orders-cdc")
	if code != 0 || !strings.Contains(exported, "kind: Pipeline") {
		t.Fatal(code, exported)
	}
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(exported), &doc); err != nil {
		t.Fatal(err)
	}
	doc["spec"].(map[string]any)["sink"].(map[string]any)["naming"] = "cli.{table}"
	file := filepath.Join(dir, "orders.yaml")
	raw, _ := yaml.Marshal(doc)
	if err := os.WriteFile(file, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	code, out, _ = runCLI(t, "", "remote", "diff", "-f", file)
	if code != 0 || !strings.Contains(out, "would_update") || !strings.Contains(out, "+") {
		t.Fatal(code, out)
	}
	code, out, _ = runCLI(t, "", "remote", "apply", "-f", file)
	// The undeployed draft is updated in place: one change, one version.
	if code != 0 || !strings.Contains(out, "orders-cdc: updated (v1)") {
		t.Fatal(code, out)
	}
	code, out, _ = runCLI(t, string(raw), "remote", "apply", "-f", "-", "--dry-run")
	if code != 0 || !strings.Contains(out, "orders-cdc: unchanged") {
		t.Fatal(code, out)
	}
	// A directory applies every manifest in it.
	manifests := filepath.Join(dir, "manifests")
	if err := os.Mkdir(manifests, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(manifests, "orders.yaml"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if code, out, stderr := runCLI(t, "", "remote", "diff", "-f", manifests); code != 0 || !strings.Contains(out, "orders-cdc: unchanged") {
		t.Fatal(code, out, stderr)
	}
	if code, _, stderr := runCLI(t, "", "remote", "diff", "-f", dir+"/nope"); code == 0 {
		t.Fatal(code, stderr)
	}
	if code, _, stderr := runCLI(t, "", "remote", "pipelines", "pause", "nope"); code != 1 || !strings.Contains(stderr, "no pipeline named 'nope'") {
		t.Fatal(code, stderr)
	}
	if code, _, stderr := runCLI(t, "", "remote", "pipelines", "pause", "orders-cdc"); code != 1 || !strings.Contains(stderr, "error: ") {
		t.Fatal(code, stderr)
	}
	if code, _, stderr := runCLI(t, "", "remote", "get", "-p", "nope"); code != 1 || !strings.Contains(stderr, "no access to project 'nope'") {
		t.Fatal(code, stderr)
	}
	// Login stores the token readable by the owner only.
	t.Setenv("BEZISYNC_URL", "")
	t.Setenv("BEZISYNC_TOKEN", "")
	code, out, stderr = runCLI(t, token+"\n", "remote", "login", "--url", e.srv.URL)
	if code != 0 || !strings.Contains(out, "Logged in to "+e.srv.URL+" as admin@example.com (project default).") {
		t.Fatal(code, out, stderr)
	}
	info, err := os.Stat(cli.ConfigPath())
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal(info, err)
	}
	if code, out, _ := runCLI(t, "", "remote", "get"); code != 0 || !strings.Contains(out, "orders-cdc") {
		t.Fatal(code, out)
	}
	if code, _, _ := runCLI(t, "", "remote", "login", "--url", e.srv.URL, "--token", "bzt_wrong"); code != 1 {
		t.Fatal(code)
	}
}

func TestCLIUninstallPlansThenRemovesEverything(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	e.admin.post("/api/v1/projects/"+e.pid+"/pipelines/"+pipe.Str("id")+"/deploy", map[string]any{}).expect(200)
	e.tick()
	if len(e.driver.Workloads) != 1 {
		t.Fatal(e.driver.Workloads)
	}
	signals := func() int64 {
		n, err := db.Scalar[int64](context.Background(), testdb.Pool(t),
			`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'bezisync' AND table_name LIKE 'signal_%'`)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if signals() != 1 {
		t.Fatal("no signal table")
	}
	code, out, _ := runCLI(t, "", "uninstall")
	if code != 0 || !strings.Contains(out, "Dry run") || !strings.Contains(out, "default/orders-cdc (runtime k8s)") || !strings.Contains(out, "bz_") {
		t.Fatal(code, out)
	}
	if len(e.driver.Workloads) != 1 {
		t.Fatal("dry run touched the runtime")
	}
	code, out, _ = runCLI(t, "", "uninstall", "--yes")
	if code != 0 || !strings.Contains(out, "orders-cdc: removed") || !strings.Contains(out, "Done.") {
		t.Fatal(code, out)
	}
	if len(e.driver.Workloads) != 0 || signals() != 0 {
		t.Fatal(e.driver.Workloads)
	}
	if l := e.admin.get("/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")).Str("lifecycle"); l != "deleted" {
		t.Fatal(l)
	}
	if _, out, _ := runCLI(t, "", "uninstall"); !strings.Contains(out, "Nothing to remove") {
		t.Fatal(out)
	}
}

func TestCLIDocumentsAndKeys(t *testing.T) {
	code, doc, _ := runCLI(t, "", "openapi")
	if code != 0 || !strings.HasSuffix(doc, "}\n") {
		t.Fatal(code)
	}
	v, err := pyjson.DecodeGeneric([]byte(doc))
	if err != nil || at(t, v, "info", "version") != version.Version || at(t, v, "info", "title") != "BeziSync API" {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "openapi.json")
	if code, _, _ := runCLI(t, "", "openapi", "--output", out); code != 0 {
		t.Fatal(code)
	}
	if raw, _ := os.ReadFile(out); string(raw) != doc {
		t.Fatal("file differs from stdout")
	}
	code, md, _ := runCLI(t, "", "check-docs")
	front, ferr := os.ReadFile("../../../docs/reference/checks.md")
	if code != 0 || !strings.HasPrefix(md, "# Preflight check reference") || (ferr == nil && string(front) != md) {
		t.Fatal(code, ferr)
	}
	code, key, _ := runCLI(t, "", "generate-master-key")
	if code != 0 || len(strings.TrimSpace(key)) != 44 {
		t.Fatal(code, key)
	}
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer healthy.Close()
	if code, _, _ := runCLI(t, "", "healthcheck", "--url", healthy.URL+"/readyz"); code != 0 {
		t.Fatal(code)
	}
	if code, _, _ := runCLI(t, "", "healthcheck", "--url", healthy.URL+"/down"); code != 1 {
		t.Fatal(code)
	}
	if code, _, _ := runCLI(t, "", "tunnel-probe", "--port", "1"); code != 1 {
		t.Fatal(code)
	}
	if code, _, stderr := runCLI(t, "", "agent"); code != 2 || !strings.Contains(stderr, "--url") {
		t.Fatal(code, stderr)
	}
	if code, _, stderr := runCLI(t, "", "tunnel", "--target", "db", "--via", "u@h"); code != 2 {
		t.Fatal(code, stderr)
	}
	if code, out, _ := runCLI(t, "", "--version"); code != 0 || !strings.Contains(out, version.Version) {
		t.Fatal(code, out)
	}
}

// Saved positions and Start over from the command line, with --yes for what changes the resume point.
func TestCLIFlinkPositions(t *testing.T) {
	e := newPipelineEnv(t)
	pipe, _ := flinkPipeline(t, e)
	ids := savePositions(t, shortOf(pipe.Str("id")), 2)
	token := e.admin.post("/api/v1/auth/tokens", map[string]any{"name": "cli"}).expect(201).Str("token")
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("BEZISYNC_URL", e.srv.URL)
	t.Setenv("BEZISYNC_TOKEN", token)
	code, out, stderr := runCLI(t, "", "remote", "get")
	if code != 0 || !strings.Contains(out, "ENGINE") || !strings.Contains(out, "flink-cdc") {
		t.Fatal(code, out, stderr)
	}
	code, out, _ = runCLI(t, "", "remote", "pipelines", "positions", "lake-flink")
	if code != 0 || !strings.Contains(out, "checkpoint") || strings.Count(out, "\n") != 3 {
		t.Fatal(code, out)
	}
	first := strconv.FormatInt(ids[0], 10)
	if code, _, stderr := runCLI(t, "", "remote", "pipelines", "restore", "lake-flink", first); code == 0 || !strings.Contains(stderr, "--yes") {
		t.Fatal(code, stderr)
	}
	code, out, stderr = runCLI(t, "", "remote", "pipelines", "restore", "lake-flink", first, "--yes")
	if code != 0 || !strings.Contains(out, "resumes from position #") {
		t.Fatal(code, out, stderr)
	}
	if code, _, stderr := runCLI(t, "", "remote", "pipelines", "start-over", "lake-flink", "--from-now", "--from-scratch", "--yes"); code == 0 ||
		!strings.Contains(stderr, "choose one") {
		t.Fatal(code, stderr)
	}
	// A PostgreSQL slot can't go back: the server's reason reaches the terminal.
	if code, _, stderr := runCLI(t, "", "remote", "pipelines", "start-over", "lake-flink", "--since", "2026-01-01T00:00:00Z", "--yes"); code == 0 ||
		!strings.Contains(stderr, "PostgreSQL") {
		t.Fatal(code, stderr)
	}
	code, out, stderr = runCLI(t, "", "remote", "pipelines", "start-over", "lake-flink", "--from-scratch", "--yes")
	if code != 0 || !strings.Contains(out, "starts over") {
		t.Fatal(code, out, stderr)
	}
}
