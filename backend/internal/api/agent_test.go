package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/agent"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

type tokenTransport struct{ token string }

func (tt tokenTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+tt.token)
	return http.DefaultTransport.RoundTrip(r)
}

type enrolledAgent struct {
	body  *resp
	agent *agent.Agent
	local *fakeDriver
}

func newLocalDriver() *fakeDriver {
	return &fakeDriver{Workloads: map[string]runtime.Workload{}, Phase: "running", Metrics: healthyMetrics, Health: [2]any{200, `{"status": "UP"}`}, OK: true}
}

func enroll(t *testing.T, srv *httptest.Server, admin *client, pid string) enrolledAgent {
	t.Helper()
	body := admin.post("/api/v1/projects/"+pid+"/runtimes", map[string]any{"name": "edge", "kind": "docker", "mode": "agent", "config": map[string]any{}}).expect(201)
	if !strings.HasPrefix(body.Str("agent_token"), "bza_") || !strings.Contains(body.Str("agent_install"), "BEZISYNC_AGENT_TOKEN=") || body.At("last_test", "ok") != false {
		t.Fatal(string(body.Raw))
	}
	local := newLocalDriver()
	a := agent.NewWithDriver(srv.URL, "docker", local, &http.Client{Transport: tokenTransport{body.Str("agent_token")}, Timeout: 10 * time.Second})
	return enrolledAgent{body, a, local}
}

func agentPoll(t *testing.T, a *agent.Agent) {
	t.Helper()
	ctx := context.Background()
	if _, err := a.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	a.ApplyDesired(ctx)
	if a.PendingResults() {
		if _, err := a.Poll(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAgentProtocolEndToEnd(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	e := enroll(t, srv, admin, pid)
	ctx := context.Background()
	rid := uuid.MustParse(e.body.Str("id"))
	proxy := &services.AgentDriver{RuntimeID: rid}
	if _, err := proxy.Observe(ctx, "abc"); err == nil {
		t.Fatal("observed a never-connected agent")
	}
	agentPoll(t, e.agent)
	if r := admin.post("/api/v1/projects/"+pid+"/runtimes/"+e.body.Str("id")+"/test", nil).expect(200); r.At("ok") != true {
		t.Fatal(string(r.Raw))
	}
	foreign := runtime.Workload{PipelineID: "x", ShortID: "other", Name: "x", Image: "img", ConfigHash: "h", CPU: "1", Memory: "1Gi", Heap: "512m", Running: true, Owner: "another-runtime"}
	_ = e.local.Apply(ctx, foreign)
	w := runtime.Workload{PipelineID: "p", ShortID: "abc", Name: "n", Image: "img", Properties: "a=b", Env: map[string]string{"SECRET": "s3cr3t-val"},
		ConfigHash: "h1", CPU: "1", Memory: "1Gi", Heap: "512m", Running: true}
	if err := proxy.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	agentPoll(t, e.agent)
	if e.local.Workloads["abc"].Env["SECRET"] != "s3cr3t-val" {
		t.Fatal(e.local.Workloads)
	}
	e.local.mu.Lock()
	e.local.ImagePull = "Downloading the image img:2 (5s so far)."
	e.local.mu.Unlock()
	agentPoll(t, e.agent)
	obs, err := proxy.Observe(ctx, "abc")
	if err != nil || obs.Phase != "running" || obs.ConfigHash == nil || *obs.ConfigHash != "h1" || obs.ImagePull != "Downloading the image img:2 (5s so far)." {
		t.Fatalf("%+v %v", obs, err)
	}
	if code, _, err := proxy.HTTPGet(ctx, "abc", 8080, "/q/health", time.Second); err != nil || code != 200 {
		t.Fatal(code, err)
	}
	if _, body, err := proxy.HTTPGet(ctx, "abc", 9404, "/metrics", time.Second); err != nil || !strings.Contains(body, "Connected") {
		t.Fatal(body, err)
	}
	e.local.LogsText = "hello from the connector"
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = e.agent.Poll(ctx)
			time.Sleep(200 * time.Millisecond)
		}
	}()
	logs, err := proxy.Logs(ctx, "abc", 300, false)
	close(stop)
	wg.Wait()
	if err != nil || logs != "hello from the connector" {
		t.Fatal(logs, err)
	}
	if e.local.Workloads["abc"].Owner != e.body.Str("id") {
		t.Fatal(e.local.Workloads["abc"].Owner)
	}
	if err := proxy.Delete(ctx, "abc"); err != nil {
		t.Fatal(err)
	}
	agentPoll(t, e.agent)
	if _, ok := e.local.Workloads["abc"]; ok {
		t.Fatal("abc not removed")
	}
	if _, ok := e.local.Workloads["other"]; !ok {
		t.Fatal("another runtime's connector was removed")
	}
	exec(t, "UPDATE runtimes SET last_seen_at = now() - interval '5 minutes' WHERE id = '"+rid.String()+"'")
	if _, err := proxy.Observe(ctx, "abc"); err == nil || !strings.Contains(err.Error(), "has not connected since") {
		t.Fatal(err)
	}
}

func TestAgentTokenIsRequiredAndRotatable(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	e := enroll(t, srv, admin, pid)
	report, _ := json.Marshal(e.agent.Report(context.Background()))
	anon := newClient(t, srv)
	anon.post("/api/v1/agent/poll", string(report), "authorization", "Bearer bza_wrong").expect(401)
	anon.post("/api/v1/agent/poll", string(report), "authorization", "Bearer "+e.body.Str("agent_token")).expect(200)
	rotated := admin.post("/api/v1/projects/"+pid+"/runtimes/"+e.body.Str("id")+"/agent-token", nil).expect(200)
	if rotated.Str("agent_token") == e.body.Str("agent_token") {
		t.Fatal("token not rotated")
	}
	anon.post("/api/v1/agent/poll", string(report), "authorization", "Bearer "+e.body.Str("agent_token")).expect(401)
	if tok := admin.get("/api/v1/projects/" + pid + "/runtimes/" + e.body.Str("id")).At("agent_token"); tok != nil {
		t.Fatal(tok)
	}
	// A docker agent may not drive a kubernetes runtime.
	var body map[string]any
	_ = json.Unmarshal(report, &body)
	body["kind"] = "kubernetes"
	if c := anon.post("/api/v1/agent/poll", body, "authorization", "Bearer "+rotated.Str("agent_token")).expect(409).Code(); c != "agent_kind_mismatch" {
		t.Fatal(c)
	}
	anon.post("/api/v1/agent/poll", map[string]any{"kind": "docker"}, "authorization", "Bearer "+rotated.Str("agent_token")).expect(422)
	if c := admin.post("/api/v1/projects/"+pid+"/runtimes/"+installFakeRuntime(t, admin, pid)+"/agent-token", nil).expect(409).Code(); c != "not_agent_runtime" {
		t.Fatal(c)
	}
}

func installFakeRuntime(t *testing.T, admin *client, pid string) string {
	t.Helper()
	installFake(t)
	return admin.post("/api/v1/projects/"+pid+"/runtimes", map[string]any{"name": "direct", "kind": "docker", "config": map[string]any{}}).expect(201).Str("id")
}

func TestKubernetesInstallCommandUsesThePublishedChart(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	res := admin.post("/api/v1/projects/"+pid+"/runtimes", map[string]any{"name": "eks", "kind": "kubernetes", "mode": "agent",
		"config": map[string]any{"namespace": "cdc", "image_pull_secrets": []string{"ghcr-pull-secret"}}}).expect(201)
	cmd := res.Str("agent_install")
	// Without a known agent chart version (the control-plane chart sets one) Helm installs the newest chart.
	for _, want := range []string{"helm install bezisync-agent oci://ghcr.io/daun-gatal/charts/bezisync-agent --namespace cdc",
		"--set 'imagePullSecrets[0].name=ghcr-pull-secret'", "--set image.pullPolicy=IfNotPresent", "--set image.repository=ghcr.io/daun-gatal/bezisync "} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("%q not in %s", want, cmd)
		}
	}
	if strings.Contains(cmd, "--version") {
		t.Fatalf("an app version passed as chart version: %s", cmd)
	}
	if l := res.List("config", "image_pull_secrets"); len(l) != 1 || l[0] != "ghcr-pull-secret" {
		t.Fatal(string(res.Raw))
	}
	// The agent chart has its own version: the control-plane chart passes the one it shipped with.
	prev := config.Get()
	pinned := *prev
	pinned.AgentChartVersion = "0.1.4"
	config.Set(&pinned)
	t.Cleanup(func() { config.Set(prev) })
	cmd = admin.post("/api/v1/projects/"+pid+"/runtimes", map[string]any{"name": "gke", "kind": "kubernetes", "mode": "agent",
		"config": map[string]any{"namespace": "cdc"}}).expect(201).Str("agent_install")
	if !strings.Contains(cmd, "oci://ghcr.io/daun-gatal/charts/bezisync-agent --version 0.1.4 --namespace cdc") {
		t.Fatal(cmd)
	}
}

// A Docker agent applies the runtime's image download timeout; the default needs no flag.
func TestDockerInstallCommandCarriesTheImageDownloadTimeout(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	base := "/api/v1/projects/" + pid + "/runtimes"
	cmd := admin.post(base, map[string]any{"name": "edge-slow", "kind": "docker", "mode": "agent",
		"config": map[string]any{"image_pull_timeout": "1h"}}).expect(201).Str("agent_install")
	if !strings.Contains(cmd, "-e BEZISYNC_AGENT_DOCKER_IMAGE_PULL_TIMEOUT=1h ") {
		t.Fatal(cmd)
	}
	cmd = admin.post(base, map[string]any{"name": "edge", "kind": "docker", "mode": "agent", "config": map[string]any{}}).expect(201).Str("agent_install")
	if strings.Contains(cmd, "IMAGE_PULL_TIMEOUT") {
		t.Fatal(cmd)
	}
}

func TestChecksRunOnTheAgentWithEncryptedArguments(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	e := enroll(t, srv, admin, pid)
	role := canaryRole(t)
	agentPoll(t, e.agent)
	cfg, _ := pgTarget(t)
	cfg["user"] = role
	ctx := context.Background()
	var out map[string]any
	var checkErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		out, checkErr = services.AgentRunCheck(ctx, uuid.MustParse(e.body.Str("id")), "source",
			connectors.Resolved{Type: "postgresql", Provider: "self_hosted", Config: cfg, Secrets: map[string]any{"password": canary}}, nil)
	}()
	var pending []models.AgentRequest
	for range 50 {
		var err error
		if pending, err = db.Select[models.AgentRequest](ctx, testdb.Pool(t), `SELECT * FROM agent_requests`); err != nil {
			t.Fatal(err)
		}
		if len(pending) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(pending) != 1 {
		t.Fatal(pending)
	}
	args, _ := json.Marshal(pending[0].Args)
	if len(pending[0].Args) != 1 || pending[0].Args["enc"] == nil || strings.Contains(string(args), canary) {
		t.Fatal(string(args))
	}
	agentPoll(t, e.agent)
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("the check never returned")
	}
	if checkErr != nil {
		t.Fatal(checkErr)
	}
	raw, _ := json.Marshal(out)
	codes := map[any]any{}
	for _, c := range out["checks"].([]any) {
		codes[at(t, c, "code")] = at(t, c, "status")
	}
	if codes["pg_login"] != "pass" || strings.Contains(string(raw), canary) {
		t.Fatal(string(raw))
	}
}

func TestOfflineAgentFallsBackToCheckingFromBeziSync(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	role := shop(t)
	installFake(t)
	base := "/api/v1/projects/" + pid
	target, _ := pgTarget(t)
	target["user"] = role
	src := admin.post(base+"/connections", map[string]any{"kind": "source", "type": "postgresql", "name": "shop-pg", "config": target,
		"secrets": map[string]any{"password": canary}}).expect(201).Str("id")
	sink := admin.post(base+"/connections", map[string]any{"kind": "sink", "type": "kafka", "name": "events",
		"config": map[string]any{"bootstrap_servers": kafkaCluster(t)}}).expect(201).Str("id")
	rt := admin.post(base+"/runtimes", map[string]any{"name": "offline", "kind": "kubernetes", "mode": "agent", "config": map[string]any{"namespace": "cdc"}}).expect(201).Str("id")
	pipe := createPipeline(t, admin, pid, ids{src, sink, rt}, "shop.orders")
	pre := admin.post(base+"/pipelines/"+pipe.Str("id")+"/preflight", nil).expect(200)
	var note any
	for _, s := range pre.List("sections") {
		for _, c := range at(t, s, "report", "checks").([]any) {
			if at(t, c, "code") == "checked_from_control_plane" {
				note = c
			}
		}
	}
	if note == nil {
		t.Fatal(string(pre.Raw))
	}
	if msg := at(t, note, "message").(string); !strings.Contains(msg, "never") && !strings.Contains(msg, "not connected") {
		t.Fatal(msg)
	}
}

// A current agent's features survive its poll: without them every agent is "too old" for sinks
// whose health is off 8080 (Apache Iceberg), and their pipelines can never deploy.
func TestAgentFeaturesReachThePreflight(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	e := enroll(t, srv, admin, pid)
	agentPoll(t, e.agent)
	ctx := context.Background()
	rt, err := services.GetRuntime(ctx, testdb.Pool(t), uuid.MustParse(e.body.Str("id")))
	if err != nil {
		t.Fatal(err)
	}
	desc, err := connectors.Describe("sink", "iceberg")
	if err != nil || desc.Sink.Image == nil || desc.Sink.Image.HealthPort == 0 {
		t.Fatal("iceberg no longer serves health off 8080", err)
	}
	if c := services.AgentHealthPortCheck(rt, desc); c != nil {
		t.Fatalf("current agent reported too old: %+v (state %v)", c, rt.AgentState)
	}
	// An agent that predates health_port sends no features, and is still blocked.
	exec(t, `UPDATE runtimes SET agent_state = agent_state - 'features' WHERE id = '`+rt.ID.String()+`'`)
	if rt, _ = services.GetRuntime(ctx, testdb.Pool(t), rt.ID); services.AgentHealthPortCheck(rt, desc) == nil {
		t.Fatal("an agent without health_port passed")
	}
}

// A workload serving health off 8080 (Apache Iceberg: 9000) keeps the port its agent probed: the
// poll used to drop health_port, so BeziSync assumed 8080, refused the agent's "UP" and every
// such pipeline sat at "Deploying" and then Unknown.
func TestAgentReportsHealthOnTheWorkloadsPort(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	e := enroll(t, srv, admin, pid)
	ctx := context.Background()
	proxy := &services.AgentDriver{RuntimeID: uuid.MustParse(e.body.Str("id"))}
	agentPoll(t, e.agent)
	w := runtime.Workload{PipelineID: "p", ShortID: "ice", Name: "n", Image: "img", Properties: "a=b",
		ConfigHash: "h1", CPU: "1", Memory: "1Gi", Heap: "512m", Running: true, HealthPort: 9000}
	if err := proxy.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	agentPoll(t, e.agent)
	agentPoll(t, e.agent) // the first poll applies the workload; the next one reports it running
	code, body, err := proxy.HTTPGet(ctx, "ice", 9000, "/q/health", time.Second)
	if err != nil || code != 200 || !strings.Contains(body, "UP") {
		t.Fatalf("health on 9000: %d %q %v", code, body, err)
	}
	if _, _, err := proxy.HTTPGet(ctx, "ice", 8080, "/q/health", time.Second); err == nil {
		t.Fatal("health on 8080 was served for a workload that serves it on 9000")
	}
}

// Flink CDC workloads reach only agents that run them (an older agent would start a Debezium
// container); their components, helper status and component logs come back through the poll.
func TestAgentRunsFlinkCDCOnlyWhenItCan(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	e := enroll(t, srv, admin, pid)
	ctx := context.Background()
	rid := e.body.Str("id")
	proxy := &services.AgentDriver{RuntimeID: uuid.MustParse(rid)}
	w := runtime.Workload{PipelineID: "p", ShortID: "lake", Name: "n", Image: "flink", ConfigHash: "h1", Running: true, Engine: "flink-cdc",
		Flink: &runtime.FlinkTopology{TaskManagers: 1, HelperImage: "bezisync"}}
	// Never connected: stored, and handed out once an agent that runs Flink CDC polls.
	if err := proxy.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	agentPoll(t, e.agent)
	agentPoll(t, e.agent)
	if got := e.local.Workloads["lake"]; !got.IsFlink() || got.Flink.HelperImage != "bezisync" {
		t.Fatalf("%+v", got)
	}
	obs, err := proxy.Observe(ctx, "lake")
	if err != nil || obs.Details["components"] == nil {
		t.Fatalf("%+v %v", obs, err)
	}
	if code, body, err := proxy.HTTPGet(ctx, "lake", 8080, "/bz/status", time.Second); err != nil || code != 200 || !strings.Contains(body, "running") {
		t.Fatal(code, body, err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = e.agent.Poll(ctx)
			time.Sleep(200 * time.Millisecond)
		}
	})
	logs, err := proxy.LogsOf(ctx, "lake", "taskmanager-1", 100, false)
	close(stop)
	wg.Wait()
	if err != nil || logs != "logs of taskmanager-1" {
		t.Fatal(logs, err)
	}

	// An agent from before Flink CDC: Apply says to upgrade, and the poll withholds the workload.
	exec(t, `UPDATE runtimes SET agent_state = jsonb_set(agent_state, '{features}', '["health_port"]') WHERE id = '`+rid+`'`)
	w.ConfigHash = "h2"
	if err := proxy.Apply(ctx, w); err == nil || !strings.Contains(err.Error(), "can't run Flink CDC pipelines. Upgrade it to BeziSync 0.3.0") {
		t.Fatal(err)
	}
	poll := func(features string) []any {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/api/v1/agent/poll",
			strings.NewReader(`{"version": "0.2.0", "kind": "docker", "features": `+features+`, "workloads": {}, "results": []}`))
		req.Header.Set("Authorization", "Bearer "+e.body.Str("agent_token"))
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = res.Body.Close() }()
		var out struct{ Workloads []any }
		if err := json.NewDecoder(res.Body).Decode(&out); err != nil || res.StatusCode != http.StatusOK {
			t.Fatal(res.StatusCode, err)
		}
		return out.Workloads
	}
	if got := poll(`["health_port"]`); len(got) != 0 {
		t.Fatal("an agent without flink_cdc got a Flink CDC workload", got)
	}
	if got := poll(`["health_port", "flink_cdc"]`); len(got) != 1 {
		t.Fatal("the upgraded agent did not get it", got)
	}
}
