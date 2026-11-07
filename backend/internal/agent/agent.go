// Package agent is `bezisync agent`: it runs next to a Docker host or in a Kubernetes
// cluster and dials out to the control plane.
//
// Each cycle it reports what is running (phase, health, metrics), receives the desired
// workloads, converges the local runtime and answers on-demand requests (logs, checks,
// HTTP reads). It never listens on a port, so remote runtimes need no inbound firewall
// rules.
package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/logging"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/version"
)

var log = logging.Logger("bezisync.agent")

// Agent is one running agent.
type Agent struct {
	URL    string
	Kind   string
	Driver runtime.Driver
	// HTTP sends polls; its transport adds the agent token.
	HTTP *http.Client

	mu      sync.Mutex
	results []map[string]any
	desired map[string]runtime.Workload
	owner   string // the runtime id, learned from the first poll
	synced  bool   // never converge (and delete) before the first desired state arrives
	seen    map[string]any
	changed chan struct{}
}

type bearer struct {
	token string
	base  http.RoundTripper
}

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.base.RoundTrip(r)
}

// New creates an agent for a runtime kind and its local configuration.
func New(ctx context.Context, url, token, kind string, cfg map[string]any, verifyTLS bool) (*Agent, error) {
	driver, err := services.BuildDriver(ctx, kind, cfg, map[string]any{})
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if !verifyTLS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // --insecure, for testing only
	}
	return NewWithDriver(url, kind, driver, &http.Client{Timeout: 30 * time.Second, Transport: bearer{token, transport}}), nil
}

// NewWithDriver creates an agent around an existing driver and HTTP client.
func NewWithDriver(url, kind string, driver runtime.Driver, client *http.Client) *Agent {
	return &Agent{URL: strings.TrimRight(url, "/"), Kind: kind, Driver: driver, HTTP: client,
		desired: map[string]runtime.Workload{}, changed: make(chan struct{}, 1)}
}

// Owner is the runtime id the control plane reported.
func (a *Agent) Owner() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.owner
}

func truncate(s string, n int) string {
	if utf8.RuneCountInString(s) > n {
		return string([]rune(s)[:n])
	}
	return s
}

// Report is the poll body: what runs here and the answers to earlier requests.
func (a *Agent) Report(ctx context.Context) *om.Map {
	a.mu.Lock()
	owner := a.owner
	ids := map[string]bool{}
	desired := make(map[string]runtime.Workload, len(a.desired))
	for id, w := range a.desired {
		ids[id] = true
		desired[id] = w
	}
	results := append([]map[string]any{}, a.results...)
	a.mu.Unlock()
	if owner != "" {
		managed, err := a.Driver.ListWorkloads(ctx, owner)
		if err != nil {
			log.Warn("cannot list workloads: " + err.Error())
		}
		for _, id := range managed {
			ids[id] = true
		}
	}
	workloads := om.New()
	for _, id := range sortedKeys(ids) {
		obs, err := a.Driver.Observe(ctx, id)
		if err != nil {
			workloads.Set(id, om.New().Set("phase", "pending").Set("reason", truncate(err.Error(), 200)))
			continue
		}
		var started any
		if obs.StartedAt != nil {
			started = pytime.ISOFormat(*obs.StartedAt)
		}
		item := om.New().Set("phase", obs.Phase).Set("ready", obs.Ready).Set("restarts", obs.Restarts).Set("started_at", started).
			Set("exit_code", obs.ExitCode).Set("reason", obs.Reason).Set("config_hash", obs.ConfigHash)
		if obs.ImagePull != "" {
			item.Set("image_pull", obs.ImagePull)
		}
		if len(obs.Details) > 0 {
			item.Set("details", obs.Details)
		}
		if obs.Phase == "running" {
			healthPort, flink := 8080, false
			if w, ok := desired[id]; ok {
				healthPort, flink = w.Health(), w.IsFlink()
			}
			item.Set("health_port", healthPort)
			second := struct {
				key  string
				port int
				path string
			}{"metrics", 9404, "/metrics"}
			if flink { // the helper's evidence instead of Debezium's JMX metrics
				second.key, second.port, second.path = "status", healthPort, "/bz/status"
			}
			for _, p := range []struct {
				key  string
				port int
				path string
			}{{"health", healthPort, "/q/health"}, second} {
				code, body, err := a.Driver.HTTPGet(ctx, id, p.port, p.path, 4*time.Second)
				if err != nil {
					item.Set(p.key, nil)
				} else {
					item.Set(p.key, []any{code, body})
				}
			}
		}
		workloads.Set(id, item)
	}
	test := a.Driver.Test(ctx)
	if results == nil {
		results = []map[string]any{}
	}
	return om.New().Set("version", version.Version).Set("kind", a.Kind).Set("features", []any{"health_port", runtime.FeatureFlinkCDC, runtime.FeaturePlacement}).Set("capacity", a.Driver.Capacity(ctx)).
		Set("local_test", localTest(test)).Set("workloads", workloads).Set("results", results)
}

// localTest is the runtime's own test as the control plane reads it; on Kubernetes it says
// whether Flink CDC pipelines can run (Services).
func localTest(test runtime.TestResult) *om.Map {
	out := om.New().Set("ok", test.OK).Set("message", test.Message)
	if v, ok := test.Details["flink_cdc"].(bool); ok {
		out.Set("flink_cdc", v)
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// workload decodes a desired workload (unknown fields are ignored).
func workload(spec map[string]any) (runtime.Workload, error) {
	raw, err := json.Marshal(spec)
	if err != nil {
		return runtime.Workload{}, err
	}
	var w runtime.Workload
	err = json.Unmarshal(raw, &w)
	return w, err
}

func decodeDesired(specs []any) map[string]runtime.Workload {
	out := map[string]runtime.Workload{}
	for _, s := range specs {
		m, ok := s.(map[string]any)
		if !ok {
			continue
		}
		w, err := workload(m)
		if err != nil {
			log.Warn("ignoring an invalid workload: " + err.Error())
			continue
		}
		out[w.ShortID] = w
	}
	return out
}

// ApplyDesired converges the local runtime to the last desired state now.
func (a *Agent) ApplyDesired(ctx context.Context) {
	a.mu.Lock()
	desired := make(map[string]runtime.Workload, len(a.desired))
	for k, v := range a.desired {
		desired[k] = v
	}
	a.mu.Unlock()
	a.apply(ctx, desired)
}

// PendingResults reports answers waiting for the next poll.
func (a *Agent) PendingResults() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.results) > 0
}

// Converge makes the local runtime match the desired workloads.
func (a *Agent) Converge(ctx context.Context, desired []any) {
	d := decodeDesired(desired)
	a.mu.Lock()
	a.desired = d
	a.mu.Unlock()
	a.apply(ctx, d)
}

func (a *Agent) apply(ctx context.Context, desired map[string]runtime.Workload) {
	for _, w := range desired {
		if err := a.Driver.Apply(ctx, w); err != nil {
			log.Warn(fmt.Sprintf("apply %s failed: %v", w.ShortID, err))
		}
	}
	owner := a.Owner()
	if owner == "" {
		return
	}
	managed, err := a.Driver.ListWorkloads(ctx, owner)
	if err != nil {
		log.Warn("cleanup failed: " + err.Error())
		return
	}
	for _, id := range managed {
		if _, ok := desired[id]; ok {
			continue
		}
		log.Info(fmt.Sprintf("removing workload %s (no longer desired)", id))
		if err := a.Driver.Delete(ctx, id); err != nil {
			log.Warn("cleanup failed: " + err.Error())
		}
	}
}

// convergeLoop applies changes off the poll loop: applying can take minutes (image
// pulls) and polling must keep reporting meanwhile, or the agent would look offline.
func (a *Agent) convergeLoop(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.changed:
		case <-t.C:
		}
		a.mu.Lock()
		synced := a.synced
		desired := make(map[string]runtime.Workload, len(a.desired))
		for k, v := range a.desired {
			desired[k] = v
		}
		a.mu.Unlock()
		if synced {
			a.apply(ctx, desired)
		}
	}
}

func intArg(v any, def int) int {
	switch n := v.(type) {
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return int(i)
		}
		if f, err := n.Float64(); err == nil {
			return int(f)
		}
	case float64:
		return int(n)
	case int:
		return n
	case string:
		var i int
		if _, err := fmt.Sscan(n, &i); err == nil {
			return i
		}
	}
	return def
}

func stringList(v any) []string {
	l, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(l))
	for _, x := range l {
		out = append(out, fmt.Sprint(x))
	}
	return out
}

// Answer runs the control plane's requests; results go out with the next poll.
func (a *Agent) Answer(ctx context.Context, requests []any) {
	var results []map[string]any
	for _, r := range requests {
		req, ok := r.(map[string]any)
		if !ok {
			continue
		}
		args, _ := req["args"].(map[string]any)
		if args == nil {
			args = map[string]any{}
		}
		result, err := a.run(ctx, fmt.Sprint(req["op"]), args)
		if err != nil {
			results = append(results, map[string]any{"id": req["id"], "ok": false, "result": map[string]any{"error": truncate(err.Error(), 500)}})
			continue
		}
		results = append(results, map[string]any{"id": req["id"], "ok": true, "result": result})
	}
	a.mu.Lock()
	a.results = results
	a.mu.Unlock()
}

func (a *Agent) run(ctx context.Context, op string, args map[string]any) (any, error) {
	switch op {
	case "logs":
		previous, _ := args["previous"].(bool)
		if component, _ := args["component"].(string); component != "" {
			c, ok := a.Driver.(runtime.ComponentLogs)
			if !ok {
				return nil, fmt.Errorf("this runtime has no component %s", component)
			}
			text, err := c.LogsOf(ctx, fmt.Sprint(args["short_id"]), component, intArg(args["tail"], 300), previous)
			return map[string]any{"text": text}, err
		}
		text, err := a.Driver.Logs(ctx, fmt.Sprint(args["short_id"]), intArg(args["tail"], 300), previous)
		return map[string]any{"text": text}, err
	case "check":
		provider, _ := args["provider"].(string)
		if provider == "" {
			provider = "self_hosted"
		}
		cfg, _ := args["config"].(map[string]any)
		secrets, _ := args["secrets"].(map[string]any)
		if secrets == nil {
			secrets = map[string]any{}
		}
		report, err := services.RunCheck(ctx, connectors.Resolved{Type: fmt.Sprint(args["type"]), Provider: provider, Config: cfg, Secrets: secrets,
			Linked: services.LinkedFromPayload(args["linked"])},
			fmt.Sprint(args["kind"]), stringList(args["tables"]))
		if err != nil {
			return nil, err
		}
		return services.ReportMap(report)
	case "http_get":
		code, body, err := a.Driver.HTTPGet(ctx, fmt.Sprint(args["short_id"]), intArg(args["port"], 0), fmt.Sprint(args["path"]), 4*time.Second)
		return map[string]any{"status": code, "body": body}, err
	}
	return nil, fmt.Errorf("unknown op %s", op)
}

// Poll sends one report and handles the answer; it returns the delay before the next poll.
func (a *Agent) Poll(ctx context.Context) (time.Duration, error) {
	body, err := json.Marshal(a.Report(ctx))
	if err != nil {
		return 5 * time.Second, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.URL+"/api/v1/agent/poll", bytes.NewReader(body))
	if err != nil {
		return 5 * time.Second, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := a.HTTP.Do(req)
	if err != nil {
		log.Warn("control plane unreachable: " + err.Error())
		return 5 * time.Second, nil
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 64<<20))
	switch {
	case res.StatusCode == http.StatusUnauthorized:
		log.Error("the control plane rejected the agent token")
		return 2 * time.Second, nil
	case res.StatusCode < 200 || res.StatusCode >= 300:
		log.Warn(fmt.Sprintf("poll failed: HTTP %d %s", res.StatusCode, truncate(string(raw), 200)))
		return 2 * time.Second, nil
	}
	decoded, err := pyjson.DecodeGeneric(raw)
	if err != nil {
		return 5 * time.Second, err
	}
	data, _ := decoded.(map[string]any)
	workloads, _ := data["workloads"].([]any)
	desired := map[string]any{}
	for _, s := range workloads {
		if m, ok := s.(map[string]any); ok {
			desired[fmt.Sprint(m["short_id"])] = m
		}
	}
	a.mu.Lock()
	if id, ok := data["runtime_id"].(string); ok && id != "" {
		a.owner = id
	}
	a.results = nil
	changed := !a.synced || !reflect.DeepEqual(desired, a.seen)
	if changed {
		a.seen = desired
		a.desired = decodeDesired(workloads)
		a.synced = true
	}
	a.mu.Unlock()
	if changed {
		select {
		case a.changed <- struct{}{}:
		default:
		}
	}
	requests, _ := data["requests"].([]any)
	a.Answer(ctx, requests)
	delay := time.Duration(float64(intArg(data["poll_seconds"], 2)) * float64(time.Second))
	a.mu.Lock()
	pending := len(a.results) > 0
	a.mu.Unlock()
	if pending {
		delay = 200 * time.Millisecond
	}
	return delay, nil
}

// Run polls until ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	go a.convergeLoop(ctx)
	for {
		delay, err := a.Poll(ctx)
		if err != nil {
			log.Warn("poll failed: " + err.Error())
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
	}
}
