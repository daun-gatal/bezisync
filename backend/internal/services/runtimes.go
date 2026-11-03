package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
	"github.com/daun-gatal/bezisync/backend/internal/schemas"
	"github.com/daun-gatal/bezisync/backend/internal/schemaval"
	"github.com/daun-gatal/bezisync/backend/internal/security"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

// DriverFactory builds direct-mode drivers; tests replace it with a fake.
var DriverFactory = buildDriver

// BuildDriver creates a driver for a runtime kind from its (stored) config and secrets.
func BuildDriver(ctx context.Context, kind string, config, secretValues map[string]any) (runtime.Driver, error) {
	return DriverFactory(ctx, kind, config, secretValues)
}

// ResetDrivers forgets cached drivers (tests).
func ResetDrivers() {
	driverMu.Lock()
	defer driverMu.Unlock()
	driverCache = map[driverKey]runtime.Driver{}
}

func buildDriver(ctx context.Context, kind string, config, secretValues map[string]any) (runtime.Driver, error) {
	k, ok := schemas.Runtime(kind)
	if !ok {
		return nil, runtime.Errorf("Unknown runtime kind '%s'.", kind)
	}
	v := &val.V{}
	cfg, okk := schemaval.Model(v, nil, k.ConfigSchema, config)
	if !okk {
		return nil, runtime.Errorf("Cannot initialise the %s runtime: %s", kind, firstError(v))
	}
	str := func(m map[string]any, key string) string {
		s, _ := m[key].(string)
		return s
	}
	switch kind {
	case "kubernetes":
		inCluster, _ := cfg["in_cluster"].(bool)
		auth := str(cfg, "cluster_auth")
		ns := str(cfg, "namespace")
		if !inCluster && (auth == "gke" || auth == "eks") {
			var source runtime.TokenSource
			if auth == "gke" {
				s, err := runtime.GKETokenSource(ctx, str(secretValues, "gcp_service_account_json"))
				if err != nil {
					return nil, err
				}
				source = s
			} else {
				source = runtime.EKSTokenSource(str(cfg, "eks_cluster_name"), str(cfg, "region"),
					str(secretValues, "aws_access_key_id"), str(secretValues, "aws_secret_access_key"))
			}
			rc, err := runtime.TokenRESTConfig(ctx, str(cfg, "endpoint"), str(cfg, "ca_certificate"), source)
			if err != nil {
				var re *runtime.Error
				if errors.As(err, &re) {
					return nil, err
				}
				return nil, runtime.Errorf("Cannot initialise the kubernetes runtime: %v", err)
			}
			return runtime.NewKubernetes(ns, false, "", rc)
		}
		return runtime.NewKubernetes(ns, inCluster, str(secretValues, "kubeconfig"), nil)
	case "docker":
		var tlsCfg *runtime.DockerTLS
		ca, cert, key := str(secretValues, "ca_pem"), str(secretValues, "cert_pem"), str(secretValues, "key_pem")
		if ca != "" || cert != "" || key != "" {
			verify, _ := cfg["tls_verify"].(bool)
			tlsCfg = &runtime.DockerTLS{CA: ca, Cert: cert, Key: key, Verify: verify}
		}
		pullTimeout, err := time.ParseDuration(str(cfg, "image_pull_timeout"))
		if err != nil {
			pullTimeout = runtime.DefaultPullTimeout // stored before the setting existed
		}
		return runtime.NewDocker(str(cfg, "host"), tlsCfg, str(cfg, "network"), pullTimeout)
	}
	return nil, runtime.Errorf("Unknown runtime kind '%s'.", kind)
}

func firstError(v *val.V) string {
	if len(v.Errors) == 0 {
		return "invalid configuration"
	}
	e := v.Errors[0]
	return strings.Join(e.Loc, ".") + ": " + e.Message
}

type driverKey struct {
	id      uuid.UUID
	version int
}

var (
	driverMu    sync.Mutex
	driverCache = map[driverKey]runtime.Driver{}
)

// DriverFor returns the driver of a runtime (agent-backed for agent runtimes; cached per
// runtime version otherwise).
func DriverFor(ctx context.Context, q db.Querier, rt *models.Runtime) (runtime.Driver, error) {
	if rt.Mode == "agent" {
		return &AgentDriver{RuntimeID: rt.ID}, nil
	}
	key := driverKey{rt.ID, rt.Version}
	driverMu.Lock()
	defer driverMu.Unlock()
	if d, ok := driverCache[key]; ok {
		return d, nil
	}
	sec := map[string]any{}
	if rt.SecretsEnc != nil && *rt.SecretsEnc != "" {
		var err error
		if sec, err = security.DecryptSecret(*rt.SecretsEnc); err != nil {
			return nil, err
		}
	}
	d, err := BuildDriver(ctx, rt.Kind, rt.Config, sec)
	if err != nil {
		return nil, err
	}
	driverCache[key] = d
	return d, nil
}

// --- agent-backed driver: the control plane records desired workloads and reads the
// agent's latest report; the agent dials out and reconciles locally. ---

// AgentOfflineAfter: no poll for this long ⇒ the runtime is unreachable.
const AgentOfflineAfter = 30 * time.Second

var cachedPaths = map[string]string{"/q/health": "health", "/metrics": "metrics"}

// AgentDriver drives a runtime served by an agent.
type AgentDriver struct{ RuntimeID uuid.UUID }

func (a *AgentDriver) state(ctx context.Context) (map[string]any, *time.Time, error) {
	rt, err := GetRuntime(ctx, db.MustPool(ctx), a.RuntimeID)
	if err != nil {
		return nil, nil, err
	}
	if rt == nil {
		return nil, nil, &runtime.Error{Msg: "Runtime was deleted."}
	}
	st := map[string]any(rt.AgentState)
	if st == nil {
		st = map[string]any{}
	}
	return st, rt.LastSeenAt, nil
}

func online(seen *time.Time) bool {
	return seen != nil && time.Since(*seen) < AgentOfflineAfter
}

func (a *AgentDriver) liveState(ctx context.Context) (map[string]any, error) {
	st, seen, err := a.state(ctx)
	if err != nil {
		return nil, err
	}
	if !online(seen) {
		when := "yet"
		if seen != nil {
			when = "since " + seen.UTC().Format("15:04:05") + " UTC"
		}
		return nil, runtime.Errorf("The agent has not connected %s.", when)
	}
	return st, nil
}

func workloads(st map[string]any) map[string]any {
	w, _ := st["workloads"].(map[string]any)
	if w == nil {
		return map[string]any{}
	}
	return w
}

// Apply stores the desired workload (encrypted) for the agent to pick up.
func (a *AgentDriver) Apply(ctx context.Context, w runtime.Workload) error {
	w.Owner = a.RuntimeID.String()
	w.Normalize()
	pulls := sha256Hex(strings.Join(append(append([]string{}, w.ImagePullSecrets...), w.ServiceAccount), ","))[:6]
	running := 0
	if w.Running {
		running = 1
	}
	key := fmt.Sprintf("%s:%d:%s", w.ConfigHash, running, pulls)
	q := db.MustPool(ctx)
	row, err := db.Find[models.AgentWorkload](ctx, q, `SELECT * FROM agent_workloads WHERE runtime_id = $1 AND short_id = $2`, a.RuntimeID, w.ShortID)
	if err != nil {
		return err
	}
	if row != nil && row.ConfigHash == key {
		return nil
	}
	spec, err := WorkloadDict(w)
	if err != nil {
		return err
	}
	enc, err := security.EncryptSecret(spec)
	if err != nil {
		return err
	}
	if row == nil {
		return db.Insert(ctx, q, "agent_workloads", &models.AgentWorkload{ID: db.NewID(), RuntimeID: a.RuntimeID, ShortID: w.ShortID,
			ConfigHash: key, SpecEnc: enc, UpdatedAt: db.Now()})
	}
	_, err = q.Exec(ctx, `UPDATE agent_workloads SET config_hash = $1, spec_enc = $2, updated_at = $3 WHERE id = $4`, key, enc, db.Now(), row.ID)
	return err
}

// WorkloadDict is dataclasses.asdict(workload) (field order kept).
func WorkloadDict(w runtime.Workload) (map[string]any, error) {
	return ToMap(w)
}

// Delete removes the desired workload.
func (a *AgentDriver) Delete(ctx context.Context, shortID string) error {
	_, err := db.MustPool(ctx).Exec(ctx, `DELETE FROM agent_workloads WHERE runtime_id = $1 AND short_id = $2`, a.RuntimeID, shortID)
	return err
}

// Observe reads the workload from the agent's latest report.
func (a *AgentDriver) Observe(ctx context.Context, shortID string) (runtime.Observation, error) {
	st, err := a.liveState(ctx)
	if err != nil {
		return runtime.Observation{}, err
	}
	report, ok := workloads(st)[shortID].(map[string]any)
	if !ok {
		return runtime.Observation{Phase: runtime.PhaseMissing}, nil
	}
	obs := runtime.Observation{Phase: "pending"}
	if p, ok := report["phase"].(string); ok {
		obs.Phase = p
	}
	obs.Ready, _ = report["ready"].(bool)
	obs.Restarts = int(toFloat(report["restarts"]))
	if s, ok := report["started_at"].(string); ok && s != "" {
		if t, err := pytime.Parse(s); err == nil {
			obs.StartedAt = &t
		}
	}
	if v, ok := report["exit_code"]; ok && v != nil {
		e := int(toFloat(v))
		obs.ExitCode = &e
	}
	obs.Reason, _ = report["reason"].(string)
	obs.ImagePull, _ = report["image_pull"].(string)
	if h, ok := report["config_hash"].(string); ok {
		obs.ConfigHash = &h
	}
	return obs, nil
}

func (a *AgentDriver) request(ctx context.Context, op string, args map[string]any, timeout time.Duration) (map[string]any, error) {
	if _, err := a.liveState(ctx); err != nil {
		return nil, err
	}
	q := db.MustPool(ctx)
	// Arguments may carry resolved secrets (connection checks): encrypted at rest.
	enc, err := security.EncryptSecret(args)
	if err != nil {
		return nil, err
	}
	id := db.NewID()
	if err := db.Insert(ctx, q, "agent_requests", &models.AgentRequest{ID: id, RuntimeID: a.RuntimeID, Op: op,
		Args: db.Map{"enc": enc}, State: "pending", CreatedAt: db.Now()}); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			_, _ = q.Exec(context.WithoutCancel(ctx), `DELETE FROM agent_requests WHERE id = $1`, id)
			return nil, ctx.Err()
		case <-time.After(400 * time.Millisecond):
		}
		row, err := db.Find[models.AgentRequest](ctx, q, `SELECT * FROM agent_requests WHERE id = $1`, id)
		if err != nil {
			return nil, err
		}
		if row != nil && row.State != "pending" {
			result := map[string]any(row.Result)
			if result == nil {
				result = map[string]any{}
			}
			if _, err := q.Exec(ctx, `DELETE FROM agent_requests WHERE id = $1`, id); err != nil {
				return nil, err
			}
			if row.State == "failed" {
				msg := "agent request failed"
				if e, ok := result["error"]; ok {
					msg = fmt.Sprint(e)
				}
				return nil, &runtime.Error{Msg: msg}
			}
			return result, nil
		}
	}
	if _, err := q.Exec(ctx, `DELETE FROM agent_requests WHERE id = $1`, id); err != nil {
		return nil, err
	}
	return nil, &runtime.Error{Msg: "The agent did not answer in time."}
}

// AgentRunCheck runs a connection check on the agent, inside the runtime's network.
func AgentRunCheck(ctx context.Context, runtimeID uuid.UUID, kind string, conn connectors.Resolved, tables []string) (map[string]any, error) {
	var t any
	if tables != nil {
		l := make([]any, len(tables))
		for i, s := range tables {
			l[i] = s
		}
		t = l
	}
	return (&AgentDriver{RuntimeID: runtimeID}).request(ctx, "check", map[string]any{
		"kind": kind, "type": conn.Type, "provider": conn.Provider, "config": conn.Config, "secrets": conn.Secrets, "tables": t,
		"linked": linkedPayload(conn),
	}, 60*time.Second)
}

// Logs asks the agent for log lines.
func (a *AgentDriver) Logs(ctx context.Context, shortID string, tail int, previous bool) (string, error) {
	res, err := a.request(ctx, "logs", map[string]any{"short_id": shortID, "tail": tail, "previous": previous}, 12*time.Second)
	if err != nil {
		return "", err
	}
	s, _ := res["text"].(string)
	return s, nil
}

// HTTPGet serves health/metrics from the latest report; other paths ask the agent.
func (a *AgentDriver) HTTPGet(ctx context.Context, shortID string, port int, path string, timeout time.Duration) (int, string, error) {
	if cached, ok := cachedPaths[path]; ok && (path == "/metrics") == (port == 9404) {
		st, err := a.liveState(ctx)
		if err != nil {
			return 0, "", err
		}
		report, _ := workloads(st)[shortID].(map[string]any)
		// Agents report health on the workload's port (health_port); older ones always on 8080.
		reported := 8080
		if v, ok := report["health_port"]; ok {
			reported = int(toFloat(v))
		}
		if cached == "health" && reported != port {
			return 0, "", &runtime.Error{Msg: fmt.Sprintf("The agent reads health on port %d; this connector serves it on %d. Upgrade the agent.", reported, port)}
		}
		value, _ := report[cached].([]any)
		if len(value) < 2 {
			return 0, "", &runtime.Error{Msg: "No recent report from the connector."}
		}
		return int(toFloat(value[0])), fmt.Sprint(value[1]), nil
	}
	res, err := a.request(ctx, "http_get", map[string]any{"short_id": shortID, "port": port, "path": path}, timeout+6*time.Second)
	if err != nil {
		return 0, "", err
	}
	body := ""
	if b, ok := res["body"]; ok && b != nil {
		body = fmt.Sprint(b)
	}
	return int(toFloat(res["status"])), body, nil
}

// Test reports whether the agent is connected and its own runtime test.
func (a *AgentDriver) Test(ctx context.Context) runtime.TestResult {
	st, seen, err := a.state(ctx)
	if err != nil {
		return runtime.TestResult{OK: false, Message: err.Error()}
	}
	if !online(seen) {
		msg := "The agent has not connected yet. Install it with the command shown for this runtime."
		if seen != nil {
			msg = "The agent was last seen " + seen.UTC().Format("2006-01-02 15:04:05") + " UTC."
		}
		return runtime.TestResult{OK: false, Message: msg}
	}
	local, _ := st["local_test"].(map[string]any)
	ok := true
	if v, present := local["ok"]; present {
		ok, _ = v.(bool)
	}
	version := "?"
	if v, present := st["version"]; present {
		version = fmt.Sprint(v)
	}
	msg := ""
	if m, present := local["message"]; present {
		msg = fmt.Sprint(m)
	}
	capacity, _ := st["capacity"].(map[string]any)
	if capacity == nil {
		capacity = map[string]any{}
	}
	return runtime.TestResult{OK: ok, Message: strings.TrimSpace(fmt.Sprintf("Agent %s connected. %s", version, msg)), Details: capacity}
}

// ListWorkloads lists the workloads of the latest report.
func (a *AgentDriver) ListWorkloads(ctx context.Context, _ string) ([]string, error) {
	st, err := a.liveState(ctx)
	if err != nil {
		return nil, err
	}
	out := []string{}
	for k := range workloads(st) {
		out = append(out, k)
	}
	return out, nil
}

// Capacity is the capacity of the latest report.
func (a *AgentDriver) Capacity(ctx context.Context) map[string]any {
	st, _, err := a.state(ctx)
	if err != nil {
		return nil
	}
	c, _ := st["capacity"].(map[string]any)
	if len(c) == 0 {
		return nil
	}
	return c
}

// linkedPayload sends a connection's links to the agent as {field: {type, provider, config, secrets}}.
func linkedPayload(conn connectors.Resolved) map[string]any {
	out := map[string]any{}
	for field, l := range conn.Linked {
		out[field] = map[string]any{"type": l.Type, "provider": l.Provider, "config": l.Config, "secrets": l.Secrets}
	}
	return out
}

// LinkedFromPayload reads the links an agent was sent (see linkedPayload).
func LinkedFromPayload(v any) map[string]connectors.Resolved {
	m, _ := v.(map[string]any)
	if len(m) == 0 {
		return nil
	}
	out := map[string]connectors.Resolved{}
	for field, raw := range m {
		l, _ := raw.(map[string]any)
		if l == nil {
			continue
		}
		cfg, _ := l["config"].(map[string]any)
		sec, _ := l["secrets"].(map[string]any)
		if sec == nil {
			sec = map[string]any{}
		}
		provider, _ := l["provider"].(string)
		typ, _ := l["type"].(string)
		out[field] = connectors.Resolved{Type: typ, Provider: provider, Config: cfg, Secrets: sec}
	}
	return out
}
