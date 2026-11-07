package connectors

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/ssrf"
)

// Elasticsearch: one index per source table, written by Flink CDC's Elasticsearch sink (one
// document per row, its id the primary key; a delete removes the document). Only Flink CDC
// writes it. BeziSync checks, reads and drops indexes over the HTTP API, through the SSRF guard.
// The sink signs in with a user and password; it has no CA option, so https works only with a
// certificate the JVM trusts (a public CA).

type elasticsearchSink struct{ BaseSink }

func init() { Register(&elasticsearchSink{BaseSink{Title: "Elasticsearch"}}) }

func (*elasticsearchSink) Kind() string { return "sink" }
func (*elasticsearchSink) Type() string { return "elasticsearch" }

type esConfig struct {
	Hosts        []string // http(s)://host:port
	Version      int
	User, Secret string
}

func esCfg(conn Resolved) (esConfig, error) {
	d, _ := Describe("sink", "elasticsearch")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return esConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return esConfig{}, err
	}
	v, _ := strconv.Atoi(Str(cfg, "version"))
	c := esConfig{Version: v, User: Str(cfg, "username"), Secret: Str(sec, "password")}
	for _, h := range strings.Split(Str(cfg, "hosts"), ",") {
		c.Hosts = append(c.Hosts, strings.TrimRight(strings.TrimSpace(h), "/"))
	}
	if c.User != "" && c.Secret == "" {
		return c, problem.New(422, "elasticsearch_config", "Signing in with a user needs its password.").
			WithErrors([]map[string]any{{"loc": []string{"secrets", "password"}, "message": "Enter the password."}})
	}
	return c, nil
}

func (s *elasticsearchSink) CompileSink(Resolved, CompileContext) (map[string]string, map[string]string, error) {
	return nil, nil, problem.New(422, "engine_unsupported", "Elasticsearch is written by Flink CDC: run this pipeline on Flink CDC.")
}

func (s *elasticsearchSink) FlinkSink(conn Resolved, _ CompileContext) (*FlinkBlock, error) {
	cfg, err := esCfg(conn)
	if err != nil {
		return nil, err
	}
	o := FlinkOptions{{"type", "elasticsearch"}, {"hosts", strings.Join(cfg.Hosts, ",")}, {"version", strconv.Itoa(cfg.Version)}}
	env := map[string]string{}
	if cfg.User != "" {
		o.Set("username", cfg.User)
		o.Set("password", "${BZ_SINK_PASSWORD}")
		env["BZ_SINK_PASSWORD"] = cfg.Secret
	}
	return &FlinkBlock{Options: o, Env: env, Connector: "elasticsearch"}, nil
}

// FlinkDestination: the index is the routed name with dots as '_', lower-cased (index names
// are lower case; the naming rule checks names with dots flattened).
func (s *elasticsearchSink) FlinkDestination(_ Resolved, _ CompileContext, routed string) string {
	return strings.ToLower(strings.ReplaceAll(routed, ".", "_"))
}

// Destination is the same name for every caller that shows one.
func (s *elasticsearchSink) Destination(conn Resolved, c CompileContext, routed string) string {
	return s.FlinkDestination(conn, c, routed)
}

// esCall makes one request to the first host that answers.
func esCall(ctx context.Context, cfg esConfig, method, path string, body any, timeout time.Duration) (int, []byte, error) {
	client := ssrf.HTTPClient(timeout) // dials only what the guard allows (ADR 0004)
	var lastErr error
	for _, h := range cfg.Hosts {
		var rd io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rd = bytes.NewReader(b)
		}
		req, err := http.NewRequestWithContext(ctx, method, h+path, rd)
		if err != nil {
			return 0, nil, err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if cfg.User != "" {
			req.SetBasicAuth(cfg.User, cfg.Secret)
		}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		_ = resp.Body.Close()
		return resp.StatusCode, data, nil
	}
	return 0, nil, lastErr
}

func (s *elasticsearchSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	run := &checks.Run{}
	cfg, err := esCfg(conn)
	if err != nil {
		var p *problem.Problem
		if errors.As(err, &p) {
			run.Add(checks.Check{Code: "elasticsearch_config", Title: "Settings", Status: checks.Fail, Category: checks.CatDestination, Blocking: true, Message: p.Detail})
			return checks.Summarize(run.Checks, started), nil
		}
		return checks.Report{}, err
	}
	u, _ := url.Parse(cfg.Hosts[0])
	port, _ := strconv.Atoi(u.Port())
	if port == 0 {
		port = map[string]int{"https": 443}[u.Scheme]
		if port == 0 {
			port = 80
		}
	}
	if !NetworkCheck(ctx, run, u.Hostname(), port, "elasticsearch_network", "Elasticsearch", 0) {
		return checks.Summarize(run.Checks, started), nil
	}
	code, body, err := esCall(ctx, cfg, http.MethodGet, "/", nil, 10*time.Second)
	switch {
	case err != nil:
		run.Add(checks.Check{Code: "elasticsearch_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: "Elasticsearch didn't answer over HTTP.", Technical: firstRunes(err.Error(), 300),
			Fix: &checks.Fix{Kind: "text", Who: "you", Code: "Check the host's scheme (http or https). https needs a certificate from a public CA: Flink CDC's Elasticsearch sink can't be given another CA."}})
		return checks.Summarize(run.Checks, started), nil
	case code == http.StatusUnauthorized || code == http.StatusForbidden:
		run.Add(checks.Check{Code: "elasticsearch_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: fmt.Sprintf("Elasticsearch refused the sign-in (HTTP %d).", code), Fix: &checks.Fix{Kind: "text", Who: "you", Code: "Check the user and password on the connection."}})
		return checks.Summarize(run.Checks, started), nil
	case code != http.StatusOK:
		run.Add(checks.Check{Code: "elasticsearch_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: fmt.Sprintf("Elasticsearch answered HTTP %d.", code), Technical: firstRunes(string(body), 300)})
		return checks.Summarize(run.Checks, started), nil
	}
	var info struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	_ = json.Unmarshal(body, &info)
	run.Passed("elasticsearch_login", "Sign in", checks.CatAuth, "Signed in (Elasticsearch "+info.Version.Number+").", "")
	major, _, _ := strings.Cut(info.Version.Number, ".")
	switch {
	case major == "":
		run.Add(checks.Check{Code: "elasticsearch_version", Title: "Version", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
			Message: "The server didn't say its version: it may not be Elasticsearch (OpenSearch isn't supported by the sink).", Technical: firstRunes(string(body), 300),
			Fix: &checks.Fix{Kind: "text", Who: "you", Code: "Point the connection at an Elasticsearch 6, 7 or 8 cluster."}})
		return checks.Summarize(run.Checks, started), nil
	case major != strconv.Itoa(cfg.Version):
		run.Add(checks.Check{Code: "elasticsearch_version", Title: "Version", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
			Message: fmt.Sprintf("The connection says version %d, and the server is %s: the sink writes with the client of the version set.", cfg.Version, info.Version.Number),
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Set the connection's version to " + major + "."}})
		return checks.Summarize(run.Checks, started), nil
	}
	run.Passed("elasticsearch_version", "Version", checks.CatDestination, "Version "+strconv.Itoa(cfg.Version)+" matches the server.", "")
	// The sink indexes into indexes it doesn't create: prove a document creates one.
	probe := fmt.Sprintf("bezisync_check_%d", time.Now().UnixNano()%1e9)
	code, body, err = esCall(ctx, cfg, http.MethodPost, "/"+probe+"/_doc?refresh=true", map[string]any{"id": 1}, 10*time.Second)
	if err != nil || (code != http.StatusCreated && code != http.StatusOK) {
		tech := string(body)
		if err != nil {
			tech = err.Error()
		}
		run.Add(checks.Check{Code: "elasticsearch_create_index", Title: "Create indexes", Status: checks.Fail, Category: checks.CatPermissions, Blocking: true,
			Message: "Writing a document to a new index failed: Flink CDC's sink writes into indexes created by their first document.", Technical: firstRunes(tech, 300),
			Fix: &checks.Fix{Kind: "text", Who: "Elasticsearch admin", Code: "Give the user the create_index, index and delete_index privileges on the indexes it writes and on bezisync_check_* (the check's test index), and allow automatic index creation (action.auto_create_index)."}})
		return checks.Summarize(run.Checks, started), nil
	}
	if code, body, err := esCall(context.WithoutCancel(ctx), cfg, http.MethodDelete, "/"+probe, nil, 10*time.Second); err != nil || code != http.StatusOK {
		tech := string(body)
		if err != nil {
			tech = err.Error()
		}
		// The sink never deletes indexes; "drop destinations" does, so a missing right shows here.
		run.Add(checks.Check{Code: "elasticsearch_create_index", Title: "Create indexes", Status: checks.Warn, Category: checks.CatPermissions,
			Message: "Created a test index, " + probe + ", but couldn't delete it: deleting a pipeline with \"drop destinations\" would fail the same way.", Technical: firstRunes(tech, 300),
			Fix: &checks.Fix{Kind: "text", Who: "Elasticsearch admin", Code: "Delete the index " + probe + ", and give the user the delete_index privilege on the indexes it writes."}})
		return checks.Summarize(run.Checks, started), nil
	}
	run.Passed("elasticsearch_create_index", "Create indexes", checks.CatPermissions, "Created and deleted a test index.", "")
	return checks.Summarize(run.Checks, started), nil
}

// Tail reads documents of an index (the Events tab, delivery checks).
func (s *elasticsearchSink) Tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error) {
	cfg, err := esCfg(conn)
	if err != nil {
		return nil, err
	}
	code, body, err := esCall(ctx, cfg, http.MethodGet, "/"+url.PathEscape(destination)+"/_search?size="+strconv.Itoa(limit), nil, timeout)
	if err != nil {
		return nil, err
	}
	if code == http.StatusNotFound {
		return []TailRecord{}, nil
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("elasticsearch answered HTTP %d: %s", code, firstRunes(string(body), 200))
	}
	var res struct {
		Hits struct {
			Hits []struct {
				Source map[string]any `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, err
	}
	out := make([]TailRecord, 0, len(res.Hits.Hits))
	for _, h := range res.Hits.Hits {
		out = append(out, TailRecord{Destination: destination, Value: h.Source})
	}
	return out, nil
}

// CleanupSink drops the pipeline's indexes ("drop destinations").
func (s *elasticsearchSink) CleanupSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := esCfg(conn)
	if err != nil {
		return nil, err
	}
	var out []ArtifactResult
	for _, dest := range sortedUnique(destinations) {
		code, body, err := esCall(ctx, cfg, http.MethodDelete, "/"+url.PathEscape(dest), nil, 30*time.Second)
		switch {
		case err != nil:
			out = append(out, ArtifactResult{Kind: "index", Name: dest, Outcome: "failed", Detail: firstRunes(err.Error(), 300)})
		case code == http.StatusOK:
			out = append(out, ArtifactResult{Kind: "index", Name: dest, Outcome: "removed"})
		case code == http.StatusNotFound:
			out = append(out, ArtifactResult{Kind: "index", Name: dest, Outcome: "not_found"})
		default:
			out = append(out, ArtifactResult{Kind: "index", Name: dest, Outcome: "failed", Detail: fmt.Sprintf("HTTP %d: %s", code, firstRunes(string(body), 200))})
		}
	}
	return out, nil
}
