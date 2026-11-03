package connectors

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
)

// HTTP sink: POST each change event as JSON to an endpoint.

type httpSink struct{ BaseSink }

func init() { Register(&httpSink{BaseSink{Title: "HTTP"}}) }

func (*httpSink) Kind() string { return "sink" }
func (*httpSink) Type() string { return "http" }

func httpCfg(conn Resolved) (map[string]any, error) {
	d, _ := Describe("sink", "http")
	return ValidateConfig(d.ConfigSchema, conn.Config)
}

func (s *httpSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	cfg, err := httpCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	raw := Str(cfg, "url")
	u, err := url.Parse(raw)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	port := 80
	if u.Scheme == "https" {
		port = 443
	}
	if p := u.Port(); p != "" {
		port, _ = strconv.Atoi(p)
	}
	host := strings.ToLower(u.Hostname())
	if !NetworkCheck(ctx, run, host, port, "http_network", "the endpoint", 0) {
		return checks.Summarize(run.Checks, started), nil
	}
	// A harmless request: the endpoint must answer HTTP (any status) within the timeout.
	timeout := min(time.Duration(Int(cfg, "timeout_ms"))*time.Millisecond, 10*time.Second)
	client := &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequestWithContext(ctx, http.MethodOptions, raw, nil)
	if err != nil {
		return checks.Report{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		run.Add(checks.Check{Code: "http_status", Title: "Endpoint answers", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
			Message: "The port is open but no HTTP response came back.", Technical: firstRunes(err.Error(), 300)})
		return checks.Summarize(run.Checks, started), nil
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 500 {
		run.Add(checks.Check{Code: "http_status", Title: "Endpoint answers", Status: checks.Warn, Category: checks.CatDestination,
			Message: fmt.Sprintf("The endpoint answered %d; deliveries may fail.", resp.StatusCode)})
	} else {
		run.Passed("http_status", "Endpoint answers", checks.CatDestination, fmt.Sprintf("Answered HTTP %d.", resp.StatusCode), "")
	}
	return checks.Summarize(run.Checks, started), nil
}

func (s *httpSink) CompileSink(conn Resolved, _ CompileContext) (map[string]string, map[string]string, error) {
	cfg, err := httpCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	return map[string]string{
		"debezium.sink.type":                   "http",
		"debezium.sink.http.url":               Str(cfg, "url"),
		"debezium.sink.http.timeout.ms":        strconv.Itoa(Int(cfg, "timeout_ms")),
		"debezium.sink.http.retries":           strconv.Itoa(Int(cfg, "retries")),
		"debezium.sink.http.retry.interval.ms": strconv.Itoa(Int(cfg, "retry_interval_ms")),
	}, map[string]string{}, nil
}
