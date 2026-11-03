package connectors

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/ssrf"
)

// Apache Pulsar sink: one persistent topic per table in a tenant/namespace, keyed by the row key.
// The connector speaks Pulsar's binary protocol (service URL); BeziSync uses the admin REST API
// (admin URL) to check the namespace, create and delete topics, and read recent messages for the
// Events tab (examinemessage: no subscription, consumers are untouched).

type pulsarSink struct{ BaseSink }

func init() { Register(&pulsarSink{BaseSink{Title: "Apache Pulsar"}}) }

func (*pulsarSink) Kind() string { return "sink" }
func (*pulsarSink) Type() string { return "pulsar" }

type pulsarConfig struct {
	ServiceURL, AdminURL, Tenant, Namespace, Token, CACert string
	CreateTopics                                           bool
}

func pulsarCfg(conn Resolved) (pulsarConfig, error) {
	d, _ := Describe("sink", "pulsar")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return pulsarConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return pulsarConfig{}, err
	}
	c := pulsarConfig{ServiceURL: Str(cfg, "service_url"), AdminURL: strings.TrimRight(Str(cfg, "admin_url"), "/"), Tenant: Str(cfg, "tenant"),
		Namespace: Str(cfg, "namespace"), CreateTopics: Bool(cfg, "create_topics"), Token: Str(sec, "token"), CACert: Str(sec, "ca_cert")}
	if strings.ContainsAny(c.ServiceURL, "${}\r\n ") {
		msg := "Enter a pulsar:// or pulsar+ssl:// URL."
		return c, problem.New(422, "pulsar_config", msg).WithErrors([]map[string]any{{"loc": []string{"config", "service_url"}, "message": msg}})
	}
	return c, nil
}

func (c pulsarConfig) caFile() string { return SecretDir + "/pulsar-ca.pem" }

func (s *pulsarSink) SecretFiles(conn Resolved) (map[string]string, error) {
	cfg, err := pulsarCfg(conn)
	if err != nil {
		return nil, err
	}
	if cfg.CACert == "" {
		return map[string]string{}, nil
	}
	return map[string]string{"pulsar-ca.pem": cfg.CACert}, nil
}

func (s *pulsarSink) CompileSink(conn Resolved, _ CompileContext) (map[string]string, map[string]string, error) {
	cfg, err := pulsarCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	const p = "debezium.sink.pulsar."
	props := map[string]string{
		"debezium.sink.type":    "pulsar",
		p + "client.serviceUrl": cfg.ServiceURL,
		p + "tenant":            cfg.Tenant,
		p + "namespace":         cfg.Namespace,
		// A failed send is only logged by the sink: a bounded send and flush make it fail the batch
		// instead of hanging, and a full queue waits rather than dropping.
		p + "producer.sendTimeoutMs":    "30000",
		p + "producer.blockIfQueueFull": "true",
		p + "timeout":                   "60000",
	}
	env := map[string]string{}
	if cfg.Token != "" {
		props[p+"client.authPluginClassName"] = "org.apache.pulsar.client.impl.auth.AuthenticationToken"
		props[p+"client.authParams"] = "token:${BZ_PULSAR_TOKEN}"
		env["BZ_PULSAR_TOKEN"] = cfg.Token
	}
	if cfg.CACert != "" {
		props[p+"client.tlsTrustCertsFilePath"] = cfg.caFile()
	}
	return props, env, nil
}

// admin calls Pulsar's admin REST API.
func (c pulsarConfig) admin(ctx context.Context, method, path string) (int, []byte, http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.AdminURL+path, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if method == http.MethodPut {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := pulsarHTTP(c).Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	return res.StatusCode, body, res.Header, err
}

func pulsarHTTP(c pulsarConfig) *http.Client {
	client := ssrf.HTTPClient(15 * time.Second)
	if c.CACert != "" {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM([]byte(c.CACert)) {
			client.Transport.(*http.Transport).TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
		}
	}
	return client
}

func (c pulsarConfig) topicPath(topic string) string {
	return fmt.Sprintf("/admin/v2/persistent/%s/%s/%s", url.PathEscape(c.Tenant), url.PathEscape(c.Namespace), url.PathEscape(topic))
}

func (s *pulsarSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	run := &checks.Run{}
	cfg, err := pulsarCfg(conn)
	if err != nil {
		var p *problem.Problem
		if errors.As(err, &p) {
			run.Add(checks.Check{Code: "pulsar_config", Title: "Settings", Status: checks.Fail, Category: checks.CatDestination, Blocking: true, Message: p.Detail})
			return checks.Summarize(run.Checks, started), nil
		}
		return checks.Report{}, err
	}
	u, perr := url.Parse(cfg.ServiceURL)
	if perr != nil || u.Hostname() == "" {
		// A malformed URL is a failed check, not an error of the check itself.
		technical := ""
		if perr != nil {
			technical = perr.Error()
		}
		run.Add(checks.Check{Code: "pulsar_config", Title: "Settings", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
			Message: "The service URL is not a pulsar:// URL.", Technical: technical})
		return checks.Summarize(run.Checks, started), nil
	}
	port := 6650
	if u.Scheme == "pulsar+ssl" {
		port = 6651
	}
	if u.Port() != "" {
		port, _ = strconv.Atoi(u.Port())
	}
	if !NetworkCheck(ctx, run, u.Hostname(), port, "pulsar_network", "the Pulsar broker", 0) {
		return checks.Summarize(run.Checks, started), nil
	}
	code, body, _, err := cfg.admin(ctx, http.MethodGet, fmt.Sprintf("/admin/v2/namespaces/%s/%s", url.PathEscape(cfg.Tenant), url.PathEscape(cfg.Namespace)))
	switch {
	case err != nil:
		run.Add(checks.Check{Code: "pulsar_admin", Title: "Admin API", Status: checks.Fail, Category: checks.CatNetwork, Blocking: true,
			Message: "Could not reach the admin URL.", Technical: firstRunes(err.Error(), 300)})
	case code == http.StatusUnauthorized || code == http.StatusForbidden:
		run.Add(checks.Check{Code: "pulsar_auth", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: "Pulsar refused the token.", Technical: firstRunes(string(body), 300),
			Fix: &checks.Fix{Kind: "text", Who: "Pulsar admin", Code: fmt.Sprintf("bin/pulsar-admin namespaces grant-permission %s/%s --role <role> --actions produce,consume", cfg.Tenant, cfg.Namespace)}})
	case code == http.StatusNotFound:
		run.Add(checks.Check{Code: "pulsar_namespace", Title: "Namespace", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
			Message: fmt.Sprintf("Namespace %s/%s does not exist.", cfg.Tenant, cfg.Namespace),
			Fix:     &checks.Fix{Kind: "text", Who: "Pulsar admin", Code: fmt.Sprintf("bin/pulsar-admin namespaces create %s/%s", cfg.Tenant, cfg.Namespace)}})
	case code >= 300:
		run.Add(checks.Check{Code: "pulsar_admin", Title: "Admin API", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
			Message: fmt.Sprintf("The admin API answered %d.", code), Technical: firstRunes(string(body), 300)})
	default:
		run.Passed("pulsar_namespace", "Namespace", checks.CatDestination, fmt.Sprintf("Namespace %s/%s exists.", cfg.Tenant, cfg.Namespace), "")
	}
	return checks.Summarize(run.Checks, started), nil
}

func (s *pulsarSink) PrepareSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := pulsarCfg(conn)
	if err != nil {
		return nil, err
	}
	var results []ArtifactResult
	for _, topic := range sortedUnique(destinations) {
		code, _, _, err := cfg.admin(ctx, http.MethodGet, cfg.topicPath(topic)+"/stats")
		if err == nil && code == http.StatusOK {
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "exists"})
			continue
		}
		if !cfg.CreateTopics {
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "failed", Detail: "Topic does not exist."})
			continue
		}
		code, body, _, err := cfg.admin(ctx, http.MethodPut, cfg.topicPath(topic))
		switch {
		case err != nil:
			return nil, err
		case code == http.StatusConflict:
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "exists"})
		case code >= 300:
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "failed", Detail: firstRunes(fmt.Sprintf("%d %s", code, body), 200)})
		default:
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "created"})
		}
	}
	return results, nil
}

func (s *pulsarSink) CleanupSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := pulsarCfg(conn)
	if err != nil {
		return nil, err
	}
	var results []ArtifactResult
	for _, topic := range sortedUnique(destinations) {
		code, body, _, err := cfg.admin(ctx, http.MethodDelete, cfg.topicPath(topic)+"?force=true")
		switch {
		case err != nil:
			return nil, err
		case code == http.StatusNotFound:
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "not_found"})
		case code >= 300:
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "failed", Detail: firstRunes(fmt.Sprintf("%d %s", code, body), 200)})
		default:
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "removed"})
		}
	}
	return results, nil
}

// Tail reads the newest messages with examinemessage (1 = the latest), oldest first.
func (s *pulsarSink) Tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error) {
	cfg, err := pulsarCfg(conn)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, max(timeout, 10*time.Second))
	defer cancel()
	var out []TailRecord
	for position := 1; position <= limit; position++ {
		code, body, header, err := cfg.admin(ctx, http.MethodGet, fmt.Sprintf("%s/examinemessage?initialPosition=latest&messagePosition=%d", cfg.topicPath(destination), position))
		if err != nil {
			if len(out) > 0 {
				break
			}
			return nil, err
		}
		// Past the oldest message Pulsar answers an error ("Incorrect position…", or 404 / 412 for an
		// empty topic): that is the end of what there is to read.
		if code == http.StatusNotFound || code == http.StatusPreconditionFailed || code == http.StatusConflict ||
			(code >= 300 && (len(out) > 0 || strings.Contains(strings.ToLower(string(body)), "incorrect"))) {
			break
		}
		if code >= 300 {
			return nil, fmt.Errorf("pulsar admin answered %d: %s", code, firstRunes(string(body), 200))
		}
		var value any = string(body)
		if v, err := decodeJSON(body); err == nil {
			value = v
		}
		var ts *string
		if ms, err := strconv.ParseInt(header.Get("X-Pulsar-Publish-Time"), 10, 64); err == nil {
			t := pytime.ISOFormat(time.UnixMilli(ms))
			ts = &t
		} else if pt := header.Get("X-Pulsar-Publish-Time"); pt != "" {
			ts = &pt
		}
		var key any
		if k := header.Get("X-Pulsar-Partition-Key"); k != "" {
			key = k
		}
		out = append([]TailRecord{{Destination: destination, Timestamp: ts, Key: key, Value: value}}, out...)
	}
	if out == nil {
		out = []TailRecord{}
	}
	return out, nil
}
