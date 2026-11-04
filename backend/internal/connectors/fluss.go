package connectors

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Apache Fluss: one primary-key table per source table in a Fluss database, created and evolved
// by Flink CDC's Fluss sink. Only Flink CDC writes it. BeziSync has no Fluss client: it checks
// the coordinators can be reached, can't read rows back, and leaves tables in place on delete.

type flussSink struct{ BaseSink }

func init() { Register(&flussSink{BaseSink{Title: "Apache Fluss"}}) }

func (*flussSink) Kind() string { return "sink" }
func (*flussSink) Type() string { return "fluss" }

type flussConfig struct {
	Servers                      []string // host:port
	Database, Auth, User, Secret string
}

func flussCfg(conn Resolved) (flussConfig, error) {
	d, _ := Describe("sink", "fluss")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return flussConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return flussConfig{}, err
	}
	c := flussConfig{Servers: strings.Split(Str(cfg, "bootstrap_servers"), ","), Database: Str(cfg, "database"), Auth: Str(cfg, "auth"),
		User: Str(cfg, "username"), Secret: Str(sec, "password")}
	fail := func(field, msg string) error {
		return problem.New(422, "fluss_config", msg).WithErrors([]map[string]any{{"loc": []string{"config", field}, "message": msg}})
	}
	switch {
	case !flinkName.MatchString(c.Database):
		return c, fail("database", "The database name may only use letters, digits and underscores.")
	case c.Auth == "sasl_plain" && (c.User == "" || c.Secret == ""):
		return c, fail("username", "SASL sign-in needs a user name and password.")
	}
	return c, nil
}

func (s *flussSink) CompileSink(Resolved, CompileContext) (map[string]string, map[string]string, error) {
	return nil, nil, problem.New(422, "engine_unsupported", "Fluss is written by Flink CDC: run this pipeline on Flink CDC.")
}

func (s *flussSink) FlinkSink(conn Resolved, _ CompileContext) (*FlinkBlock, error) {
	cfg, err := flussCfg(conn)
	if err != nil {
		return nil, err
	}
	o := FlinkOptions{{"type", "fluss"}, {"bootstrap.servers", strings.Join(cfg.Servers, ",")}}
	env := map[string]string{}
	if cfg.Auth == "sasl_plain" {
		o = append(o, FlinkOptions{
			{"properties.client.security.protocol", "sasl"},
			{"properties.client.security.sasl.mechanism", "PLAIN"},
			{"properties.client.security.sasl.username", cfg.User},
			{"properties.client.security.sasl.password", "${BZ_SINK_PASSWORD}"},
		}...)
		env["BZ_SINK_PASSWORD"] = cfg.Secret
	}
	return &FlinkBlock{Options: o, Env: env, Connector: "fluss"}, nil
}

// FlinkDestination: the routed name, lower-cased, in the connection's database.
func (s *flussSink) FlinkDestination(conn Resolved, _ CompileContext, routed string) string {
	return olapDestination(conn, routed)
}

// Destination is the same name for every caller that shows one.
func (s *flussSink) Destination(conn Resolved, _ CompileContext, routed string) string {
	return olapDestination(conn, routed)
}

func (s *flussSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	run := &checks.Run{}
	cfg, err := flussCfg(conn)
	if err != nil {
		var p *problem.Problem
		if errors.As(err, &p) {
			run.Add(checks.Check{Code: "fluss_config", Title: "Settings", Status: checks.Fail, Category: checks.CatDestination, Blocking: true, Message: p.Detail})
			return checks.Summarize(run.Checks, started), nil
		}
		return checks.Report{}, err
	}
	// One reachable coordinator is enough: the client learns the cluster from it.
	host, port, _ := net.SplitHostPort(cfg.Servers[0])
	n, _ := strconv.Atoi(port)
	NetworkCheck(ctx, run, host, n, "fluss_network", "the Fluss coordinator", 0)
	return checks.Summarize(run.Checks, started), nil
}

// CleanupSink: BeziSync can't drop Fluss tables; it says which ones stay.
func (s *flussSink) CleanupSink(_ context.Context, _ Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	var out []ArtifactResult
	for _, dest := range sortedUnique(destinations) {
		out = append(out, ArtifactResult{Kind: "table", Name: dest, Outcome: "retained",
			Detail: "BeziSync can't drop Fluss tables: drop it with Fluss's SQL client (DROP TABLE " + dest + ")."})
	}
	return out, nil
}
