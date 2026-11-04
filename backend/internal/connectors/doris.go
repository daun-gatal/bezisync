package connectors

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Apache Doris: one unique-key table per source table in one database, written by Flink CDC's
// Doris sink (Stream Load through the FE, or straight to the BEs listed; tables are created and
// evolve with the source). Only Flink CDC writes it.

type dorisSink struct{ BaseSink }

func init() { Register(&dorisSink{BaseSink{Title: "Apache Doris"}}) }

func (*dorisSink) Kind() string { return "sink" }
func (*dorisSink) Type() string { return "doris" }

var doris = olapSink{typ: "doris", title: "Doris",
	probe: func(table string, replicas int) string {
		return fmt.Sprintf(`CREATE TABLE %s (id INT NOT NULL) UNIQUE KEY (id) DISTRIBUTED BY HASH (id) BUCKETS 1 PROPERTIES ("replication_num" = "%d")`, table, replicas)
	},
	grant: func(c olapConfig) string {
		return fmt.Sprintf("GRANT SELECT_PRIV, LOAD_PRIV, ALTER_PRIV, CREATE_PRIV, DROP_PRIV ON internal.%s.* TO '%s'@'%%';", c.Database, c.User)
	}}

func (s *dorisSink) CompileSink(Resolved, CompileContext) (map[string]string, map[string]string, error) {
	return nil, nil, problem.New(422, "engine_unsupported", "Doris is written by Flink CDC: run this pipeline on Flink CDC.")
}

func (s *dorisSink) FlinkSink(conn Resolved, c CompileContext) (*FlinkBlock, error) {
	cfg, err := doris.cfg(conn)
	if err != nil {
		return nil, err
	}
	fc := c.Spec.FlinkCDC.Effective()
	o := FlinkOptions{
		{"type", "doris"},
		{"fenodes", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.HTTP))},
	}
	if cfg.BENodes != "" {
		o.Set("benodes", cfg.BENodes)
	}
	o = append(o, FlinkOptions{
		{"jdbc-url", "jdbc:mysql://" + net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.QueryPort))},
		{"username", cfg.User},
		{"password", "${BZ_SINK_PASSWORD}"},
		{"sink.buffer-flush.interval", strconv.Itoa(fc.FreshnessSeconds) + "s"},
		{"table.create.properties.replication_num", strconv.Itoa(cfg.Replicas)},
	}...)
	return &FlinkBlock{Options: o, Env: map[string]string{"BZ_SINK_PASSWORD": cfg.Password}, Connector: "doris", NoDefaults: true}, nil
}

func (s *dorisSink) FlinkDestination(conn Resolved, _ CompileContext, routed string) string {
	return olapDestination(conn, routed)
}

// Destination is the same name for every caller that shows one.
func (s *dorisSink) Destination(conn Resolved, _ CompileContext, routed string) string {
	return olapDestination(conn, routed)
}

func (s *dorisSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	return doris.check(ctx, conn)
}

func (s *dorisSink) Tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error) {
	return doris.tail(ctx, conn, destination, limit, timeout)
}

func (s *dorisSink) CleanupSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	return doris.cleanup(ctx, conn, destinations)
}
