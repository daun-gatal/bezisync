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

// StarRocks: one primary-key table per source table in one database, written by Flink CDC's
// StarRocks sink (Stream Load over the FE's HTTP port; tables are created and evolve with the
// source). Only Flink CDC writes it.

type starrocksSink struct{ BaseSink }

func init() { Register(&starrocksSink{BaseSink{Title: "StarRocks"}}) }

func (*starrocksSink) Kind() string { return "sink" }
func (*starrocksSink) Type() string { return "starrocks" }

var starrocks = olapSink{typ: "starrocks", title: "StarRocks",
	probe: func(table string, replicas int) string {
		return fmt.Sprintf(`CREATE TABLE %s (id INT NOT NULL) PRIMARY KEY (id) DISTRIBUTED BY HASH (id) PROPERTIES ("replication_num" = "%d")`, table, replicas)
	},
	grant: func(c olapConfig) string {
		return fmt.Sprintf("GRANT CREATE DATABASE ON CATALOG default_catalog TO USER '%s';\nGRANT ALL ON DATABASE %s TO USER '%s';\nGRANT ALL ON ALL TABLES IN DATABASE %s TO USER '%s';",
			c.User, c.Database, c.User, c.Database, c.User)
	}}

func (s *starrocksSink) CompileSink(Resolved, CompileContext) (map[string]string, map[string]string, error) {
	return nil, nil, problem.New(422, "engine_unsupported", "StarRocks is written by Flink CDC: run this pipeline on Flink CDC.")
}

func (s *starrocksSink) FlinkSink(conn Resolved, c CompileContext) (*FlinkBlock, error) {
	cfg, err := starrocks.cfg(conn)
	if err != nil {
		return nil, err
	}
	fc := c.Spec.FlinkCDC.Effective()
	o := FlinkOptions{
		{"type", "starrocks"},
		{"jdbc-url", "jdbc:mysql://" + net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.QueryPort))},
		{"load-url", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.HTTP))},
		{"username", cfg.User},
		{"password", "${BZ_SINK_PASSWORD}"},
		{"sink.label-prefix", "bz_" + c.ShortID},
		{"sink.buffer-flush.interval-ms", strconv.Itoa(fc.FreshnessSeconds * 1000)},
		{"table.create.properties.replication_num", strconv.Itoa(cfg.Replicas)},
	}
	return &FlinkBlock{Options: o, Env: map[string]string{"BZ_SINK_PASSWORD": cfg.Password}, Connector: "starrocks", Drivers: []string{"mysql"}}, nil
}

func (s *starrocksSink) FlinkDestination(conn Resolved, _ CompileContext, routed string) string {
	return olapDestination(conn, routed)
}

// Destination is the same name for every caller that shows one.
func (s *starrocksSink) Destination(conn Resolved, _ CompileContext, routed string) string {
	return olapDestination(conn, routed)
}

func (s *starrocksSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	return starrocks.check(ctx, conn)
}

func (s *starrocksSink) Tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error) {
	return starrocks.tail(ctx, conn, destination, limit, timeout)
}

func (s *starrocksSink) CleanupSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	return starrocks.cleanup(ctx, conn, destinations)
}
