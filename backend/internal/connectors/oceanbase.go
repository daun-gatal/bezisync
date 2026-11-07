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

// OceanBase (MySQL tenants): one table per source table in a database of the tenant, created and
// written by Flink CDC's OceanBase sink over JDBC, with OceanBase Connector/J (LGPL, in the image)
// so no GPL MySQL driver is needed. BeziSync checks, reads and drops over the MySQL protocol. The
// session time zone is UTC (the tenant default is +08:00, which would shift timestamps), and only
// adding and renaming columns are applied as schema changes upstream.

type oceanbaseSink struct{ BaseSink }

func init() { Register(&oceanbaseSink{BaseSink{Title: "OceanBase"}}) }

func (*oceanbaseSink) Kind() string { return "sink" }
func (*oceanbaseSink) Type() string { return "oceanbase" }

var oceanbase = olapSink{typ: "oceanbase", title: "OceanBase", noHTTP: true, mysqlDrop: true,
	probe: func(table string, _ int) string {
		return fmt.Sprintf("CREATE TABLE %s (id INT NOT NULL PRIMARY KEY)", table)
	},
	grant: func(c olapConfig) string {
		return fmt.Sprintf("GRANT ALL PRIVILEGES ON %s.* TO '%s';\nGRANT CREATE ON *.* TO '%s';", olapQuote(c.Database), userOnly(c.User), userOnly(c.User))
	}}

// userOnly is the user name without OceanBase's @tenant#cluster suffix (as GRANT names it).
func userOnly(u string) string {
	for i, r := range u {
		if r == '@' || r == '#' {
			return u[:i]
		}
	}
	return u
}

func (s *oceanbaseSink) CompileSink(Resolved, CompileContext) (map[string]string, map[string]string, error) {
	return nil, nil, problem.New(422, "engine_unsupported", "OceanBase is written by Flink CDC: run this pipeline on Flink CDC.")
}

func (s *oceanbaseSink) FlinkSink(conn Resolved, _ CompileContext) (*FlinkBlock, error) {
	cfg, err := oceanbase.cfg(conn)
	if err != nil {
		return nil, err
	}
	// The URL is built from checked parts only: host, port, database and BeziSync's own
	// parameters (no user-given JDBC options such as allowLoadLocalInfile).
	url := "jdbc:oceanbase://" + net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.QueryPort)) + "/" + cfg.Database +
		"?characterEncoding=UTF-8&serverTimezone=UTC"
	o := FlinkOptions{
		{"type", "oceanbase"},
		{"url", url},
		{"username", cfg.User},
		{"password", "${BZ_SINK_PASSWORD}"},
		{"driver-class-name", "com.oceanbase.jdbc.Driver"},
	}
	return &FlinkBlock{Options: o, Env: map[string]string{"BZ_SINK_PASSWORD": cfg.Password}, Connector: "oceanbase", Extra: []string{"oceanbase-client"}}, nil
}

func (s *oceanbaseSink) FlinkDestination(conn Resolved, _ CompileContext, routed string) string {
	return databaseDestination(conn, routed)
}

// Destination is the same name for every caller that shows one.
func (s *oceanbaseSink) Destination(conn Resolved, _ CompileContext, routed string) string {
	return databaseDestination(conn, routed)
}

func (s *oceanbaseSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	return oceanbase.check(ctx, conn)
}

func (s *oceanbaseSink) Tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error) {
	return oceanbase.tail(ctx, conn, destination, limit, timeout)
}

func (s *oceanbaseSink) CleanupSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	return oceanbase.cleanup(ctx, conn, destinations)
}
