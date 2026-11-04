package connectors

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// StarRocks: one primary-key table per source table in one database, written by Flink CDC's
// StarRocks sink (Stream Load over the FE's HTTP port; tables are created and evolve with the
// source). Only Flink CDC writes it. BeziSync checks and reads it over the FE's MySQL protocol.
// Stream Load has no TLS: the descriptor and the guide say to keep StarRocks inside a trusted
// network.

type starrocksSink struct{ BaseSink }

func init() { Register(&starrocksSink{BaseSink{Title: "StarRocks"}}) }

func (*starrocksSink) Kind() string { return "sink" }
func (*starrocksSink) Type() string { return "starrocks" }

type starrocksConfig struct {
	Host            string
	QueryPort, HTTP int
	Database, User  string
	Replicas        int
	Password        string
}

func starrocksCfg(conn Resolved) (starrocksConfig, error) {
	d, _ := Describe("sink", "starrocks")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return starrocksConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return starrocksConfig{}, err
	}
	c := starrocksConfig{Host: Str(cfg, "host"), QueryPort: Int(cfg, "query_port"), HTTP: Int(cfg, "http_port"), Database: Str(cfg, "database"),
		User: Str(cfg, "user"), Replicas: Int(cfg, "replicas"), Password: Str(sec, "password")}
	if !flinkName.MatchString(c.Database) {
		return c, problem.New(422, "starrocks_config", "The database name may only use letters, digits and underscores.").
			WithErrors([]map[string]any{{"loc": []string{"config", "database"}, "message": "Use letters, digits and '_'."}})
	}
	return c, nil
}

func (c starrocksConfig) open(ctx context.Context, timeout time.Duration) (*sql.Conn, func(), error) {
	mc := mysql.NewConfig()
	mc.User, mc.Passwd, mc.Net = c.User, c.Password, "tcp"
	mc.Addr = net.JoinHostPort(c.Host, strconv.Itoa(c.QueryPort))
	mc.Timeout, mc.ReadTimeout, mc.WriteTimeout = timeout, 30*time.Second, 30*time.Second
	mc.AllowNativePasswords = true
	mc.InterpolateParams = true
	mc.TLSConfig = "false"
	connector, err := mysql.NewConnector(mc)
	if err != nil {
		return nil, nil, err
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return conn, func() { _ = conn.Close(); _ = db.Close() }, nil
}

func srQuote(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

// srTable quotes "database.table".
func srTable(destination string) string {
	db, table, _ := strings.Cut(destination, ".")
	return srQuote(db) + "." + srQuote(table)
}

func (s *starrocksSink) CompileSink(Resolved, CompileContext) (map[string]string, map[string]string, error) {
	return nil, nil, problem.New(422, "engine_unsupported", "StarRocks is written by Flink CDC: run this pipeline on Flink CDC.")
}

func (s *starrocksSink) FlinkSink(conn Resolved, c CompileContext) (*FlinkBlock, error) {
	cfg, err := starrocksCfg(conn)
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

// FlinkDestination: the routed name, lower-cased, in the connection's database.
func (s *starrocksSink) FlinkDestination(conn Resolved, _ CompileContext, routed string) string {
	return Str(conn.Config, "database") + "." + strings.ToLower(strings.ReplaceAll(routed, ".", "_"))
}

// Destination is the same name for every caller that shows one.
func (s *starrocksSink) Destination(conn Resolved, c CompileContext, routed string) string {
	return s.FlinkDestination(conn, c, routed)
}

func (s *starrocksSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	run := &checks.Run{}
	cfg, err := starrocksCfg(conn)
	if err != nil {
		var p *problem.Problem
		if errors.As(err, &p) {
			run.Add(checks.Check{Code: "starrocks_config", Title: "Settings", Status: checks.Fail, Category: checks.CatDestination, Blocking: true, Message: p.Detail})
			return checks.Summarize(run.Checks, started), nil
		}
		return checks.Report{}, err
	}
	if !NetworkCheck(ctx, run, cfg.Host, cfg.QueryPort, "starrocks_network", "the StarRocks FE (MySQL protocol)", 0) {
		return checks.Summarize(run.Checks, started), nil
	}
	if !NetworkCheck(ctx, run, cfg.Host, cfg.HTTP, "starrocks_http", "the StarRocks FE (HTTP, Stream Load)", 0) {
		return checks.Summarize(run.Checks, started), nil
	}
	c, done, err := cfg.open(ctx, 10*time.Second)
	if err != nil {
		run.Add(checks.Check{Code: "starrocks_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: "StarRocks refused the sign-in.", Technical: firstRunes(err.Error(), 300)})
		return checks.Summarize(run.Checks, started), nil
	}
	defer done()
	var version string
	if err := c.QueryRowContext(ctx, "SELECT current_version()").Scan(&version); err != nil {
		version = "unknown version"
	}
	run.Passed("starrocks_login", "Sign in", checks.CatAuth, "Signed in to StarRocks "+version+".", "")
	// The sink creates the database and its tables: prove the rights on a scratch table.
	if _, err := c.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+srQuote(cfg.Database)); err != nil {
		run.Add(checks.Check{Code: "starrocks_create_table", Title: "Create tables", Status: checks.Fail, Category: checks.CatPermissions, Blocking: true,
			Message: "BeziSync could not create the database " + cfg.Database + ".", Technical: firstRunes(err.Error(), 300),
			Fix: &checks.Fix{Kind: "sql", Who: "StarRocks admin", Code: starrocksGrant(cfg)}})
		return checks.Summarize(run.Checks, started), nil
	}
	probe := fmt.Sprintf("%s.bezisync_check_%d", cfg.Database, time.Now().UnixNano()%1e9)
	if _, err := c.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT NOT NULL) PRIMARY KEY (id) DISTRIBUTED BY HASH (id) PROPERTIES (\"replication_num\" = \"%d\")",
		srTable(probe), cfg.Replicas)); err != nil {
		run.Add(checks.Check{Code: "starrocks_create_table", Title: "Create tables", Status: checks.Fail, Category: checks.CatPermissions, Blocking: true,
			Message: "BeziSync could not create a table in " + cfg.Database + ": the pipeline creates its tables.", Technical: firstRunes(err.Error(), 300),
			Fix: &checks.Fix{Kind: "sql", Who: "StarRocks admin", Code: starrocksGrant(cfg)}})
		return checks.Summarize(run.Checks, started), nil
	}
	_, _ = c.ExecContext(context.WithoutCancel(ctx), "DROP TABLE "+srTable(probe)+" FORCE")
	run.Passed("starrocks_create_table", "Create tables", checks.CatPermissions, "Created and dropped a test table in "+cfg.Database+".", "")
	return checks.Summarize(run.Checks, started), nil
}

func starrocksGrant(c starrocksConfig) string {
	return fmt.Sprintf("GRANT CREATE DATABASE ON CATALOG default_catalog TO USER '%s';\nGRANT ALL ON DATABASE %s TO USER '%s';\nGRANT ALL ON ALL TABLES IN DATABASE %s TO USER '%s';",
		c.User, c.Database, c.User, c.Database, c.User)
}

func (s *starrocksSink) Tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error) {
	cfg, err := starrocksCfg(conn)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, max(timeout, 15*time.Second))
	defer cancel()
	c, done, err := cfg.open(ctx, 10*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	rows, err := c.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT %d", srTable(destination), limit))
	if err != nil {
		if srMissing(err) {
			return []TailRecord{}, nil
		}
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := []TailRecord{}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			switch v := vals[i].(type) {
			case []byte:
				row[col] = string(v)
			case time.Time:
				row[col] = v.UTC().Format(time.RFC3339Nano)
			default:
				row[col] = v
			}
		}
		out = append(out, TailRecord{Destination: destination, Value: row})
	}
	return out, rows.Err()
}

func srMissing(err error) bool {
	t := strings.ToLower(err.Error())
	return strings.Contains(t, "unknown table") || strings.Contains(t, "unknown database") || strings.Contains(t, "does not exist") ||
		strings.Contains(t, "not found")
}

func (s *starrocksSink) CleanupSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := starrocksCfg(conn)
	if err != nil {
		return nil, err
	}
	c, done, err := cfg.open(ctx, 10*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	var results []ArtifactResult
	for _, dest := range sortedUnique(destinations) {
		if _, err := c.ExecContext(ctx, "DROP TABLE "+srTable(dest)+" FORCE"); err != nil {
			outcome := "failed"
			if srMissing(err) {
				outcome = "not_found"
			}
			results = append(results, ArtifactResult{Kind: "table", Name: dest, Outcome: outcome, Detail: firstRunes(err.Error(), 200)})
			continue
		}
		results = append(results, ArtifactResult{Kind: "table", Name: dest, Outcome: "removed"})
	}
	return results, nil
}
