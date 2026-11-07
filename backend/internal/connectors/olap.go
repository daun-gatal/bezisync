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

// Analytics databases that speak the MySQL protocol on their FE and load over HTTP (StarRocks,
// Doris): written by Flink CDC, checked and read by BeziSync over the MySQL protocol. Neither
// loads over TLS upstream; the forms say to keep them in a trusted network.

type olapConfig struct {
	Host            string
	QueryPort, HTTP int
	Database, User  string
	Password        string
	Replicas        int
	BENodes         string // Doris: BE HTTP addresses to load into directly
}

// olapSink is what differs between them.
type olapSink struct {
	typ, title string
	// probe creates a one-column test table (database.table, replicas).
	probe func(table string, replicas int) string
	// grant is the SQL that gives a user what pipelines need.
	grant func(c olapConfig) string
}

func (o olapSink) cfg(conn Resolved) (olapConfig, error) {
	d, _ := Describe("sink", o.typ)
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return olapConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return olapConfig{}, err
	}
	c := olapConfig{Host: Str(cfg, "host"), QueryPort: Int(cfg, "query_port"), HTTP: Int(cfg, "http_port"), Database: Str(cfg, "database"),
		User: Str(cfg, "user"), Replicas: Int(cfg, "replicas"), Password: Str(sec, "password"), BENodes: Str(cfg, "be_nodes")}
	if !flinkName.MatchString(c.Database) {
		return c, problem.New(422, o.typ+"_config", "The database name may only use letters, digits and underscores.").
			WithErrors([]map[string]any{{"loc": []string{"config", "database"}, "message": "Use letters, digits and '_'."}})
	}
	return c, nil
}

func (c olapConfig) open(ctx context.Context, timeout time.Duration) (*sql.Conn, func(), error) {
	mc := mysql.NewConfig()
	mc.User, mc.Passwd, mc.Net = c.User, c.Password, "tcp"
	mc.Addr = net.JoinHostPort(c.Host, strconv.Itoa(c.QueryPort))
	mc.Timeout, mc.ReadTimeout, mc.WriteTimeout = timeout, 30*time.Second, 30*time.Second
	mc.AllowNativePasswords = true
	mc.InterpolateParams = true
	mc.TLSConfig = "false"
	mc.DialFunc = DialContext // the SSRF guard (ADR 0004)
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

func olapQuote(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

// olapTable quotes "database.table".
func olapTable(destination string) string {
	db, table, _ := strings.Cut(destination, ".")
	return olapQuote(db) + "." + olapQuote(table)
}

// olapDestination: the routed name, lower-cased, in the connection's database.
func olapDestination(conn Resolved, routed string) string {
	return Str(conn.Config, "database") + "." + strings.ToLower(strings.ReplaceAll(routed, ".", "_"))
}

func (o olapSink) check(ctx context.Context, conn Resolved) (checks.Report, error) {
	started := time.Now()
	run := &checks.Run{}
	cfg, err := o.cfg(conn)
	if err != nil {
		var p *problem.Problem
		if errors.As(err, &p) {
			run.Add(checks.Check{Code: o.typ + "_config", Title: "Settings", Status: checks.Fail, Category: checks.CatDestination, Blocking: true, Message: p.Detail})
			return checks.Summarize(run.Checks, started), nil
		}
		return checks.Report{}, err
	}
	if !NetworkCheck(ctx, run, cfg.Host, cfg.QueryPort, o.typ+"_network", "the "+o.title+" FE (MySQL protocol)", 0) {
		return checks.Summarize(run.Checks, started), nil
	}
	if !NetworkCheck(ctx, run, cfg.Host, cfg.HTTP, o.typ+"_http", "the "+o.title+" FE (HTTP, Stream Load)", 0) {
		return checks.Summarize(run.Checks, started), nil
	}
	c, done, err := cfg.open(ctx, 10*time.Second)
	if err != nil {
		run.Add(checks.Check{Code: o.typ + "_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: o.title + " refused the sign-in.", Technical: firstRunes(err.Error(), 300)})
		return checks.Summarize(run.Checks, started), nil
	}
	defer done()
	var version string
	if err := c.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		version = "(version unknown)"
	}
	run.Passed(o.typ+"_login", "Sign in", checks.CatAuth, "Signed in ("+version+").", "")
	// The sink creates the database and its tables: prove the rights on a scratch table.
	fail := func(msg string, err error) checks.Report {
		run.Add(checks.Check{Code: o.typ + "_create_table", Title: "Create tables", Status: checks.Fail, Category: checks.CatPermissions, Blocking: true,
			Message: msg, Technical: firstRunes(err.Error(), 300), Fix: &checks.Fix{Kind: "sql", Who: o.title + " admin", Code: o.grant(cfg)}})
		return checks.Summarize(run.Checks, started)
	}
	if _, err := c.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+olapQuote(cfg.Database)); err != nil {
		return fail("BeziSync could not create the database "+cfg.Database+".", err), nil
	}
	probe := fmt.Sprintf("%s.bezisync_check_%d", cfg.Database, time.Now().UnixNano()%1e9)
	if _, err := c.ExecContext(ctx, o.probe(olapTable(probe), cfg.Replicas)); err != nil {
		return fail("BeziSync could not create a table in "+cfg.Database+": the pipeline creates its tables.", err), nil
	}
	_, _ = c.ExecContext(context.WithoutCancel(ctx), "DROP TABLE "+olapTable(probe)+" FORCE")
	run.Passed(o.typ+"_create_table", "Create tables", checks.CatPermissions, "Created and dropped a test table in "+cfg.Database+".", "")
	return checks.Summarize(run.Checks, started), nil
}

func (o olapSink) tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error) {
	cfg, err := o.cfg(conn)
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
	rows, err := c.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT %d", olapTable(destination), limit))
	if err != nil {
		if olapMissing(err) {
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

func olapMissing(err error) bool {
	t := strings.ToLower(err.Error())
	return strings.Contains(t, "unknown table") || strings.Contains(t, "unknown database") || strings.Contains(t, "does not exist") ||
		strings.Contains(t, "not found") || strings.Contains(t, "not exist")
}

func (o olapSink) cleanup(ctx context.Context, conn Resolved, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := o.cfg(conn)
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
		if _, err := c.ExecContext(ctx, "DROP TABLE "+olapTable(dest)+" FORCE"); err != nil {
			outcome := "failed"
			if olapMissing(err) {
				outcome = "not_found"
			}
			results = append(results, ArtifactResult{Kind: "table", Name: dest, Outcome: outcome, Detail: firstRunes(err.Error(), 200)})
			continue
		}
		results = append(results, ArtifactResult{Kind: "table", Name: dest, Outcome: "removed"})
	}
	return results, nil
}
