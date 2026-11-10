package connectors

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	mssql "github.com/microsoft/go-mssqldb"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// JDBC sink: one table per source table in PostgreSQL, MySQL, MariaDB or SQL Server, written by
// Debezium's JDBC sink connector inside Debezium Server (it reads the change records directly,
// so the workload uses the "connect" format). Rows are upserted (or inserted) by the record key,
// deletes delete the row, and tables are created and get new columns as the source changes.
// The official images ship the drivers.

type jdbcSink struct{ BaseSink }

func init() { Register(&jdbcSink{BaseSink{Title: "Database (JDBC)"}}) }

func (*jdbcSink) Kind() string { return "sink" }
func (*jdbcSink) Type() string { return "jdbc" }

type jdbcConfig struct {
	DBType, Host, Database, Schema, User, TLS, Password string
	Port                                                int
}

var jdbcIdentRe = regexp.MustCompile(`^[A-Za-z0-9_$-]+$`)

func jdbcCfg(conn Resolved) (jdbcConfig, error) {
	d, _ := Describe("sink", "jdbc")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return jdbcConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return jdbcConfig{}, err
	}
	c := jdbcConfig{DBType: Str(cfg, "database_type"), Host: Str(cfg, "host"), Port: Int(cfg, "port"), Database: Str(cfg, "database"),
		Schema: Str(cfg, "schema"), User: Str(cfg, "user"), TLS: Str(cfg, "tls"), Password: Str(sec, "password")}
	if c.DBType != "postgresql" {
		// MySQL and MariaDB have no schemas; SQL Server creates tables in the login's default schema.
		c.Schema = ""
	}
	fail := func(field, msg string) error {
		return problem.New(422, "jdbc_config", msg).WithErrors([]map[string]any{{"loc": []string{"config", field}, "message": msg}})
	}
	// They are placed in a JDBC URL and in SQL: letters, digits, '_', '-' and '$' only.
	if !jdbcIdentRe.MatchString(c.Database) {
		return c, fail("database", "Use letters, digits, '_', '-' and '$' in the database name.")
	}
	if c.Schema != "" && !jdbcIdentRe.MatchString(c.Schema) {
		return c, fail("schema", "Use letters, digits, '_', '-' and '$' in the schema name.")
	}
	if strings.ContainsAny(c.Host, "/?;:@ ") && !strings.Contains(c.Host, "]") {
		return c, fail("host", "Enter a host name or address only.")
	}
	return c, nil
}

// url is the JDBC URL for Debezium's connector.
func (c jdbcConfig) url() string {
	hostPort := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	switch c.DBType {
	case "mysql":
		mode := map[string]string{"disable": "DISABLED", "require": "REQUIRED", "verify": "VERIFY_IDENTITY"}[c.TLS]
		return fmt.Sprintf("jdbc:mysql://%s/%s?sslMode=%s", hostPort, c.Database, mode)
	case "mariadb":
		mode := map[string]string{"disable": "disable", "require": "trust", "verify": "verify-full"}[c.TLS]
		return fmt.Sprintf("jdbc:mariadb://%s/%s?sslMode=%s", hostPort, c.Database, mode)
	case "sqlserver":
		encrypt, trust := c.TLS != "disable", c.TLS != "verify"
		return fmt.Sprintf("jdbc:sqlserver://%s;databaseName=%s;encrypt=%t;trustServerCertificate=%t", hostPort, c.Database, encrypt, trust)
	}
	mode := map[string]string{"disable": "disable", "require": "require", "verify": "verify-full"}[c.TLS]
	// Tables go to the schema through the URL: the table name format cannot carry it (Quarkus
	// would expand Debezium's ${topic} placeholder).
	return fmt.Sprintf("jdbc:postgresql://%s/%s?sslmode=%s&currentSchema=%s", hostPort, c.Database, mode, c.Schema)
}

func (s *jdbcSink) CompileSink(conn Resolved, c CompileContext) (map[string]string, map[string]string, error) {
	cfg, err := jdbcCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	opts, err := SinkOptions("jdbc", c.Spec)
	if err != nil {
		return nil, nil, err
	}
	const p = "debezium.sink.jdbc."
	upsert := Str(opts, "write_mode") == "upsert"
	// Insert-only tables have no key (a re-delivered change after a restart would break one),
	// so deletes cannot find their row.
	deletes := upsert && Str(opts, "deletes") == "delete"
	props := map[string]string{
		"debezium.sink.type":           "jdbc",
		p + "connection.url":           cfg.url(),
		p + "connection.username":      cfg.User,
		p + "connection.password":      "${BZ_SINK_PASSWORD}",
		p + "insert.mode":              Str(opts, "write_mode"),
		p + "delete.enabled":           fmt.Sprint(deletes),
		p + "truncate.enabled":         fmt.Sprint(upsert),
		p + "schema.evolution":         Str(opts, "schema_evolution"),
		p + "batch.size":               Str(opts, "batch_size"),
		p + "use.time.zone":            "UTC",
		p + "primary.key.mode":         "none",
		p + "connection.pool.min_size": "1",
		p + "connection.pool.max_size": "4",
	}
	if upsert {
		// Upserts and deletes find rows by the record key: the primary key, or the key columns
		// chosen in the Transform step.
		props[p+"primary.key.mode"] = "record_key"
	}
	return props, map[string]string{"BZ_SINK_PASSWORD": cfg.Password}, nil
}

// Destination is the table the connector writes: the target schema (PostgreSQL, SQL Server) and
// the routed name with dots as underscores.
func (s *jdbcSink) Destination(conn Resolved, _ CompileContext, routed string) string {
	table := strings.ReplaceAll(routed, ".", "_")
	schema := Str(conn.Config, "schema")
	if Str(conn.Config, "database_type") != "postgresql" || schema == "" {
		return table
	}
	return schema + "." + table
}

// --- direct access (checks, the Events tab, cleanup) ---

func (c jdbcConfig) open(ctx context.Context, timeout time.Duration) (*sql.DB, error) {
	var db *sql.DB
	switch c.DBType {
	case "mysql", "mariadb":
		mc := mysql.NewConfig()
		mc.User, mc.Passwd, mc.Net, mc.Addr, mc.DBName = c.User, c.Password, "tcp", net.JoinHostPort(c.Host, strconv.Itoa(c.Port)), c.Database
		mc.Timeout, mc.ReadTimeout, mc.WriteTimeout = timeout, 15*time.Second, 15*time.Second
		mc.AllowNativePasswords = true
		mc.TLSConfig = map[string]string{"disable": "false", "require": "skip-verify", "verify": "true"}[c.TLS]
		mc.DialFunc = DialContext // the SSRF guard (ADR 0004)
		connector, err := mysql.NewConnector(mc)
		if err != nil {
			return nil, err
		}
		db = sql.OpenDB(connector)
	case "sqlserver":
		q := url.Values{}
		q.Set("database", c.Database)
		q.Set("dial timeout", strconv.Itoa(int(timeout.Seconds())))
		q.Set("encrypt", fmt.Sprint(c.TLS != "disable"))
		q.Set("TrustServerCertificate", fmt.Sprint(c.TLS != "verify"))
		u := &url.URL{Scheme: "sqlserver", User: url.UserPassword(c.User, c.Password), Host: net.JoinHostPort(c.Host, strconv.Itoa(c.Port)), RawQuery: q.Encode()}
		connector, err := mssql.NewConnector(u.String())
		if err != nil {
			return nil, err
		}
		connector.Dialer = guardedDialer{} // the SSRF guard (ADR 0004)
		db = sql.OpenDB(connector)
	default:
		mode := map[string]string{"disable": "disable", "require": "require", "verify": "verify-full"}[c.TLS]
		pc, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s connect_timeout=%d application_name=bezisync",
			pgQuote(c.Host), c.Port, pgQuote(c.Database), pgQuote(c.User), pgQuote(c.Password), mode, int(timeout.Seconds())))
		if err != nil {
			return nil, err
		}
		pc.DialFunc = DialContext // the SSRF guard (ADR 0004)
		db = stdlib.OpenDB(*pc)
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// quote quotes an identifier for the database.
func (c jdbcConfig) quote(name string) string {
	switch c.DBType {
	case "mysql", "mariadb":
		return myQ(name)
	case "sqlserver":
		return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// qualified quotes schema.table (or table) as the connector names it. Unquoted names fold to
// lower case in PostgreSQL, as the connector writes them without quotes.
func (c jdbcConfig) qualified(destination string) string {
	parts := strings.Split(destination, ".")
	for i, p := range parts {
		if c.DBType == "postgresql" {
			p = strings.ToLower(p)
		}
		parts[i] = c.quote(p)
	}
	return strings.Join(parts, ".")
}

func (c jdbcConfig) defaultPort() int {
	return map[string]int{"postgresql": 5432, "mysql": 3306, "mariadb": 3306, "sqlserver": 1433}[c.DBType]
}

func (s *jdbcSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	run := &checks.Run{}
	cfg, err := jdbcCfg(conn)
	if err != nil {
		var p *problem.Problem
		if errors.As(err, &p) {
			run.Add(checks.Check{Code: "jdbc_config", Title: "Settings", Status: checks.Fail, Category: checks.CatDestination, Blocking: true, Message: p.Detail})
			return checks.Summarize(run.Checks, started), nil
		}
		return checks.Report{}, err
	}
	if cfg.Port == 0 {
		cfg.Port = cfg.defaultPort()
	}
	if !NetworkCheck(ctx, run, cfg.Host, cfg.Port, "jdbc_network", "the database", 0) {
		return checks.Summarize(run.Checks, started), nil
	}
	db, err := cfg.open(ctx, 10*time.Second)
	if err != nil {
		run.Add(checks.Check{Code: "jdbc_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: "The database refused the connection.", Technical: firstRunes(err.Error(), 300),
			Fix: &checks.Fix{Kind: "text", Who: "database admin", Code: jdbcGrant(cfg)}})
		return checks.Summarize(run.Checks, started), nil
	}
	defer func() { _ = db.Close() }()
	run.Passed("jdbc_login", "Sign in", checks.CatAuth, "Signed in to "+cfg.Database+".", "")
	// Ask whether the login may create tables where the connector creates them (schema evolution
	// needs it), without creating one (ADR 0016): a definite no warns, as before; a right the
	// database doesn't show is Unknown.
	status, msg, schema := jdbcCreateRight(ctx, db, cfg)
	c := checks.Check{Code: "jdbc_create_table", Title: "Create tables", Status: status, Category: checks.CatPermissions, Message: msg}
	if status != checks.Pass {
		c.Fix = &checks.Fix{Kind: "sql", Who: "database admin", Code: jdbcGrantIn(cfg, schema)}
	}
	run.Add(c)
	return checks.Summarize(run.Checks, started), nil
}

func jdbcGrant(c jdbcConfig) string { return jdbcGrantIn(c, "") }

// jdbcGrantIn is jdbcGrant for the schema the check found (SQL Server: the login's default
// schema; "" is dbo).
func jdbcGrantIn(c jdbcConfig, schema string) string {
	switch c.DBType {
	case "mysql", "mariadb":
		return fmt.Sprintf("GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP ON `%s`.* TO '%s'@'%%';", c.Database, c.User)
	case "sqlserver":
		if schema == "" {
			schema = "dbo"
		}
		return fmt.Sprintf("USE [%s];\nCREATE USER [%s] FOR LOGIN [%s] WITH DEFAULT_SCHEMA = [%s];\nGRANT CREATE TABLE TO [%s];\nGRANT ALTER, SELECT, INSERT, UPDATE, DELETE ON SCHEMA::[%s] TO [%s];",
			c.Database, c.User, c.User, schema, c.User, schema, c.User)
	}
	return fmt.Sprintf("GRANT USAGE, CREATE ON SCHEMA %s TO %s;\nALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s;",
		c.Schema, c.User, c.Schema, c.User)
}

func (s *jdbcSink) Tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error) {
	cfg, err := jdbcCfg(conn)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, max(timeout, 15*time.Second))
	defer cancel()
	db, err := cfg.open(ctx, 10*time.Second)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d", cfg.qualified(destination), limit)
	if cfg.DBType == "sqlserver" {
		query = fmt.Sprintf("SELECT TOP %d * FROM %s", limit, cfg.qualified(destination))
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		if jdbcMissingTable(err) {
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
		for i, c := range cols {
			switch v := vals[i].(type) {
			case []byte:
				row[c] = string(v)
			case time.Time:
				row[c] = v.UTC().Format(time.RFC3339Nano)
			default:
				row[c] = v
			}
		}
		out = append(out, TailRecord{Destination: destination, Value: row})
	}
	return out, rows.Err()
}

func jdbcMissingTable(err error) bool {
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "does not exist") || strings.Contains(text, "doesn't exist") || strings.Contains(text, "invalid object name") ||
		strings.Contains(text, "42p01") || strings.Contains(text, "1146")
}

func (s *jdbcSink) CleanupSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := jdbcCfg(conn)
	if err != nil {
		return nil, err
	}
	db, err := cfg.open(ctx, 10*time.Second)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	var results []ArtifactResult
	for _, dest := range sortedUnique(destinations) {
		if _, err := db.ExecContext(ctx, "DROP TABLE "+cfg.qualified(dest)); err != nil {
			outcome := "failed"
			if jdbcMissingTable(err) || strings.Contains(strings.ToLower(err.Error()), "unknown table") {
				outcome = "not_found"
			}
			results = append(results, ArtifactResult{Kind: "table", Name: dest, Outcome: outcome, Detail: firstRunes(err.Error(), 200)})
			continue
		}
		results = append(results, ArtifactResult{Kind: "table", Name: dest, Outcome: "removed"})
	}
	return results, nil
}

// CheckPipeline: upserts and deletes need a key on every table; names must fit the database.
func (s *jdbcSink) CheckPipeline(_ context.Context, conn Resolved, c CompileContext, facts PipelineFacts) []checks.Check {
	var out []checks.Check
	keyed := Str(facts.Options, "write_mode") == "upsert"
	if facts.Tables != nil && keyed {
		var keyless []string
		for _, t := range facts.Tables {
			if len(t.PrimaryKey) == 0 && len(c.Spec.Keys[t.Identifier()]) == 0 {
				keyless = append(keyless, t.Identifier())
			}
		}
		if len(keyless) > 0 {
			out = append(out, checks.Check{Code: "jdbc_no_key", Title: "Row keys", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
				Message: fmt.Sprintf("%s %s no primary key: the connector cannot upsert or delete %s rows and stops.", strings.Join(keyless, ", "),
					pluralVerb(len(keyless), "has", "have"), pluralVerb(len(keyless), "its", "their")),
				Fix: &checks.Fix{Kind: "text", Who: "you", Code: "Choose key columns for them in the Transform step (Message key), or insert only and ignore deletes in the Destination step."}})
		} else {
			out = append(out, checks.Check{Code: "jdbc_no_key", Title: "Row keys", Status: checks.Pass, Category: checks.CatDestination,
				Message: "Every table has a key: rows are upserted and deleted by it."})
		}
	}
	limit := map[string]int{"postgresql": 63, "mysql": 64, "mariadb": 64, "sqlserver": 128}[Str(conn.Config, "database_type")]
	for _, dest := range sortedUnique(facts.Destinations) {
		name := dest[strings.LastIndex(dest, ".")+1:]
		if limit > 0 && len(name) > limit {
			out = append(out, checks.Check{Code: "jdbc_name_length", Title: "Table names", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
				Message: fmt.Sprintf("%s is longer than the %d characters the database allows.", name, limit),
				Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Shorten the destination naming in the Destination step."}})
			break
		}
	}
	return out
}

// jdbcCreateRight asks the database whether the login may create tables where the connector
// creates them; it returns the result, its message and the schema (for the fix).
func jdbcCreateRight(ctx context.Context, db *sql.DB, cfg jdbcConfig) (status, msg, schema string) {
	unknown := func(what string, err error) (string, string, string) {
		m := "BeziSync couldn't ask the database " + what + ": the first table a pipeline creates shows a missing right."
		if err != nil {
			m += " (" + firstRunes(err.Error(), 200) + ")"
		}
		return checks.Unknown, m, schema
	}
	switch cfg.DBType {
	case "postgresql":
		// The connector creates its tables in this schema (currentSchema=, lower-cased unquoted)
		// and never creates the schema itself.
		var name sql.NullString
		var exists, usage, create bool
		err := db.QueryRowContext(ctx, `SELECT s.name, n.oid IS NOT NULL,
  CASE WHEN n.oid IS NULL THEN false ELSE has_schema_privilege(n.oid, 'USAGE') END,
  CASE WHEN n.oid IS NULL THEN false ELSE has_schema_privilege(n.oid, 'CREATE') END
FROM (SELECT coalesce(nullif($1, ''), current_schema()) AS name) s LEFT JOIN pg_namespace n ON n.nspname = s.name`,
			strings.ToLower(cfg.Schema)).Scan(&name, &exists, &usage, &create)
		if err != nil {
			return unknown("which schema rights the user has", err)
		}
		if !name.Valid {
			return unknown("which schema the user creates tables in (no current schema)", nil)
		}
		schema = name.String
		switch {
		case !exists:
			return checks.Warn, "The schema " + schema + " doesn't exist: create it (pipelines don't); existing tables elsewhere still work.", schema
		case usage && create:
			return checks.Pass, "May create tables in the schema " + schema + ".", schema
		}
		return checks.Warn, "The user can't create tables in the schema " + schema + " (needs USAGE and CREATE): pipelines that create their tables will fail; existing tables still work.", schema
	case "sqlserver":
		var table, alter sql.NullInt64
		var name sql.NullString
		err := db.QueryRowContext(ctx, `SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE'), HAS_PERMS_BY_NAME(SCHEMA_NAME(), 'SCHEMA', 'ALTER'), SCHEMA_NAME()`).
			Scan(&table, &alter, &name)
		if err != nil {
			return unknown("which rights the user has", err)
		}
		schema = name.String
		switch sqlserverCreateVerdict(table, alter) {
		case checks.Pass:
			return checks.Pass, "May create tables in the schema " + schema + ".", schema
		case checks.Warn:
			return checks.Warn, "The user can't create tables in the schema " + schema + " (needs CREATE TABLE on the database and ALTER on the schema): pipelines that create their tables will fail; existing tables still work.", schema
		}
		return unknown("which schema the user creates tables in (the login has no default schema)", nil)
	}
	// MySQL, MariaDB: what SHOW GRANTS shows.
	rows, err := db.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return unknown("which grants the user has", err)
	}
	defer func() { _ = rows.Close() }()
	var grants []string
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err != nil {
			return unknown("which grants the user has", err)
		}
		grants = append(grants, g)
	}
	if err := rows.Err(); err != nil {
		return unknown("which grants the user has", err)
	}
	if onDB, _, revoked := mysqlGrantsCreate(grants, cfg.Database); onDB && !revoked {
		return checks.Pass, "May create tables in " + cfg.Database + ".", schema
	}
	return checks.Unknown, "The user's grants don't show the right to create tables in " + cfg.Database +
		" (it may come from a role, or a partial revoke hides it): the first table a pipeline creates shows whether it's there.", schema
}

// sqlserverCreateVerdict maps HAS_PERMS_BY_NAME's answers: both 1 Pass, a 0 Warn, a NULL (no
// default schema, or nothing to ask about) Unknown.
func sqlserverCreateVerdict(table, alter sql.NullInt64) string {
	switch {
	case !table.Valid || !alter.Valid:
		return checks.Unknown
	case table.Int64 == 1 && alter.Int64 == 1:
		return checks.Pass
	}
	return checks.Warn
}
