package connectors

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
)

// CockroachDB source: Debezium's connector reads a sinkless changefeed over its own SQL
// connection (no Kafka in between). It resumes from the last resolved timestamp, which the
// cluster keeps only for its GC TTL. BeziSync's tables live in a bezisync schema, as on PostgreSQL.

const crdbMaxTables = 10_000

var crdbSystemSchemas = []string{"pg_catalog", "information_schema", "crdb_internal", "pg_extension", pgManagedSchema}

var crdbVersionRe = regexp.MustCompile(`CockroachDB \w+ v(\d+)\.(\d+)`)
var crdbTTLRe = regexp.MustCompile(`gc\.ttlseconds = (\d+)`)

type cockroachSource struct{ BaseSource }

func init() { Register(&cockroachSource{BaseSource{Title: "CockroachDB"}}) }

func (*cockroachSource) Kind() string { return "source" }
func (*cockroachSource) Type() string { return "cockroachdb" }

type crdbConfig struct {
	Host, Database, User, SSLMode string
	Port                          int
}

func crdbCfg(conn Resolved) (crdbConfig, map[string]any, error) {
	d, _ := Describe("source", "cockroachdb")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return crdbConfig{}, nil, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return crdbConfig{}, nil, err
	}
	return crdbConfig{Host: Str(cfg, "host"), Port: Int(cfg, "port"), Database: Str(cfg, "database"),
		User: Str(cfg, "user"), SSLMode: Str(cfg, "ssl_mode")}, sec, nil
}

func crdbCAFile() string { return SecretDir + "/cockroachdb-ca.crt" }

// crdbConnect opens a session (through the SSH tunnel when configured).
func crdbConnect(ctx context.Context, conn Resolved, timeout time.Duration) (*pgx.Conn, func(), error) {
	cfg, sec, err := crdbCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	host, port, closeTunnel, err := Tunnel(ctx, conn.Config, conn.Secrets, cfg.Host, cfg.Port)
	if err != nil {
		return nil, nil, err
	}
	cleanup := []func(){closeTunnel}
	done := func() {
		for i := len(cleanup) - 1; i >= 0; i-- {
			cleanup[i]()
		}
	}
	params := []string{
		"host=" + pgQuote(host), "port=" + strconv.Itoa(port), "dbname=" + pgQuote(cfg.Database),
		"user=" + pgQuote(cfg.User), "sslmode=" + cfg.SSLMode,
		"connect_timeout=" + strconv.Itoa(int(timeout.Seconds())), "application_name=bezisync",
	}
	if pw := Str(sec, "password"); pw != "" {
		params = append(params, "password="+pgQuote(pw))
	}
	if root := Str(sec, "ssl_root_cert"); root != "" {
		f, err := os.CreateTemp("", "bezisync-*.pem")
		if err != nil {
			done()
			return nil, nil, err
		}
		_, _ = f.WriteString(root)
		_ = f.Close()
		cleanup = append(cleanup, func() { _ = os.Remove(f.Name()) })
		params = append(params, "sslrootcert="+pgQuote(f.Name()))
	}
	pc, err := pgx.ParseConfig(strings.Join(params, " "))
	if err != nil {
		done()
		return nil, nil, err
	}
	pc.DialFunc = DialContext // the SSRF guard (ADR 0004)
	pc.RuntimeParams["statement_timeout"] = "10s"
	cctx, cancel := context.WithTimeout(ctx, timeout+5*time.Second)
	defer cancel()
	c, err := pgx.ConnectConfig(cctx, pc)
	if err != nil {
		done()
		return nil, nil, &PgOperationalError{err}
	}
	cleanup = append(cleanup, func() { _ = c.Close(context.Background()) })
	return c, done, nil
}

func crdbLoginFailure(message string, cfg crdbConfig) checks.Check {
	lower := strings.ToLower(message)
	tech := strings.TrimSpace(message)
	c := checks.Check{Code: "crdb_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
		Message: "Could not sign in to the cluster.", Technical: tech}
	switch {
	case strings.Contains(lower, "password authentication failed") || strings.Contains(lower, "authentication failed"):
		c.Message = fmt.Sprintf("The cluster rejected the password for %s.", cfg.User)
	case strings.Contains(lower, "does not exist") && strings.Contains(lower, "database"):
		c.Message = fmt.Sprintf("Database %s does not exist.", cfg.Database)
	case strings.Contains(lower, "node is running secure mode") || strings.Contains(lower, "ssl") || strings.Contains(lower, "certificate"):
		c.Code, c.Title, c.Category = "crdb_tls", "TLS", checks.CatNetwork
		c.Message = "The TLS connection failed."
		c.Why = "Secure clusters need TLS (require or stricter); CockroachDB Cloud also needs its CA certificate for verify-full."
	}
	return c
}

func (s *cockroachSource) Check(ctx context.Context, conn Resolved, tables []string) (checks.Report, error) {
	started := time.Now()
	cfg, _, err := crdbCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	var reachable bool
	if Str(conn.Config, "ssh_host") != "" {
		reachable = SSHChecks(ctx, run, conn.Config, conn.Secrets, cfg.Host, cfg.Port, "crdb")
	} else {
		reachable = NetworkCheck(ctx, run, cfg.Host, cfg.Port, "crdb_network", "the cluster", 0)
	}
	if !reachable {
		return checks.Summarize(run.Checks, started), nil
	}
	db, done, err := crdbConnect(ctx, conn, 6*time.Second)
	if err != nil {
		var op *PgOperationalError
		if errors.As(err, &op) {
			run.Add(crdbLoginFailure(op.Error(), cfg))
			return checks.Summarize(run.Checks, started), nil
		}
		return checks.Report{}, err
	}
	defer done()
	run.Passed("crdb_login", "Sign in", checks.CatAuth, fmt.Sprintf("Signed in as %s.", cfg.User), "")
	for _, step := range []func() error{
		func() error { return s.serverChecks(ctx, db, run, cfg) },
		func() error {
			if len(tables) == 0 {
				return nil
			}
			return s.tableChecks(ctx, db, run, tables, cfg)
		},
	} {
		if err := step(); err != nil {
			if isPgOperational(err) {
				run.Add(crdbLoginFailure(err.Error(), cfg))
				return checks.Summarize(run.Checks, started), nil
			}
			return checks.Report{}, err
		}
	}
	return checks.Summarize(run.Checks, started), nil
}

// isPgPermission: SQLSTATE 42501 (insufficient privilege).
func isPgPermission(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42501"
}

func (s *cockroachSource) serverChecks(ctx context.Context, db *pgx.Conn, run *checks.Run, cfg crdbConfig) error {
	version, err := pgOne[string](ctx, db, "SELECT version()")
	if err != nil {
		return err
	}
	m := crdbVersionRe.FindStringSubmatch(version)
	if m == nil {
		run.Add(checks.Check{Code: "crdb_version", Title: "CockroachDB version", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: "This is not a CockroachDB cluster.", Technical: version})
		return nil
	}
	major, _ := strconv.Atoi(m[1])
	minor, _ := strconv.Atoi(m[2])
	if major < 22 || (major == 22 && minor < 2) {
		run.Add(checks.Check{Code: "crdb_version", Title: "CockroachDB version", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: fmt.Sprintf("CockroachDB v%s.%s is too old: the changefeed privilege needs v22.2 or newer.", m[1], m[2]), Technical: version})
	} else {
		run.Passed("crdb_version", "CockroachDB version", checks.CatCDC, fmt.Sprintf("CockroachDB v%s.%s.", m[1], m[2]), "")
	}
	enabled, err := pgOne[bool](ctx, db, "SHOW CLUSTER SETTING kv.rangefeed.enabled")
	switch {
	case isPgPermission(err):
		run.Add(checks.Check{Code: "crdb_rangefeed", Title: "Rangefeeds", Status: checks.Warn, Category: checks.CatCDC,
			Message:   fmt.Sprintf("%s may not read cluster settings, so BeziSync can't confirm that rangefeeds are on.", cfg.User),
			Technical: "Changefeeds need kv.rangefeed.enabled = true; reading it needs VIEWCLUSTERSETTING.",
			Fix:       &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("GRANT SYSTEM VIEWCLUSTERSETTING TO %s;", QuoteIdent(cfg.User))}})
	case err != nil:
		return err
	case enabled:
		run.Passed("crdb_rangefeed", "Rangefeeds", checks.CatCDC, "Rangefeeds are on, so changefeeds can run.", "kv.rangefeed.enabled = true")
	default:
		run.Add(checks.Check{Code: "crdb_rangefeed", Title: "Rangefeeds", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message:   "Rangefeeds are off, so the cluster can't stream changes.",
			Technical: "kv.rangefeed.enabled = false",
			Fix:       &checks.Fix{Kind: "sql", Who: "DBA", Code: "SET CLUSTER SETTING kv.rangefeed.enabled = true;"}})
	}
	canCreate, err := pgOne[bool](ctx, db, "SELECT has_database_privilege(current_database(), 'CREATE')")
	if err != nil {
		return err
	}
	schemaExists, err := pgOne[bool](ctx, db, "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)", pgManagedSchema)
	if err != nil {
		return err
	}
	ownsSchema := false
	if schemaExists {
		if ownsSchema, err = pgOne[bool](ctx, db, "SELECT has_schema_privilege($1, 'CREATE')", pgManagedSchema); err != nil {
			return err
		}
	}
	if canCreate || ownsSchema {
		run.Passed("crdb_create", "BeziSync tables", checks.CatPermissions, "Can create BeziSync's signal table in the bezisync schema.", "")
	} else {
		run.Add(checks.Check{Code: "crdb_create", Title: "BeziSync tables", Status: checks.Fail, Category: checks.CatPermissions, Blocking: true,
			Message:   fmt.Sprintf("%s cannot create the bezisync schema for BeziSync's signal table.", cfg.User),
			Technical: "Snapshots on demand go through bezisync.signal_<id>.",
			Fix: &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.bezisync;\nGRANT ALL ON SCHEMA %s.bezisync TO %s;",
				QuoteIdent(cfg.Database), QuoteIdent(cfg.Database), QuoteIdent(cfg.User))}})
	}
	return nil
}

// crdbGCTTL is a table's gc.ttlseconds (nil when its zone configuration is not readable).
func crdbGCTTL(ctx context.Context, db *pgx.Conn, table string) (*int64, error) {
	raw, err := pgOne[string](ctx, db, "SELECT raw_config_sql FROM [SHOW ZONE CONFIGURATION FROM TABLE "+qualified(table)+"]")
	if isPgPermission(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	m := crdbTTLRe.FindStringSubmatch(raw)
	if m == nil {
		return nil, nil
	}
	v, _ := strconv.ParseInt(m[1], 10, 64)
	return &v, nil
}

func (s *cockroachSource) tableChecks(ctx context.Context, db *pgx.Conn, run *checks.Run, tables []string, cfg crdbConfig) error {
	var missing, noFeed, noSelect []string
	var shortest *int64
	shortestTable := ""
	for _, t := range tables {
		schema, table, _ := strings.Cut(t, ".")
		exists, err := pgOne[bool](ctx, db, `SELECT EXISTS (SELECT 1 FROM information_schema.tables
		                                       WHERE table_schema = $1 AND table_name = $2 AND table_type = 'BASE TABLE')`, schema, table)
		if err != nil {
			return err
		}
		if !exists {
			missing = append(missing, t)
			continue
		}
		privileges := map[string]bool{}
		rows, err := db.Query(ctx, "SELECT privilege_type FROM [SHOW GRANTS ON TABLE "+qualified(t)+" FOR "+QuoteIdent(cfg.User)+"]")
		if err != nil {
			return err
		}
		for rows.Next() {
			var p string
			if err := rows.Scan(&p); err != nil {
				rows.Close()
				return err
			}
			privileges[p] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if !privileges["CHANGEFEED"] && !privileges["ALL"] {
			noFeed = append(noFeed, t)
		}
		if !privileges["SELECT"] && !privileges["ALL"] {
			noSelect = append(noSelect, t)
		}
		ttl, err := crdbGCTTL(ctx, db, t)
		if err != nil {
			return err
		}
		if ttl != nil && (shortest == nil || *ttl < *shortest) {
			shortest, shortestTable = ttl, t
		}
	}
	if len(missing) > 0 {
		run.Add(checks.Check{Code: "crdb_tables_exist", Title: "Selected tables exist", Status: checks.Fail, Category: checks.CatTables, Blocking: true,
			Message: fmt.Sprintf("Not found: %s.", strings.Join(first(missing, 10), ", "))})
	}
	grant := func(privilege string, list []string) *checks.Fix {
		names := make([]string, 0, len(list))
		for _, t := range first(list, 50) {
			names = append(names, qualified(t))
		}
		return &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("GRANT %s ON TABLE %s TO %s;", privilege, strings.Join(names, ", "), QuoteIdent(cfg.User))}
	}
	if len(noFeed) > 0 {
		run.Add(checks.Check{Code: "crdb_tables_changefeed", Title: "Changefeed permission", Status: checks.Fail, Category: checks.CatPermissions, Blocking: true,
			Message: fmt.Sprintf("%s may not open a changefeed on %s.", cfg.User, strings.Join(first(noFeed, 10), ", ")),
			Fix:     grant("CHANGEFEED", noFeed)})
	}
	if len(noSelect) > 0 {
		run.Add(checks.Check{Code: "crdb_tables_select", Title: "Read access", Status: checks.Fail, Category: checks.CatPermissions, Blocking: true,
			Message: fmt.Sprintf("No SELECT permission on %s.", strings.Join(first(noSelect, 10), ", ")),
			Why:     "The first copy and re-copies read the existing rows.", Fix: grant("SELECT", noSelect)})
	}
	switch {
	case shortest == nil && len(missing) < len(tables):
		run.Add(checks.Check{Code: "crdb_gc_ttl", Title: "Garbage collection window", Status: checks.Warn, Category: checks.CatResilience,
			Message:   "BeziSync can't read the tables' zone configuration, so it can't tell how long a stopped pipeline can resume.",
			Technical: "A changefeed resumes only within the tables' gc.ttlseconds."})
	case shortest != nil && *shortest < 3600:
		run.Add(checks.Check{Code: "crdb_gc_ttl", Title: "Garbage collection window", Status: checks.Warn, Category: checks.CatResilience,
			Message:   fmt.Sprintf("%s keeps old versions for %s only: a pipeline stopped for longer must copy its tables again.", shortestTable, HumanSeconds(*shortest)),
			Technical: "A changefeed resumes only within the tables' gc.ttlseconds.",
			Fix:       &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("ALTER TABLE %s CONFIGURE ZONE USING gc.ttlseconds = 14400;", qualified(shortestTable))}})
	case shortest != nil:
		run.Passed("crdb_gc_ttl", "Garbage collection window", checks.CatResilience,
			fmt.Sprintf("A stopped pipeline can resume within %s (%s's gc.ttlseconds).", HumanSeconds(*shortest), shortestTable), "")
	}
	if len(missing) == 0 && len(noFeed) == 0 && len(noSelect) == 0 {
		run.Passed("crdb_tables", "Selected tables", checks.CatTables, fmt.Sprintf("%d tables can be captured.", len(tables)), "")
	}
	return nil
}

func (s *cockroachSource) Discover(ctx context.Context, conn Resolved) (Discovery, error) {
	db, done, err := crdbConnect(ctx, conn, 10*time.Second)
	if err != nil {
		return Discovery{}, err
	}
	defer done()
	type trow struct{ schema, name string }
	var trows []trow
	rows, err := db.Query(ctx, fmt.Sprintf(`SELECT table_schema, table_name FROM information_schema.tables
	        WHERE table_type = 'BASE TABLE' AND table_schema <> ALL($1) ORDER BY 1, 2 LIMIT %d`, crdbMaxTables+1), crdbSystemSchemas)
	if err != nil {
		return Discovery{}, err
	}
	for rows.Next() {
		var r trow
		if err := rows.Scan(&r.schema, &r.name); err != nil {
			rows.Close()
			return Discovery{}, err
		}
		trows = append(trows, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Discovery{}, err
	}
	truncated := len(trows) > crdbMaxTables
	trows = first(trows, crdbMaxTables)
	key := func(schema, table string) string { return schema + "." + table }
	estimates := map[string]int64{}
	schemas := map[string]bool{}
	for _, r := range trows {
		schemas[r.schema] = true
	}
	for _, schema := range sortedKeys(schemas) {
		// SHOW TABLES reads the statistics the cluster already keeps.
		rows, err := db.Query(ctx, "SELECT table_name, estimated_row_count FROM [SHOW TABLES FROM "+QuoteIdent(schema)+"]")
		if err != nil {
			return Discovery{}, err
		}
		for rows.Next() {
			var name string
			var n *int64
			if err := rows.Scan(&name, &n); err != nil {
				rows.Close()
				return Discovery{}, err
			}
			if n != nil {
				estimates[key(schema, name)] = *n
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return Discovery{}, err
		}
	}
	cols := map[string][]Column{}
	hidden := map[string]bool{}
	crows, err := db.Query(ctx, `SELECT table_schema, table_name, column_name, data_type, is_nullable = 'YES', is_hidden = 'YES'
	        FROM information_schema.columns WHERE table_schema <> ALL($1) ORDER BY table_schema, table_name, ordinal_position LIMIT 200000`, crdbSystemSchemas)
	if err != nil {
		return Discovery{}, err
	}
	for crows.Next() {
		var schema, table, column, dataType string
		var nullable, isHidden bool
		if err := crows.Scan(&schema, &table, &column, &dataType, &nullable, &isHidden); err != nil {
			crows.Close()
			return Discovery{}, err
		}
		if isHidden {
			hidden[key(schema, table)+"."+column] = true
			if column != "rowid" {
				continue
			}
		}
		cols[key(schema, table)] = append(cols[key(schema, table)], Column{Name: column, Type: dataType, Nullable: nullable, PII: LooksLikePII(column)})
	}
	crows.Close()
	if err := crows.Err(); err != nil {
		return Discovery{}, err
	}
	pks := map[string][]string{}
	krows, err := db.Query(ctx, `SELECT k.table_schema, k.table_name, k.column_name FROM information_schema.table_constraints c
	        JOIN information_schema.key_column_usage k ON k.constraint_schema = c.constraint_schema AND k.constraint_name = c.constraint_name
	         AND k.table_name = c.table_name
	        WHERE c.constraint_type = 'PRIMARY KEY' AND c.table_schema <> ALL($1) ORDER BY 1, 2, k.ordinal_position`, crdbSystemSchemas)
	if err != nil {
		return Discovery{}, err
	}
	for krows.Next() {
		var schema, table, column string
		if err := krows.Scan(&schema, &table, &column); err != nil {
			krows.Close()
			return Discovery{}, err
		}
		pks[key(schema, table)] = append(pks[key(schema, table)], column)
	}
	krows.Close()
	if err := krows.Err(); err != nil {
		return Discovery{}, err
	}
	var tables []TableInfo
	for _, r := range trows {
		k := key(r.schema, r.name)
		var warnings []string
		pk := pks[k]
		if len(pk) == 1 && pk[0] == "rowid" && hidden[k+".rowid"] {
			warnings = append(warnings, "No primary key: events are keyed by CockroachDB's hidden rowid, which also appears in every row.")
		}
		var rowsEst *int64
		if n, ok := estimates[k]; ok {
			rowsEst = &n
		}
		tables = append(tables, TableInfo{Schema: r.schema, Name: r.name, RowsEstimate: rowsEst, PrimaryKey: pk, Columns: cols[k],
			Warnings: warnings, Selectable: true})
	}
	return NewDiscovery(tables, truncated), nil
}

func (s *cockroachSource) CompileSource(conn Resolved, c CompileContext) (map[string]string, map[string]string, bool, error) {
	cfg, sec, err := crdbCfg(conn)
	if err != nil {
		return nil, nil, false, err
	}
	spec := c.Spec
	tables := append(append([]string{}, spec.Source.Tables.Include...), pgSignalTable(c))
	if canaryOn(c) {
		tables = append(tables, pgCanaryTable(c))
	}
	props := map[string]string{
		"debezium.source.connector.class":                  "io.debezium.connector.cockroachdb.CockroachDBConnector",
		"debezium.source.database.hostname":                cfg.Host,
		"debezium.source.database.port":                    strconv.Itoa(cfg.Port),
		"debezium.source.database.dbname":                  cfg.Database,
		"debezium.source.database.user":                    cfg.User,
		"debezium.source.database.sslmode":                 cfg.SSLMode,
		"debezium.source.cockroachdb.changefeed.sink.type": "sinkless",
		"debezium.source.table.include.list":               joinRegex(tables),
		"debezium.source.signal.enabled.channels":          "source",
		"debezium.source.signal.data.collection":           cfg.Database + "." + pgSignalTable(c),
		"debezium.source.heartbeat.interval.ms":            strconv.Itoa(spec.Policies.HeartbeatSeconds * 1000),
	}
	env := map[string]string{}
	if Str(sec, "password") != "" {
		props["debezium.source.database.password"] = "${BZ_SRC_PASSWORD}"
		env["BZ_SRC_PASSWORD"] = PyStr(sec["password"])
	}
	if Str(sec, "ssl_root_cert") != "" {
		props["debezium.source.database.sslrootcert"] = crdbCAFile()
	}
	excludes, masks := ColumnRules(spec)
	if len(excludes) > 0 {
		props["debezium.source.column.exclude.list"] = joinRegex(excludes)
	}
	if len(masks) > 0 {
		props["debezium.source.column.mask.hash.v2.SHA-256.with.salt."+Salt(c.PipelineID)] = joinRegex(masks)
	}
	return props, env, false, nil
}

// SourceFiles mounts the cluster's CA certificate for verify-ca / verify-full.
func (s *cockroachSource) SourceFiles(conn Resolved) (map[string]string, error) {
	_, sec, err := crdbCfg(conn)
	if err != nil {
		return nil, err
	}
	if root := Str(sec, "ssl_root_cert"); root != "" {
		return map[string]string{crdbCAFile(): root}, nil
	}
	return map[string]string{}, nil
}

func (*cockroachSource) CanaryTable(_ Resolved, c CompileContext) (string, error) {
	return pgCanaryTable(c), nil
}

func (s *cockroachSource) Prepare(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	db, done, err := crdbConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	// CockroachDB checks CREATE on the database even when the schema already exists.
	schemaExists, err := pgOne[bool](ctx, db, "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)", pgManagedSchema)
	if err != nil {
		return nil, err
	}
	if !schemaExists {
		if _, err := db.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgManagedSchema); err != nil {
			return nil, err
		}
	}
	create := func(table, columns string) (ArtifactResult, error) {
		existed, err := pgOne[bool](ctx, db, "SELECT to_regclass($1) IS NOT NULL", table)
		if err != nil {
			return ArtifactResult{}, err
		}
		if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+table+" ("+columns+")"); err != nil {
			return ArtifactResult{}, err
		}
		return ArtifactResult{Kind: "table", Name: table, Outcome: pick(existed, "exists", "created")}, nil
	}
	signal, err := create(pgSignalTable(c), "id STRING PRIMARY KEY, type STRING NOT NULL, data STRING")
	if err != nil {
		return nil, err
	}
	results := []ArtifactResult{signal}
	if canaryOn(c) {
		canary, err := create(pgCanaryTable(c), "id STRING PRIMARY KEY, sent_at TIMESTAMPTZ NOT NULL DEFAULT now()")
		if err != nil {
			return nil, err
		}
		results = append(results, canary)
	}
	return results, nil
}

func (s *cockroachSource) Cleanup(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	db, done, err := crdbConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	var results []ArtifactResult
	for _, table := range []string{pgSignalTable(c), pgCanaryTable(c)} {
		existed, err := pgOne[bool](ctx, db, "SELECT to_regclass($1) IS NOT NULL", table)
		if err != nil {
			return nil, err
		}
		if !existed {
			if table == pgSignalTable(c) {
				results = append(results, ArtifactResult{Kind: "table", Name: table, Outcome: "not_found"})
			}
			continue
		}
		if _, err := db.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			return nil, err
		}
		results = append(results, ArtifactResult{Kind: "table", Name: table, Outcome: "removed"})
	}
	return results, nil
}

func (s *cockroachSource) WriteCanary(ctx context.Context, conn Resolved, c CompileContext, token string) error {
	db, done, err := crdbConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return err
	}
	defer done()
	if _, err := db.Exec(ctx, "INSERT INTO "+pgCanaryTable(c)+" (id) VALUES ($1)", token); err != nil {
		return err
	}
	_, err = db.Exec(ctx, "DELETE FROM "+pgCanaryTable(c)+" WHERE sent_at < now() - INTERVAL '1 day'")
	return err
}

func (s *cockroachSource) Signal(ctx context.Context, conn Resolved, c CompileContext, signalType string, data map[string]any) (string, error) {
	cfg, _, err := crdbCfg(conn)
	if err != nil {
		return "", err
	}
	// Debezium names CockroachDB data collections <database>.<schema>.<table>.
	if dc, ok := data["data-collections"]; ok {
		copied := map[string]any{}
		for k, v := range data {
			copied[k] = v
		}
		qualified := []any{}
		switch list := dc.(type) {
		case []any:
			for _, t := range list {
				qualified = append(qualified, cfg.Database+"."+PyStr(t))
			}
		case []string:
			for _, t := range list {
				qualified = append(qualified, cfg.Database+"."+t)
			}
		}
		copied["data-collections"] = qualified
		data = copied
	}
	db, done, err := crdbConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return "", err
	}
	defer done()
	payload, err := PyDumps(data)
	if err != nil {
		return "", err
	}
	signalID := hex.EncodeToString(uuidBytes())
	_, err = db.Exec(ctx, "INSERT INTO "+pgSignalTable(c)+" (id, type, data) VALUES ($1, $2, $3)", signalID, signalType, payload)
	return signalID, err
}

func (s *cockroachSource) Probe(ctx context.Context, conn Resolved, c CompileContext, _ int64) (*SourceImpact, error) {
	db, done, err := crdbConnect(ctx, conn, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	var kept *int64
	for _, t := range c.Spec.Source.Tables.Include {
		ttl, err := crdbGCTTL(ctx, db, t)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "42P01" { // the table is gone; the checks say so
				continue
			}
			return nil, err
		}
		if ttl != nil && (kept == nil || *ttl < *kept) {
			kept = ttl
		}
	}
	var behind *int64
	if OffsetsReader != nil {
		rows, err := OffsetsReader(ctx, c.ShortID)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			raw, _ := r["raw"].(map[string]any)
			if ms, err := strconv.ParseInt(PyStr(raw["offset.timestamp"]), 10, 64); err == nil {
				// Against the cluster's clock, not BeziSync's.
				now, err := pgOne[time.Time](ctx, db, "SELECT now()")
				if err != nil {
					return nil, err
				}
				v := max(int64(now.Sub(time.UnixMilli(ms)).Seconds()), 0)
				behind = &v
				break
			}
		}
	}
	return TimeWindowImpact(behind, kept, "the cluster",
		"Raise the tables' gc.ttlseconds (ALTER TABLE … CONFIGURE ZONE USING gc.ttlseconds = …) to survive longer outages."), nil
}
