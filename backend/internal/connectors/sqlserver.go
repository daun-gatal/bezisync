package connectors

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
)

// SQL Server source (SQL Server change data capture).

const msManagedSchema = "bezisync"

type sqlserverSource struct{ BaseSource }

func init() { Register(&sqlserverSource{BaseSource{Title: "SQL Server"}}) }

func (*sqlserverSource) Kind() string { return "source" }
func (*sqlserverSource) Type() string { return "sqlserver" }

type msConfig struct {
	Host, Database, User string
	Port                 int
	Encrypt, TrustCert   bool
}

func msCfg(conn Resolved) (msConfig, map[string]any, error) {
	d, _ := Describe("source", "sqlserver")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return msConfig{}, nil, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return msConfig{}, nil, err
	}
	return msConfig{Host: Str(cfg, "host"), Port: Int(cfg, "port"), Database: Str(cfg, "database"), User: Str(cfg, "user"),
		Encrypt: Bool(cfg, "encrypt"), TrustCert: Bool(cfg, "trust_server_certificate")}, sec, nil
}

func msQ(name string) string { return "[" + strings.ReplaceAll(name, "]", "]]") + "]" }

// msConnect opens a session. Like pymssql (FreeTDS), the connection settings' encrypt
// flags only shape Debezium's connection; BeziSync's own session negotiates encryption
// without validating the certificate.
func msConnect(ctx context.Context, conn Resolved, timeout time.Duration) (*sql.Conn, func(), error) {
	cfg, sec, err := msCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	host, port, closeTunnel, err := Tunnel(ctx, conn.Config, conn.Secrets, cfg.Host, cfg.Port)
	if err != nil {
		return nil, nil, err
	}
	q := url.Values{}
	q.Set("database", cfg.Database)
	q.Set("dial timeout", strconv.Itoa(int(timeout.Seconds())))
	q.Set("connection timeout", "15")
	q.Set("encrypt", "false")
	q.Set("TrustServerCertificate", "true")
	u := &url.URL{Scheme: "sqlserver", User: url.UserPassword(cfg.User, PyStr(sec["password"])),
		Host: fmt.Sprintf("%s:%d", host, port), RawQuery: q.Encode()}
	connector, err := mssql.NewConnector(u.String())
	if err != nil {
		closeTunnel()
		return nil, nil, err
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	c, err := db.Conn(ctx)
	if err == nil {
		err = c.PingContext(ctx)
	}
	if err != nil {
		if c != nil {
			_ = c.Close()
		}
		_ = db.Close()
		closeTunnel()
		return nil, nil, &MsError{err}
	}
	return c, func() { _ = c.Close(); _ = db.Close(); closeTunnel() }, nil
}

// MsError is a database error (pymssql.Error).
type MsError struct{ Err error }

func (e *MsError) Error() string { return e.Err.Error() }
func (e *MsError) Unwrap() error { return e.Err }

func isMsError(err error) bool {
	var me *MsError
	var se mssql.Error
	return errors.As(err, &me) || errors.As(err, &se)
}

func msSignalTable(c CompileContext) string { return msManagedSchema + ".signal_" + c.ShortID }

func (s *sqlserverSource) Check(ctx context.Context, conn Resolved, tables []string) (checks.Report, error) {
	started := time.Now()
	cfg, _, err := msCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	var reachable bool
	if Str(conn.Config, "ssh_host") != "" {
		reachable = SSHChecks(ctx, run, conn.Config, conn.Secrets, cfg.Host, cfg.Port, "mssql")
	} else {
		reachable = NetworkCheck(ctx, run, cfg.Host, cfg.Port, "mssql_network", "the database", 0)
	}
	if !reachable {
		return checks.Summarize(run.Checks, started), nil
	}
	fail := func(err error) (checks.Report, error) {
		text := err.Error()
		message := "Could not sign in."
		if strings.Contains(text, "18456") || strings.Contains(text, "Login failed") {
			message = "The database rejected the login."
		}
		run.Add(checks.Check{Code: "mssql_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: message, Technical: firstRunes(text, 400)})
		return checks.Summarize(run.Checks, started), nil
	}
	db, done, err := msConnect(ctx, conn, 6*time.Second)
	if err != nil {
		if isMsError(err) {
			return fail(err)
		}
		return checks.Report{}, err
	}
	defer done()
	run.Passed("mssql_login", "Sign in", checks.CatAuth, fmt.Sprintf("Signed in as %s.", cfg.User), "")
	if err := s.serverChecks(ctx, db, run, conn.Provider, cfg); err != nil {
		if isMsError(err) {
			return fail(err)
		}
		return checks.Report{}, err
	}
	if len(tables) > 0 {
		if err := s.tableChecks(ctx, db, run, tables); err != nil {
			if isMsError(err) {
				return fail(err)
			}
			return checks.Report{}, err
		}
	}
	return checks.Summarize(run.Checks, started), nil
}

func firstRunes(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n])
	}
	return s
}

func (s *sqlserverSource) serverChecks(ctx context.Context, db *sql.Conn, run *checks.Run, provider string, cfg msConfig) error {
	var enabled sql.NullBool
	err := db.QueryRowContext(ctx, "SELECT is_cdc_enabled FROM sys.databases WHERE name = @p1", cfg.Database).Scan(&enabled)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return &MsError{err}
	}
	if !enabled.Valid || !enabled.Bool {
		fix := fmt.Sprintf("USE %s;\nEXEC sys.sp_cdc_enable_db;", msQ(cfg.Database))
		if provider == "aws_rds" {
			fix = fmt.Sprintf("EXEC msdb.dbo.rds_cdc_enable_db '%s';", cfg.Database)
		}
		run.Add(checks.Check{Code: "mssql_cdc_db", Title: "Change data capture", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: fmt.Sprintf("CDC is not enabled on database %s.", cfg.Database), Why: "Debezium reads SQL Server's CDC change tables.",
			Fix: &checks.Fix{Kind: "sql", Who: "DBA", Code: fix}})
	} else {
		run.Passed("mssql_cdc_db", "Change data capture", checks.CatCDC, fmt.Sprintf("CDC is enabled on %s.", cfg.Database), "")
	}
	var status sql.NullString
	err = db.QueryRowContext(ctx, "SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE 'SQL Server Agent%'").Scan(&status)
	switch {
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		run.Add(checks.Check{Code: "mssql_agent", Title: "SQL Server Agent", Status: checks.Warn, Category: checks.CatCDC,
			Message: "Could not confirm that SQL Server Agent is running (needs VIEW SERVER STATE)."})
	case err == nil && strings.ToLower(nullStr(status)) != "running":
		run.Add(checks.Check{Code: "mssql_agent", Title: "SQL Server Agent", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: "SQL Server Agent is not running, so CDC tables are not filled.",
			Fix:     &checks.Fix{Kind: "text", Who: "DBA", Code: "Start the SQL Server Agent service (and set it to start automatically)."}})
	case err == nil:
		run.Passed("mssql_agent", "SQL Server Agent", checks.CatCDC, "SQL Server Agent is running.", "")
	}
	var owner sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT IS_ROLEMEMBER('db_owner')").Scan(&owner); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return &MsError{err}
	}
	if owner.Valid && owner.Int64 == 1 {
		run.Passed("mssql_permissions", "Permissions", checks.CatPermissions, "Member of db_owner.", "")
	} else {
		run.Add(checks.Check{Code: "mssql_permissions", Title: "Permissions", Status: checks.Warn, Category: checks.CatPermissions,
			Message: "Not db_owner: BeziSync cannot create its signal table or enable CDC on it.",
			Fix:     &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("ALTER ROLE db_owner ADD MEMBER %s;", msQ(cfg.User))}})
	}
	return nil
}

func nullStr(s sql.NullString) string {
	if !s.Valid {
		return "None"
	}
	return s.String
}

func (s *sqlserverSource) tableChecks(ctx context.Context, db *sql.Conn, run *checks.Run, tables []string) error {
	state := map[string][2]bool{}
	if err := eachRow(ctx, db, "SELECT s.name + '.' + t.name, CASE WHEN ct.object_id IS NULL THEN 0 ELSE 1 END, "+
		"CASE WHEN EXISTS (SELECT 1 FROM sys.indexes i WHERE i.object_id = t.object_id AND i.is_primary_key = 1) THEN 1 ELSE 0 END "+
		"FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id "+
		"LEFT JOIN cdc.change_tables ct ON ct.source_object_id = t.object_id", nil, func(rows *sql.Rows) error {
		var name string
		var cdc, pk int64
		if err := rows.Scan(&name, &cdc, &pk); err != nil {
			return err
		}
		state[name] = [2]bool{cdc != 0, pk != 0}
		return nil
	}); err != nil {
		return &MsError{err}
	}
	var missing, noCDC, unkeyed []string
	for _, t := range tables {
		st, ok := state[t]
		if !ok {
			missing = append(missing, t)
			continue
		}
		if !st[0] {
			noCDC = append(noCDC, t)
		}
		if !st[1] {
			unkeyed = append(unkeyed, t)
		}
	}
	if len(missing) > 0 {
		run.Add(checks.Check{Code: "mssql_tables_exist", Title: "Selected tables exist", Status: checks.Fail, Category: checks.CatTables, Blocking: true,
			Message: fmt.Sprintf("Not found: %s.", strings.Join(first(missing, 10), ", "))})
	}
	if len(noCDC) > 0 {
		var stmts []string
		for _, t := range first(noCDC, 20) {
			schema, name, _ := strings.Cut(t, ".")
			stmts = append(stmts, fmt.Sprintf("EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'%s', @role_name = NULL, @supports_net_changes = 0;", schema, name))
		}
		run.Add(checks.Check{Code: "mssql_tables_cdc", Title: "CDC on tables", Status: checks.Fail, Category: checks.CatTables, Blocking: true,
			Message: fmt.Sprintf("CDC is not enabled on %s.", strings.Join(first(noCDC, 10), ", ")),
			Fix:     &checks.Fix{Kind: "sql", Who: "DBA", Code: strings.Join(stmts, "\n")}})
	}
	if len(unkeyed) > 0 {
		run.Add(checks.Check{Code: "mssql_tables_keys", Title: "Row keys", Status: checks.Warn, Category: checks.CatTables,
			Message: fmt.Sprintf("No primary key on %s: events cannot be keyed per row.", strings.Join(first(unkeyed, 10), ", "))})
	}
	if len(missing) == 0 && len(noCDC) == 0 {
		run.Passed("mssql_tables", "Selected tables", checks.CatTables, fmt.Sprintf("%d tables have CDC enabled.", len(tables)), "")
	}
	return nil
}

func (s *sqlserverSource) Discover(ctx context.Context, conn Resolved) (Discovery, error) {
	db, done, err := msConnect(ctx, conn, 10*time.Second)
	if err != nil {
		return Discovery{}, err
	}
	defer done()
	type trow struct {
		schema, name string
		count        sql.NullInt64
		cdc          int64
	}
	var trows []trow
	if err := eachRow(ctx, db, fmt.Sprintf("SELECT TOP (%d) s.name, t.name, SUM(p.rows), ", 10_001)+
		"CASE WHEN ct.object_id IS NULL THEN 0 ELSE 1 END "+
		"FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id "+
		"LEFT JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1) "+
		"LEFT JOIN cdc.change_tables ct ON ct.source_object_id = t.object_id "+
		fmt.Sprintf("WHERE t.is_ms_shipped = 0 AND s.name NOT IN ('cdc', '%s') ", msManagedSchema)+
		"GROUP BY s.name, t.name, ct.object_id ORDER BY s.name, t.name", nil, func(rows *sql.Rows) error {
		var r trow
		if err := rows.Scan(&r.schema, &r.name, &r.count, &r.cdc); err != nil {
			return err
		}
		trows = append(trows, r)
		return nil
	}); err != nil {
		return Discovery{}, err
	}
	pks := map[[2]string][]string{}
	if err := eachRow(ctx, db, "SELECT s.name, t.name, c.name FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id "+
		"AND ic.index_id = i.index_id JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "+
		"JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id "+
		"WHERE i.is_primary_key = 1 ORDER BY ic.key_ordinal", nil, func(rows *sql.Rows) error {
		var schema, table, column string
		if err := rows.Scan(&schema, &table, &column); err != nil {
			return err
		}
		k := [2]string{schema, table}
		pks[k] = append(pks[k], column)
		return nil
	}); err != nil {
		return Discovery{}, err
	}
	cols := map[[2]string][]Column{}
	if err := eachRow(ctx, db, "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "+
		"ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION", nil, func(rows *sql.Rows) error {
		var schema, table, column, dtype, nullable string
		if err := rows.Scan(&schema, &table, &column, &dtype, &nullable); err != nil {
			return err
		}
		k := [2]string{schema, table}
		cols[k] = append(cols[k], Column{Name: column, Type: dtype, Nullable: nullable == "YES", PII: LooksLikePII(column)})
		return nil
	}); err != nil {
		return Discovery{}, err
	}
	var tables []TableInfo
	for _, r := range first(trows, 10_000) {
		var warnings []string
		if r.cdc == 0 {
			warnings = append(warnings, "CDC is not enabled on this table yet.")
		}
		pk := pks[[2]string{r.schema, r.name}]
		if len(pk) == 0 {
			warnings = append(warnings, "No primary key — events cannot be keyed per row.")
		}
		var count *int64
		if r.count.Valid {
			count = &r.count.Int64
		}
		tables = append(tables, TableInfo{Schema: r.schema, Name: r.name, RowsEstimate: count, PrimaryKey: pk,
			Columns: cols[[2]string{r.schema, r.name}], Warnings: warnings, Selectable: true})
	}
	return NewDiscovery(tables, len(trows) > 10_000), nil
}

func (s *sqlserverSource) RoutePattern(c CompileContext) (string, string) {
	naming := strings.ReplaceAll(c.Spec.Sink.Naming, "{schema}", "$2")
	naming = strings.ReplaceAll(naming, "{table}", "$3")
	naming = strings.ReplaceAll(naming, "{pipeline}", c.Name)
	return JavaRegexLiteral(c.Prefix()) + `\.([^.]+)\.([^.]+)\.([^.]+)`, naming
}

// TopicPattern matches "<prefix>.<database>.<schema>.<table>" (topics carry the database).
func (s *sqlserverSource) TopicPattern(c CompileContext, table string) string {
	return JavaRegexLiteral(c.Prefix()) + `\.[^.]+\.` + JavaRegexLiteral(table)
}

func boolStr(b bool) string { return pick(b, "true", "false") }

func (s *sqlserverSource) CompileSource(conn Resolved, c CompileContext) (map[string]string, map[string]string, bool, error) {
	cfg, sec, err := msCfg(conn)
	if err != nil {
		return nil, nil, false, err
	}
	spec := c.Spec
	tables := append(append([]string{}, spec.Source.Tables.Include...), msSignalTable(c))
	props := map[string]string{
		"debezium.source.connector.class":                                        "io.debezium.connector.sqlserver.SqlServerConnector",
		"debezium.source.database.hostname":                                      cfg.Host,
		"debezium.source.database.port":                                          strconv.Itoa(cfg.Port),
		"debezium.source.database.user":                                          cfg.User,
		"debezium.source.database.password":                                      "${BZ_SRC_PASSWORD}",
		"debezium.source.database.names":                                         cfg.Database,
		"debezium.source.database.encrypt":                                       boolStr(cfg.Encrypt),
		"debezium.source.database.trustServerCertificate":                        boolStr(cfg.TrustCert),
		"debezium.source.table.include.list":                                     joinRegex(tables),
		"debezium.source.signal.enabled.channels":                                "source",
		"debezium.source.signal.data.collection":                                 cfg.Database + "." + msSignalTable(c),
		"debezium.source.heartbeat.interval.ms":                                  strconv.Itoa(spec.Policies.HeartbeatSeconds * 1000),
		"debezium.source.schema.history.internal.store.only.captured.tables.ddl": "true",
		"debezium.source.include.schema.changes":                                 "false",
	}
	excludes, masks := ColumnRules(spec)
	if len(excludes) > 0 {
		props["debezium.source.column.exclude.list"] = joinRegex(excludes)
	}
	if len(masks) > 0 {
		props["debezium.source.column.mask.hash.v2.SHA-256.with.salt."+Salt(c.PipelineID)] = joinRegex(masks)
	}
	return props, map[string]string{"BZ_SRC_PASSWORD": PyStr(sec["password"])}, true, nil
}

func (s *sqlserverSource) Prepare(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	name := "signal_" + c.ShortID
	db, done, err := msConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	if _, err := db.ExecContext(ctx, fmt.Sprintf("IF SCHEMA_ID('%s') IS NULL EXEC('CREATE SCHEMA %s')", msManagedSchema, msQ(msManagedSchema))); err != nil {
		return nil, err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("IF OBJECT_ID('%s.%s') IS NULL CREATE TABLE %s.%s ", msManagedSchema, name, msQ(msManagedSchema), msQ(name))+
		"(id VARCHAR(64) PRIMARY KEY, type VARCHAR(64) NOT NULL, data VARCHAR(4000))"); err != nil {
		return nil, err
	}
	var n int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cdc.change_tables ct JOIN sys.tables t ON ct.source_object_id = t.object_id "+
		fmt.Sprintf("JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = '%s' AND t.name = @p1", msManagedSchema), name).Scan(&n); err != nil {
		return nil, err
	}
	if n == 0 {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'%s', ", msManagedSchema, name)+
			"@role_name = NULL, @supports_net_changes = 0"); err != nil {
			return nil, err
		}
	}
	return []ArtifactResult{{Kind: "table", Name: msSignalTable(c), Outcome: "exists", Detail: "CDC enabled for signals."}}, nil
}

func (s *sqlserverSource) Cleanup(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	name := "signal_" + c.ShortID
	db, done, err := msConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	var id sql.NullInt64
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT OBJECT_ID('%s.%s')", msManagedSchema, name)).Scan(&id); err != nil {
		return nil, err
	}
	if !id.Valid {
		return []ArtifactResult{{Kind: "table", Name: msSignalTable(c), Outcome: "not_found"}}, nil
	}
	_, _ = db.ExecContext(ctx, fmt.Sprintf("EXEC sys.sp_cdc_disable_table @source_schema = N'%s', @source_name = N'%s', ", msManagedSchema, name)+
		"@capture_instance = N'all'")
	if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s.%s", msQ(msManagedSchema), msQ(name))); err != nil {
		return nil, err
	}
	return []ArtifactResult{{Kind: "table", Name: msSignalTable(c), Outcome: "removed", Detail: "CDC change tables of your own tables are left enabled."}}, nil
}

func (s *sqlserverSource) Signal(ctx context.Context, conn Resolved, c CompileContext, signalType string, data map[string]any) (string, error) {
	cfg, _, err := msCfg(conn)
	if err != nil {
		return "", err
	}
	signalID := hex.EncodeToString(uuidBytes())
	// Debezium expects fully qualified data-collections (database.schema.table) for SQL Server.
	if dc, ok := data["data-collections"]; ok {
		copied := map[string]any{}
		for k, v := range data {
			copied[k] = v
		}
		var qualified []any
		if list, ok := dc.([]any); ok {
			for _, t := range list {
				qualified = append(qualified, cfg.Database+"."+PyStr(t))
			}
		} else if list, ok := dc.([]string); ok {
			for _, t := range list {
				qualified = append(qualified, cfg.Database+"."+t)
			}
		}
		if qualified == nil {
			qualified = []any{}
		}
		copied["data-collections"] = qualified
		data = copied
	}
	db, done, err := msConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return "", err
	}
	defer done()
	payload, err := PyDumps(data)
	if err != nil {
		return "", err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s.%s (id, type, data) VALUES (@p1, @p2, @p3)", msQ(msManagedSchema), msQ("signal_"+c.ShortID)),
		signalID, signalType, payload)
	return signalID, err
}

func (s *sqlserverSource) Probe(ctx context.Context, conn Resolved, c CompileContext, _ int64) (*SourceImpact, error) {
	var position *string
	if OffsetsReader != nil {
		if rows, err := OffsetsReader(ctx, c.ShortID); err == nil {
			for _, row := range rows {
				if raw, ok := row["raw"].(map[string]any); ok {
					if v, ok := raw["commit_lsn"]; ok && v != nil && PyStr(v) != "" {
						p := PyStr(v)
						position = &p
						break
					}
				}
			}
		}
	}
	db, done, err := msConnect(ctx, conn, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	// Each measurement needs its own permission; one that fails stays unknown instead of failing the probe.
	var minLSNs []string
	if tables := c.Spec.Source.Tables.Include; len(tables) > 0 {
		args := make([]any, len(tables))
		marks := make([]string, len(tables))
		for i, t := range tables {
			args[i] = t
			marks[i] = fmt.Sprintf("@p%d", i+1)
		}
		_ = eachRow(ctx, db, "SELECT CONVERT(varchar(20), sys.fn_cdc_get_min_lsn(ct.capture_instance), 2) FROM cdc.change_tables ct "+
			"JOIN sys.tables t ON ct.source_object_id = t.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id "+
			"WHERE s.name + '.' + t.name IN ("+strings.Join(marks, ", ")+")", args, func(rows *sql.Rows) error {
			vals, err := scanStrings(rows)
			if err == nil && vals[0].Valid {
				minLSNs = append(minLSNs, vals[0].String)
			}
			return err
		})
	}
	var retention *int64
	_ = eachRow(ctx, db, "EXEC sys.sp_cdc_help_jobs", nil, func(rows *sql.Rows) error {
		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		vals, err := scanStrings(rows)
		if err != nil {
			return err
		}
		row := map[string]string{}
		for i, col := range cols {
			row[col] = vals[i].String
		}
		if row["job_type"] == "cleanup" {
			if n, err := strconv.ParseInt(row["retention"], 10, 64); err == nil {
				retention = &n
			}
		}
		return nil
	})
	var reuse sql.NullString
	_ = db.QueryRowContext(ctx, "SELECT log_reuse_wait_desc FROM sys.databases WHERE database_id = DB_ID()").Scan(&reuse)
	var used, size sql.NullInt64
	// The DMV needs VIEW SERVER PERFORMANCE STATE on SQL Server 2022; the database's own file
	// catalogue gives the same numbers to a db_owner.
	if err := db.QueryRowContext(ctx, "SELECT used_log_space_in_bytes, total_log_size_in_bytes FROM sys.dm_db_log_space_usage").Scan(&used, &size); err != nil {
		_ = db.QueryRowContext(ctx, "SELECT SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS bigint)) * 8192, SUM(CAST(size AS bigint)) * 8192 "+
			"FROM sys.database_files WHERE type = 1").Scan(&used, &size)
	}
	var reuseP *string
	if reuse.Valid {
		reuseP = &reuse.String
	}
	var usedP, sizeP *int64
	if used.Valid && size.Valid && size.Int64 > 0 {
		usedP, sizeP = &used.Int64, &size.Int64
	}
	return mssqlImpact(minLSNs, position, retention, reuseP, usedP, sizeP), nil
}

// msLSN turns an LSN as Debezium stores it ("0000002d:00000a38:0003") or as SQL Server prints
// binary(10) ("0000002D00000A380003") into one comparable form, or "" when it isn't one.
func msLSN(s string) string {
	h := strings.ToUpper(strings.ReplaceAll(s, ":", ""))
	if len(h) != 20 {
		return ""
	}
	if _, err := hex.DecodeString(h); err != nil {
		return ""
	}
	return h
}

// mssqlImpact reports SQL Server's CDC retention, whether the stored position is still inside it
// (compared with the oldest change kept for the captured tables; a capture instance with nothing
// readable reports zeros and is skipped), and the transaction log.
func mssqlImpact(minLSNs []string, position *string, retentionMinutes *int64, reuseWait *string, logUsed, logSize *int64) *SourceImpact {
	impact := &SourceImpact{LogUsedBytes: logUsed, LogSizeBytes: logSize}
	if retentionMinutes != nil {
		w := max(*retentionMinutes, 0) * 60
		impact.WindowSeconds = &w
	}
	if reuseWait != nil {
		st := strings.ToLower(*reuseWait)
		impact.LogStatus = &st
	}
	oldest := ""
	for _, m := range minLSNs {
		if l := msLSN(m); l != "" && strings.Trim(l, "0") != "" && (oldest == "" || l < oldest) {
			oldest = l
		}
	}
	if position != nil {
		if pos := msLSN(*position); pos != "" && oldest != "" {
			kept := pos >= oldest
			impact.PositionKept = &kept
		}
	}
	parts := []string{}
	if impact.WindowSeconds != nil {
		parts = append(parts, fmt.Sprintf("CDC keeps %d h of changes", *impact.WindowSeconds/3600))
	}
	switch {
	case impact.PositionKept == nil:
	case *impact.PositionKept:
		parts = append(parts, "the pipeline's position is inside it")
	default:
		impact.Lost = true
		r := "SQL Server's CDC cleanup removed changes this pipeline had not read; they are lost."
		impact.Risk = &r
		impact.RiskFix = "Re-snapshot the tables (Snapshots tab). Raise the cleanup retention (sys.sp_cdc_change_job @job_type = 'cleanup', " +
			"@retention = <minutes>) so a pause can't outlast it."
	}
	if !impact.Lost && logUsed != nil && logSize != nil && reuseWait != nil && strings.EqualFold(*reuseWait, "REPLICATION") &&
		float64(*logUsed) >= float64(*logSize)*0.8 {
		r := fmt.Sprintf("The transaction log is %d%% full and can't be truncated until the CDC capture job reads it.", *logUsed*100 / *logSize)
		impact.Risk = &r
		impact.RiskFix = "Check that SQL Server Agent and the CDC capture job are running; the log grows until they catch up."
	}
	if len(parts) > 0 {
		impact.Summary = strings.Join(parts, "; ") + "."
	} else {
		impact.Summary = "Nothing could be measured (needs db_owner on the database)."
	}
	return impact
}
