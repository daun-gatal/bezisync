package connectors

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

// MySQL and MariaDB source (binlog, row-based).

const (
	myManagedDB = "bezisync"
	myMaxTables = 10_000
)

var (
	mySystemDBs          = []any{"mysql", "information_schema", "performance_schema", "sys", myManagedDB}
	myRequiredPrivileges = []string{"SELECT", "RELOAD", "SHOW DATABASES", "REPLICATION SLAVE", "REPLICATION CLIENT"}
)

// OffsetsReader reads a pipeline's stored Debezium offsets (set by the offsets service;
// injected to avoid an import cycle).
var OffsetsReader func(ctx context.Context, shortID string) ([]map[string]any, error)

type mysqlSource struct{ BaseSource }

func init() { Register(&mysqlSource{BaseSource{Title: "MySQL / MariaDB"}}) }

func (*mysqlSource) Kind() string { return "source" }
func (*mysqlSource) Type() string { return "mysql" }

type myConfig struct {
	Host, User, SSLMode string
	Port                int
}

func myCfg(conn Resolved) (myConfig, map[string]any, error) {
	d, _ := Describe("source", "mysql")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return myConfig{}, nil, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return myConfig{}, nil, err
	}
	return myConfig{Host: Str(cfg, "host"), Port: Int(cfg, "port"), User: Str(cfg, "user"), SSLMode: Str(cfg, "ssl_mode")}, sec, nil
}

func myQ(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

// myConnect opens a session. TLS follows PyMySQL: "required" and stricter modes encrypt
// without verifying (no CA is configured); "preferred" and "disabled" do not use TLS.
func myConnect(ctx context.Context, conn Resolved, timeout time.Duration) (*sql.Conn, func(), error) {
	cfg, sec, err := myCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	host, port, closeTunnel, err := Tunnel(ctx, conn.Config, conn.Secrets, cfg.Host, cfg.Port)
	if err != nil {
		return nil, nil, err
	}
	mc := mysql.NewConfig()
	mc.User = cfg.User
	mc.Passwd = PyStr(sec["password"])
	mc.Net = "tcp"
	mc.Addr = net.JoinHostPort(host, strconv.Itoa(port))
	mc.Timeout = timeout
	mc.ReadTimeout = 15 * time.Second
	mc.WriteTimeout = 15 * time.Second
	mc.AllowNativePasswords = true
	// Client-side parameters, like PyMySQL: statements such as SHOW … LIKE ? cannot be
	// prepared on the server.
	mc.InterpolateParams = true
	// The connection charset is a driver option: Params would become SET statements.
	if err := mc.Apply(mysql.Charset("utf8mb4", "")); err != nil {
		closeTunnel()
		return nil, nil, err
	}
	switch cfg.SSLMode {
	case "required", "verify_ca", "verify_identity":
		mc.TLSConfig = "skip-verify"
	default:
		mc.TLSConfig = "false"
	}
	connector, err := mysql.NewConnector(mc)
	if err != nil {
		closeTunnel()
		return nil, nil, err
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	c, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		closeTunnel()
		return nil, nil, &MyOperationalError{err}
	}
	return c, func() { _ = c.Close(); _ = db.Close(); closeTunnel() }, nil
}

// MyOperationalError is a connection-level failure (PyMySQL's OperationalError).
type MyOperationalError struct{ Err error }

func (e *MyOperationalError) Error() string { return e.Err.Error() }
func (e *MyOperationalError) Unwrap() error { return e.Err }

func myVar(ctx context.Context, c *sql.Conn, name string) (*string, error) {
	var k, v sql.NullString
	err := c.QueryRowContext(ctx, "SHOW GLOBAL VARIABLES LIKE ?", name).Scan(&k, &v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s := v.String
	if !v.Valid {
		s = "None"
	}
	return &s, nil
}

func deref(p *string, def string) string {
	if p == nil || *p == "" {
		return def
	}
	return *p
}

// MyServerID is a stable, pipeline-specific replica server id.
func MyServerID(pipelineID string) int64 {
	return 1_000_000 + int64(crc32.ChecksumIEEE([]byte(pipelineID)))%3_000_000_000
}

func mySignalTable(c CompileContext) string { return myManagedDB + ".signal_" + c.ShortID }
func myCanaryTable(c CompileContext) string { return myManagedDB + ".canary_" + c.ShortID }

func (*mysqlSource) CanaryTable(_ Resolved, c CompileContext) (string, error) {
	return myCanaryTable(c), nil
}

func isMyOperational(err error) bool {
	var op *MyOperationalError
	if errors.As(err, &op) {
		return true
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number >= 1040 && me.Number <= 1053 || me.Number == 2002 || me.Number == 2003 || me.Number == 2006 || me.Number == 2013
	}
	var ne net.Error
	return errors.As(err, &ne) || errors.Is(err, mysql.ErrInvalidConn)
}

func (s *mysqlSource) Check(ctx context.Context, conn Resolved, tables []string) (checks.Report, error) {
	started := time.Now()
	cfg, _, err := myCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	var reachable bool
	if Str(conn.Config, "ssh_host") != "" {
		reachable = SSHChecks(ctx, run, conn.Config, conn.Secrets, cfg.Host, cfg.Port, "mysql")
	} else {
		reachable = NetworkCheck(ctx, run, cfg.Host, cfg.Port, "mysql_network", "the database", 0)
	}
	if !reachable {
		return checks.Summarize(run.Checks, started), nil
	}
	fail := func(err error) (checks.Report, error) {
		message := "Could not sign in to the database."
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1045 {
			message = "The database rejected the password."
		}
		run.Add(checks.Check{Code: "mysql_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: message, Technical: err.Error()})
		return checks.Summarize(run.Checks, started), nil
	}
	db, done, err := myConnect(ctx, conn, 6*time.Second)
	if err != nil {
		if isMyOperational(err) {
			return fail(err)
		}
		return checks.Report{}, err
	}
	defer done()
	run.Passed("mysql_login", "Sign in", checks.CatAuth, fmt.Sprintf("Signed in as %s.", cfg.User), "")
	if err := s.serverChecks(ctx, db, run, conn.Provider, cfg.User); err != nil {
		if isMyOperational(err) {
			return fail(err)
		}
		return checks.Report{}, err
	}
	if len(tables) > 0 {
		if err := s.tableChecks(ctx, db, run, tables); err != nil {
			if isMyOperational(err) {
				return fail(err)
			}
			return checks.Report{}, err
		}
	}
	return checks.Summarize(run.Checks, started), nil
}

func (s *mysqlSource) serverChecks(ctx context.Context, c *sql.Conn, run *checks.Run, provider, user string) error {
	v, err := myVar(ctx, c, "version")
	if err != nil {
		return err
	}
	version := deref(v, "")
	mariadb := strings.Contains(strings.ToLower(version), "mariadb")
	run.Passed("mysql_version", "Server version", checks.CatCDC, fmt.Sprintf("%s %s.", pick(mariadb, "MariaDB", "MySQL"), version), "")
	lb, err := myVar(ctx, c, "log_bin")
	if err != nil {
		return err
	}
	logBin := strings.ToUpper(deref(lb, ""))
	if logBin != "ON" {
		run.Add(checks.Check{Code: "mysql_binlog", Title: "Binary log", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: "The binary log is off, so changes cannot be read.", Technical: "log_bin = " + deref(&logBin, "OFF"),
			Fix: &checks.Fix{Kind: "config", RestartRequired: true, Who: "DBA", Code: "# my.cnf\n[mysqld]\nlog_bin = mysql-bin\nserver_id = 1\nbinlog_format = ROW"}})
	} else {
		run.Passed("mysql_binlog", "Binary log", checks.CatCDC, "The binary log is on.", "")
	}
	bf, err := myVar(ctx, c, "binlog_format")
	if err != nil {
		return err
	}
	format := strings.ToUpper(deref(bf, ""))
	if format != "ROW" {
		fix := &checks.Fix{Kind: "sql", Who: "DBA", Code: "SET GLOBAL binlog_format = 'ROW';\n-- and persist it in my.cnf"}
		if provider == "aws_rds" {
			fix = &checks.Fix{Kind: "shell", Who: "DBA", Code: "aws rds modify-db-parameter-group --db-parameter-group-name <group> " +
				"--parameters ParameterName=binlog_format,ParameterValue=ROW,ApplyMethod=immediate"}
		}
		run.Add(checks.Check{Code: "mysql_binlog_format", Title: "Row-based binlog", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: "Changes are logged as statements, not rows.", Technical: "binlog_format = " + format, Fix: fix})
	} else {
		run.Passed("mysql_binlog_format", "Row-based binlog", checks.CatCDC, "binlog_format = ROW.", "")
	}
	ri, err := myVar(ctx, c, "binlog_row_image")
	if err != nil {
		return err
	}
	image := strings.ToUpper(deref(ri, "FULL"))
	if image != "FULL" {
		run.Add(checks.Check{Code: "mysql_row_image", Title: "Full row images", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: "Updates would only contain changed columns.", Technical: "binlog_row_image = " + image,
			Fix: &checks.Fix{Kind: "sql", Who: "DBA", Code: "SET GLOBAL binlog_row_image = 'FULL';"}})
	}
	gm, err := myVar(ctx, c, "gtid_mode")
	if err != nil {
		return err
	}
	gtid := strings.ToUpper(deref(gm, ""))
	if !mariadb && gtid != "ON" {
		run.Add(checks.Check{Code: "mysql_gtid", Title: "Failover safety", Status: checks.Warn, Category: checks.CatResilience,
			Message:   "GTIDs are off: after a failover to a replica the pipeline must re-snapshot.",
			Technical: "gtid_mode = " + deref(&gtid, "OFF"),
			Fix: &checks.Fix{Kind: "sql", Who: "DBA", Code: "SET GLOBAL enforce_gtid_consistency = ON;\nSET GLOBAL gtid_mode = OFF_PERMISSIVE;\n" +
				"SET GLOBAL gtid_mode = ON_PERMISSIVE;\nSET GLOBAL gtid_mode = ON;"}})
	} else {
		run.Passed("mysql_gtid", "Failover safety", checks.CatResilience, "GTIDs are enabled.", "")
	}
	if err := s.retentionCheck(ctx, c, run, provider); err != nil {
		return err
	}
	var parts []string
	if err := eachRow(ctx, c, "SHOW GRANTS", nil, func(rows *sql.Rows) error {
		vals, err := scanStrings(rows)
		if err != nil {
			return err
		}
		parts = append(parts, strings.ToUpper(vals[0].String))
		return nil
	}); err != nil {
		return err
	}
	grants := strings.Join(parts, " ")
	var missing []string
	for _, p := range myRequiredPrivileges {
		if !strings.Contains(grants, p) && !strings.Contains(grants, "ALL PRIVILEGES") {
			missing = append(missing, p)
		}
	}
	if len(missing) > 0 {
		run.Add(checks.Check{Code: "mysql_grants", Title: "Permissions", Status: checks.Fail, Category: checks.CatPermissions, Blocking: true,
			Message: fmt.Sprintf("%s is missing %s.", user, strings.Join(missing, ", ")),
			Fix: &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("GRANT %s ON *.* TO '%s'@'%%';\n", strings.Join(myRequiredPrivileges, ", "), user) +
				fmt.Sprintf("GRANT ALL ON %s.* TO '%s'@'%%';", myManagedDB, user)}})
	} else {
		run.Passed("mysql_grants", "Permissions", checks.CatPermissions, "Has the replication and read permissions it needs.", "")
	}
	return nil
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (s *mysqlSource) retentionCheck(ctx context.Context, c *sql.Conn, run *checks.Run, provider string) error {
	var seconds *int64
	set := func(v int64) { seconds = &v }
	if provider == "aws_rds" {
		err := eachRow(ctx, c, "CALL mysql.rds_show_configuration", nil, func(rows *sql.Rows) error {
			vals, err := scanStrings(rows)
			if err != nil {
				return err
			}
			if len(vals) >= 2 && vals[0].String == "binlog retention hours" {
				if !vals[1].Valid || vals[1].String == "NULL" {
					set(0)
				} else if n, err := strconv.ParseInt(vals[1].String, 10, 64); err == nil {
					set(n * 3600)
				}
			}
			return nil
		})
		if err != nil {
			var me *mysql.MySQLError
			if !errors.As(err, &me) && !isMyOperational(err) {
				return err
			}
			seconds = nil
		}
		if seconds != nil && *seconds < 24*3600 {
			run.Add(checks.Check{Code: "mysql_retention", Title: "Binlog retention", Status: checks.Fail, Category: checks.CatResilience, Blocking: true,
				Message: "RDS purges binlogs almost immediately; the pipeline would lose its place.",
				Fix:     &checks.Fix{Kind: "sql", Who: "DBA", Code: "CALL mysql.rds_set_configuration('binlog retention hours', 72);"}})
			return nil
		}
	} else {
		raw, err := myVar(ctx, c, "binlog_expire_logs_seconds")
		if err != nil {
			return err
		}
		if raw != nil && isDigits(*raw) {
			n, _ := strconv.ParseInt(*raw, 10, 64)
			set(n)
		}
		if seconds == nil || *seconds == 0 {
			days, err := myVar(ctx, c, "expire_logs_days")
			if err != nil {
				return err
			}
			seconds = nil
			if days != nil && *days != "" {
				if f, ok := domain.PyFloat(*days); ok && f > 0 {
					set(int64(f * 86400))
				}
			}
		}
	}
	if seconds != nil && *seconds > 0 && *seconds < 72*3600 {
		run.Add(checks.Check{Code: "mysql_retention", Title: "Binlog retention", Status: checks.Warn, Category: checks.CatResilience,
			Message: fmt.Sprintf("Binlogs are kept for %d h. A pipeline paused longer than that must re-snapshot.", *seconds/3600),
			Fix:     &checks.Fix{Kind: "sql", Who: "DBA", Code: "SET GLOBAL binlog_expire_logs_seconds = 604800;  -- 7 days"}})
	} else {
		msg := "Binlogs are not expired automatically."
		if seconds != nil && *seconds != 0 {
			msg = "Binlogs are kept long enough."
		}
		run.Passed("mysql_retention", "Binlog retention", checks.CatResilience, msg, "")
	}
	return nil
}

func (s *mysqlSource) tableChecks(ctx context.Context, c *sql.Conn, run *checks.Run, tables []string) error {
	type entry struct {
		name  string
		hasPK bool
	}
	var found []entry
	index := map[string]int{}
	for _, t := range tables {
		schema, name, ok := strings.Cut(t, ".")
		if !ok {
			return fmt.Errorf("not enough values to unpack (expected 2, got 1)")
		}
		var pk, exists int64
		if err := c.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema=? AND table_name=? "+
			"AND constraint_type='PRIMARY KEY'", schema, name).Scan(&pk); err != nil {
			return err
		}
		if err := c.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=? AND table_name=?", schema, name).Scan(&exists); err != nil {
			return err
		}
		if exists != 0 {
			key := schema + "." + name
			if i, ok := index[key]; ok {
				found[i].hasPK = pk != 0
			} else {
				index[key] = len(found)
				found = append(found, entry{key, pk != 0})
			}
		}
	}
	var missing []string
	for _, t := range tables {
		if _, ok := index[t]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		run.Add(checks.Check{Code: "mysql_tables_exist", Title: "Selected tables exist", Status: checks.Fail, Category: checks.CatTables, Blocking: true,
			Message: fmt.Sprintf("Not found: %s.", strings.Join(first(missing, 10), ", "))})
	}
	var unkeyed []string
	for _, f := range found {
		if !f.hasPK {
			unkeyed = append(unkeyed, f.name)
		}
	}
	if len(unkeyed) > 0 {
		var alters []string
		for _, t := range first(unkeyed, 20) {
			schema, name, _ := strings.Cut(t, ".")
			alters = append(alters, fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN bz_row_id BIGINT AUTO_INCREMENT PRIMARY KEY;", myQ(schema), myQ(name)))
		}
		run.Add(checks.Check{Code: "mysql_tables_keys", Title: "Row keys", Status: checks.Warn, Category: checks.CatTables,
			Message: fmt.Sprintf("No primary key on %s: events cannot be keyed per row.", strings.Join(first(unkeyed, 10), ", ")),
			Fix:     &checks.Fix{Kind: "sql", Who: "DBA", Code: strings.Join(alters, "\n")}})
	}
	if len(found) > 0 && len(missing) == 0 {
		run.Passed("mysql_tables", "Selected tables", checks.CatTables, fmt.Sprintf("%d tables can be captured.", len(found)), "")
	}
	return nil
}

func nullInt(s sql.NullString) *int64 {
	if !s.Valid {
		return nil
	}
	n, err := strconv.ParseInt(s.String, 10, 64)
	if err != nil {
		if u, err2 := strconv.ParseUint(s.String, 10, 64); err2 == nil {
			n = int64(u)
		} else if f, err3 := strconv.ParseFloat(s.String, 64); err3 == nil {
			n = int64(f)
		} else {
			return nil
		}
	}
	return &n
}

func (s *mysqlSource) Discover(ctx context.Context, conn Resolved) (Discovery, error) {
	c, done, err := myConnect(ctx, conn, 10*time.Second)
	if err != nil {
		return Discovery{}, err
	}
	defer done()
	ph := strings.TrimSuffix(strings.Repeat("?, ", len(mySystemDBs)), ", ")
	type trow struct {
		schema, name string
		approx, size sql.NullString
	}
	var trows []trow
	if err := eachRow(ctx, c, fmt.Sprintf("SELECT table_schema, table_name, table_rows, data_length + index_length FROM information_schema.tables "+
		"WHERE table_type='BASE TABLE' AND table_schema NOT IN (%s) ORDER BY 1, 2 LIMIT %d", ph, myMaxTables+1), mySystemDBs, func(rows *sql.Rows) error {
		var r trow
		if err := rows.Scan(&r.schema, &r.name, &r.approx, &r.size); err != nil {
			return err
		}
		trows = append(trows, r)
		return nil
	}); err != nil {
		return Discovery{}, err
	}
	pks := map[[2]string][]string{}
	if err := eachRow(ctx, c, fmt.Sprintf("SELECT table_schema, table_name, column_name FROM information_schema.key_column_usage "+
		"WHERE constraint_name='PRIMARY' AND table_schema NOT IN (%s) ORDER BY ordinal_position", ph), mySystemDBs, func(rows *sql.Rows) error {
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
	if err := eachRow(ctx, c, fmt.Sprintf("SELECT table_schema, table_name, column_name, data_type, is_nullable='YES' FROM information_schema.columns "+
		"WHERE table_schema NOT IN (%s) ORDER BY table_schema, table_name, ordinal_position LIMIT 200000", ph), mySystemDBs, func(rows *sql.Rows) error {
		var schema, table, column, dtype string
		var nullable sql.NullInt64
		if err := rows.Scan(&schema, &table, &column, &dtype, &nullable); err != nil {
			return err
		}
		k := [2]string{schema, table}
		cols[k] = append(cols[k], Column{Name: column, Type: dtype, Nullable: nullable.Valid && nullable.Int64 != 0, PII: LooksLikePII(column)})
		return nil
	}); err != nil {
		return Discovery{}, err
	}
	truncated := len(trows) > myMaxTables
	var tables []TableInfo
	for _, r := range first(trows, myMaxTables) {
		pk := pks[[2]string{r.schema, r.name}]
		var warnings []string
		if len(pk) == 0 {
			warnings = []string{"No primary key — events cannot be keyed per row."}
		}
		tables = append(tables, TableInfo{Schema: r.schema, Name: r.name, RowsEstimate: nullInt(r.approx), BytesEstimate: nullInt(r.size),
			PrimaryKey: pk, Columns: cols[[2]string{r.schema, r.name}], Warnings: warnings, Selectable: true})
	}
	return NewDiscovery(tables, truncated), nil
}

func (s *mysqlSource) CompileSource(conn Resolved, c CompileContext) (map[string]string, map[string]string, bool, error) {
	cfg, sec, err := myCfg(conn)
	if err != nil {
		return nil, nil, false, err
	}
	spec := c.Spec
	tables := append(append([]string{}, spec.Source.Tables.Include...), mySignalTable(c))
	if spec.Policies.DeliveryCanary {
		tables = append(tables, myCanaryTable(c))
	}
	dbSet := map[string]bool{}
	for _, t := range tables {
		db, _, _ := strings.Cut(t, ".")
		dbSet[db] = true
	}
	databases := make([]string, 0, len(dbSet))
	for d := range dbSet {
		databases = append(databases, d)
	}
	sort.Strings(databases)
	cls := "io.debezium.connector.mysql.MySqlConnector"
	if conn.Provider == "mariadb" {
		cls = "io.debezium.connector.mariadb.MariaDbConnector"
	}
	props := map[string]string{
		"debezium.source.connector.class":                                        cls,
		"debezium.source.database.hostname":                                      cfg.Host,
		"debezium.source.database.port":                                          strconv.Itoa(cfg.Port),
		"debezium.source.database.user":                                          cfg.User,
		"debezium.source.database.password":                                      "${BZ_SRC_PASSWORD}",
		"debezium.source.database.ssl.mode":                                      cfg.SSLMode,
		"debezium.source.database.server.id":                                     strconv.FormatInt(MyServerID(c.PipelineID), 10),
		"debezium.source.database.include.list":                                  joinRegex(databases),
		"debezium.source.table.include.list":                                     joinRegex(tables),
		"debezium.source.signal.enabled.channels":                                "source",
		"debezium.source.signal.data.collection":                                 mySignalTable(c),
		"debezium.source.heartbeat.interval.ms":                                  strconv.Itoa(spec.Policies.HeartbeatSeconds * 1000),
		"debezium.source.schema.history.internal.store.only.captured.tables.ddl": "true",
		"debezium.source.include.schema.changes":                                 "false",
	}
	if spec.Policies.HeartbeatSeconds != 0 {
		props["debezium.source.heartbeat.action.query"] = fmt.Sprintf(
			"INSERT INTO %s.heartbeat (pipeline, ts) VALUES ('%s', NOW(6)) ON DUPLICATE KEY UPDATE ts = NOW(6)", myManagedDB, c.PipelineID)
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

func (s *mysqlSource) WriteCanary(ctx context.Context, conn Resolved, c CompileContext, token string) error {
	db, done, err := myConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return err
	}
	defer done()
	table := myQ(myManagedDB) + "." + myQ("canary_"+c.ShortID)
	if _, err := db.ExecContext(ctx, "INSERT INTO "+table+" (id) VALUES (?)", token); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "DELETE FROM "+table+" WHERE sent_at < NOW(6) - INTERVAL 1 DAY")
	return err
}

func (s *mysqlSource) Prepare(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	db, done, err := myConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	stmts := []string{
		"CREATE DATABASE IF NOT EXISTS " + myQ(myManagedDB),
		"CREATE TABLE IF NOT EXISTS " + myQ(myManagedDB) + ".heartbeat (pipeline VARCHAR(64) PRIMARY KEY, ts DATETIME(6) NOT NULL)",
		"CREATE TABLE IF NOT EXISTS " + myQ(myManagedDB) + "." + myQ("signal_"+c.ShortID) + " " +
			"(id VARCHAR(64) PRIMARY KEY, type VARCHAR(64) NOT NULL, data VARCHAR(8192))",
	}
	if c.Spec.Policies.DeliveryCanary {
		stmts = append(stmts, "CREATE TABLE IF NOT EXISTS "+myQ(myManagedDB)+"."+myQ("canary_"+c.ShortID)+" "+
			"(id VARCHAR(64) PRIMARY KEY, sent_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6))")
	}
	for _, st := range stmts {
		if _, err := db.ExecContext(ctx, st); err != nil {
			return nil, err
		}
	}
	out := []ArtifactResult{
		{Kind: "table", Name: myManagedDB + ".heartbeat", Outcome: "exists"},
		{Kind: "table", Name: mySignalTable(c), Outcome: "exists"},
	}
	if c.Spec.Policies.DeliveryCanary {
		out = append(out, ArtifactResult{Kind: "table", Name: myCanaryTable(c), Outcome: "exists"})
	}
	return out, nil
}

func (s *mysqlSource) Cleanup(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	db, done, err := myConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	for _, st := range []string{
		"DROP TABLE IF EXISTS " + myQ(myManagedDB) + "." + myQ("signal_"+c.ShortID),
		"DROP TABLE IF EXISTS " + myQ(myManagedDB) + "." + myQ("canary_"+c.ShortID),
	} {
		if _, err := db.ExecContext(ctx, st); err != nil {
			return nil, err
		}
	}
	var n int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=? AND table_name='heartbeat'", myManagedDB).Scan(&n); err != nil {
		return nil, err
	}
	if n != 0 {
		if _, err := db.ExecContext(ctx, "DELETE FROM "+myQ(myManagedDB)+".heartbeat WHERE pipeline=?", c.PipelineID); err != nil {
			return nil, err
		}
	}
	return []ArtifactResult{{Kind: "table", Name: mySignalTable(c), Outcome: "removed", Detail: "MySQL keeps no per-reader state; nothing else to release."}}, nil
}

func (s *mysqlSource) Signal(ctx context.Context, conn Resolved, c CompileContext, signalType string, data map[string]any) (string, error) {
	signalID := hex.EncodeToString(uuidBytes())
	db, done, err := myConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return "", err
	}
	defer done()
	payload, err := PyDumps(data)
	if err != nil {
		return "", err
	}
	_, err = db.ExecContext(ctx, "INSERT INTO "+myQ(myManagedDB)+"."+myQ("signal_"+c.ShortID)+" (id, type, data) VALUES (?, ?, ?)", signalID, signalType, payload)
	return signalID, err
}

func (s *mysqlSource) Probe(ctx context.Context, conn Resolved, c CompileContext, limit int64) (*SourceImpact, error) {
	var positionFile *string
	if OffsetsReader != nil {
		if rows, err := OffsetsReader(ctx, c.ShortID); err == nil {
			for _, row := range rows {
				if raw, ok := row["raw"].(map[string]any); ok {
					if f, ok := raw["file"]; ok && f != nil && PyStr(f) != "" {
						s := PyStr(f)
						positionFile = &s
						break
					}
				}
			}
		}
	}
	db, done, err := myConnect(ctx, conn, 5*time.Second)
	if err != nil {
		return nil, err
	}
	var logs []binlogFile
	err = eachRow(ctx, db, "SHOW BINARY LOGS", nil, func(rows *sql.Rows) error {
		vals, err := scanStrings(rows)
		if err != nil {
			return err
		}
		size, _ := strconv.ParseInt(vals[1].String, 10, 64)
		logs = append(logs, binlogFile{vals[0].String, size})
		return nil
	})
	done()
	if err != nil {
		return nil, err
	}
	return mysqlImpact(logs, positionFile, limit), nil
}

// binlogFile is one row of SHOW BINARY LOGS, oldest first.
type binlogFile struct {
	name string
	size int64
}

// mysqlImpact places the pipeline's stored binlog position among the server's binlog files.
func mysqlImpact(logs []binlogFile, positionFile *string, limit int64) *SourceImpact {
	if len(logs) == 0 {
		return &SourceImpact{Summary: "No binary logs reported."}
	}
	if positionFile == nil {
		return &SourceImpact{Summary: fmt.Sprintf("%d binlog files on the server; position not stored yet.", len(logs))}
	}
	idx := -1
	for i, l := range logs {
		if l.name == *positionFile {
			idx = i
			break
		}
	}
	if idx < 0 {
		r := "The server purged the binlog this pipeline still needed; changes were lost."
		return &SourceImpact{Lost: true, ReadingFile: positionFile, Summary: "The binlog file at the stored position was purged.", Risk: &r,
			RiskFix: "Reset the pipeline's position and re-snapshot. Increase binlog retention."}
	}
	var retained int64
	for _, l := range logs[idx:] {
		retained += l.size
	}
	// "oldest": the next purge removes the file the pipeline reads. A lone file is never purged
	// (MySQL keeps the active binlog), so it is "ok".
	oldest := idx == 0 && len(logs) > 1
	status := pick(oldest, "oldest", "ok")
	rf := float64(retained)
	impact := &SourceImpact{RetainedBytes: &retained, LogStatus: &status, LimitBytes: &limit, ReadingFile: positionFile,
		Summary: fmt.Sprintf("Reading %s; %s of binlog not yet consumed", *positionFile, domain.HumanBytes(&rf))}
	lf := float64(limit)
	switch {
	case oldest:
		r := "The pipeline is reading the oldest binlog file; the next purge would lose its place."
		impact.Risk = &r
		impact.RiskFix = "Resume or speed up the pipeline now, or increase binlog retention."
	case retained > limit:
		r := fmt.Sprintf("%s of binlog is waiting to be read (limit %s).", domain.HumanBytes(&rf), domain.HumanBytes(&lf))
		impact.Risk = &r
		impact.RiskFix = "The pipeline is behind; check the destination and the connector size."
	}
	return impact
}
