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
)

// Vitess source: Debezium streams VStream events from VTGate over gRPC; BeziSync's checks and
// table list use VTGate's MySQL protocol. Its position is one GTID set per shard (VGTID), and
// how long the tablets keep their binlogs is not visible through VTGate.

const vtMaxTables = 10_000

// vtTabletTypes maps BeziSync's tablet type to Debezium's name for it.
var vtTabletTypes = map[string]string{"primary": "MASTER", "replica": "REPLICA", "rdonly": "RDONLY"}

// vtUnsupported are column types the Vitess connector does not capture.
var vtUnsupported = map[string]bool{"geometry": true, "point": true, "linestring": true, "polygon": true, "multipoint": true,
	"multilinestring": true, "multipolygon": true, "geometrycollection": true}

type vitessSource struct{ BaseSource }

func init() { Register(&vitessSource{BaseSource{Title: "Vitess"}}) }

func (*vitessSource) Kind() string { return "source" }
func (*vitessSource) Type() string { return "vitess" }

type vtConfig struct {
	Host, Keyspace, TabletType, User string
	GRPCPort, MySQLPort              int
}

func vtCfg(conn Resolved) (vtConfig, map[string]any, error) {
	d, _ := Describe("source", "vitess")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return vtConfig{}, nil, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return vtConfig{}, nil, err
	}
	return vtConfig{Host: Str(cfg, "host"), GRPCPort: Int(cfg, "port"), MySQLPort: Int(cfg, "mysql_port"), Keyspace: Str(cfg, "keyspace"),
		TabletType: Str(cfg, "tablet_type"), User: Str(cfg, "user")}, sec, nil
}

// vtConnect opens a MySQL-protocol session on VTGate (through the SSH tunnel when configured).
func vtConnect(ctx context.Context, conn Resolved, timeout time.Duration) (*sql.Conn, func(), error) {
	cfg, sec, err := vtCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	host, port, closeTunnel, err := Tunnel(ctx, conn.Config, conn.Secrets, cfg.Host, cfg.MySQLPort)
	if err != nil {
		return nil, nil, &MyOperationalError{err} // the tunnel to the MySQL port (the gRPC one is checked first)
	}
	mc := mysql.NewConfig()
	mc.User = cfg.User
	if mc.User == "" {
		mc.User = "bezisync" // VTGate without authentication accepts any user
	}
	mc.Passwd = Str(sec, "password")
	mc.Net = "tcp"
	mc.Addr = net.JoinHostPort(host, strconv.Itoa(port))
	mc.Timeout = timeout
	mc.ReadTimeout = 15 * time.Second
	mc.WriteTimeout = 15 * time.Second
	mc.AllowNativePasswords = true
	mc.AllowCleartextPasswords = true // VTGate's static auth may only offer mysql_clear_password
	mc.InterpolateParams = true
	mc.TLSConfig = "preferred"
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

func (s *vitessSource) Check(ctx context.Context, conn Resolved, tables []string) (checks.Report, error) {
	started := time.Now()
	cfg, _, err := vtCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	var reachable bool
	if Str(conn.Config, "ssh_host") != "" {
		reachable = SSHChecks(ctx, run, conn.Config, conn.Secrets, cfg.Host, cfg.GRPCPort, "vt")
	} else {
		reachable = NetworkCheck(ctx, run, cfg.Host, cfg.GRPCPort, "vt_grpc", "VTGate's gRPC port", 0)
		reachable = NetworkCheck(ctx, run, cfg.Host, cfg.MySQLPort, "vt_mysql", "VTGate's MySQL port", 0) && reachable
	}
	if !reachable {
		return checks.Summarize(run.Checks, started), nil
	}
	fail := func(err error) (checks.Report, error) {
		message := "Could not sign in to VTGate."
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1045 {
			message = "VTGate rejected the user name or password."
		}
		run.Add(checks.Check{Code: "vt_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: message, Technical: err.Error(), Why: "BeziSync reads tables through VTGate's MySQL port; Debezium streams over its gRPC port."})
		return checks.Summarize(run.Checks, started), nil
	}
	db, done, err := vtConnect(ctx, conn, 6*time.Second)
	if err != nil {
		if isMyOperational(err) {
			return fail(err)
		}
		return checks.Report{}, err
	}
	defer done()
	run.Passed("vt_login", "Sign in", checks.CatAuth, "Signed in to VTGate.", "")
	if err := s.serverChecks(ctx, db, run, cfg); err != nil {
		if isMyOperational(err) {
			return fail(err)
		}
		return checks.Report{}, err
	}
	if len(tables) > 0 && !run.Blocked() {
		if err := s.tableChecks(ctx, db, run, tables, cfg); err != nil {
			if isMyOperational(err) {
				return fail(err)
			}
			return checks.Report{}, err
		}
	}
	return checks.Summarize(run.Checks, started), nil
}

func (s *vitessSource) serverChecks(ctx context.Context, db *sql.Conn, run *checks.Run, cfg vtConfig) error {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT @@version").Scan(&version); err != nil {
		return err
	}
	if !strings.Contains(version, "Vitess") {
		run.Add(checks.Check{Code: "vt_version", Title: "VTGate", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: "This is a MySQL server, not VTGate.", Technical: "@@version = " + version,
			Why: "For a plain MySQL server, use a MySQL / MariaDB connection."})
		return nil
	}
	run.Passed("vt_version", "VTGate", checks.CatCDC, "Connected to VTGate (MySQL "+strings.TrimSuffix(version, "-Vitess")+").", "")
	var keyspaces []string
	if err := eachRow(ctx, db, "SHOW KEYSPACES", nil, func(rows *sql.Rows) error {
		var k string
		if err := rows.Scan(&k); err != nil {
			return err
		}
		keyspaces = append(keyspaces, k)
		return nil
	}); err != nil {
		return err
	}
	if !containsStr(keyspaces, cfg.Keyspace) {
		run.Add(checks.Check{Code: "vt_keyspace", Title: "Keyspace", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message:   fmt.Sprintf("VTGate has no keyspace %s.", cfg.Keyspace),
			Technical: "Keyspaces: " + strings.Join(first(keyspaces, 20), ", ")})
		return nil
	}
	run.Passed("vt_keyspace", "Keyspace", checks.CatCDC, fmt.Sprintf("Keyspace %s exists.", cfg.Keyspace), "")
	shards := map[string]bool{}
	serving := map[string]bool{}
	want := strings.ToUpper(cfg.TabletType)
	if err := eachRow(ctx, db, "SHOW VITESS_TABLETS", nil, func(rows *sql.Rows) error {
		vals, err := scanStrings(rows)
		if err != nil {
			return err
		}
		if len(vals) < 5 || vals[1].String != cfg.Keyspace {
			return nil
		}
		shards[vals[2].String] = true
		if vals[3].String == want && vals[4].String == "SERVING" {
			serving[vals[2].String] = true
		}
		return nil
	}); err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) { // older VTGates or restricted users: the tablet list is optional
			run.Add(checks.Check{Code: "vt_tablets", Title: "Tablets", Status: checks.Warn, Category: checks.CatCDC,
				Message: "BeziSync can't list the tablets, so it can't confirm each shard has a serving " + cfg.TabletType + " tablet.", Technical: err.Error()})
			return nil
		}
		return err
	}
	var missing []string
	for _, shard := range sortedKeys(shards) {
		if !serving[shard] {
			missing = append(missing, shard)
		}
	}
	switch {
	case len(shards) == 0:
		run.Add(checks.Check{Code: "vt_tablets", Title: "Tablets", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: fmt.Sprintf("Keyspace %s has no tablets.", cfg.Keyspace)})
	case len(missing) > 0:
		run.Add(checks.Check{Code: "vt_tablets", Title: "Tablets", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: fmt.Sprintf("No serving %s tablet in shard %s of %s.", cfg.TabletType, strings.Join(missing, ", "), cfg.Keyspace),
			Why:     "VStream reads every shard from a tablet of the chosen type."})
	default:
		run.Passed("vt_tablets", "Tablets", checks.CatCDC, fmt.Sprintf("Every shard of %s (%d) has a serving %s tablet.", cfg.Keyspace, len(shards), cfg.TabletType), "")
	}
	run.Add(checks.Check{Code: "vt_binlog_retention", Title: "Binlog retention", Status: checks.Warn, Category: checks.CatResilience,
		Message: "How long the tablets keep their binlogs is not visible through VTGate: a pipeline stopped for longer must copy its tables again.",
		Why:     "Keep binlogs on every tablet's MySQL for at least a day (binlog_expire_logs_seconds) and watch it with your Vitess monitoring.",
		Fix:     &checks.Fix{Kind: "sql", Who: "DBA", Code: "-- on each tablet's MySQL (or in the vttablet's my.cnf):\nSET PERSIST binlog_expire_logs_seconds = 86400;"}})
	return nil
}

func (s *vitessSource) tableChecks(ctx context.Context, db *sql.Conn, run *checks.Run, tables []string, cfg vtConfig) error {
	found := map[string]bool{}
	if err := eachRow(ctx, db, "SELECT table_name FROM information_schema.tables WHERE table_schema = ?", []any{cfg.Keyspace}, func(rows *sql.Rows) error {
		var t string
		if err := rows.Scan(&t); err != nil {
			return err
		}
		found[cfg.Keyspace+"."+t] = true
		return nil
	}); err != nil {
		return err
	}
	var missing, elsewhere []string
	for _, t := range tables {
		schema, _, _ := strings.Cut(t, ".")
		switch {
		case schema != cfg.Keyspace:
			elsewhere = append(elsewhere, t)
		case !found[t]:
			missing = append(missing, t)
		}
	}
	if len(elsewhere) > 0 {
		run.Add(checks.Check{Code: "vt_tables_keyspace", Title: "One keyspace", Status: checks.Fail, Category: checks.CatTables, Blocking: true,
			Message: fmt.Sprintf("Not in keyspace %s: %s. A Vitess connection captures one keyspace.", cfg.Keyspace, strings.Join(first(elsewhere, 10), ", "))})
	}
	if len(missing) > 0 {
		run.Add(checks.Check{Code: "vt_tables_exist", Title: "Selected tables exist", Status: checks.Fail, Category: checks.CatTables, Blocking: true,
			Message: fmt.Sprintf("Not found: %s.", strings.Join(first(missing, 10), ", "))})
	}
	if len(missing) == 0 && len(elsewhere) == 0 {
		run.Passed("vt_tables", "Selected tables", checks.CatTables, fmt.Sprintf("%d tables can be captured.", len(tables)), "")
	}
	return nil
}

func (s *vitessSource) Discover(ctx context.Context, conn Resolved) (Discovery, error) {
	cfg, _, err := vtCfg(conn)
	if err != nil {
		return Discovery{}, err
	}
	db, done, err := vtConnect(ctx, conn, 10*time.Second)
	if err != nil {
		return Discovery{}, err
	}
	defer done()
	var names []string
	if err := eachRow(ctx, db, fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'
	        ORDER BY table_name LIMIT %d`, vtMaxTables+1), []any{cfg.Keyspace}, func(rows *sql.Rows) error {
		var t string
		if err := rows.Scan(&t); err != nil {
			return err
		}
		names = append(names, t)
		return nil
	}); err != nil {
		return Discovery{}, err
	}
	truncated := len(names) > vtMaxTables
	names = first(names, vtMaxTables)
	cols := map[string][]Column{}
	if err := eachRow(ctx, db, `SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns
	        WHERE table_schema = ? ORDER BY table_name, ordinal_position`, []any{cfg.Keyspace}, func(rows *sql.Rows) error {
		var table, column, dataType, nullable string
		if err := rows.Scan(&table, &column, &dataType, &nullable); err != nil {
			return err
		}
		cols[table] = append(cols[table], Column{Name: column, Type: strings.ToLower(dataType), Nullable: nullable == "YES", PII: LooksLikePII(column)})
		return nil
	}); err != nil {
		return Discovery{}, err
	}
	pks := map[string][]string{}
	if err := eachRow(ctx, db, `SELECT table_name, column_name FROM information_schema.key_column_usage
	        WHERE table_schema = ? AND constraint_name = 'PRIMARY' ORDER BY table_name, ordinal_position`, []any{cfg.Keyspace}, func(rows *sql.Rows) error {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			return err
		}
		pks[table] = append(pks[table], column)
		return nil
	}); err != nil {
		return Discovery{}, err
	}
	var tables []TableInfo
	for _, name := range names {
		var warnings []string
		if len(pks[name]) == 0 {
			warnings = append(warnings, "No primary key: updates and deletes can't be keyed.")
		}
		var unsupported []string
		for _, c := range cols[name] {
			if vtUnsupported[c.Type] {
				unsupported = append(unsupported, c.Name)
			}
		}
		if len(unsupported) > 0 {
			warnings = append(warnings, fmt.Sprintf("Debezium doesn't capture spatial columns (%s).", strings.Join(first(unsupported, 5), ", ")))
		}
		// VTGate answers information_schema from one shard, so its row counts would be partial.
		tables = append(tables, TableInfo{Schema: cfg.Keyspace, Name: name, PrimaryKey: pks[name], Columns: cols[name], Warnings: warnings, Selectable: true})
	}
	return NewDiscovery(tables, truncated), nil
}

func (s *vitessSource) CompileSource(conn Resolved, c CompileContext) (map[string]string, map[string]string, bool, error) {
	cfg, sec, err := vtCfg(conn)
	if err != nil {
		return nil, nil, false, err
	}
	spec := c.Spec
	props := map[string]string{
		"debezium.source.connector.class":       "io.debezium.connector.vitess.VitessConnector",
		"debezium.source.database.hostname":     cfg.Host,
		"debezium.source.database.port":         strconv.Itoa(cfg.GRPCPort),
		"debezium.source.vitess.keyspace":       cfg.Keyspace,
		"debezium.source.vitess.tablet.type":    vtTabletTypes[cfg.TabletType],
		"debezium.source.table.include.list":    joinRegex(spec.Source.Tables.Include),
		"debezium.source.heartbeat.interval.ms": strconv.Itoa(spec.Policies.HeartbeatSeconds * 1000),
	}
	env := map[string]string{}
	if cfg.User != "" {
		props["debezium.source.vitess.database.user"] = cfg.User
	}
	if Str(sec, "password") != "" {
		props["debezium.source.vitess.database.password"] = "${BZ_SRC_PASSWORD}"
		env["BZ_SRC_PASSWORD"] = PyStr(sec["password"])
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

func (s *vitessSource) Probe(ctx context.Context, conn Resolved, c CompileContext, _ int64) (*SourceImpact, error) {
	shards := 0
	if OffsetsReader != nil {
		rows, err := OffsetsReader(ctx, c.ShortID)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			if p, ok := r["position"].(string); ok && strings.HasPrefix(p, "VGTID across ") {
				shards, _ = strconv.Atoi(strings.Fields(strings.TrimPrefix(p, "VGTID across "))[0])
				break
			}
		}
	}
	summary := "How long the tablets keep their binlogs is not visible through VTGate, so BeziSync can't tell how long the pipeline may stop."
	if shards > 0 {
		summary = fmt.Sprintf("The stored position covers %d shard(s). ", shards) + summary
	}
	return &SourceImpact{Summary: summary}, nil
}
