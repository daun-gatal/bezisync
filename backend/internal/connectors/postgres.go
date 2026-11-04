package connectors

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

// PostgreSQL source (pgoutput logical decoding).

const (
	pgManagedSchema = "bezisync"
	pgMaxTables     = 10_000
)

type postgresSource struct{ BaseSource }

func init() { Register(&postgresSource{BaseSource{Title: "PostgreSQL"}}) }

func (*postgresSource) Kind() string { return "source" }
func (*postgresSource) Type() string { return "postgresql" }

type pgConfig struct {
	Host, Database, User, SSLMode string
	Port                          int
}

func pgCfg(conn Resolved) (pgConfig, map[string]any, error) {
	d, _ := Describe("source", "postgresql")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return pgConfig{}, nil, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return pgConfig{}, nil, err
	}
	return pgConfig{Host: Str(cfg, "host"), Port: Int(cfg, "port"), Database: Str(cfg, "database"),
		User: Str(cfg, "user"), SSLMode: Str(cfg, "ssl_mode")}, sec, nil
}

// QuoteIdent quotes a PostgreSQL identifier.
func QuoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func pgWALFix(provider string) *checks.Fix {
	switch provider {
	case "aws_rds":
		return &checks.Fix{Kind: "shell", RestartRequired: true, Who: "DBA", Code: "aws rds modify-db-parameter-group --db-parameter-group-name <your-parameter-group> \\\n" +
			"  --parameters \"ParameterName=rds.logical_replication,ParameterValue=1,ApplyMethod=pending-reboot\"\n" +
			"aws rds reboot-db-instance --db-instance-identifier <your-instance>"}
	case "gcp_cloudsql":
		return &checks.Fix{Kind: "shell", RestartRequired: true, Who: "DBA", Code: "gcloud sql instances patch <instance> --database-flags=cloudsql.logical_decoding=on"}
	case "azure_flexible":
		return &checks.Fix{Kind: "shell", RestartRequired: true, Who: "DBA", Code: "az postgres flexible-server parameter set -g <resource-group> -s <server> --name wal_level --value logical\n" +
			"az postgres flexible-server restart -g <resource-group> -n <server>"}
	}
	return &checks.Fix{Kind: "sql", RestartRequired: true, Who: "DBA", Code: "ALTER SYSTEM SET wal_level = logical;\n-- then restart PostgreSQL"}
}

func pgReplicationFix(provider, user string) *checks.Fix {
	if provider == "aws_rds" {
		return &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("GRANT rds_replication TO %s;", QuoteIdent(user))}
	}
	return &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("ALTER ROLE %s WITH REPLICATION;", QuoteIdent(user))}
}

// pgConnect opens an autocommit session (through the SSH tunnel when configured).
func pgConnect(ctx context.Context, conn Resolved, timeout time.Duration) (*pgx.Conn, func(), error) {
	cfg, sec, err := pgCfg(conn)
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
		"user=" + pgQuote(cfg.User), "password=" + pgQuote(Str(sec, "password")), "sslmode=" + cfg.SSLMode,
		"connect_timeout=" + strconv.Itoa(int(timeout.Seconds())), "application_name=bezisync",
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
	pc.RuntimeParams["options"] = "-c statement_timeout=10000"
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

// PgOperationalError is a connection-level failure (psycopg's OperationalError).
type PgOperationalError struct{ Err error }

func (e *PgOperationalError) Error() string { return e.Err.Error() }
func (e *PgOperationalError) Unwrap() error { return e.Err }

func pgQuote(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `'`, `\'`) + "'"
}

func pgOne[T any](ctx context.Context, c *pgx.Conn, sql string, args ...any) (T, error) {
	var v T
	err := c.QueryRow(ctx, sql, args...).Scan(&v)
	return v, err
}

// pgAll reads the first column of every row.
func pgAll[T any](ctx context.Context, c *pgx.Conn, sql string, args ...any) ([]T, error) {
	rows, err := c.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[T])
}

func (s *postgresSource) Check(ctx context.Context, conn Resolved, tables []string) (checks.Report, error) {
	started := time.Now()
	cfg, _, err := pgCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	var reachable bool
	if Str(conn.Config, "ssh_host") != "" {
		reachable = SSHChecks(ctx, run, conn.Config, conn.Secrets, cfg.Host, cfg.Port, "pg")
	} else {
		reachable = NetworkCheck(ctx, run, cfg.Host, cfg.Port, "pg_network", "the database", 0)
	}
	if !reachable {
		return checks.Summarize(run.Checks, started), nil
	}
	pg, done, err := pgConnect(ctx, conn, 6*time.Second)
	if err != nil {
		var op *PgOperationalError
		if errors.As(err, &op) {
			run.Add(pgLoginFailure(op.Error(), cfg))
			return checks.Summarize(run.Checks, started), nil
		}
		return checks.Report{}, err
	}
	defer done()
	run.Passed("pg_login", "Sign in", checks.CatAuth, fmt.Sprintf("Signed in as %s.", cfg.User), "")
	if err := s.serverChecks(ctx, pg, run, conn.Provider, cfg.User); err != nil {
		if isPgOperational(err) {
			run.Add(pgLoginFailure(err.Error(), cfg))
			return checks.Summarize(run.Checks, started), nil
		}
		return checks.Report{}, err
	}
	if len(tables) > 0 {
		if err := s.tableChecks(ctx, pg, run, tables, cfg.User); err != nil {
			if isPgOperational(err) {
				run.Add(pgLoginFailure(err.Error(), cfg))
				return checks.Summarize(run.Checks, started), nil
			}
			return checks.Report{}, err
		}
	}
	return checks.Summarize(run.Checks, started), nil
}

// isPgOperational: connection-level errors (lost connection, timeouts) count like
// psycopg.OperationalError; SQL errors do not.
func isPgOperational(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return strings.HasPrefix(pgErr.Code, "08") || pgErr.Code == "57014" // connection exception, query canceled (statement_timeout)
	}
	return pgconn.Timeout(err) || errors.Is(err, context.DeadlineExceeded)
}

func (s *postgresSource) serverChecks(ctx context.Context, pg *pgx.Conn, run *checks.Run, provider, user string) error {
	verStr, err := pgOne[string](ctx, pg, "SHOW server_version_num")
	if err != nil {
		return err
	}
	version, _ := strconv.Atoi(verStr)
	major := version / 10000
	if major < 12 {
		run.Add(checks.Check{Code: "pg_version", Title: "PostgreSQL version", Status: checks.Fail, Category: checks.CatCDC,
			Message: fmt.Sprintf("PostgreSQL %d is too old. Debezium 3 needs 12 or newer.", major), Blocking: true})
	} else {
		run.Passed("pg_version", "PostgreSQL version", checks.CatCDC, fmt.Sprintf("PostgreSQL %d.", major), "")
	}
	walLevel, err := pgOne[string](ctx, pg, "SHOW wal_level")
	if err != nil {
		return err
	}
	if walLevel == "logical" {
		run.Passed("pg_wal_level", "Logical replication", checks.CatCDC, "Logical replication is on.", "wal_level = logical")
	} else {
		run.Add(checks.Check{Code: "pg_wal_level", Title: "Logical replication", Status: checks.Fail, Category: checks.CatCDC,
			Message:   "The database isn't set up to publish changes yet.",
			Technical: fmt.Sprintf("wal_level = %s; Debezium needs wal_level = logical to decode the WAL.", walLevel),
			Why:       "Debezium reads changes from the database log. The setting needs a restart, so plan it.",
			Fix:       pgWALFix(provider), Blocking: true, Doc: "checks/pg_wal_level"})
	}
	var repl, super *bool
	err = pg.QueryRow(ctx, "SELECT rolreplication, rolsuper FROM pg_roles WHERE rolname = current_user").Scan(&repl, &super)
	roleFound := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	rdsMember, err := pgOne[*bool](ctx, pg, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_replication') "+
		"AND pg_has_role(current_user, 'rds_replication', 'member')")
	if err != nil {
		return err
	}
	if roleFound && (truthy(repl) || truthy(super) || truthy(rdsMember)) {
		run.Passed("pg_replication_role", "Replication permission", checks.CatPermissions, fmt.Sprintf("%s may stream changes.", user), "")
	} else {
		run.Add(checks.Check{Code: "pg_replication_role", Title: "Replication permission", Status: checks.Fail, Category: checks.CatPermissions,
			Message:   fmt.Sprintf("User %s is not allowed to stream changes.", user),
			Technical: "The role needs the REPLICATION attribute (or rds_replication on RDS).",
			Fix:       pgReplicationFix(provider, user), Blocking: true, Doc: "checks/pg_replication_role"})
	}
	maxSlots, err := pgIntSetting(ctx, pg, "SHOW max_replication_slots")
	if err != nil {
		return err
	}
	usedSlots, err := pgOne[int64](ctx, pg, "SELECT count(*) FROM pg_replication_slots")
	if err != nil {
		return err
	}
	maxSenders, err := pgIntSetting(ctx, pg, "SHOW max_wal_senders")
	if err != nil {
		return err
	}
	usedSenders, err := pgOne[int64](ctx, pg, "SELECT count(*) FROM pg_stat_replication")
	if err != nil {
		return err
	}
	detail := fmt.Sprintf("Replication slots %d of %d used · WAL senders %d of %d.", usedSlots, maxSlots, usedSenders, maxSenders)
	switch {
	case usedSlots >= maxSlots || usedSenders >= maxSenders:
		run.Add(checks.Check{Code: "pg_capacity", Title: "Replication capacity", Status: checks.Fail, Category: checks.CatCapacity,
			Message: "No free replication slot or WAL sender for a new pipeline.", Technical: detail,
			Fix: &checks.Fix{Kind: "sql", RestartRequired: true, Who: "DBA", Code: "-- drop slots nobody uses:\nSELECT slot_name, active FROM pg_replication_slots;\n" +
				"-- or raise the limits (restart required):\n" +
				"ALTER SYSTEM SET max_replication_slots = 20;\nALTER SYSTEM SET max_wal_senders = 20;"},
			Blocking: true})
	case float64(usedSlots) >= float64(maxSlots)*0.8:
		run.Add(checks.Check{Code: "pg_capacity", Title: "Replication capacity", Status: checks.Warn, Category: checks.CatCapacity,
			Message: "Replication slots are almost exhausted.", Technical: detail})
	default:
		run.Passed("pg_capacity", "Replication capacity", checks.CatCapacity, detail, "")
	}
	canCreate, err := pgOne[*bool](ctx, pg, "SELECT has_database_privilege(current_database(), 'CREATE')")
	if err != nil {
		return err
	}
	if truthy(canCreate) {
		run.Passed("pg_create", "Manage publications", checks.CatPermissions, "Can create the publication and the bezisync heartbeat schema.", "")
	} else {
		dbName, err := pgOne[string](ctx, pg, "SELECT current_database()")
		if err != nil {
			return err
		}
		run.Add(checks.Check{Code: "pg_create", Title: "Manage publications", Status: checks.Fail, Category: checks.CatPermissions,
			Message:   fmt.Sprintf("%s cannot create the publication or the bezisync schema.", user),
			Technical: "Needs CREATE on the database for CREATE PUBLICATION / CREATE SCHEMA bezisync.",
			Fix:       &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("GRANT CREATE ON DATABASE %s TO %s;", QuoteIdent(dbName), QuoteIdent(user))},
			Blocking:  true})
	}
	if major >= 13 {
		keep, err := pgOne[string](ctx, pg, "SHOW max_slot_wal_keep_size")
		if err != nil {
			return err
		}
		if keep == "-1" || keep == "-1MB" {
			run.Add(checks.Check{Code: "pg_wal_retention", Title: "WAL retention limit", Status: checks.Warn, Category: checks.CatResilience,
				Message:   "If a pipeline stops, the database keeps WAL for it without limit.",
				Technical: "max_slot_wal_keep_size = -1 (unbounded).",
				Why: "A stopped pipeline's slot can fill the disk. BeziSync alerts on retained WAL; a cap " +
					"protects the database but invalidates the slot (re-snapshot needed) when reached.",
				Fix: &checks.Fix{Kind: "sql", Who: "DBA", Code: "ALTER SYSTEM SET max_slot_wal_keep_size = '50GB';\nSELECT pg_reload_conf();"}})
		} else {
			run.Passed("pg_wal_retention", "WAL retention limit", checks.CatResilience, fmt.Sprintf("WAL kept for slots is capped at %s.", keep), "")
		}
	}
	if major >= 17 {
		run.Passed("pg_failover", "Failover safety", checks.CatResilience, "PostgreSQL 17 can synchronise slots to standbys (failover slots).", "")
	} else {
		run.Add(checks.Check{Code: "pg_failover", Title: "Failover safety", Status: checks.Warn, Category: checks.CatResilience,
			Message:   "Slots are not failover-safe. After a primary failover the pipeline must re-snapshot.",
			Technical: fmt.Sprintf("Failover slots need PostgreSQL 17+ (this is %d).", major), Doc: "checks/pg_failover"})
	}
	return nil
}

func pgIntSetting(ctx context.Context, pg *pgx.Conn, sql string) (int64, error) {
	s, err := pgOne[string](ctx, pg, sql)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(s), 10, 64)
}

func truthy(b *bool) bool { return b != nil && *b }

func qualified(t string) string {
	schema, table, _ := strings.Cut(t, ".")
	if table == "" && !strings.Contains(t, ".") {
		return QuoteIdent(schema)
	}
	return QuoteIdent(schema) + "." + QuoteIdent(table)
}

func first[T any](l []T, n int) []T {
	if len(l) > n {
		return l[:n]
	}
	return l
}

func (s *postgresSource) tableChecks(ctx context.Context, pg *pgx.Conn, run *checks.Run, tables []string, user string) error {
	rows, err := pg.Query(ctx, `
            SELECT n.nspname || '.' || c.relname, c.relreplident,
                   EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary),
                   has_table_privilege(c.oid, 'SELECT'),
                   pg_has_role(current_user, c.relowner, 'MEMBER')
              FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname || '.' || c.relname = ANY($1)
            `, tables)
	if err != nil {
		return err
	}
	type info struct {
		name              string
		ident             string
		hasPK, sel, owner bool
	}
	var found []info
	seen := map[string]int{}
	for rows.Next() {
		var i info
		var ident any
		if err := rows.Scan(&i.name, &ident, &i.hasPK, &i.sel, &i.owner); err != nil {
			rows.Close()
			return err
		}
		i.ident = pgChar(ident)
		if idx, ok := seen[i.name]; ok { // dict: later rows replace, keeping first position
			found[idx] = i
			continue
		}
		seen[i.name] = len(found)
		found = append(found, i)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	var missing []string
	for _, t := range tables {
		if _, ok := seen[t]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		run.Add(checks.Check{Code: "pg_tables_exist", Title: "Selected tables exist", Status: checks.Fail, Category: checks.CatTables,
			Message: fmt.Sprintf("Not found: %s.", strings.Join(first(missing, 10), ", ")), Blocking: true})
	}
	var noSelect, unkeyed, nothing, notOwner []string
	for _, f := range found {
		if !f.sel {
			noSelect = append(noSelect, f.name)
		}
		if !f.hasPK && f.ident == "d" {
			unkeyed = append(unkeyed, f.name)
		}
		if f.ident == "n" {
			nothing = append(nothing, f.name)
		}
		if !f.owner {
			notOwner = append(notOwner, f.name)
		}
	}
	if len(noSelect) > 0 {
		var grants []string
		for _, t := range first(noSelect, 50) {
			grants = append(grants, fmt.Sprintf("GRANT SELECT ON %s TO %s;", qualified(t), QuoteIdent(user)))
		}
		run.Add(checks.Check{Code: "pg_tables_select", Title: "Read access", Status: checks.Fail, Category: checks.CatTables,
			Message: fmt.Sprintf("No SELECT permission on %s.", strings.Join(first(noSelect, 10), ", ")), Blocking: true,
			Why: "The initial snapshot reads existing rows with SELECT.",
			Fix: &checks.Fix{Kind: "sql", Who: "DBA", Code: strings.Join(grants, "\n")}})
	}
	if len(unkeyed) > 0 {
		var alters []string
		for _, t := range first(unkeyed, 50) {
			alters = append(alters, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL;", qualified(t)))
		}
		run.Add(checks.Check{Code: "pg_tables_keys", Title: "Row keys", Status: checks.Warn, Category: checks.CatTables,
			Message:   fmt.Sprintf("No primary key on %s: updates and deletes cannot be keyed.", strings.Join(first(unkeyed, 10), ", ")),
			Technical: "Tables without PK and with REPLICA IDENTITY DEFAULT emit no before-image keys.",
			Fix:       &checks.Fix{Kind: "sql", Who: "DBA", Code: strings.Join(alters, "\n")}})
	}
	if len(nothing) > 0 {
		run.Add(checks.Check{Code: "pg_tables_identity", Title: "Replica identity", Status: checks.Fail, Category: checks.CatTables, Blocking: true,
			Message: fmt.Sprintf("REPLICA IDENTITY NOTHING on %s: updates/deletes are lost.", strings.Join(first(nothing, 10), ", "))})
	}
	if len(notOwner) > 0 {
		var names []string
		for _, t := range first(notOwner, 50) {
			names = append(names, qualified(t))
		}
		run.Add(checks.Check{Code: "pg_tables_owner", Title: "Publication ownership", Status: checks.Warn, Category: checks.CatTables,
			Message:   "BeziSync's user does not own some tables, so it cannot add them to the publication itself.",
			Technical: fmt.Sprintf("Not owned: %s. A table owner must create the publication.", strings.Join(first(notOwner, 10), ", ")),
			Fix:       &checks.Fix{Kind: "sql", Who: "table owner", Code: "CREATE PUBLICATION <pipeline-publication> FOR TABLE " + strings.Join(names, ", ") + ";"}})
	}
	if len(found) > 0 && len(missing) == 0 && len(noSelect) == 0 && len(nothing) == 0 {
		run.Passed("pg_tables", "Selected tables", checks.CatTables, fmt.Sprintf("%d tables can be captured.", len(found)), "")
	}
	return nil
}

// pgChar reads a PostgreSQL "char" value (pgx returns it as a rune/int32 or string).
func pgChar(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case int32:
		return string(x)
	case int8:
		return string(rune(x))
	case uint8:
		return string(rune(x))
	case []byte:
		return string(x)
	}
	return fmt.Sprint(v)
}

func (s *postgresSource) Discover(ctx context.Context, conn Resolved) (Discovery, error) {
	pg, done, err := pgConnect(ctx, conn, 10*time.Second)
	if err != nil {
		return Discovery{}, err
	}
	defer done()
	rows, err := pg.Query(ctx, fmt.Sprintf(`
                SELECT n.nspname, c.relname, c.reltuples::bigint, pg_total_relation_size(c.oid), c.relreplident::text,
                       ARRAY(SELECT a.attname::text FROM pg_index i
                               JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                              WHERE i.indrelid = c.oid AND i.indisprimary ORDER BY a.attnum),
                       has_table_privilege(c.oid, 'SELECT')
                  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition
                   AND n.nspname NOT IN ('pg_catalog', 'information_schema', '%s')
                   AND n.nspname NOT LIKE 'pg\_toast%%' AND n.nspname NOT LIKE 'pg\_temp%%'
                 ORDER BY 1, 2 LIMIT %d
                `, pgManagedSchema, pgMaxTables+1))
	if err != nil {
		return Discovery{}, err
	}
	type trow struct {
		schema, name string
		tuples, size *int64
		replident    string
		pk           []string
		canSelect    bool
	}
	var trows []trow
	for rows.Next() {
		var r trow
		if err := rows.Scan(&r.schema, &r.name, &r.tuples, &r.size, &r.replident, &r.pk, &r.canSelect); err != nil {
			rows.Close()
			return Discovery{}, err
		}
		trows = append(trows, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Discovery{}, err
	}
	truncated := len(trows) > pgMaxTables
	trows = first(trows, pgMaxTables)
	cols := map[[2]string][]Column{}
	crows, err := pg.Query(ctx, fmt.Sprintf(`
                SELECT table_schema::text, table_name::text, column_name::text, data_type::text, is_nullable = 'YES'
                  FROM information_schema.columns
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema', '%s')
                 ORDER BY table_schema, table_name, ordinal_position LIMIT 200000
                `, pgManagedSchema))
	if err != nil {
		return Discovery{}, err
	}
	for crows.Next() {
		var schema, table, column, dataType string
		var nullable bool
		if err := crows.Scan(&schema, &table, &column, &dataType, &nullable); err != nil {
			crows.Close()
			return Discovery{}, err
		}
		k := [2]string{schema, table}
		cols[k] = append(cols[k], Column{Name: column, Type: dataType, Nullable: nullable, PII: LooksLikePII(column)})
	}
	crows.Close()
	if err := crows.Err(); err != nil {
		return Discovery{}, err
	}
	identities := map[string]string{"d": "default", "n": "nothing", "f": "full", "i": "index"}
	var tables []TableInfo
	for _, r := range trows {
		var warnings []string
		identity, ok := identities[r.replident]
		if !ok {
			identity = r.replident
		}
		if len(r.pk) == 0 && identity == "default" {
			warnings = append(warnings, "No primary key — updates and deletes can't be keyed (set REPLICA IDENTITY FULL).")
		}
		if identity == "nothing" {
			warnings = append(warnings, "REPLICA IDENTITY NOTHING — updates and deletes would be lost.")
		}
		if !r.canSelect {
			warnings = append(warnings, "No SELECT permission.")
		}
		var rowsEst *int64
		if r.tuples != nil && *r.tuples >= 0 {
			rowsEst = r.tuples
		}
		id := identity
		tables = append(tables, TableInfo{Schema: r.schema, Name: r.name, RowsEstimate: rowsEst, BytesEstimate: r.size,
			PrimaryKey: r.pk, ReplicaIdentity: &id, Columns: cols[[2]string{r.schema, r.name}], Warnings: warnings, Selectable: r.canSelect})
	}
	return NewDiscovery(tables, truncated), nil
}

func pgSlotName(c CompileContext) string    { return "bz_" + c.ShortID }
func pgSignalTable(c CompileContext) string { return pgManagedSchema + ".signal_" + c.ShortID }
func pgCanaryTable(c CompileContext) string { return pgManagedSchema + ".canary_" + c.ShortID }
func canaryOn(c CompileContext) bool        { return c.Spec.Policies.DeliveryCanary }
func (*postgresSource) CanaryTable(_ Resolved, c CompileContext) (string, error) {
	return pgCanaryTable(c), nil
}

// Salt is the per-pipeline salt of masked column hashes.
func Salt(pipelineID string) string {
	sum := sha256Hex("bezisync-mask:" + pipelineID)
	return sum[:16]
}

// ColumnRules lists table.column for each rule kind, in spec column order.
func ColumnRules(spec domain.PipelineSpec) (excludes, masks []string) {
	for _, t := range domain.ColumnKeys(spec.Source.Columns) {
		rules := spec.Source.Columns[t]
		for _, c := range rules.Exclude {
			excludes = append(excludes, t+"."+c)
		}
	}
	for _, t := range domain.ColumnKeys(spec.Source.Columns) {
		rules := spec.Source.Columns[t]
		for _, c := range rules.Mask {
			masks = append(masks, t+"."+c)
		}
	}
	return excludes, masks
}

func joinRegex(items []string) string {
	out := make([]string, len(items))
	for i, s := range items {
		out[i] = JavaRegexLiteral(s)
	}
	return strings.Join(out, ",")
}

func (s *postgresSource) CompileSource(conn Resolved, c CompileContext) (map[string]string, map[string]string, bool, error) {
	cfg, sec, err := pgCfg(conn)
	if err != nil {
		return nil, nil, false, err
	}
	spec := c.Spec
	tables := append(append([]string{}, spec.Source.Tables.Include...), pgSignalTable(c))
	if canaryOn(c) {
		tables = append(tables, pgCanaryTable(c))
	}
	props := map[string]string{
		"debezium.source.connector.class":             "io.debezium.connector.postgresql.PostgresConnector",
		"debezium.source.database.hostname":           cfg.Host,
		"debezium.source.database.port":               strconv.Itoa(cfg.Port),
		"debezium.source.database.dbname":             cfg.Database,
		"debezium.source.database.user":               cfg.User,
		"debezium.source.database.password":           "${BZ_SRC_PASSWORD}",
		"debezium.source.database.sslmode":            cfg.SSLMode,
		"debezium.source.plugin.name":                 "pgoutput",
		"debezium.source.slot.name":                   pgSlotName(c),
		"debezium.source.publication.name":            pgSlotName(c),
		"debezium.source.publication.autocreate.mode": "filtered",
		"debezium.source.table.include.list":          joinRegex(tables),
		"debezium.source.signal.enabled.channels":     "source",
		"debezium.source.signal.data.collection":      pgSignalTable(c),
		"debezium.source.heartbeat.interval.ms":       strconv.Itoa(spec.Policies.HeartbeatSeconds * 1000),
	}
	if spec.Policies.HeartbeatSeconds != 0 {
		props["debezium.source.heartbeat.action.query"] = fmt.Sprintf(
			"INSERT INTO %s.heartbeat (pipeline, ts) VALUES ('%s', now()) ON CONFLICT (pipeline) DO UPDATE SET ts = EXCLUDED.ts",
			pgManagedSchema, c.PipelineID)
	}
	excludes, masks := ColumnRules(spec)
	if len(excludes) > 0 {
		props["debezium.source.column.exclude.list"] = joinRegex(excludes)
	}
	if len(masks) > 0 {
		props["debezium.source.column.mask.hash.v2.SHA-256.with.salt."+Salt(c.PipelineID)] = joinRegex(masks)
	}
	env := map[string]string{"BZ_SRC_PASSWORD": PyStr(sec["password"])}
	return props, env, false, nil
}

func (s *postgresSource) Prepare(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	var results []ArtifactResult
	pg, done, err := pgConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	exec := func(sql string, args ...any) error {
		_, err := pg.Exec(ctx, sql, args...)
		return err
	}
	if err := exec("CREATE SCHEMA IF NOT EXISTS " + pgManagedSchema); err != nil {
		return nil, err
	}
	if err := exec("CREATE TABLE IF NOT EXISTS " + pgManagedSchema + ".heartbeat " +
		"(pipeline text PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now())"); err != nil {
		return nil, err
	}
	results = append(results, ArtifactResult{Kind: "table", Name: pgManagedSchema + ".heartbeat", Outcome: "exists"})
	existed, err := pgOne[bool](ctx, pg, "SELECT to_regclass($1) IS NOT NULL", pgSignalTable(c))
	if err != nil {
		return nil, err
	}
	if err := exec("CREATE TABLE IF NOT EXISTS " + pgSignalTable(c) + " " +
		"(id varchar(64) PRIMARY KEY, type varchar(64) NOT NULL, data varchar(8192))"); err != nil {
		return nil, err
	}
	outcome := "created"
	if existed {
		outcome = "exists"
	}
	results = append(results, ArtifactResult{Kind: "table", Name: pgSignalTable(c), Outcome: outcome})
	if canaryOn(c) {
		if err := exec("CREATE TABLE IF NOT EXISTS " + pgCanaryTable(c) + " " +
			"(id varchar(64) PRIMARY KEY, sent_at timestamptz NOT NULL DEFAULT now())"); err != nil {
			return nil, err
		}
		results = append(results, ArtifactResult{Kind: "table", Name: pgCanaryTable(c), Outcome: "exists"})
	}
	results = append(results,
		ArtifactResult{Kind: "replication_slot", Name: pgSlotName(c), Outcome: "exists", Detail: "Created by Debezium on first start."},
		ArtifactResult{Kind: "publication", Name: pgSlotName(c), Outcome: "exists", Detail: "Created and kept in sync with the table list by Debezium."},
	)
	return results, nil
}

func slotActive(ctx context.Context, pg *pgx.Conn, slot string) (*bool, error) {
	var active *bool
	err := pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	f := false
	if active == nil {
		active = &f
	}
	return active, nil
}

func (s *postgresSource) Cleanup(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	var results []ArtifactResult
	slot := pgSlotName(c)
	pg, done, err := pgConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	for range 30 {
		active, err := slotActive(ctx, pg, slot)
		if err != nil {
			return nil, err
		}
		if active == nil || !*active {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	active, err := slotActive(ctx, pg, slot)
	if err != nil {
		return nil, err
	}
	switch {
	case active == nil:
		results = append(results, ArtifactResult{Kind: "replication_slot", Name: slot, Outcome: "not_found"})
	case *active:
		results = append(results, ArtifactResult{Kind: "replication_slot", Name: slot, Outcome: "failed", Detail: "The slot is still in use by a running connector."})
	default:
		if _, err := pg.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot); err != nil {
			return nil, err
		}
		results = append(results, ArtifactResult{Kind: "replication_slot", Name: slot, Outcome: "removed", Detail: "The database no longer retains WAL for this pipeline."})
	}
	// Flink CDC copies with temporary slots <slot>_<n> and can leave them behind (FLINK-CDC #4522):
	// every inactive one of this pipeline goes too.
	extra, err := pgAll[string](ctx, pg, `SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE $1 AND NOT active ORDER BY 1`,
		strings.ReplaceAll(slot, "_", `\_`)+`\_%`)
	if err != nil {
		return nil, err
	}
	for _, name := range extra {
		if _, err := pg.Exec(ctx, "SELECT pg_drop_replication_slot($1)", name); err != nil {
			return nil, err
		}
		results = append(results, ArtifactResult{Kind: "replication_slot", Name: name, Outcome: "removed"})
	}
	exists, err := pgOne[bool](ctx, pg, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", slot)
	if err != nil {
		return nil, err
	}
	if _, err := pg.Exec(ctx, "DROP PUBLICATION IF EXISTS "+QuoteIdent(slot)); err != nil {
		return nil, err
	}
	results = append(results, ArtifactResult{Kind: "publication", Name: slot, Outcome: pick(exists, "removed", "not_found")})
	existed, err := pgOne[bool](ctx, pg, "SELECT to_regclass($1) IS NOT NULL", pgSignalTable(c))
	if err != nil {
		return nil, err
	}
	if _, err := pg.Exec(ctx, "DROP TABLE IF EXISTS "+pgSignalTable(c)); err != nil {
		return nil, err
	}
	results = append(results, ArtifactResult{Kind: "table", Name: pgSignalTable(c), Outcome: pick(existed, "removed", "not_found")})
	canary, err := pgOne[bool](ctx, pg, "SELECT to_regclass($1) IS NOT NULL", pgCanaryTable(c))
	if err != nil {
		return nil, err
	}
	if canary {
		if _, err := pg.Exec(ctx, "DROP TABLE IF EXISTS "+pgCanaryTable(c)); err != nil {
			return nil, err
		}
		results = append(results, ArtifactResult{Kind: "table", Name: pgCanaryTable(c), Outcome: "removed"})
	}
	hb, err := pgOne[bool](ctx, pg, "SELECT to_regclass('"+pgManagedSchema+".heartbeat') IS NOT NULL")
	if err != nil {
		return nil, err
	}
	if hb {
		if _, err := pg.Exec(ctx, "DELETE FROM "+pgManagedSchema+".heartbeat WHERE pipeline = $1", c.PipelineID); err != nil {
			return nil, err
		}
	}
	return results, nil
}

func pick(b bool, yes, no string) string {
	if b {
		return yes
	}
	return no
}

func (s *postgresSource) ForgetPosition(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	slot := pgSlotName(c)
	pg, done, err := pgConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	active, err := slotActive(ctx, pg, slot)
	if err != nil {
		return nil, err
	}
	if active == nil {
		return []ArtifactResult{{Kind: "replication_slot", Name: slot, Outcome: "not_found"}}, nil
	}
	if *active {
		return []ArtifactResult{{Kind: "replication_slot", Name: slot, Outcome: "failed", Detail: "The slot is in use; pause the pipeline first."}}, nil
	}
	if _, err := pg.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot); err != nil {
		return nil, err
	}
	return []ArtifactResult{{Kind: "replication_slot", Name: slot, Outcome: "removed"}}, nil
}

func (s *postgresSource) Probe(ctx context.Context, conn Resolved, c CompileContext, limit int64) (*SourceImpact, error) {
	pg, done, err := pgConnect(ctx, conn, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	verStr, err := pgOne[string](ctx, pg, "SHOW server_version_num")
	if err != nil {
		return nil, err
	}
	version, _ := strconv.Atoi(verStr)
	major := version / 10000
	cols := "active, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::float8"
	if major >= 13 {
		cols += ", wal_status::text, safe_wal_size::float8"
	} else {
		cols += ", NULL::text, NULL::float8"
	}
	var active *bool
	var retained, safe *float64
	var walStatus *string
	slot := pgSlotName(c)
	err = pg.QueryRow(ctx, "SELECT "+cols+" FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&active, &retained, &walStatus, &safe)
	if errors.Is(err, pgx.ErrNoRows) {
		return &SourceImpact{Slot: &slot, Summary: "The replication slot does not exist yet."}, nil
	}
	if err != nil {
		return nil, err
	}
	return pgImpact(slot, active, retained, safe, walStatus, limit), nil
}

// pgImpact turns one pg_replication_slots row into the impact report. walStatus is nil before
// PostgreSQL 13 (no wal_status or safe_wal_size columns).
func pgImpact(slot string, active *bool, retained, safe *float64, walStatus *string, limit int64) *SourceImpact {
	var retainedI, safeI *int64
	if retained != nil {
		v := int64(*retained)
		retainedI = &v
	}
	if safe != nil {
		v := max(int64(*safe), 0)
		safeI = &v
	}
	act := truthy(active)
	var retainedF *float64
	if retainedI != nil {
		f := float64(*retainedI)
		retainedF = &f
	}
	impact := &SourceImpact{RetainedBytes: retainedI, SlotActive: &act, LogStatus: walStatus, HeadroomBytes: safeI, LimitBytes: &limit, Slot: &slot,
		NoCap:   walStatus != nil && *walStatus != "lost" && safe == nil,
		Summary: fmt.Sprintf("Slot %s · %s of WAL retained", pick(act, "active", "inactive"), domain.HumanBytes(retainedF))}
	limitF := float64(limit)
	switch {
	case walStatus != nil && *walStatus == "lost":
		impact.Lost = true
		r := "The database removed WAL this pipeline still needed; changes were lost."
		impact.Risk = &r
		impact.RiskFix = "Reset the pipeline's offsets and re-snapshot. Raise max_slot_wal_keep_size to prevent it."
	case walStatus != nil && *walStatus == "unreserved":
		r := "The slot is about to be invalidated: the WAL it needs exceeds max_slot_wal_keep_size."
		impact.Risk = &r
		impact.RiskFix = "Resolve the backlog now (resume or speed up the pipeline), or raise max_slot_wal_keep_size."
	case retainedI != nil && *retainedI > limit:
		r := fmt.Sprintf("The source retains %s of WAL for this pipeline (limit %s).", domain.HumanBytes(retainedF), domain.HumanBytes(&limitF))
		impact.Risk = &r
		impact.RiskFix = "The pipeline is behind or paused, so the database cannot free WAL. Resume it, fix the destination, " +
			"or delete the pipeline to release the slot."
	}
	return impact
}

func (s *postgresSource) WriteCanary(ctx context.Context, conn Resolved, c CompileContext, token string) error {
	pg, done, err := pgConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return err
	}
	defer done()
	if _, err := pg.Exec(ctx, "INSERT INTO "+pgCanaryTable(c)+" (id) VALUES ($1)", token); err != nil {
		return err
	}
	_, err = pg.Exec(ctx, "DELETE FROM "+pgCanaryTable(c)+" WHERE sent_at < now() - interval '1 day'")
	return err
}

func (s *postgresSource) Signal(ctx context.Context, conn Resolved, c CompileContext, signalType string, data map[string]any) (string, error) {
	signalID := hex.EncodeToString(uuidBytes())
	pg, done, err := pgConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return "", err
	}
	defer done()
	payload, err := PyDumps(data)
	if err != nil {
		return "", err
	}
	_, err = pg.Exec(ctx, "INSERT INTO "+pgSignalTable(c)+" (id, type, data) VALUES ($1, $2, $3)", signalID, signalType, payload)
	return signalID, err
}

func pgLoginFailure(message string, cfg pgConfig) checks.Check {
	lower := strings.ToLower(message)
	tech := strings.TrimSpace(message)
	switch {
	case strings.Contains(lower, "password authentication failed") || strings.Contains(lower, "authentication failed"):
		return checks.Check{Code: "pg_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: fmt.Sprintf("The database rejected the password for %s.", cfg.User), Technical: tech}
	case strings.Contains(lower, "no pg_hba.conf entry"):
		return checks.Check{Code: "pg_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: "The database does not accept connections from this address.", Technical: tech,
			Fix: &checks.Fix{Kind: "config", Who: "DBA", Code: fmt.Sprintf("# pg_hba.conf\nhostssl %s %s <runtime-cidr> scram-sha-256\n", cfg.Database, cfg.User) +
				"hostssl replication " + cfg.User + " <runtime-cidr> scram-sha-256"}}
	case strings.Contains(lower, "does not exist") && strings.Contains(lower, "database"):
		return checks.Check{Code: "pg_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: fmt.Sprintf("Database %s does not exist.", cfg.Database), Technical: tech}
	case strings.Contains(lower, "ssl") || strings.Contains(lower, "certificate"):
		return checks.Check{Code: "pg_tls", Title: "TLS", Status: checks.Fail, Category: checks.CatNetwork, Blocking: true,
			Message: "The TLS connection failed.", Technical: tech, Why: "Check the TLS mode and CA certificate for this database."}
	}
	return checks.Check{Code: "pg_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
		Message: "Could not sign in to the database.", Technical: tech}
}

func uuidBytes() []byte {
	u := uuid.New()
	return u[:]
}

// PyDumps is json.dumps(value) with Python's default separators (unsorted keys are not
// reproducible after decoding, so keys are sorted).
func PyDumps(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return pyDumpsRaw(raw)
}
