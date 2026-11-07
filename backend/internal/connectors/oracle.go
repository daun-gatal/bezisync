package connectors

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	goora "github.com/sijms/go-ora/v2"
	"github.com/sijms/go-ora/v2/network"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

// Oracle source (LogMiner). BeziSync's own tables (signals, the delivery canary) live in the
// connector user's schema as BZ_SIGNAL_<id> / BZ_CANARY_<id>: Oracle has no schema without a user.

const oraMaxTables = 10_000

// oraSystemPrivileges are what Debezium's LogMiner adapter needs; SET CONTAINER only in a CDB.
var oraSystemPrivileges = []string{"CREATE SESSION", "FLASHBACK ANY TABLE", "SELECT ANY TABLE", "SELECT ANY TRANSACTION",
	"LOGMINING", "CREATE TABLE", "LOCK ANY TABLE", "CREATE SEQUENCE"}

var oraRoles = []string{"SELECT_CATALOG_ROLE", "EXECUTE_CATALOG_ROLE"}

// oraViews are the dynamic performance views LogMiner reads (granted as V_$… objects).
var oraViews = []string{"V$DATABASE", "V$LOG", "V$LOG_HISTORY", "V$LOGMNR_LOGS", "V$LOGMNR_CONTENTS", "V$LOGMNR_PARAMETERS",
	"V$LOGFILE", "V$ARCHIVED_LOG", "V$ARCHIVE_DEST_STATUS", "V$TRANSACTION", "V$MYSTAT", "V$STATNAME"}

// oraLOBTypes are only captured with lob.enabled; oraUnsupported never are.
var (
	oraLOBTypes     = map[string]bool{"CLOB": true, "NCLOB": true, "BLOB": true, "XMLTYPE": true}
	oraUnsupported  = map[string]bool{"LONG": true, "LONG RAW": true, "BFILE": true, "ANYDATA": true, "UROWID": true}
	oraUnsupportedN = "LONG, LONG RAW, BFILE, ANYDATA, UROWID and object types"
)

type oracleSource struct{ BaseSource }

func init() { Register(&oracleSource{BaseSource{Title: "Oracle"}}) }

func (*oracleSource) Kind() string { return "source" }
func (*oracleSource) Type() string { return "oracle" }

type oraConfig struct {
	Host, Database, PDB, Service, User string
	Port                               int
	LOB                                bool
}

func oraCfg(conn Resolved) (oraConfig, map[string]any, error) {
	d, _ := Describe("source", "oracle")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return oraConfig{}, nil, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return oraConfig{}, nil, err
	}
	pdb := ""
	if p := OptStr(cfg, "pdb"); p != nil {
		pdb = strings.TrimSpace(*p)
	}
	return oraConfig{Host: Str(cfg, "host"), Port: Int(cfg, "port"), Database: strings.TrimSpace(Str(cfg, "database")), PDB: pdb,
		Service: strings.TrimSpace(Str(cfg, "service_name")), User: Str(cfg, "user"), LOB: Bool(cfg, "lob_enabled")}, sec, nil
}

// owner is the connector user's schema (Oracle folds unquoted names to upper case).
func (c oraConfig) owner() string { return strings.ToUpper(c.User) }

// container is the database Debezium names in data collections: the PDB, else the database.
func (c oraConfig) container() string {
	if c.PDB != "" {
		return strings.ToUpper(c.PDB)
	}
	return strings.ToUpper(c.Database)
}

// tableService is the service BeziSync reads tables from: the PDB's (by default its name), or
// the database's own, which is also what Debezium signs in to.
func (c oraConfig) tableService() string {
	switch {
	case c.PDB == "":
		return c.Database
	case c.Service != "":
		return c.Service
	}
	return c.PDB
}

func oraSignalTable(c CompileContext) string { return "BZ_SIGNAL_" + strings.ToUpper(c.ShortID) }
func oraCanaryTable(c CompileContext) string { return "BZ_CANARY_" + strings.ToUpper(c.ShortID) }

// OraIdent quotes an Oracle identifier.
func OraIdent(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

func oraQualified(t string) string {
	schema, table, _ := strings.Cut(t, ".")
	return OraIdent(schema) + "." + OraIdent(table)
}

// OraOperationalError is a failure to reach or sign in to the database.
type OraOperationalError struct{ Err error }

func (e *OraOperationalError) Error() string { return e.Err.Error() }
func (e *OraOperationalError) Unwrap() error { return e.Err }

func oraCode(err error) int {
	var oe *network.OracleError
	if errors.As(err, &oe) {
		return oe.ErrCode
	}
	return 0
}

// oraConnect opens a session on a service (through the SSH tunnel when configured).
func oraConnect(ctx context.Context, conn Resolved, service string, timeout time.Duration) (*sql.Conn, func(), error) {
	cfg, sec, err := oraCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	host, port, dial, closeTunnel, err := Tunnel(ctx, conn.Config, conn.Secrets, cfg.Host, cfg.Port)
	if err != nil {
		return nil, nil, err
	}
	url := goora.BuildUrl(host, port, service, cfg.User, PyStr(sec["password"]), map[string]string{
		"CONNECTION TIMEOUT": strconv.Itoa(max(int(timeout.Seconds()), 1)),
		"TIMEOUT":            "30",
		"PROGRAM":            "bezisync",
	})
	connector, ok := goora.NewConnector(url).(*goora.OracleConnector)
	if !ok {
		closeTunnel()
		return nil, nil, fmt.Errorf("unexpected Oracle connector")
	}
	connector.Dialer(guardedDialer{dial: dial}) // the SSRF guard, or this connection's own tunnel (ADR 0004)
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	cctx, cancel := context.WithTimeout(ctx, timeout+5*time.Second)
	defer cancel()
	c, err := db.Conn(cctx)
	if err == nil {
		err = c.PingContext(cctx)
	}
	if err != nil {
		if c != nil {
			_ = c.Close()
		}
		_ = db.Close()
		closeTunnel()
		return nil, nil, &OraOperationalError{err}
	}
	return c, func() { _ = c.Close(); _ = db.Close(); closeTunnel() }, nil
}

func isOraOperational(err error) bool {
	var op *OraOperationalError
	if errors.As(err, &op) {
		return true
	}
	switch oraCode(err) {
	case 3113, 3114, 3135, 12170, 12514, 12541, 12543:
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) || errors.Is(err, context.DeadlineExceeded)
}

func oraOne[T any](ctx context.Context, c *sql.Conn, query string, args ...any) (T, error) {
	var v T
	err := c.QueryRowContext(ctx, query, args...).Scan(&v)
	return v, err
}

func oraLoginFailure(err error, cfg oraConfig, service string) checks.Check {
	tech := strings.TrimSpace(err.Error())
	c := checks.Check{Code: "ora_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
		Message: "Could not sign in to the database.", Technical: tech}
	switch oraCode(err) {
	case 1017:
		c.Message = fmt.Sprintf("The database rejected the user name or password for %s.", cfg.User)
	case 28000:
		c.Message = fmt.Sprintf("The account %s is locked.", cfg.User)
		c.Fix = &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("ALTER USER %s ACCOUNT UNLOCK;", cfg.User)}
	case 28001:
		c.Message = fmt.Sprintf("The password of %s has expired.", cfg.User)
		c.Fix = &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("ALTER USER %s IDENTIFIED BY \"<new password>\";", cfg.User)}
	case 12514, 12505:
		c.Message = fmt.Sprintf("The listener does not know the service %s.", service)
		c.Why = "Debezium connects to the database's service name, and BeziSync reads tables through the PDB's."
		c.Fix = &checks.Fix{Kind: "shell", Who: "DBA", Code: "lsnrctl services   # lists the service names this listener serves"}
	}
	return c
}

func (s *oracleSource) Check(ctx context.Context, conn Resolved, tables []string) (checks.Report, error) {
	started := time.Now()
	cfg, _, err := oraCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	var reachable bool
	if Str(conn.Config, "ssh_host") != "" {
		reachable = SSHChecks(ctx, run, conn.Config, conn.Secrets, cfg.Host, cfg.Port, "ora")
	} else {
		reachable = NetworkCheck(ctx, run, cfg.Host, cfg.Port, "ora_network", "the database", 0)
	}
	if !reachable {
		return checks.Summarize(run.Checks, started), nil
	}
	// Debezium signs in to the database's own service (the CDB root of a multitenant database).
	root, doneRoot, err := oraConnect(ctx, conn, cfg.Database, 6*time.Second)
	if err != nil {
		if isOraOperational(err) {
			run.Add(oraLoginFailure(err, cfg, cfg.Database))
			return checks.Summarize(run.Checks, started), nil
		}
		return checks.Report{}, err
	}
	defer doneRoot()
	run.Passed("ora_login", "Sign in", checks.CatAuth, fmt.Sprintf("Signed in to %s as %s.", cfg.Database, cfg.User), "")
	if err := s.serverChecks(ctx, root, run, conn.Provider, cfg); err != nil {
		if isOraOperational(err) {
			run.Add(oraLoginFailure(err, cfg, cfg.Database))
			return checks.Summarize(run.Checks, started), nil
		}
		return checks.Report{}, err
	}
	tablesDB := root
	if cfg.PDB != "" {
		pdb, donePDB, err := oraConnect(ctx, conn, cfg.tableService(), 6*time.Second)
		if err != nil {
			if isOraOperational(err) {
				c := oraLoginFailure(err, cfg, cfg.tableService())
				c.Code, c.Title = "ora_pdb", "Pluggable database"
				run.Add(c)
				return checks.Summarize(run.Checks, started), nil
			}
			return checks.Report{}, err
		}
		defer donePDB()
		con, err := oraOne[string](ctx, pdb, "SELECT sys_context('USERENV', 'CON_NAME') FROM dual")
		if err != nil {
			return checks.Report{}, err
		}
		if !strings.EqualFold(con, cfg.PDB) {
			run.Add(checks.Check{Code: "ora_pdb", Title: "Pluggable database", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
				Message:   fmt.Sprintf("The service %s opens %s, not the pluggable database %s.", cfg.tableService(), con, cfg.PDB),
				Technical: "Set the service name of the PDB (Expert) when it differs from the PDB's name."})
			return checks.Summarize(run.Checks, started), nil
		}
		run.Passed("ora_pdb", "Pluggable database", checks.CatCDC, fmt.Sprintf("Reads tables from %s.", con), "")
		tablesDB = pdb
	}
	if err := s.managedChecks(ctx, tablesDB, run, cfg); err != nil {
		return checks.Report{}, err
	}
	if len(tables) > 0 {
		if err := s.tableChecks(ctx, tablesDB, run, tables, cfg, conn.Provider); err != nil {
			if isOraOperational(err) {
				run.Add(oraLoginFailure(err, cfg, cfg.tableService()))
				return checks.Summarize(run.Checks, started), nil
			}
			return checks.Report{}, err
		}
	}
	return checks.Summarize(run.Checks, started), nil
}

func oraArchiveLogFix(provider string) *checks.Fix {
	switch provider {
	case "aws_rds":
		return &checks.Fix{Kind: "shell", Who: "DBA", Code: "# RDS keeps the database in ARCHIVELOG mode while automated backups are on:\n" +
			"aws rds modify-db-instance --db-instance-identifier <your-instance> --backup-retention-period 7 --apply-immediately"}
	case "oci":
		return &checks.Fix{Kind: "shell", Who: "DBA", Code: "# Enable automatic backups for the DB system (OCI console → Database → Backups);\n" +
			"# Base Database Service runs in ARCHIVELOG mode while they are on."}
	}
	return &checks.Fix{Kind: "sql", Who: "DBA", RestartRequired: true, Code: "SHUTDOWN IMMEDIATE;\nSTARTUP MOUNT;\nALTER DATABASE ARCHIVELOG;\n" +
		"ALTER DATABASE OPEN;\nALTER PLUGGABLE DATABASE ALL OPEN;   -- multitenant databases only"}
}

func oraSupplementalFix(provider string) *checks.Fix {
	if provider == "aws_rds" {
		return &checks.Fix{Kind: "sql", Who: "DBA", Code: "BEGIN rdsadmin.rdsadmin_util.alter_supplemental_logging(p_action => 'ADD'); END;\n/"}
	}
	return &checks.Fix{Kind: "sql", Who: "DBA", Code: "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;"}
}

func (s *oracleSource) serverChecks(ctx context.Context, db *sql.Conn, run *checks.Run, provider string, cfg oraConfig) error {
	// version_full (18c+) has the release update ("19.21.0.0.0"); version only the base release.
	version, err := oraOne[string](ctx, db, "SELECT version_full FROM product_component_version WHERE product LIKE 'Oracle%' AND ROWNUM = 1")
	if oraCode(err) == 904 {
		version, err = oraOne[string](ctx, db, "SELECT version FROM product_component_version WHERE product LIKE 'Oracle%' AND ROWNUM = 1")
	}
	if err != nil {
		return err
	}
	major, _ := strconv.Atoi(strings.SplitN(version, ".", 2)[0])
	if major < 19 {
		run.Add(checks.Check{Code: "ora_version", Title: "Oracle version", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: fmt.Sprintf("Oracle %s is too old. Debezium 3 supports 19c and newer.", version)})
	} else {
		run.Passed("ora_version", "Oracle version", checks.CatCDC, fmt.Sprintf("Oracle %s.", version), "")
	}
	missingPrivs, err := s.privilegeChecks(ctx, db, run, provider, cfg)
	if err != nil {
		return err
	}
	if missingPrivs["V$DATABASE"] {
		// Without V$DATABASE nothing about logging can be read; the permission check says what to grant.
		return nil
	}
	var logMode, minimal, all string
	if err := db.QueryRowContext(ctx, "SELECT log_mode, supplemental_log_data_min, supplemental_log_data_all FROM v$database").Scan(&logMode, &minimal, &all); err != nil {
		return err
	}
	if logMode == "ARCHIVELOG" {
		run.Passed("ora_archivelog", "Archive log mode", checks.CatCDC, "The database archives its redo logs.", "log_mode = ARCHIVELOG")
	} else {
		run.Add(checks.Check{Code: "ora_archivelog", Title: "Archive log mode", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message:   "The database doesn't archive its redo logs, so changes can be lost before Debezium reads them.",
			Technical: "log_mode = " + logMode + "; LogMiner needs ARCHIVELOG.", Why: "Switching modes restarts the database: plan it.",
			Fix: oraArchiveLogFix(provider)})
	}
	if minimal == "YES" || minimal == "IMPLICIT" {
		run.Passed("ora_supplemental", "Supplemental logging", checks.CatCDC, "Minimal supplemental logging is on.", "supplemental_log_data_min = "+minimal)
	} else {
		run.Add(checks.Check{Code: "ora_supplemental", Title: "Supplemental logging", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message:   "The redo log doesn't record enough to rebuild changed rows.",
			Technical: "supplemental_log_data_min = " + minimal + "; LogMiner needs at least minimal supplemental logging.",
			Fix:       oraSupplementalFix(provider)})
	}
	if err := s.redoChecks(ctx, db, run); err != nil {
		return err
	}
	return s.retentionCheck(ctx, db, run, provider)
}

// privilegeChecks reports missing privileges and returns the views that cannot be read.
func (s *oracleSource) privilegeChecks(ctx context.Context, db *sql.Conn, run *checks.Run, provider string, cfg oraConfig) (map[string]bool, error) {
	have := map[string]bool{}
	for _, q := range []string{"SELECT privilege FROM session_privs", "SELECT role FROM session_roles"} {
		if err := eachRow(ctx, db, q, nil, func(rows *sql.Rows) error {
			var p string
			if err := rows.Scan(&p); err != nil {
				return err
			}
			have[p] = true
			return nil
		}); err != nil {
			return nil, err
		}
	}
	want := append([]string{}, oraSystemPrivileges...)
	if cfg.PDB != "" {
		want = append(want, "SET CONTAINER")
	}
	var privs, roles []string
	for _, p := range want {
		if !have[p] {
			privs = append(privs, p)
		}
	}
	for _, r := range oraRoles {
		if !have[r] {
			roles = append(roles, r)
		}
	}
	unreadable := map[string]bool{}
	var views []string
	for _, v := range oraViews {
		var one int
		err := db.QueryRowContext(ctx, "SELECT 1 FROM "+v+" WHERE ROWNUM = 1").Scan(&one)
		// ORA-00942 / ORA-01031: not visible; anything else (no rows, LogMiner not started) is readable.
		if code := oraCode(err); code == 942 || code == 1031 {
			unreadable[v] = true
			views = append(views, v)
		} else if err != nil && !errors.Is(err, sql.ErrNoRows) && code == 0 && isOraOperational(err) {
			return nil, err
		}
	}
	packages, err := oraOne[int64](ctx, db, "SELECT COUNT(*) FROM all_objects WHERE owner = 'SYS' AND object_type = 'PACKAGE' AND object_name IN ('DBMS_LOGMNR', 'DBMS_LOGMNR_D')")
	if err != nil {
		return nil, err
	}
	var pkgs []string
	if packages < 2 {
		pkgs = []string{"DBMS_LOGMNR", "DBMS_LOGMNR_D"}
	}
	missing := append(append(append([]string{}, privs...), roles...), views...)
	missing = append(missing, pkgs...)
	if len(missing) == 0 {
		run.Passed("ora_privileges", "Permissions", checks.CatPermissions, fmt.Sprintf("%s has every privilege LogMiner needs.", cfg.User), "")
		return unreadable, nil
	}
	run.Add(checks.Check{Code: "ora_privileges", Title: "Permissions", Status: checks.Fail, Category: checks.CatPermissions, Blocking: true,
		Message: fmt.Sprintf("%s is missing %s%s.", cfg.User, strings.Join(first(missing, 8), ", "), pick(len(missing) > 8, fmt.Sprintf(" and %d more", len(missing)-8), "")),
		Why:     "Debezium reads the redo log with LogMiner and the tables with flashback queries.",
		Fix:     oraGrantFix(provider, cfg, privs, roles, views, pkgs)})
	return unreadable, nil
}

func oraGrantFix(provider string, cfg oraConfig, privs, roles, views, pkgs []string) *checks.Fix {
	user := cfg.owner()
	scope := ""
	if cfg.PDB != "" {
		scope = " CONTAINER=ALL"
	}
	var lines []string
	if len(privs)+len(roles) > 0 {
		lines = append(lines, fmt.Sprintf("GRANT %s TO %s%s;", strings.Join(append(append([]string{}, privs...), roles...), ", "), user, scope))
	}
	for _, v := range views {
		object := strings.Replace(v, "V$", "V_$", 1)
		if provider == "aws_rds" {
			lines = append(lines, fmt.Sprintf("BEGIN rdsadmin.rdsadmin_util.grant_sys_object('%s', '%s', 'SELECT'); END;\n/", object, user))
		} else {
			lines = append(lines, fmt.Sprintf("GRANT SELECT ON %s TO %s%s;", object, user, scope))
		}
	}
	for _, p := range pkgs {
		if provider == "aws_rds" {
			lines = append(lines, fmt.Sprintf("BEGIN rdsadmin.rdsadmin_util.grant_sys_object('%s', '%s', 'EXECUTE'); END;\n/", p, user))
		} else {
			lines = append(lines, fmt.Sprintf("GRANT EXECUTE ON %s TO %s%s;", p, user, scope))
		}
	}
	return &checks.Fix{Kind: "sql", Who: "DBA", Code: strings.Join(lines, "\n")}
}

func (s *oracleSource) redoChecks(ctx context.Context, db *sql.Conn, run *checks.Run) error {
	var groups, smallest sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*), MIN(bytes) FROM v$log").Scan(&groups, &smallest); err != nil {
		if code := oraCode(err); code == 942 || code == 1031 {
			return nil
		}
		return err
	}
	switches, err := oraOne[int64](ctx, db, "SELECT COUNT(*) FROM v$log_history WHERE first_time > SYSDATE - 1/24")
	if err != nil {
		if code := oraCode(err); code == 942 || code == 1031 {
			return nil
		}
		return err
	}
	size := float64(smallest.Int64)
	detail := fmt.Sprintf("%d redo log groups, the smallest %s; %d log switches in the last hour.", groups.Int64, domain.HumanBytes(&size), switches)
	if groups.Int64 < 3 || switches > 12 {
		run.Add(checks.Check{Code: "ora_redo", Title: "Redo logs", Status: checks.Warn, Category: checks.CatCapacity,
			Message:   "The redo logs switch often or there are few of them, which slows LogMiner down under load.",
			Technical: detail, Why: "Each switch makes LogMiner rebuild its session; Debezium recommends at least 3 groups and about 4 switches an hour.",
			Fix: &checks.Fix{Kind: "sql", Who: "DBA", Code: "-- add larger groups, then drop the small ones once they are INACTIVE:\n" +
				"ALTER DATABASE ADD LOGFILE GROUP <n> SIZE 1G;"}})
		return nil
	}
	run.Passed("ora_redo", "Redo logs", checks.CatCapacity, detail, "")
	return nil
}

// oraRetention is how far back the archived logs reach (nil when none is kept yet) and, on RDS,
// the configured retention hours.
func oraRetention(ctx context.Context, db *sql.Conn) (*int64, error) {
	var seconds sql.NullFloat64
	err := db.QueryRowContext(ctx, `SELECT (CAST(SYSTIMESTAMP AS DATE) - MIN(first_time)) * 86400 FROM v$archived_log
	                                 WHERE deleted = 'NO' AND status = 'A' AND standby_dest = 'NO'`).Scan(&seconds)
	if err != nil {
		return nil, err
	}
	if !seconds.Valid {
		return nil, nil
	}
	v := int64(seconds.Float64)
	return &v, nil
}

func (s *oracleSource) retentionCheck(ctx context.Context, db *sql.Conn, run *checks.Run, provider string) error {
	if provider == "aws_rds" {
		hours, err := oraOne[sql.NullString](ctx, db, "SELECT value FROM rdsadmin.rds_configuration WHERE name = 'archivelog retention hours'")
		if err == nil {
			h, _ := strconv.Atoi(strings.TrimSpace(hours.String))
			if h < 24 {
				run.Add(checks.Check{Code: "ora_archive_retention", Title: "Archived log retention", Status: checks.Warn, Category: checks.CatResilience,
					Message:   fmt.Sprintf("RDS keeps archived redo logs for %d hours, so a pipeline stopped for longer loses changes.", h),
					Technical: "rdsadmin.rds_configuration: archivelog retention hours = " + strings.TrimSpace(hours.String),
					Fix: &checks.Fix{Kind: "sql", Who: "DBA", Code: "BEGIN rdsadmin.rdsadmin_util.set_configuration('archivelog retention hours', 24); END;\n/\n" +
						"-- keep the extra archived logs in mind when sizing storage"}})
			} else {
				run.Passed("ora_archive_retention", "Archived log retention", checks.CatResilience, fmt.Sprintf("RDS keeps archived logs for %d hours.", h), "")
			}
			return nil
		}
		if code := oraCode(err); code != 942 && code != 1031 {
			return err
		}
	}
	kept, err := oraRetention(ctx, db)
	if err != nil {
		if code := oraCode(err); code == 942 || code == 1031 {
			return nil
		}
		return err
	}
	switch {
	case kept == nil:
		run.Passed("ora_archive_retention", "Archived log retention", checks.CatResilience, "No archived log yet: the database has not switched logs since archiving began.", "")
	case *kept < 24*3600:
		run.Add(checks.Check{Code: "ora_archive_retention", Title: "Archived log retention", Status: checks.Warn, Category: checks.CatResilience,
			Message:   fmt.Sprintf("Archived logs reach back %s, so a pipeline stopped for longer loses changes.", HumanSeconds(*kept)),
			Technical: "The oldest archived log still on disk (v$archived_log) is that old.",
			Why:       "Keep at least a day of archived logs (RMAN deletion policy, FRA size) so pipelines survive an outage.",
			Fix: &checks.Fix{Kind: "shell", Who: "DBA", Code: "rman target / <<'EOF'\nCONFIGURE ARCHIVELOG DELETION POLICY TO APPLIED ON ALL STANDBY;\n" +
				"# delete archived logs only when older than a day:\nDELETE ARCHIVELOG ALL COMPLETED BEFORE 'SYSDATE-1';\nEOF"}})
	default:
		run.Passed("ora_archive_retention", "Archived log retention", checks.CatResilience, fmt.Sprintf("Archived logs reach back %s.", HumanSeconds(*kept)), "")
	}
	return nil
}

// managedChecks: BeziSync creates its signal (and canary) tables in the user's own schema.
func (s *oracleSource) managedChecks(ctx context.Context, db *sql.Conn, run *checks.Run, cfg oraConfig) error {
	unlimited, err := oraOne[int64](ctx, db, "SELECT COUNT(*) FROM session_privs WHERE privilege = 'UNLIMITED TABLESPACE'")
	if err != nil {
		return err
	}
	quota, err := oraOne[int64](ctx, db, `SELECT COUNT(*) FROM user_ts_quotas q JOIN user_users u ON u.default_tablespace = q.tablespace_name
	                                       WHERE q.max_bytes = -1 OR q.max_bytes - q.bytes > 1048576`)
	if err != nil {
		return err
	}
	if unlimited > 0 || quota > 0 {
		run.Passed("ora_managed_tables", "BeziSync tables", checks.CatPermissions, fmt.Sprintf("Can create BeziSync's signal table in %s.", cfg.owner()), "")
		return nil
	}
	tablespace, err := oraOne[string](ctx, db, "SELECT default_tablespace FROM user_users")
	if err != nil {
		return err
	}
	run.Add(checks.Check{Code: "ora_managed_tables", Title: "BeziSync tables", Status: checks.Fail, Category: checks.CatPermissions, Blocking: true,
		Message:   fmt.Sprintf("%s has no quota on its default tablespace %s, so BeziSync cannot create its signal table.", cfg.owner(), tablespace),
		Technical: "Snapshots on demand are sent through BZ_SIGNAL_<id> in the connector user's schema.",
		Fix:       &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("ALTER USER %s QUOTA 100M ON %s;", cfg.owner(), tablespace)}})
	return nil
}

func (s *oracleSource) tableChecks(ctx context.Context, db *sql.Conn, run *checks.Run, tables []string, cfg oraConfig, provider string) error {
	found := map[string]bool{}
	keyed := map[string]bool{}
	logged := map[string]bool{}
	var rowsTotal int64
	allLogged, err := oraOne[string](ctx, db, "SELECT supplemental_log_data_all FROM v$database")
	if err != nil && oraCode(err) != 942 && oraCode(err) != 1031 {
		return err
	}
	for _, t := range tables {
		schema, table, _ := strings.Cut(t, ".")
		var numRows sql.NullInt64
		err := db.QueryRowContext(ctx, "SELECT num_rows FROM all_tables WHERE owner = :1 AND table_name = :2", schema, table).Scan(&numRows)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		found[t] = true
		rowsTotal += numRows.Int64
		pk, err := oraOne[int64](ctx, db, "SELECT COUNT(*) FROM all_constraints WHERE owner = :1 AND table_name = :2 AND constraint_type = 'P'", schema, table)
		if err != nil {
			return err
		}
		keyed[t] = pk > 0
		lg, err := oraOne[int64](ctx, db, "SELECT COUNT(*) FROM all_log_groups WHERE owner = :1 AND table_name = :2 AND log_group_type = 'ALL COLUMN LOGGING'", schema, table)
		if err != nil {
			return err
		}
		logged[t] = lg > 0 || allLogged == "YES"
	}
	var missing, unlogged, unkeyed []string
	for _, t := range tables {
		switch {
		case !found[t]:
			missing = append(missing, t)
		case !logged[t]:
			unlogged = append(unlogged, t)
		}
		if found[t] && !keyed[t] {
			unkeyed = append(unkeyed, t)
		}
	}
	if len(missing) > 0 {
		run.Add(checks.Check{Code: "ora_tables_exist", Title: "Selected tables exist", Status: checks.Fail, Category: checks.CatTables, Blocking: true,
			Message: fmt.Sprintf("Not found: %s.", strings.Join(first(missing, 10), ", "))})
	}
	if len(unlogged) > 0 {
		var alters []string
		for _, t := range first(unlogged, 50) {
			alters = append(alters, fmt.Sprintf("ALTER TABLE %s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;", oraQualified(t)))
		}
		run.Add(checks.Check{Code: "ora_tables_logging", Title: "Table logging", Status: checks.Fail, Category: checks.CatTables, Blocking: true,
			Message:   fmt.Sprintf("Supplemental logging of all columns is off for %s, so their updates can't be rebuilt.", strings.Join(first(unlogged, 10), ", ")),
			Technical: "Debezium needs (ALL) COLUMNS supplemental logging on every captured table (or on the whole database).",
			Fix:       &checks.Fix{Kind: "sql", Who: "DBA", Code: strings.Join(alters, "\n")}})
	}
	if len(unkeyed) > 0 {
		run.Add(checks.Check{Code: "ora_tables_keys", Title: "Row keys", Status: checks.Warn, Category: checks.CatTables,
			Message:   fmt.Sprintf("No primary key on %s: events carry no key, and re-copies use ROWID.", strings.Join(first(unkeyed, 10), ", ")),
			Technical: "Without a key, consumers cannot tell which row an update or delete belongs to."})
	}
	if undo, err := oraOne[string](ctx, db, "SELECT value FROM v$parameter WHERE name = 'undo_retention'"); err == nil {
		seconds, _ := strconv.ParseInt(undo, 10, 64)
		estimate := rowsTotal / 10_000
		if seconds > 0 && estimate > seconds {
			run.Add(checks.Check{Code: "ora_undo", Title: "Undo retention", Status: checks.Warn, Category: checks.CatResilience,
				Message: fmt.Sprintf("Copying the existing rows may take about %s, longer than the %s of undo the database keeps.",
					HumanSeconds(estimate), HumanSeconds(seconds)),
				Technical: "The first copy reads every table AS OF one SCN; when undo is gone it fails with ORA-01555 (snapshot too old).",
				Fix:       &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("ALTER SYSTEM SET UNDO_RETENTION = %d;", (estimate*3/2/60+1)*60)}})
		}
	} else if oraCode(err) != 942 && oraCode(err) != 1031 {
		return err
	}
	if len(found) > 0 && len(missing) == 0 && len(unlogged) == 0 {
		run.Passed("ora_tables", "Selected tables", checks.CatTables, fmt.Sprintf("%d tables can be captured.", len(found)), "")
	}
	return nil
}

func (s *oracleSource) Discover(ctx context.Context, conn Resolved) (Discovery, error) {
	cfg, _, err := oraCfg(conn)
	if err != nil {
		return Discovery{}, err
	}
	db, done, err := oraConnect(ctx, conn, cfg.tableService(), 10*time.Second)
	if err != nil {
		return Discovery{}, err
	}
	defer done()
	type trow struct {
		schema, name string
		rows, bytes  *int64
	}
	var trows []trow
	if err := eachRow(ctx, db, fmt.Sprintf(`
	        SELECT t.owner, t.table_name, t.num_rows, t.num_rows * t.avg_row_len
	          FROM all_tables t JOIN all_users u ON u.username = t.owner
	         WHERE u.oracle_maintained = 'N' AND t.temporary = 'N' AND t.nested = 'NO' AND t.secondary = 'N'
	           AND t.table_name NOT LIKE 'BIN$%%' AND t.table_name <> 'LOG_MINING_FLUSH'
	           AND NOT (t.owner = :1 AND (t.table_name LIKE 'BZ\_SIGNAL\_%%' ESCAPE '\' OR t.table_name LIKE 'BZ\_CANARY\_%%' ESCAPE '\'))
	         ORDER BY 1, 2 FETCH FIRST %d ROWS ONLY`, oraMaxTables+1), []any{cfg.owner()}, func(rows *sql.Rows) error {
		var r trow
		var n, b sql.NullInt64
		if err := rows.Scan(&r.schema, &r.name, &n, &b); err != nil {
			return err
		}
		if n.Valid {
			r.rows = &n.Int64
		}
		if b.Valid {
			r.bytes = &b.Int64
		}
		trows = append(trows, r)
		return nil
	}); err != nil {
		return Discovery{}, err
	}
	truncated := len(trows) > oraMaxTables
	trows = first(trows, oraMaxTables)
	owners := map[string]bool{}
	for _, r := range trows {
		owners[r.schema] = true
	}
	key := func(schema, table string) string { return schema + "." + table }
	cols := map[string][]Column{}
	pks := map[string][]string{}
	logged := map[string]bool{}
	allLogged, err := oraOne[string](ctx, db, "SELECT supplemental_log_data_all FROM v$database")
	if err != nil && oraCode(err) != 942 && oraCode(err) != 1031 {
		return Discovery{}, err
	}
	for _, owner := range sortedKeys(owners) {
		if err := eachRow(ctx, db, `SELECT table_name, column_name, data_type, data_type_owner, nullable FROM all_tab_columns
		                             WHERE owner = :1 ORDER BY table_name, column_id`, []any{owner}, func(rows *sql.Rows) error {
			var table, column, dataType, nullable string
			var typeOwner sql.NullString
			if err := rows.Scan(&table, &column, &dataType, &typeOwner, &nullable); err != nil {
				return err
			}
			typ := oraTypeName(dataType)
			if typeOwner.Valid && typeOwner.String != "" && typ != "XMLTYPE" {
				typ = typeOwner.String + "." + typ // an object type: not captured
			}
			cols[key(owner, table)] = append(cols[key(owner, table)], Column{Name: column, Type: typ, Nullable: nullable == "Y", PII: LooksLikePII(column)})
			return nil
		}); err != nil {
			return Discovery{}, err
		}
		if err := eachRow(ctx, db, `SELECT cc.table_name, cc.column_name FROM all_constraints c
		                              JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
		                             WHERE c.owner = :1 AND c.constraint_type = 'P' ORDER BY cc.table_name, cc.position`, []any{owner}, func(rows *sql.Rows) error {
			var table, column string
			if err := rows.Scan(&table, &column); err != nil {
				return err
			}
			pks[key(owner, table)] = append(pks[key(owner, table)], column)
			return nil
		}); err != nil {
			return Discovery{}, err
		}
		if err := eachRow(ctx, db, `SELECT table_name FROM all_log_groups WHERE owner = :1 AND log_group_type = 'ALL COLUMN LOGGING'`,
			[]any{owner}, func(rows *sql.Rows) error {
				var table string
				if err := rows.Scan(&table); err != nil {
					return err
				}
				logged[key(owner, table)] = true
				return nil
			}); err != nil {
			return Discovery{}, err
		}
	}
	var tables []TableInfo
	for _, r := range trows {
		k := key(r.schema, r.name)
		var warnings []string
		selectable := true
		if TopicSafe(r.schema) != r.schema || TopicSafe(r.name) != r.name {
			selectable = false
			warnings = append(warnings, "Its name has characters Debezium replaces in topic names ('#', '$'), so BeziSync can't route it.")
		}
		if len(pks[k]) == 0 {
			warnings = append(warnings, "No primary key: events carry no key.")
		}
		if !logged[k] && allLogged != "YES" {
			warnings = append(warnings, "Supplemental logging of all columns is off: the final checks show the fix.")
		}
		var lobs, unsupported []string
		for _, c := range cols[k] {
			switch {
			case oraLOBTypes[c.Type]:
				lobs = append(lobs, c.Name)
			case oraUnsupported[c.Type] || strings.Contains(c.Type, "."):
				unsupported = append(unsupported, c.Name)
			}
		}
		if len(lobs) > 0 && !cfg.LOB {
			warnings = append(warnings, fmt.Sprintf("LOB columns (%s) arrive empty unless the connection captures LOBs.", strings.Join(first(lobs, 5), ", ")))
		}
		if len(unsupported) > 0 {
			warnings = append(warnings, fmt.Sprintf("Debezium doesn't capture %s (%s are not supported).", strings.Join(first(unsupported, 5), ", "), oraUnsupportedN))
		}
		tables = append(tables, TableInfo{Schema: r.schema, Name: r.name, RowsEstimate: r.rows, BytesEstimate: r.bytes,
			PrimaryKey: pks[k], Columns: cols[k], Warnings: warnings, Selectable: selectable})
	}
	return NewDiscovery(tables, truncated), nil
}

// oraTypeName drops the precision Oracle reports in timestamp types (TIMESTAMP(6) → TIMESTAMP).
func oraTypeName(t string) string {
	if i := strings.Index(t, "("); i > 0 && strings.HasPrefix(t, "TIMESTAMP") {
		return t[:i] + t[strings.Index(t, ")")+1:]
	}
	return t
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (s *oracleSource) CompileSource(conn Resolved, c CompileContext) (map[string]string, map[string]string, bool, error) {
	cfg, sec, err := oraCfg(conn)
	if err != nil {
		return nil, nil, false, err
	}
	spec := c.Spec
	tables := append(append([]string{}, spec.Source.Tables.Include...), cfg.owner()+"."+oraSignalTable(c))
	if canaryOn(c) {
		tables = append(tables, cfg.owner()+"."+oraCanaryTable(c))
	}
	props := map[string]string{
		"debezium.source.connector.class":                                        "io.debezium.connector.oracle.OracleConnector",
		"debezium.source.database.hostname":                                      cfg.Host,
		"debezium.source.database.port":                                          strconv.Itoa(cfg.Port),
		"debezium.source.database.user":                                          cfg.User,
		"debezium.source.database.password":                                      "${BZ_SRC_PASSWORD}",
		"debezium.source.database.dbname":                                        cfg.Database,
		"debezium.source.table.include.list":                                     joinRegex(tables),
		"debezium.source.signal.enabled.channels":                                "source",
		"debezium.source.signal.data.collection":                                 cfg.container() + "." + cfg.owner() + "." + oraSignalTable(c),
		"debezium.source.heartbeat.interval.ms":                                  strconv.Itoa(spec.Policies.HeartbeatSeconds * 1000),
		"debezium.source.schema.history.internal.store.only.captured.tables.ddl": "true",
		"debezium.source.include.schema.changes":                                 "false",
		"debezium.source.lob.enabled":                                            boolStr(cfg.LOB),
	}
	if cfg.PDB != "" {
		props["debezium.source.database.pdb.name"] = strings.ToUpper(cfg.PDB)
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

// ManagedTopicPattern: the signal table's topic (Debezium writes '#' of a common user as '_').
func (s *oracleSource) ManagedTopicPattern(c CompileContext) string {
	return JavaRegexLiteral(c.Prefix()) + `\.[^.]+\.` + JavaRegexLiteral(oraSignalTable(c))
}

func (*oracleSource) CanaryTable(conn Resolved, c CompileContext) (string, error) {
	cfg, _, err := oraCfg(conn)
	if err != nil {
		return "", err
	}
	return TopicSafe(cfg.owner()) + "." + oraCanaryTable(c), nil
}

// oraCreate creates a table unless it exists and turns on all-column logging for it.
func oraCreate(ctx context.Context, db *sql.Conn, owner, table, columns string) (string, error) {
	exists, err := oraOne[int64](ctx, db, "SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2", owner, table)
	if err != nil {
		return "", err
	}
	name := OraIdent(owner) + "." + OraIdent(table)
	outcome := "exists"
	if exists == 0 {
		if _, err := db.ExecContext(ctx, "CREATE TABLE "+name+" ("+columns+")"); err != nil && oraCode(err) != 955 {
			return "", err
		}
		outcome = "created"
	}
	// ORA-32588: the table already logs all columns.
	if _, err := db.ExecContext(ctx, "ALTER TABLE "+name+" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"); err != nil && oraCode(err) != 32588 {
		return "", err
	}
	return outcome, nil
}

func (s *oracleSource) Prepare(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	cfg, _, err := oraCfg(conn)
	if err != nil {
		return nil, err
	}
	if c.Engine == "flink-cdc" {
		// No signal or canary table (Flink CDC reads neither); on a multitenant database the flush
		// table its Debezium 1.9 needs in the CDB root (ADR 0006).
		if cfg.PDB == "" {
			return nil, nil
		}
		r, err := s.prepareFlush(ctx, conn, cfg)
		if err != nil {
			return nil, err
		}
		return []ArtifactResult{r}, nil
	}
	db, done, err := oraConnect(ctx, conn, cfg.tableService(), 6*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	outcome, err := oraCreate(ctx, db, cfg.owner(), oraSignalTable(c), "ID VARCHAR2(64) PRIMARY KEY, TYPE VARCHAR2(64) NOT NULL, DATA VARCHAR2(4000)")
	if err != nil {
		return nil, err
	}
	results := []ArtifactResult{{Kind: "table", Name: cfg.owner() + "." + oraSignalTable(c), Outcome: outcome}}
	if canaryOn(c) {
		outcome, err := oraCreate(ctx, db, cfg.owner(), oraCanaryTable(c), "ID VARCHAR2(64) PRIMARY KEY, SENT_AT TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL")
		if err != nil {
			return nil, err
		}
		results = append(results, ArtifactResult{Kind: "table", Name: cfg.owner() + "." + oraCanaryTable(c), Outcome: outcome})
	}
	return results, nil
}

func (s *oracleSource) Cleanup(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	cfg, _, err := oraCfg(conn)
	if err != nil {
		return nil, err
	}
	if c.Engine == "flink-cdc" {
		// It created no table of its own; the flush table is the connector user's, shared by every
		// pipeline (either engine) that mines as that user.
		return nil, nil
	}
	db, done, err := oraConnect(ctx, conn, cfg.tableService(), 6*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	var results []ArtifactResult
	for _, table := range []string{oraSignalTable(c), oraCanaryTable(c)} {
		_, err := db.ExecContext(ctx, "DROP TABLE "+OraIdent(cfg.owner())+"."+OraIdent(table)+" PURGE")
		switch {
		case oraCode(err) == 942:
			if table == oraSignalTable(c) {
				results = append(results, ArtifactResult{Kind: "table", Name: cfg.owner() + "." + table, Outcome: "not_found"})
			}
		case err != nil:
			return nil, err
		default:
			results = append(results, ArtifactResult{Kind: "table", Name: cfg.owner() + "." + table, Outcome: "removed"})
		}
	}
	return results, nil
}

func (s *oracleSource) WriteCanary(ctx context.Context, conn Resolved, c CompileContext, token string) error {
	cfg, _, err := oraCfg(conn)
	if err != nil {
		return err
	}
	db, done, err := oraConnect(ctx, conn, cfg.tableService(), 6*time.Second)
	if err != nil {
		return err
	}
	defer done()
	table := OraIdent(cfg.owner()) + "." + OraIdent(oraCanaryTable(c))
	if _, err := db.ExecContext(ctx, "INSERT INTO "+table+" (ID) VALUES (:1)", token); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "DELETE FROM "+table+" WHERE SENT_AT < SYSTIMESTAMP - INTERVAL '1' DAY")
	return err
}

func (s *oracleSource) Signal(ctx context.Context, conn Resolved, c CompileContext, signalType string, data map[string]any) (string, error) {
	cfg, _, err := oraCfg(conn)
	if err != nil {
		return "", err
	}
	// Debezium names Oracle data collections <database>.<schema>.<table>.
	if dc, ok := data["data-collections"]; ok {
		copied := map[string]any{}
		for k, v := range data {
			copied[k] = v
		}
		qualified := []any{}
		switch list := dc.(type) {
		case []any:
			for _, t := range list {
				qualified = append(qualified, cfg.container()+"."+PyStr(t))
			}
		case []string:
			for _, t := range list {
				qualified = append(qualified, cfg.container()+"."+t)
			}
		}
		copied["data-collections"] = qualified
		data = copied
	}
	db, done, err := oraConnect(ctx, conn, cfg.tableService(), 6*time.Second)
	if err != nil {
		return "", err
	}
	defer done()
	payload, err := PyDumps(data)
	if err != nil {
		return "", err
	}
	signalID := hex.EncodeToString(uuidBytes())
	_, err = db.ExecContext(ctx, "INSERT INTO "+OraIdent(cfg.owner())+"."+OraIdent(oraSignalTable(c))+" (ID, TYPE, DATA) VALUES (:1, :2, :3)",
		signalID, signalType, payload)
	return signalID, err
}

func (s *oracleSource) Probe(ctx context.Context, conn Resolved, c CompileContext, _ int64) (*SourceImpact, error) {
	cfg, _, err := oraCfg(conn)
	if err != nil {
		return nil, err
	}
	db, done, err := oraConnect(ctx, conn, cfg.Database, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	kept, err := oraRetention(ctx, db)
	if err != nil {
		return nil, err
	}
	var scn *int64
	if OffsetsReader != nil {
		rows, err := OffsetsReader(ctx, c.ShortID)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			raw, _ := r["raw"].(map[string]any)
			if n, err := strconv.ParseInt(PyStr(raw["scn"]), 10, 64); err == nil {
				scn = &n
				break
			}
		}
	}
	fix := "Keep archived logs longer (RMAN deletion policy, recovery area size; on RDS 'archivelog retention hours')."
	if scn == nil && c.Engine == "flink-cdc" {
		return oraFlinkImpact(kept), nil
	}
	if scn == nil {
		return TimeWindowImpact(nil, kept, "the archive", fix), nil
	}
	// The oldest change still readable: online redo or a kept archived log.
	oldest, err := oraOne[sql.NullInt64](ctx, db, `SELECT MIN(first_change#) FROM (
	        SELECT first_change# FROM v$archived_log WHERE deleted = 'NO' AND status = 'A' AND standby_dest = 'NO'
	        UNION ALL SELECT first_change# FROM v$log)`)
	if err != nil {
		return nil, err
	}
	var behind *int64
	var seconds sql.NullFloat64
	// SCN_TO_TIMESTAMP fails (ORA-08181) for SCNs older than the database still maps to time.
	err = db.QueryRowContext(ctx, "SELECT (CAST(SYSTIMESTAMP AS DATE) - CAST(SCN_TO_TIMESTAMP(:1) AS DATE)) * 86400 FROM dual", *scn).Scan(&seconds)
	switch {
	case err == nil && seconds.Valid:
		v := max(int64(seconds.Float64), 0)
		behind = &v
	case err != nil && oraCode(err) != 8181:
		return nil, err
	}
	impact := TimeWindowImpact(behind, kept, "the archive", fix)
	if oldest.Valid && *scn < oldest.Int64 && !impact.Lost {
		impact.Lost = true
		r := "The redo the pipeline still needed was deleted: changes it had not read are gone."
		impact.Risk = &r
		impact.RiskFix = "Reset the pipeline's position and copy the tables again. " + fix
	}
	return impact, nil
}

// oraFlinkImpact: a Flink CDC pipeline's SCN is inside its encrypted saved state, so how much redo
// it still needs isn't measured; only the archive's window is (ADR 0006).
func oraFlinkImpact(kept *int64) *SourceImpact {
	impact := &SourceImpact{RetentionSeconds: kept}
	window := "how long the archive keeps changes is unknown"
	if kept != nil {
		window = "the archive keeps changes for " + HumanSeconds(*kept)
	}
	impact.Summary = "Flink CDC keeps its position in its saved state, so how much redo it still needs isn't measured; " + window + "."
	return impact
}
