package connectors

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

// What Flink CDC's Oracle reader needs from the database beyond what Debezium needs (ADR 0006,
// Rule 7): a version banner it can read, ANALYZE on the captured tables and, on a multitenant
// database, the flush table in the CDB root or the right to create it there.

// FlinkCheckFacts are what a source's Flink CDC checks need to know about the pipeline.
type FlinkCheckFacts struct {
	// Tables are the spec's tables.
	Tables []string
	// Copies: the pipeline copies tables when it starts without a saved position (its snapshot
	// mode, or Start over's choice, is not "from now").
	Copies bool
	// CopyNow: this deploy will copy (it copies and no saved position is left to restore).
	CopyNow bool
}

// FlinkSourceChecker is a source with checks of its own for Flink CDC pipelines.
type FlinkSourceChecker interface {
	FlinkChecks(ctx context.Context, conn Resolved, facts FlinkCheckFacts) []checks.Check
}

// oraVersionBanner is what Debezium 1.9.8 reads: BANNER_FULL (18c and later), else BANNER. found
// is false when no banner starts with "Oracle Database"; the banner is then the first one there is.
func oraVersionBanner(ctx context.Context, db *sql.Conn) (banner string, found bool, err error) {
	err = db.QueryRowContext(ctx, "SELECT banner_full FROM v$version WHERE banner_full LIKE 'Oracle Database%'").Scan(&banner)
	if oraCode(err) == 904 { // before 18c: no BANNER_FULL
		err = db.QueryRowContext(ctx, "SELECT banner FROM v$version WHERE banner LIKE 'Oracle Database%'").Scan(&banner)
	}
	switch {
	case err == sql.ErrNoRows:
		var other string
		if e := db.QueryRowContext(ctx, "SELECT banner FROM v$version WHERE ROWNUM = 1").Scan(&other); e == nil {
			return other, false, nil
		}
		return "", false, nil
	case err != nil:
		return "", false, err
	}
	return banner, true, nil
}

// FlinkChecks runs from BeziSync; the source's own ora_login check already blocks a database that
// can't be reached, so here that is only a warning.
func (s *oracleSource) FlinkChecks(ctx context.Context, conn Resolved, facts FlinkCheckFacts) []checks.Check {
	cfg, _, err := oraCfg(conn)
	if err != nil {
		return []checks.Check{oraFlinkUnknown(err)}
	}
	root, done, err := oraConnect(ctx, conn, cfg.Database, 6*time.Second)
	if err != nil {
		return []checks.Check{oraFlinkUnknown(err)}
	}
	defer done()
	var out []checks.Check
	banner, found, err := oraVersionBanner(ctx, root)
	switch code := oraCode(err); {
	case code == 942 || code == 1031:
		out = append(out, checks.Check{Code: "flink_oracle_version", Title: "Oracle version", Category: checks.CatCDC, Status: checks.Fail, Blocking: true,
			Message: fmt.Sprintf("%s can't read V$VERSION, which Flink CDC's Oracle reader reads before it starts.", cfg.owner()),
			Fix:     &checks.Fix{Kind: "sql", Who: "DBA", Code: oraContainerAll(cfg, "GRANT SELECT_CATALOG_ROLE TO "+OraIdent(cfg.owner()))}})
	case err != nil:
		return []checks.Check{oraFlinkUnknown(err)}
	default:
		out = append(out, flinkVersionCheck(banner, found))
	}
	tables := root
	if cfg.PDB != "" {
		pdb, donePDB, err := oraConnect(ctx, conn, cfg.tableService(), 6*time.Second)
		if err != nil {
			return append(out, oraFlinkUnknown(err))
		}
		defer donePDB()
		tables = pdb
	}
	a, err := oraAnalyzeFacts(ctx, tables, cfg, facts.Tables)
	if err != nil {
		return append(out, oraFlinkUnknown(err))
	}
	out = append(out, flinkAnalyzeCheck(cfg, a, facts))
	if cfg.PDB != "" {
		out = append(out, s.flushCheck(ctx, root, cfg))
	}
	return out
}

// flinkVersionCheck: the banner rule of Debezium 1.9.8 (FlinkReadsOracleBanner).
func flinkVersionCheck(banner string, found bool) checks.Check {
	first, _, _ := strings.Cut(strings.ReplaceAll(banner, "\r", ""), "\n")
	first = strings.TrimSpace(first)
	if FlinkReadsOracleBanner(banner, found) {
		return checks.Check{Code: "flink_oracle_version", Title: "Oracle version", Category: checks.CatCDC, Status: checks.Pass,
			Message: "Flink CDC's Oracle reader can read this database (" + first + ")."}
	}
	if first == "" {
		first = "no version banner"
	}
	return checks.Check{Code: "flink_oracle_version", Title: "Oracle version", Category: checks.CatCDC, Status: checks.Fail, Blocking: true,
		Message:   fmt.Sprintf("Flink CDC's Oracle reader can't read this database's version (%s), so its pipeline would fail at start.", first),
		Why:       "Flink CDC 3.7 reads Oracle with Debezium 1.9, which knows Enterprise, Standard and Express Edition up to 23ai, not Oracle Database Free or Oracle AI Database 26ai.",
		Technical: "Debezium 1.9.8 parses V$VERSION.BANNER_FULL LIKE 'Oracle Database%' ending in '- Production' and a Version line.",
		Fix: &checks.Fix{Kind: "text", Who: "you",
			Code: "Run this pipeline on Debezium, which reads this release (Iceberg and the event destinations), or read from an Enterprise, Standard or Express Edition database up to 23ai."}}
}

// oraAnalyze is what decides whether the first copy's ANALYZE TABLE can run.
type oraAnalyze struct {
	Privileged bool     // ANALYZE ANY in the tables' container
	OwnsAll    bool     // the user owns every captured table
	Locked     []string // captured tables whose statistics are locked (ORA-38029)
}

func oraAnalyzeFacts(ctx context.Context, db *sql.Conn, cfg oraConfig, tables []string) (oraAnalyze, error) {
	var a oraAnalyze
	n, err := oraOne[int64](ctx, db, "SELECT COUNT(*) FROM session_privs WHERE privilege = 'ANALYZE ANY'")
	if err != nil {
		return a, err
	}
	a.Privileged = n > 0
	a.OwnsAll = len(tables) > 0
	for _, t := range tables {
		schema, table, _ := strings.Cut(t, ".")
		if schema != cfg.owner() {
			a.OwnsAll = false
		}
		var locked sql.NullString
		err := db.QueryRowContext(ctx, "SELECT stattype_locked FROM all_tab_statistics WHERE owner = :1 AND table_name = :2 AND object_type = 'TABLE'",
			schema, table).Scan(&locked)
		switch {
		case err == sql.ErrNoRows: // a missing table fails in the source checks
		case err != nil:
			return a, err
		case locked.Valid && locked.String != "":
			a.Locked = append(a.Locked, t)
		}
	}
	return a, nil
}

// flinkAnalyzeCheck: the first copy runs ANALYZE TABLE … COMPUTE STATISTICS on every table.
func flinkAnalyzeCheck(cfg oraConfig, a oraAnalyze, facts FlinkCheckFacts) checks.Check {
	c := checks.Check{Code: "flink_oracle_analyze", Title: "Table statistics", Category: checks.CatPermissions}
	effect := "It reads each table in full and replaces its optimizer statistics; query plans that use them may change."
	switch {
	case len(a.Locked) > 0:
		stmts := make([]string, len(a.Locked))
		for i, t := range a.Locked {
			schema, table, _ := strings.Cut(t, ".")
			stmts[i] = fmt.Sprintf("EXEC DBMS_STATS.UNLOCK_TABLE_STATS('%s', '%s');", strings.ReplaceAll(schema, "'", "''"), strings.ReplaceAll(table, "'", "''"))
		}
		c.Status, c.Blocking = checks.Fail, facts.Copies
		if !facts.Copies {
			c.Status = checks.Warn
		}
		c.Message = fmt.Sprintf("%s %s locked statistics, and Flink CDC's first copy runs ANALYZE TABLE on each table (it fails with ORA-38029).",
			strings.Join(a.Locked, ", "), oraPlural(len(a.Locked), "has", "have"))
		c.Why = effect
		c.Fix = &checks.Fix{Kind: "sql", Who: "DBA", Code: strings.Join(stmts, "\n")}
	case !a.Privileged && !a.OwnsAll:
		grant := "GRANT ANALYZE ANY TO " + OraIdent(cfg.owner()) + ";"
		if cfg.PDB != "" {
			grant = "-- In the PDB only\nALTER SESSION SET CONTAINER = " + strings.ToUpper(cfg.PDB) + ";\n" + grant
		}
		c.Fix = &checks.Fix{Kind: "sql", Who: "DBA", Code: grant}
		c.Why = effect
		if facts.Copies {
			c.Status, c.Blocking = checks.Fail, true
			c.Message = fmt.Sprintf("%s may not analyze the tables, and Flink CDC's first copy runs ANALYZE TABLE … COMPUTE STATISTICS on each one (it fails with ORA-01031).", cfg.owner())
		} else {
			c.Status = checks.Warn
			c.Message = fmt.Sprintf("%s may not analyze the tables. This pipeline reads changes from now, but a Start over that copies the tables runs ANALYZE TABLE on each one and would fail.", cfg.owner())
		}
	case facts.CopyNow:
		c.Status = checks.Warn
		c.Message = "This deploy copies the tables: Flink CDC's Oracle reader first runs ANALYZE TABLE … COMPUTE STATISTICS on each one. " + effect
	default:
		c.Status = checks.Pass
		c.Message = fmt.Sprintf("%s may analyze the tables. A copy runs ANALYZE TABLE … COMPUTE STATISTICS on each one first.", cfg.owner())
	}
	return c
}

// flushCheck: the flush table is in the CDB root, or the user may create it there.
func (s *oracleSource) flushCheck(ctx context.Context, root *sql.Conn, cfg oraConfig) checks.Check {
	name := cfg.owner() + "." + OraFlushTable
	exists, err := oraOne[int64](ctx, root, "SELECT COUNT(*) FROM user_tables WHERE table_name = :1", OraFlushTable)
	if err != nil {
		return oraFlinkUnknown(err)
	}
	if exists > 0 {
		return checks.Check{Code: "flink_oracle_flush_table", Title: "LogMiner flush table", Category: checks.CatCDC, Status: checks.Pass,
			Message: name + " is in the CDB root."}
	}
	create, err := oraOne[int64](ctx, root, "SELECT COUNT(*) FROM session_privs WHERE privilege IN ('CREATE TABLE', 'CREATE ANY TABLE')")
	if err != nil {
		return oraFlinkUnknown(err)
	}
	room, err := oraOne[int64](ctx, root, `SELECT (SELECT COUNT(*) FROM session_privs WHERE privilege = 'UNLIMITED TABLESPACE')
	    + (SELECT COUNT(*) FROM user_ts_quotas q JOIN user_users u ON u.default_tablespace = q.tablespace_name
	       WHERE q.max_bytes = -1 OR q.max_bytes - q.bytes > 1048576) FROM dual`)
	if err != nil {
		return oraFlinkUnknown(err)
	}
	if create > 0 && room > 0 {
		return checks.Check{Code: "flink_oracle_flush_table", Title: "LogMiner flush table", Category: checks.CatCDC, Status: checks.Pass,
			Message: "BeziSync creates " + name + " in the CDB root when it deploys (Debezium's LogMiner reader keeps its last flushed position there)."}
	}
	q := OraIdent(cfg.owner()) + "." + OraIdent(OraFlushTable)
	return checks.Check{Code: "flink_oracle_flush_table", Title: "LogMiner flush table", Category: checks.CatCDC, Status: checks.Fail, Blocking: true,
		Message: fmt.Sprintf("%s can't create %s in the CDB root, where Flink CDC's Oracle reader records its position (without it every flush fails with ORA-00942).", cfg.owner(), name),
		Why:     "Debezium 1.9 creates the table in the PDB but writes it from the CDB root, where LogMiner runs.",
		Fix: &checks.Fix{Kind: "sql", Who: "DBA", Code: "-- In the CDB root (CDB$ROOT)\nCREATE TABLE " + q + " (LAST_SCN NUMBER(19,0));\nINSERT INTO " + q +
			" VALUES (0);\nCOMMIT;"}}
}

// oraContainerAll adds CONTAINER=ALL to a grant for a common user of a multitenant database.
func oraContainerAll(cfg oraConfig, grant string) string {
	if cfg.PDB != "" {
		return grant + " CONTAINER=ALL;"
	}
	return grant + ";"
}

func oraPlural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

func oraFlinkUnknown(err error) checks.Check {
	return checks.Check{Code: "flink_oracle_unknown", Title: "Oracle for Flink CDC", Category: checks.CatCDC, Status: checks.Warn,
		Message: "Could not read what Flink CDC's Oracle reader needs from the database: " + domain.Truncate(err.Error(), 200)}
}

// FlinkDebeziumOnlyChecks are Oracle source checks about what only Debezium uses: the signal
// table's quota in the PDB and the undo its flashback copy reads. A Flink CDC pipeline leaves them
// out (its flush table and copy are checked by FlinkChecks).
func FlinkDebeziumOnlyChecks(sourceType string) []string {
	if sourceType == "oracle" {
		return slices.Clone([]string{"ora_managed_tables", "ora_undo"})
	}
	return nil
}
