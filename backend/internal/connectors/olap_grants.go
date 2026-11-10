package connectors

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
)

// StarRocks, Doris and OceanBase: whether the login may create the pipeline's tables, read from
// SHOW GRANTS without creating anything, and the database created on deploy (ADR 0016 D4).

// olapQuerier is what the check and PrepareSink need from a connection (*sql.Conn).
type olapQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// olapDatabaseExists looks the database up. StarRocks hides databases the login has no right
// on, so false means "missing or not visible".
func olapDatabaseExists(ctx context.Context, q olapQuerier, database string) (bool, error) {
	rows, err := q.QueryContext(ctx, "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE LOWER(SCHEMA_NAME) = LOWER(?)", database)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	var n int
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return false, err
		}
	}
	return n > 0, rows.Err()
}

// olapShowGrants is SHOW GRANTS as column names and rows (NULL as "").
func olapShowGrants(ctx context.Context, q olapQuerier) ([]string, [][]string, error) {
	rows, err := q.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	var out [][]string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		row := make([]string, len(cols))
		for i, v := range vals {
			row[i] = v.String
		}
		out = append(out, row)
	}
	return cols, out, rows.Err()
}

func column(cols []string, name string) int {
	for i, c := range cols {
		if strings.EqualFold(c, name) {
			return i
		}
	}
	return -1
}

// starrocksGrantRe reads "GRANT <privileges> ON <scope> TO …" (StarRocks 3.x).
var starrocksGrantRe = regexp.MustCompile(`(?is)^\s*GRANT\s+(.+?)\s+ON\s+(.+?)\s+TO\s`)

// starrocksGrantsCreate: a default_catalog row granting CREATE TABLE or ALL on the database or on
// all databases. Rows of other catalogs, role grants and the 2.x layout (no Grants column) don't
// count.
func starrocksGrantsCreate(cols []string, rows [][]string, database string) bool {
	ci, gi := column(cols, "Catalog"), column(cols, "Grants")
	if ci < 0 || gi < 0 {
		return false
	}
	for _, row := range rows {
		if !strings.EqualFold(strings.TrimSpace(row[ci]), "default_catalog") {
			continue
		}
		m := starrocksGrantRe.FindStringSubmatch(row[gi])
		if m == nil {
			continue
		}
		create := false
		for _, p := range strings.Split(m[1], ",") {
			switch strings.ToUpper(strings.Join(strings.Fields(p), " ")) {
			case "CREATE TABLE", "ALL", "ALL PRIVILEGES":
				create = true
			}
		}
		if !create {
			continue
		}
		scope := strings.ToUpper(strings.Join(strings.Fields(strings.ReplaceAll(m[2], "`", "")), " "))
		if scope == "ALL DATABASES" || scope == "DATABASE "+strings.ToUpper(database) {
			return true
		}
	}
	return false
}

// dorisPrivs reads a Doris privilege column: "internal.cdc: Select_priv,Create_priv; internal.x: …"
// (scoped) or "Admin_priv,Node_priv" (global). NULL and N/A are no privileges.
func dorisPrivs(value string) map[string][]string {
	out := map[string][]string{}
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "NULL") || strings.EqualFold(value, "N/A") {
		return out
	}
	for _, entry := range strings.Split(value, ";") {
		scope, privs, scoped := strings.Cut(entry, ": ")
		if !scoped {
			scope, privs = "", entry
		}
		for _, p := range strings.Split(privs, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out[strings.ToLower(strings.TrimSpace(scope))] = append(out[strings.ToLower(strings.TrimSpace(scope))], p)
			}
		}
	}
	return out
}

func dorisCreates(privs []string) bool {
	for _, p := range privs {
		if strings.EqualFold(p, "Create_priv") || strings.EqualFold(p, "Admin_priv") {
			return true
		}
	}
	return false
}

// dorisGrantsCreate: CREATE_PRIV or ADMIN_PRIV globally, on the internal catalog, or on
// internal.<db>. Missing columns (the 1.x layout) count as nothing shown.
func dorisGrantsCreate(cols []string, rows [][]string, database string) bool {
	g, c, d := column(cols, "GlobalPrivs"), column(cols, "CatalogPrivs"), column(cols, "DatabasePrivs")
	for _, row := range rows {
		if g >= 0 {
			for _, privs := range dorisPrivs(row[g]) {
				if dorisCreates(privs) {
					return true
				}
			}
		}
		if c >= 0 && dorisCreates(dorisPrivs(row[c])["internal"]) {
			return true
		}
		if d >= 0 && dorisCreates(dorisPrivs(row[d])["internal."+strings.ToLower(database)]) {
			return true
		}
	}
	return false
}

// createRight answers the check: Pass when the grants show the right, Unknown otherwise (grant
// text can't show its absence: roles, external authorizers; ADR 0016 D2).
func (o olapSink) createRight(ctx context.Context, q olapQuerier, database string) (status, msg string) {
	unknown := func(m string) (string, string) {
		return checks.Unknown, m + " The first table the pipeline creates shows a missing right."
	}
	exists, err := olapDatabaseExists(ctx, q, database)
	if err != nil {
		return unknown("BeziSync couldn't look the database " + database + " up (" + firstRunes(err.Error(), 200) + ").")
	}
	cols, rows, err := olapShowGrants(ctx, q)
	if err != nil {
		return unknown("BeziSync couldn't read the user's grants (" + firstRunes(err.Error(), 200) + ").")
	}
	var onDB, global bool
	switch o.typ {
	case "starrocks":
		onDB = starrocksGrantsCreate(cols, rows, database)
	case "doris":
		onDB = dorisGrantsCreate(cols, rows, database)
	default: // OceanBase: MySQL-format grants in the first column
		lines := make([]string, len(rows))
		for i, r := range rows {
			if len(r) > 0 {
				lines[i] = r[0]
			}
		}
		var revoked bool
		onDB, global, revoked = mysqlGrantsCreate(lines, database)
		if revoked {
			onDB, global = false, false
		}
	}
	switch {
	case exists && onDB:
		return checks.Pass, "May create tables in " + database + "."
	case exists:
		return unknown("The user's grants don't show the right to create tables in " + database + " (it may come from a role).")
	case global:
		return checks.Pass, database + " doesn't exist yet: BeziSync creates it on deploy, and the user may create databases and tables."
	}
	return unknown(database + " is missing or not visible to the user: BeziSync creates it on deploy if the user may.")
}

// prepare creates the database the pipeline writes to, if missing (ADR 0016 D4; the check used to).
func (o olapSink) prepare(ctx context.Context, conn Resolved) ([]ArtifactResult, error) {
	cfg, err := o.cfg(conn)
	if err != nil {
		return nil, err
	}
	failed := func(err error) []ArtifactResult {
		return []ArtifactResult{{Kind: "database", Name: cfg.Database, Outcome: "failed",
			Detail: fmt.Sprintf("create database %s on the destination, or let BeziSync reach %s and create it (%s)", cfg.Database,
				net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.QueryPort)), firstRunes(err.Error(), 200))}}
	}
	c, done, err := cfg.open(ctx, 10*time.Second)
	if err != nil {
		return failed(err), nil
	}
	defer done()
	return o.prepareWith(ctx, c, cfg.Database, failed), nil
}

func (o olapSink) prepareWith(ctx context.Context, q olapQuerier, database string, failed func(error) []ArtifactResult) []ArtifactResult {
	exists, err := olapDatabaseExists(ctx, q, database)
	if err == nil && exists {
		return []ArtifactResult{{Kind: "database", Name: database, Outcome: "exists"}}
	}
	if _, err := q.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+olapQuote(database)); err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1007 { // database exists
			return []ArtifactResult{{Kind: "database", Name: database, Outcome: "exists"}}
		}
		return failed(err)
	}
	return []ArtifactResult{{Kind: "database", Name: database, Outcome: "created"}}
}
