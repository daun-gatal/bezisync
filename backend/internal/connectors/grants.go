package connectors

import (
	"regexp"
	"strings"
)

// Reading MySQL-format SHOW GRANTS (MySQL, MariaDB, OceanBase) to see whether a login may create
// tables, without trying (ADR 0016). Grant text can show a right but never prove its absence
// (roles, proxies, external authorizers), so callers turn "not shown" into Unknown, never Fail.

// mysqlGrantRe reads "GRANT <privileges> ON <scope> TO …"; table-level and routine grants have a
// scope with a table part and are skipped by mysqlGrantsCreate.
var mysqlGrantRe = regexp.MustCompile(`(?is)^\s*GRANT\s+(.+?)\s+ON\s+(\S+)\s+TO\s`)

// mysqlCreatesTables: an exact CREATE or ALL [PRIVILEGES] item. CREATE VIEW, CREATE ROUTINE,
// CREATE USER, CREATE TEMPORARY TABLES, CREATE SYNONYM … are other rights.
func mysqlCreatesTables(privileges string) bool {
	for _, p := range strings.Split(privileges, ",") {
		switch strings.ToUpper(strings.Join(strings.Fields(p), " ")) {
		case "CREATE", "ALL", "ALL PRIVILEGES":
			return true
		}
	}
	return false
}

// mysqlLikeMatch matches a database name against a grant's database pattern: '_' and '%' are
// wildcards, '\_' and '\%' literals (MySQL's rule for database names in grants).
func mysqlLikeMatch(pattern, name string) bool {
	var b strings.Builder
	b.WriteString("(?i)^")
	for i := 0; i < len(pattern); i++ {
		switch c := pattern[i]; {
		case c == '\\' && i+1 < len(pattern):
			i++
			b.WriteString(regexp.QuoteMeta(string(pattern[i])))
		case c == '%':
			b.WriteString(".*")
		case c == '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	return err == nil && re.MatchString(name)
}

// mysqlGrantsCreate reads SHOW GRANTS rows: whether a grant shows the right to create tables in
// database db (onDB), anywhere (global: `*.*`, which in MySQL also covers creating databases),
// and whether a REVOKE row (partial revokes) makes the reading unreliable.
func mysqlGrantsCreate(rows []string, db string) (onDB, global, revoked bool) {
	for _, row := range rows {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(row)), "REVOKE ") {
			revoked = true
			continue
		}
		m := mysqlGrantRe.FindStringSubmatch(row)
		if m == nil || !mysqlCreatesTables(m[1]) {
			continue
		}
		scope := strings.ReplaceAll(m[2], "`", "")
		dbPart, table, ok := strings.Cut(scope, ".")
		if !ok || table != "*" {
			continue // a table-level grant: not a right to create tables
		}
		switch {
		case dbPart == "*":
			global, onDB = true, true
		case mysqlLikeMatch(dbPart, db):
			onDB = true
		}
	}
	return onDB, global, revoked
}
