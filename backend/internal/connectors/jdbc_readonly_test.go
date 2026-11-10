package connectors

import (
	"context"
	"database/sql"
	"net/url"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

// ADR 0016 D6, E17/E18: the MySQL-format grant reader (MySQL, MariaDB, OceanBase). The first
// row is MySQL 8.4's root grant as captured from the BeziSync lab (2026-11-10).
func TestMySQLGrantsAllowCreate(t *testing.T) {
	captured := "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, SHUTDOWN, PROCESS, FILE, REFERENCES, INDEX, ALTER, SHOW DATABASES, SUPER, " +
		"CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, " +
		"CREATE USER, EVENT, TRIGGER, CREATE TABLESPACE, CREATE ROLE, DROP ROLE ON *.* TO `root`@`%` WITH GRANT OPTION"
	for _, c := range []struct {
		name            string
		rows            []string
		db              string
		onDB, global, r bool
	}{
		{"captured root grant", []string{captured}, "warehouse", true, true, false},
		{"ALL PRIVILEGES on *.*", []string{"GRANT ALL PRIVILEGES ON *.* TO 'u'@'%'"}, "w", true, true, false},
		{"CREATE on the database", []string{"GRANT USAGE ON *.* TO `u`@`%`", "GRANT SELECT, INSERT, CREATE ON `warehouse`.* TO `u`@`%`"}, "warehouse", true, false, false},
		{"unquoted scope", []string{"GRANT CREATE, ALTER ON warehouse.* TO 'u'@'%'"}, "warehouse", true, false, false},
		{"escaped underscore pattern", []string{"GRANT ALL ON `ware\\_house`.* TO 'u'@'%'"}, "ware_house", true, false, false},
		{"escaped underscore doesn't match other characters", []string{"GRANT ALL ON `ware\\_house`.* TO 'u'@'%'"}, "wareXhouse", false, false, false},
		{"percent pattern", []string{"GRANT CREATE ON `ware%`.* TO 'u'@'%'"}, "warehouse_2", true, false, false},
		{"CREATE VIEW only", []string{"GRANT SELECT, CREATE VIEW ON `warehouse`.* TO 'u'@'%'"}, "warehouse", false, false, false},
		{"CREATE TEMPORARY TABLES only", []string{"GRANT CREATE TEMPORARY TABLES ON `warehouse`.* TO 'u'@'%'"}, "warehouse", false, false, false},
		{"another database", []string{"GRANT ALL ON `other`.* TO 'u'@'%'"}, "warehouse", false, false, false},
		{"table level", []string{"GRANT ALL ON `warehouse`.`orders` TO 'u'@'%'"}, "warehouse", false, false, false},
		{"role only", []string{"GRANT USAGE ON *.* TO `u`@`%`", "GRANT `writer`@`%` TO `u`@`%`"}, "warehouse", false, false, false},
		{"partial revoke", []string{"GRANT ALL ON *.* TO 'u'@'%'", "REVOKE CREATE ON `warehouse`.* FROM 'u'@'%'"}, "warehouse", true, true, true},
		{"OceanBase CREATE SYNONYM only", []string{"GRANT CREATE SYNONYM ON *.* TO 'u'"}, "cdc", false, false, false},
	} {
		onDB, global, revoked := mysqlGrantsCreate(c.rows, c.db)
		if onDB != c.onDB || global != c.global || revoked != c.r {
			t.Errorf("%s: onDB=%v global=%v revoked=%v, want %v %v %v", c.name, onDB, global, revoked, c.onDB, c.global, c.r)
		}
	}
}

// E16: SQL Server's two HAS_PERMS_BY_NAME answers.
func TestSQLServerCreateRight(t *testing.T) {
	v := func(n int64) sql.NullInt64 { return sql.NullInt64{Int64: n, Valid: true} }
	for _, c := range []struct {
		table, alter sql.NullInt64
		want         string
	}{
		{v(1), v(1), checks.Pass},
		{v(0), v(1), checks.Warn},
		{v(1), v(0), checks.Warn},
		{v(1), sql.NullInt64{}, checks.Unknown}, // no default schema
		{sql.NullInt64{}, v(1), checks.Unknown},
	} {
		if got := sqlserverCreateVerdict(c.table, c.alter); got != c.want {
			t.Errorf("%v %v: %s, want %s", c.table, c.alter, got, c.want)
		}
	}
}

// E15: PostgreSQL answers exactly, against a real server: USAGE and CREATE pass, a missing one
// or a missing schema warns, the schema is lower-cased as the connector names it, and nothing is
// created by the check.
func TestJDBCPostgresCreateRight(t *testing.T) {
	admin := testdb.AdminURL()
	if admin == "" {
		t.Skip("BEZISYNC_TEST_DATABASE_URL not set")
	}
	allowLoopback(t)
	ctx := context.Background()
	c, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	var exists bool
	_ = c.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'jdbc_ro_check')").Scan(&exists)
	if !exists {
		if _, err := c.Exec(ctx, "CREATE DATABASE jdbc_ro_check"); err != nil {
			t.Fatal(err)
		}
	}
	u, _ := url.Parse(admin)
	u.Path = "/jdbc_ro_check"
	dbc, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dbc.Close(ctx) }()
	if _, err := dbc.Exec(ctx, `
CREATE SCHEMA IF NOT EXISTS ro_app;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bz_ro_create') THEN CREATE ROLE bz_ro_create LOGIN PASSWORD 'ro-pw'; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bz_ro_usage') THEN CREATE ROLE bz_ro_usage LOGIN PASSWORD 'ro-pw'; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bz_ro_none') THEN CREATE ROLE bz_ro_none LOGIN PASSWORD 'ro-pw'; END IF;
END $$;
GRANT CONNECT ON DATABASE jdbc_ro_check TO bz_ro_create, bz_ro_usage, bz_ro_none;
GRANT USAGE, CREATE ON SCHEMA ro_app TO bz_ro_create;
GRANT USAGE ON SCHEMA ro_app TO bz_ro_usage;`); err != nil {
		t.Fatal(err)
	}
	tables := func() int {
		var n int
		if err := dbc.QueryRow(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname IN ('ro_app', 'public')").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := tables()
	port, _ := strconv.Atoi(u.Port())
	if port == 0 {
		port = 5432
	}
	for _, cs := range []struct{ user, schema, want, msg string }{
		{"bz_ro_create", "ro_app", checks.Pass, "May create tables in the schema ro_app."},
		{"bz_ro_create", "RO_App", checks.Pass, "May create tables in the schema ro_app."},
		{"bz_ro_usage", "ro_app", checks.Warn, "The user can't create tables in the schema ro_app (needs USAGE and CREATE)"},
		{"bz_ro_none", "ro_app", checks.Warn, "The user can't create tables in the schema ro_app"},
		{"bz_ro_create", "no_such_schema", checks.Warn, "The schema no_such_schema doesn't exist"},
	} {
		report, err := (&jdbcSink{}).Check(ctx, Resolved{Type: "jdbc", Config: map[string]any{
			"database_type": "postgresql", "host": u.Hostname(), "port": port, "database": "jdbc_ro_check",
			"schema": cs.schema, "user": cs.user, "tls": "disable"}, Secrets: map[string]any{"password": "ro-pw"}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		var got *checks.Check
		for i := range report.Checks {
			if report.Checks[i].Code == "jdbc_create_table" {
				got = &report.Checks[i]
			}
		}
		if got == nil {
			t.Fatalf("%s %s: no jdbc_create_table in %+v", cs.user, cs.schema, report.Checks)
		}
		if got.Status != cs.want || got.Blocking || len(got.Message) < len(cs.msg) || got.Message[:len(cs.msg)] != cs.msg {
			t.Errorf("%s %s: %s %q", cs.user, cs.schema, got.Status, got.Message)
		}
	}
	if after := tables(); after != before {
		t.Fatalf("the check created tables: %d before, %d after", before, after)
	}
}
