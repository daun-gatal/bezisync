package connectors

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
)

// recDriver is a database/sql driver that records every statement and answers from answer.
type recDriver struct {
	mu     sync.Mutex
	stmts  []string
	answer func(query string) (cols []string, rows [][]driver.Value, err error)
}

func (d *recDriver) Open(string) (driver.Conn, error)             { return &recConn{d}, nil }
func (d *recDriver) Connect(context.Context) (driver.Conn, error) { return &recConn{d}, nil }
func (d *recDriver) Driver() driver.Driver                        { return d }

func (d *recDriver) record(q string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stmts = append(d.stmts, q)
}

func (d *recDriver) conn(t *testing.T) *sql.Conn {
	t.Helper()
	db := sql.OpenDB(d)
	t.Cleanup(func() { _ = db.Close() })
	c, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

type recConn struct{ d *recDriver }

func (c *recConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("recDriver: no prepared statements")
}
func (c *recConn) Close() error              { return nil }
func (c *recConn) Begin() (driver.Tx, error) { return nil, errors.New("recDriver: no transactions") }
func (c *recConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	c.d.record(q)
	cols, rows, err := c.d.answer(q)
	if err != nil {
		return nil, err
	}
	return &recRows{cols: cols, rows: rows}, nil
}
func (c *recConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	c.d.record(q)
	if _, _, err := c.d.answer(q); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

type recRows struct {
	cols []string
	rows [][]driver.Value
	i    int
}

func (r *recRows) Columns() []string { return r.cols }
func (r *recRows) Close() error      { return nil }
func (r *recRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.i])
	r.i++
	return nil
}

// Grants of the `bz_writer` user that writes database `cdc`, as StarRocks 3.4, Doris 3.0 and
// OceanBase CE 4.3 print them (BeziSync lab, 2026-11-10); the column layouts follow each vendor's
// SHOW GRANTS reference, and the readers find columns by name.
var (
	starrocksCols     = []string{"UserIdentity", "Catalog", "Grants"}
	starrocksCaptured = [][]string{
		{"'bz_writer'@'%'", "default_catalog", "GRANT DELETE, DROP, INSERT, SELECT, ALTER, EXPORT, UPDATE ON ALL TABLES IN DATABASE cdc TO USER 'bz_writer'@'%'"},
		{"'bz_writer'@'%'", "default_catalog", "GRANT CREATE TABLE, DROP, ALTER, CREATE VIEW, CREATE FUNCTION, CREATE MATERIALIZED VIEW, CREATE PIPE ON DATABASE cdc TO USER 'bz_writer'@'%'"},
		{"'bz_writer'@'%'", "default_catalog", "GRANT CREATE DATABASE ON CATALOG default_catalog TO USER 'bz_writer'@'%'"},
	}
	dorisCols = []string{"UserIdentity", "Comment", "Password", "Roles", "GlobalPrivs", "CatalogPrivs", "DatabasePrivs", "TablePrivs", "ColPrivs",
		"ResourcePrivs", "CloudClusterPrivs", "CloudStagePrivs", "StorageVaultPrivs", "WorkloadGroupPrivs", "ComputeGroupPrivs"}
	dorisCaptured = []string{"'bz_writer'@'%'", "", "Yes", "", "", "",
		"internal.cdc: Select_priv,Load_priv,Alter_priv,Create_priv,Drop_priv; internal.information_schema: Select_priv; internal.mysql: Select_priv",
		"", "", "", "", "", "", "normal: Usage_priv", ""}
	oceanbaseCaptured = []string{"GRANT CREATE ON *.* TO 'bz_writer'", "GRANT ALL PRIVILEGES ON `cdc`.* TO 'bz_writer'"}
)

func dorisRow(global, catalog, database string) []string {
	r := append([]string(nil), dorisCaptured...)
	r[4], r[5], r[6] = global, catalog, database
	return r
}

// E19: StarRocks grants, read by column name, default_catalog only.
func TestStarRocksGrants(t *testing.T) {
	for _, c := range []struct {
		name string
		cols []string
		rows [][]string
		db   string
		want bool
	}{
		{"captured: CREATE TABLE on the database", starrocksCols, starrocksCaptured, "cdc", true},
		{"captured: another database", starrocksCols, starrocksCaptured, "other", false},
		{"ALL ON ALL DATABASES", starrocksCols, [][]string{{"'u'@'%'", "default_catalog", "GRANT ALL ON ALL DATABASES TO USER 'u'@'%'"}}, "cdc", true},
		{"backtick-quoted database", starrocksCols, [][]string{{"'u'@'%'", "default_catalog", "GRANT ALL ON DATABASE `cdc` TO USER 'u'@'%'"}}, "cdc", true},
		{"an external catalog's grant", starrocksCols, [][]string{{"'u'@'%'", "hive_catalog", "GRANT CREATE TABLE ON DATABASE cdc TO USER 'u'@'%'"}}, "cdc", false},
		{"CREATE VIEW only", starrocksCols, [][]string{{"'u'@'%'", "default_catalog", "GRANT CREATE VIEW ON DATABASE cdc TO USER 'u'@'%'"}}, "cdc", false},
		{"role only", starrocksCols, [][]string{{"'u'@'%'", "NULL", "GRANT 'db_admin' TO USER 'u'@'%'"}}, "cdc", false},
		{"2.x layout", []string{"UserIdentity", "AuthInfo", "GlobalPrivs", "DatabasePrivs"}, [][]string{{"'u'@'%'", "", "Admin_priv", "cdc: ALL"}}, "cdc", false},
	} {
		if got := starrocksGrantsCreate(c.cols, c.rows, c.db); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// E20: Doris privilege columns.
func TestDorisGrants(t *testing.T) {
	for _, c := range []struct {
		name string
		cols []string
		rows [][]string
		db   string
		want bool
	}{
		{"captured: Create_priv on internal.cdc", dorisCols, [][]string{dorisCaptured}, "cdc", true},
		{"captured: another database", dorisCols, [][]string{dorisCaptured}, "other", false},
		{"the internal catalog", dorisCols, [][]string{dorisRow("", "internal: Select_priv,Create_priv", "")}, "any", true},
		{"global Admin_priv", dorisCols, [][]string{dorisRow("Node_priv,Admin_priv ", "", "")}, "any", true},
		{"N/A everywhere", dorisCols, [][]string{dorisRow("N/A", "N/A", "N/A")}, "cdc", false},
		{"Load only", dorisCols, [][]string{dorisRow("", "", "internal.cdc: Select_priv,Load_priv")}, "cdc", false},
		{"1.x layout", []string{"UserIdentity", "Password", "GlobalPrivs", "DatabasePrivs"},
			[][]string{{"'u'@'%'", "Yes", "", "default_cluster:cdc: Select_priv,Create_priv"}}, "cdc", false},
	} {
		if got := dorisGrantsCreate(c.cols, c.rows, c.db); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// olapFake answers the two questions the check asks.
func olapFake(exists bool, cols []string, rows [][]string) *recDriver {
	return &recDriver{answer: func(q string) ([]string, [][]driver.Value, error) {
		switch {
		case strings.Contains(q, "information_schema.SCHEMATA"):
			n := int64(0)
			if exists {
				n = 1
			}
			return []string{"COUNT(*)"}, [][]driver.Value{{n}}, nil
		case q == "SHOW GRANTS":
			out := make([][]driver.Value, len(rows))
			for i, r := range rows {
				out[i] = make([]driver.Value, len(r))
				for j, v := range r {
					out[i][j] = v
				}
			}
			return cols, out, nil
		}
		return nil, nil, errors.New("unexpected statement: " + q)
	}}
}

// E21-E23: what each engine's check answers, and that it sends nothing but the two reads.
func TestOlapCheckCreatesNothing(t *testing.T) {
	obCols := []string{"Grants for bz_writer@%"}
	obRows := [][]string{{oceanbaseCaptured[0]}, {oceanbaseCaptured[1]}}
	for _, c := range []struct {
		name   string
		o      olapSink
		fake   *recDriver
		db     string
		status string
		inMsg  string
	}{
		{"StarRocks captured", starrocks, olapFake(true, starrocksCols, starrocksCaptured), "cdc", checks.Pass, "May create tables in cdc."},
		{"StarRocks database missing or hidden", starrocks, olapFake(false, starrocksCols, starrocksCaptured), "cdc", checks.Unknown, "missing or not visible"},
		{"StarRocks role only", starrocks, olapFake(true, starrocksCols, [][]string{{"'u'@'%'", "NULL", "GRANT 'db_admin' TO USER 'u'@'%'"}}), "cdc",
			checks.Unknown, "it may come from a role"},
		{"Doris captured", doris, olapFake(true, dorisCols, [][]string{dorisCaptured}), "cdc", checks.Pass, "May create tables in cdc."},
		{"Doris database missing", doris, olapFake(false, dorisCols, [][]string{dorisCaptured}), "cdc", checks.Unknown, "missing or not visible"},
		{"OceanBase captured", oceanbase, olapFake(true, obCols, obRows), "cdc", checks.Pass, "May create tables in cdc."},
		{"OceanBase database missing, CREATE on *.*", oceanbase, olapFake(false, obCols, obRows), "cdc", checks.Pass, "BeziSync creates it on deploy"},
		{"OceanBase CREATE SYNONYM only", oceanbase, olapFake(true, obCols, [][]string{{"GRANT CREATE SYNONYM ON *.* TO 'u'"}}), "cdc", checks.Unknown, "don't show"},
	} {
		status, msg := c.o.createRight(context.Background(), c.fake.conn(t), c.db)
		if status != c.status || !strings.Contains(msg, c.inMsg) {
			t.Errorf("%s: %s %q", c.name, status, msg)
		}
		for _, s := range c.fake.stmts {
			if !strings.HasPrefix(s, "SELECT ") && s != "SHOW GRANTS" {
				t.Errorf("%s: the check sent %q", c.name, s)
			}
		}
	}
}

// E24: the database is created on deploy, only when missing.
func TestOlapPrepareSink(t *testing.T) {
	failed := func(err error) []ArtifactResult {
		return []ArtifactResult{{Kind: "database", Name: "cdc", Outcome: "failed", Detail: err.Error()}}
	}
	exec := func(exists bool, createErr error) *recDriver {
		return &recDriver{answer: func(q string) ([]string, [][]driver.Value, error) {
			switch {
			case strings.Contains(q, "information_schema.SCHEMATA"):
				n := int64(0)
				if exists {
					n = 1
				}
				return []string{"COUNT(*)"}, [][]driver.Value{{n}}, nil
			case strings.HasPrefix(q, "CREATE DATABASE IF NOT EXISTS"):
				return nil, nil, createErr
			}
			return nil, nil, errors.New("unexpected statement: " + q)
		}}
	}
	for _, c := range []struct {
		name    string
		d       *recDriver
		outcome string
		creates bool
	}{
		{"exists", exec(true, nil), "exists", false},
		{"missing", exec(false, nil), "created", true},
		{"1007: created meanwhile, or hidden", exec(false, &mysql.MySQLError{Number: 1007, Message: "database exists"}), "exists", true},
		{"refused", exec(false, &mysql.MySQLError{Number: 1044, Message: "Access denied"}), "failed", true},
	} {
		got := doris.prepareWith(context.Background(), c.d.conn(t), "cdc", failed)
		if len(got) != 1 || got[0].Outcome != c.outcome {
			t.Errorf("%s: %+v", c.name, got)
		}
		sent := strings.Join(c.d.stmts, "; ")
		if strings.Contains(sent, "CREATE DATABASE") != c.creates {
			t.Errorf("%s: sent %s", c.name, sent)
		}
	}

	// Unreachable: failed, with what to do (the deploy stops on it).
	allowLoopback(t)
	got, err := (&starrocksSink{}).PrepareSink(context.Background(), Resolved{Type: "starrocks", Config: map[string]any{
		"host": "127.0.0.1", "query_port": 1, "http_port": 1, "database": "cdc", "user": "bz", "replicas": 1}, Secrets: map[string]any{}}, CompileContext{}, nil)
	if err != nil || len(got) != 1 || got[0].Outcome != "failed" ||
		!strings.Contains(got[0].Detail, "create database cdc on the destination, or let BeziSync reach 127.0.0.1:1") {
		t.Fatalf("%+v %v", got, err)
	}
}

// E25: the OLAP codes are built from the type, so the literal-code catalog test can't see them.
func TestOlapCodesInCatalog(t *testing.T) {
	known := map[string]bool{}
	for _, e := range checks.LoadCatalog().Entries {
		known[e.Code] = true
	}
	for _, o := range []olapSink{starrocks, doris, oceanbase} {
		for _, suffix := range []string{"_config", "_login", "_create_table"} {
			if !known[o.typ+suffix] {
				t.Errorf("%s%s has no catalog entry", o.typ, suffix)
			}
		}
	}
}
