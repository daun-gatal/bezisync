package connectors

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// E11 (ADR 0006): the banners Debezium 1.9.8 can read, measured or as Oracle prints them.
func TestFlinkReadsOracleBanner(t *testing.T) {
	cases := []struct {
		banner string
		found  bool
		want   bool
	}{
		{"Oracle Database 21c Express Edition Release 21.0.0.0.0 - Production\nVersion 21.3.0.0.0", true, true},
		{"Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production\nVersion 19.22.0.0.0", true, true},
		{"Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production\r\nVersion 19.3.0.0.0", true, true},
		{"Oracle Database 23ai Enterprise Edition Release 23.0.0.0.0 - Production\nVersion 23.4.0.24.05", true, true},
		{"Oracle Database 12c Enterprise Edition Release 12.2.0.1.0 - 64bit Production", true, true},
		// Oracle Database Free: no "- Production", and the Version line breaks the second form.
		{"Oracle Database 23ai Free Release 23.0.0.0.0 - Develop, Learn, and Run for Free\nVersion 23.9.0.25.07", true, false},
		// 26ai: no banner starts with "Oracle Database" (found is false).
		{"Oracle AI Database 26ai Free Release 23.26.3.0.0 - Develop, Learn, and Run for Free", false, false},
		{"Oracle AI Database 26ai Enterprise Edition Release 23.26.0.0.0 - Production\nVersion 23.26.0.0.0", true, false},
		{"Oracle Database something else", true, false},
		{"", false, false},
	}
	for _, c := range cases {
		if got := FlinkReadsOracleBanner(c.banner, c.found); got != c.want {
			t.Errorf("%q (found %v) = %v, want %v", c.banner, c.found, got, c.want)
		}
	}
}

// oracleFlinkConn is an Oracle XE 21c (CDB XE, PDB XEPDB1) prepared like the e2e fixture
// deploy/e2e/fixtures/15-source-oracle-xe.yaml: BEZISYNC_TEST_ORACLE_FLINK=host:port, with
// BEZISYNC_SSRF_ALLOW_CIDRS covering it.
func oracleFlinkConn(t *testing.T) Resolved {
	t.Helper()
	addr := os.Getenv("BEZISYNC_TEST_ORACLE_FLINK")
	if addr == "" {
		t.Skip("BEZISYNC_TEST_ORACLE_FLINK not set")
	}
	host, port, _ := net.SplitHostPort(addr)
	p, _ := strconv.Atoi(port)
	return Resolved{Type: "oracle", Provider: "self_hosted",
		Config:  map[string]any{"host": host, "port": p, "database": "XE", "pdb": "XEPDB1", "user": "c##cdc_reader"},
		Secrets: map[string]any{"password": "cdc-reader-pw"}}
}

// E15, E16: on Flink CDC, Prepare keeps the flush table and its one row in the CDB root (again and
// again, and after the row is gone), creates no signal or canary table; Cleanup drops nothing.
func TestOraclePrepareFlinkLive(t *testing.T) {
	conn := oracleFlinkConn(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	src := &oracleSource{}
	root, done, err := oraConnect(ctx, conn, "XE", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	rows := func() int64 {
		n, err := oraOne[int64](ctx, root, `SELECT COUNT(*) FROM "C##CDC_READER"."LOG_MINING_FLUSH"`)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if _, err := root.ExecContext(ctx, `DROP TABLE "C##CDC_READER"."LOG_MINING_FLUSH" PURGE`); err != nil && oraCode(err) != 942 {
		t.Fatal(err)
	}
	c := CompileContext{PipelineID: "7a1c2d3e-4b5a-4c7d-8e9f-0a1b2c3d4e5f", ShortID: "7a1c2d3e4b5a", Engine: "flink-cdc"}
	c.Spec.Policies.DeliveryCanary = true
	got, err := src.Prepare(ctx, conn, c)
	if err != nil || len(got) != 1 || got[0].Outcome != "created" || got[0].Name != "C##CDC_READER.LOG_MINING_FLUSH" || rows() != 1 {
		t.Fatalf("first: %v %+v", err, got)
	}
	if got, err = src.Prepare(ctx, conn, c); err != nil || got[0].Outcome != "exists" || rows() != 1 {
		t.Fatalf("again: %v %+v", err, got)
	}
	if _, err := root.ExecContext(ctx, `DELETE FROM "C##CDC_READER"."LOG_MINING_FLUSH"`); err != nil {
		t.Fatal(err)
	}
	if got, err = src.Prepare(ctx, conn, c); err != nil || got[0].Outcome != "exists" || rows() != 1 {
		t.Fatalf("emptied: %v %+v", err, got)
	}
	pdb, donePDB, err := oraConnect(ctx, conn, "XEPDB1", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer donePDB()
	n, err := oraOne[int64](ctx, pdb, "SELECT COUNT(*) FROM all_tables WHERE table_name IN (:1, :2)", oraSignalTable(c), oraCanaryTable(c))
	if err != nil || n != 0 {
		t.Fatalf("signal or canary table on Flink: %d %v", n, err)
	}
	if got, err := src.Cleanup(ctx, conn, c); err != nil || len(got) != 0 || rows() != 1 {
		t.Fatalf("cleanup: %v %+v", err, got)
	}
	// Without a PDB there is nothing to prepare.
	plain := conn
	plain.Config = map[string]any{"host": conn.Config["host"], "port": conn.Config["port"], "database": "XE", "user": "c##cdc_reader"}
	if got, err := src.Prepare(ctx, plain, c); err != nil || len(got) != 0 {
		t.Fatalf("non-CDB: %v %+v", err, got)
	}
}

// E22: on Flink CDC the probe reports the archive's window and never claims a loss it can't see.
func TestOracleProbeFlinkSummary(t *testing.T) {
	week := int64(7 * 24 * 3600)
	i := oraFlinkImpact(&week)
	if i.Lost || i.Risk != nil || i.BehindSeconds != nil || *i.RetentionSeconds != week || !strings.Contains(i.Summary, "saved state") || !strings.Contains(i.Summary, "7 d") && !strings.Contains(i.Summary, "7 days") {
		t.Fatalf("%+v", i)
	}
	if i := oraFlinkImpact(nil); !strings.Contains(i.Summary, "unknown") {
		t.Fatalf("%+v", i)
	}
}

// E12 (pure): ANALYZE needs the privilege or ownership; locked statistics and missing rights block
// only a pipeline that copies; a deploy that copies warns of the statistics it replaces.
func TestAnalyzeCheck(t *testing.T) {
	cdb := oraConfig{Database: "ORCLCDB", PDB: "orclpdb1", User: "c##cdc"}
	cases := []struct {
		name    string
		a       oraAnalyze
		f       FlinkCheckFacts
		status  string
		block   bool
		snippet string
	}{
		{"privileged, streaming", oraAnalyze{Privileged: true}, FlinkCheckFacts{Copies: true}, "pass", false, "may analyze"},
		{"owner", oraAnalyze{OwnsAll: true}, FlinkCheckFacts{Copies: true}, "pass", false, "may analyze"},
		{"copies now", oraAnalyze{Privileged: true}, FlinkCheckFacts{Copies: true, CopyNow: true}, "warn", false, "replaces its optimizer statistics"},
		{"no right, copies", oraAnalyze{}, FlinkCheckFacts{Copies: true}, "fail", true, "ORA-01031"},
		{"no right, from now", oraAnalyze{}, FlinkCheckFacts{}, "warn", false, "Start over"},
		{"locked", oraAnalyze{Privileged: true, Locked: []string{"SHOP.ORDERS"}}, FlinkCheckFacts{Copies: true}, "fail", true, "ORA-38029"},
		{"locked, from now", oraAnalyze{Privileged: true, Locked: []string{"SHOP.ORDERS"}}, FlinkCheckFacts{}, "warn", false, "ORA-38029"},
	}
	for _, c := range cases {
		got := flinkAnalyzeCheck(cdb, c.a, c.f)
		if got.Status != c.status || got.Blocking != c.block || !strings.Contains(got.Message, c.snippet) {
			t.Errorf("%s: %s %v %q", c.name, got.Status, got.Blocking, got.Message)
		}
	}
	// The least grant: inside the PDB, not CONTAINER=ALL.
	got := flinkAnalyzeCheck(cdb, oraAnalyze{}, FlinkCheckFacts{Copies: true})
	if !strings.Contains(got.Fix.Code, "ALTER SESSION SET CONTAINER = ORCLPDB1;") || strings.Contains(got.Fix.Code, "CONTAINER=ALL") ||
		!strings.Contains(got.Fix.Code, `GRANT ANALYZE ANY TO "C##CDC";`) {
		t.Fatal(got.Fix.Code)
	}
	if got := flinkAnalyzeCheck(oraConfig{Database: "ORCL", User: "cdc"}, oraAnalyze{}, FlinkCheckFacts{Copies: true}); strings.Contains(got.Fix.Code, "SESSION") {
		t.Fatal(got.Fix.Code)
	}
	locked := flinkAnalyzeCheck(cdb, oraAnalyze{Privileged: true, Locked: []string{"SHOP.ORDERS"}}, FlinkCheckFacts{Copies: true})
	if locked.Fix == nil || !strings.Contains(locked.Fix.Code, "DBMS_STATS.UNLOCK_TABLE_STATS('SHOP', 'ORDERS')") {
		t.Fatal(locked.Fix)
	}
	// The version check reads the first banner line.
	if v := flinkVersionCheck("Oracle AI Database 26ai Free Release 23.26.3.0.0 - Develop", false); v.Status != "fail" || !v.Blocking || !strings.Contains(v.Message, "26ai") {
		t.Fatalf("%+v", v)
	}
	if v := flinkVersionCheck("Oracle Database 21c Express Edition Release 21.0.0.0.0 - Production\nVersion 21.3.0.0.0", true); v.Status != "pass" || strings.Contains(v.Message, "\n") {
		t.Fatalf("%+v", v)
	}
}

// E12–E14 live: the checks against the e2e-like XE (ANALYZE ANY granted; the flush table there or
// creatable), and an unreachable database only warns.
func TestOracleFlinkChecksLive(t *testing.T) {
	conn := oracleFlinkConn(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	got := map[string]string{}
	for _, c := range (&oracleSource{}).FlinkChecks(ctx, conn, FlinkCheckFacts{Tables: []string{"SHOP.ORDERS"}, Copies: true}) {
		got[c.Code] = c.Status
	}
	if got["flink_oracle_version"] != "pass" || got["flink_oracle_analyze"] != "pass" || got["flink_oracle_flush_table"] != "pass" || got["flink_oracle_unknown"] != "" {
		t.Fatalf("%v", got)
	}
}

func TestOracleFlinkChecksUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn := Resolved{Type: "oracle", Config: map[string]any{"host": "127.0.0.1", "port": 1, "database": "XE", "pdb": "XEPDB1", "user": "c##cdc"},
		Secrets: map[string]any{"password": "pw"}}
	out := (&oracleSource{}).FlinkChecks(ctx, conn, FlinkCheckFacts{Copies: true})
	if len(out) != 1 || out[0].Code != "flink_oracle_unknown" || out[0].Status != "warn" || out[0].Blocking {
		t.Fatalf("%+v", out)
	}
}
