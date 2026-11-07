package connectors

import (
	"context"
	"net"
	"os"
	"strconv"
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
