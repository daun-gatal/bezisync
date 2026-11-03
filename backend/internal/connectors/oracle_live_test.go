package connectors_test

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

// Against a real Oracle (gvenzl/oracle-free, multitenant FREE / FREEPDB1, prepared like the e2e
// fixture deploy/e2e/fixtures/15-source-oracle.yaml). BEZISYNC_TEST_ORACLE=host:port, with
// BEZISYNC_SSRF_ALLOW_CIDRS covering it.
func oracleConn(t *testing.T, user, password string) connectors.Resolved {
	t.Helper()
	addr := os.Getenv("BEZISYNC_TEST_ORACLE")
	if addr == "" {
		t.Skip("BEZISYNC_TEST_ORACLE not set")
	}
	host, port, _ := net.SplitHostPort(addr)
	p, _ := strconv.Atoi(port)
	return connectors.Resolved{Type: "oracle", Provider: "self_hosted",
		Config:  map[string]any{"host": host, "port": p, "database": "FREE", "pdb": "FREEPDB1", "user": user},
		Secrets: map[string]any{"password": password}}
}

func byCode(r checks.Report) map[string]checks.Check {
	out := map[string]checks.Check{}
	for _, c := range r.Checks {
		out[c.Code] = c
	}
	return out
}

func TestOracleLive(t *testing.T) {
	conn := oracleConn(t, "c##dbzuser", "dbz")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	src, _ := connectors.GetSource("oracle")

	report, err := src.Check(ctx, conn, []string{"SHOP.ORDERS", "SHOP.NOKEY", "SHOP.MISSING"})
	if err != nil {
		t.Fatal(err)
	}
	got := byCode(report)
	for code, status := range map[string]string{"ora_network_reachable": checks.Pass, "ora_login": checks.Pass, "ora_version": checks.Pass,
		"ora_privileges": checks.Pass, "ora_archivelog": checks.Pass, "ora_supplemental": checks.Pass, "ora_pdb": checks.Pass,
		"ora_managed_tables": checks.Pass, "ora_tables_exist": checks.Fail, "ora_tables_logging": checks.Fail, "ora_tables_keys": checks.Warn} {
		if got[code].Status != status {
			t.Errorf("%s: %s (%s %s), want %s", code, got[code].Status, got[code].Message, got[code].Technical, status)
		}
	}
	if fix := got["ora_tables_logging"].Fix; fix == nil || !strings.Contains(fix.Code, `ALTER TABLE "SHOP"."NOKEY" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;`) {
		t.Errorf("logging fix: %+v", fix)
	}

	bad := oracleConn(t, "c##dbzuser", "wrong")
	report, err = src.Check(ctx, bad, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c := byCode(report)["ora_login"]; c.Status != checks.Fail || !strings.Contains(c.Message, "rejected") {
		t.Errorf("bad password: %+v", c)
	}
	limited := oracleConn(t, "c##limited", "limited")
	report, err = src.Check(ctx, limited, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c := byCode(report)["ora_privileges"]; c.Status != checks.Fail || c.Fix == nil || !strings.Contains(c.Fix.Code, "LOGMINING") ||
		!strings.Contains(c.Fix.Code, "GRANT SELECT ON V_$DATABASE TO C##LIMITED CONTAINER=ALL;") {
		t.Errorf("limited user: %+v", c)
	}

	d, err := src.Discover(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	tables := map[string]connectors.TableInfo{}
	for _, ti := range d.Tables {
		tables[ti.Identifier()] = ti
	}
	orders, ok := tables["SHOP.ORDERS"]
	if !ok || len(orders.PrimaryKey) != 1 || orders.PrimaryKey[0] != "ID" || !orders.Selectable {
		t.Fatalf("orders: %+v", orders)
	}
	types := map[string]string{}
	for _, c := range orders.Columns {
		types[c.Name] = c.Type
	}
	if types["EMAIL"] != "VARCHAR2" || types["CREATED"] != "TIMESTAMP" || types["NOTE"] != "CLOB" {
		t.Errorf("column types: %v", types)
	}
	if !strings.Contains(strings.Join(orders.Warnings, " "), "LOB columns (NOTE)") {
		t.Errorf("LOB warning missing: %v", orders.Warnings)
	}
	for id := range tables {
		if strings.HasPrefix(id, "C##DBZUSER.BZ_") || strings.HasPrefix(id, "SYS.") {
			t.Errorf("listed %s", id)
		}
	}

	cctx := connectors.CompileContext{PipelineID: "00000000-0000-0000-0000-00000000abcd", ShortID: "livetest",
		Spec: domain.MustSpec(map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"SHOP.ORDERS"}}},
			"policies": map[string]any{"delivery_canary": true}})}
	results, err := src.Prepare(ctx, conn, cctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("prepare: %+v", results)
	}
	if _, err := src.Prepare(ctx, conn, cctx); err != nil { // idempotent
		t.Fatal(err)
	}
	if _, err := src.Signal(ctx, conn, cctx, "execute-snapshot", map[string]any{"data-collections": []any{"SHOP.ORDERS"}, "type": "incremental"}); err != nil {
		t.Fatal(err)
	}
	if err := src.WriteCanary(ctx, conn, cctx, "tok"); err != nil {
		t.Fatal(err)
	}
	if table, _ := src.CanaryTable(conn, cctx); table != "C__DBZUSER.BZ_CANARY_LIVETEST" {
		t.Errorf("canary table %s", table)
	}
	impact, err := src.Probe(ctx, conn, cctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if impact.Risk != nil || impact.Lost {
		t.Errorf("probe: %+v", impact)
	}
	removed, err := src.Cleanup(ctx, conn, cctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 2 || removed[0].Outcome != "removed" || removed[1].Outcome != "removed" {
		t.Fatalf("cleanup: %+v", removed)
	}
	if removed, err = src.Cleanup(ctx, conn, cctx); err != nil || removed[0].Outcome != "not_found" {
		t.Fatalf("second cleanup: %+v %v", removed, err)
	}
}
