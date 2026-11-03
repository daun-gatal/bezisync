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

// Against a real insecure CockroachDB (deploy/e2e/fixtures/16-source-cockroachdb.yaml):
// BEZISYNC_TEST_COCKROACHDB=host:port, with BEZISYNC_SSRF_ALLOW_CIDRS covering it.
func TestCockroachDBLive(t *testing.T) {
	addr := os.Getenv("BEZISYNC_TEST_COCKROACHDB")
	if addr == "" {
		t.Skip("BEZISYNC_TEST_COCKROACHDB not set")
	}
	host, port, _ := net.SplitHostPort(addr)
	p, _ := strconv.Atoi(port)
	conn := connectors.Resolved{Type: "cockroachdb", Provider: "self_hosted",
		Config:  map[string]any{"host": host, "port": p, "database": "shop", "user": "cdc_reader", "ssl_mode": "disable"},
		Secrets: map[string]any{}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	src, _ := connectors.GetSource("cockroachdb")

	report, err := src.Check(ctx, conn, []string{"public.orders", "public.nokey", "public.missing"})
	if err != nil {
		t.Fatal(err)
	}
	got := byCode(report)
	for code, status := range map[string]string{"crdb_login": checks.Pass, "crdb_version": checks.Pass, "crdb_rangefeed": checks.Pass,
		"crdb_create": checks.Pass, "crdb_tables_exist": checks.Fail, "crdb_tables_changefeed": checks.Fail, "crdb_gc_ttl": checks.Pass} {
		if got[code].Status != status {
			t.Errorf("%s: %s (%s), want %s", code, got[code].Status, got[code].Message, status)
		}
	}
	if fix := got["crdb_tables_changefeed"].Fix; fix == nil || fix.Code != `GRANT CHANGEFEED ON TABLE "public"."nokey" TO "cdc_reader";` {
		t.Errorf("changefeed fix: %+v", fix)
	}

	d, err := src.Discover(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	tables := map[string]connectors.TableInfo{}
	for _, ti := range d.Tables {
		tables[ti.Identifier()] = ti
	}
	if orders := tables["public.orders"]; len(orders.PrimaryKey) != 1 || orders.PrimaryKey[0] != "id" || orders.RowsEstimate == nil {
		t.Errorf("orders: %+v", orders)
	}
	if nokey := tables["public.nokey"]; !strings.Contains(strings.Join(nokey.Warnings, " "), "hidden rowid") {
		t.Errorf("nokey: %+v", nokey)
	}
	if _, ok := tables["bezisync.signal_live"]; ok {
		t.Error("BeziSync's own tables are listed")
	}

	cctx := connectors.CompileContext{PipelineID: "00000000-0000-0000-0000-00000000abcd", ShortID: "live",
		Spec: domain.MustSpec(map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"public.orders"}}},
			"policies": map[string]any{"delivery_canary": true}})}
	if results, err := src.Prepare(ctx, conn, cctx); err != nil || len(results) != 2 {
		t.Fatalf("prepare: %+v %v", results, err)
	}
	if _, err := src.Prepare(ctx, conn, cctx); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Signal(ctx, conn, cctx, "execute-snapshot", map[string]any{"data-collections": []any{"public.orders"}, "type": "incremental"}); err != nil {
		t.Fatal(err)
	}
	if err := src.WriteCanary(ctx, conn, cctx, "tok"); err != nil {
		t.Fatal(err)
	}
	impact, err := src.Probe(ctx, conn, cctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if impact.RetentionSeconds == nil || *impact.RetentionSeconds <= 0 || impact.Risk != nil {
		t.Errorf("probe: %+v", impact)
	}
	removed, err := src.Cleanup(ctx, conn, cctx)
	if err != nil || len(removed) != 2 {
		t.Fatalf("cleanup: %+v %v", removed, err)
	}
	if removed, err = src.Cleanup(ctx, conn, cctx); err != nil || removed[0].Outcome != "not_found" {
		t.Fatalf("second cleanup: %+v %v", removed, err)
	}
}
