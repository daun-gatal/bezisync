package connectors

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

// TestOlapFlinkColumnsLive reads a table's columns and adds one through StarRocks and Doris
// (ADR 0012) when BEZISYNC_TEST_STARROCKS / BEZISYNC_TEST_DORIS are set to host:port of their FE
// (user root; its password in BEZISYNC_TEST_<TYPE>_PASSWORD, none when unset). It works in
// database bz_live and drops it.
func TestOlapFlinkColumnsLive(t *testing.T) {
	cases := map[string]struct {
		sink   olapSink
		create string
	}{
		"BEZISYNC_TEST_STARROCKS": {starrocks, "CREATE TABLE bz_live.orders (id INT NOT NULL, status STRING, op_flag STRING) PRIMARY KEY (id) DISTRIBUTED BY HASH(id) PROPERTIES ('replication_num' = '1')"},
		"BEZISYNC_TEST_DORIS":     {doris, "CREATE TABLE bz_live.orders (id INT NOT NULL, status STRING, op_flag STRING) UNIQUE KEY (id) DISTRIBUTED BY HASH(id) PROPERTIES ('replication_num' = '1')"},
	}
	ran := false
	for env, c := range cases {
		addr := os.Getenv(env)
		if addr == "" {
			continue
		}
		ran = true
		t.Run(c.sink.typ, func(t *testing.T) {
			s, err := config.Load(func(k string) (string, bool) {
				if k == "BEZISYNC_SSRF_ALLOW_CIDRS" {
					return `["127.0.0.0/8"]`, true
				}
				return "", false
			})
			if err != nil {
				t.Fatal(err)
			}
			config.Set(s)
			t.Cleanup(config.Reset)
			host, port, _ := strings.Cut(addr, ":")
			conn := Resolved{Type: c.sink.typ, Config: map[string]any{"host": host, "query_port": port, "http_port": 8030, "database": "bz_live", "user": "root", "replicas": 1},
				Secrets: map[string]any{"password": os.Getenv(env + "_PASSWORD")}}
			cfg, err := c.sink.cfg(conn)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			db, done, err := cfg.open(ctx, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer done()
			for _, stmt := range []string{"DROP DATABASE IF EXISTS bz_live FORCE", "CREATE DATABASE bz_live", c.create} {
				if _, err := db.ExecContext(ctx, stmt); err != nil {
					t.Fatal(stmt, err)
				}
			}
			t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP DATABASE IF EXISTS bz_live FORCE") })
			var sink FlinkColumns = &starrocksSink{}
			if c.sink.typ == "doris" {
				sink = &dorisSink{}
			}
			cols, exists, err := sink.FlinkTableColumns(ctx, conn, "bz_live.orders")
			if err != nil || !exists || len(cols) != 3 || cols[0].Name != "id" || cols[0].Nullable || !cols[1].Nullable {
				t.Fatalf("%+v %v %v", cols, exists, err)
			}
			stmts, err := sink.AddFlinkColumns(ctx, conn, "bz_live.orders", []domain.FlinkColumn{{Name: "test_field", Type: "VARCHAR"}, {Name: "n", Type: "BIGINT"}})
			if err != nil || len(stmts) != 2 {
				t.Fatal(stmts, err)
			}
			cols, _, err = sink.FlinkTableColumns(ctx, conn, "bz_live.orders")
			if err != nil || len(cols) != 5 || cols[3].Name != "test_field" || cols[4].Name != "n" || !cols[4].Nullable {
				t.Fatalf("%+v %v", cols, err)
			}
			if _, exists, err := sink.FlinkTableColumns(ctx, conn, "bz_live.nope"); exists || err != nil {
				t.Fatal(exists, err)
			}
		})
	}
	if !ran {
		t.Skip("BEZISYNC_TEST_STARROCKS / BEZISYNC_TEST_DORIS not set")
	}
}
