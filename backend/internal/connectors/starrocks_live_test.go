package connectors_test

import (
	"context"
	"database/sql"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
)

// Against a real StarRocks: BEZISYNC_TEST_STARROCKS_HOST, _QUERY_PORT and _HTTP_PORT (root, no
// password, a single node).
func TestStarRocksLive(t *testing.T) {
	host := os.Getenv("BEZISYNC_TEST_STARROCKS_HOST")
	if host == "" {
		t.Skip("BEZISYNC_TEST_STARROCKS_HOST not set")
	}
	qp, _ := strconv.Atoi(os.Getenv("BEZISYNC_TEST_STARROCKS_QUERY_PORT"))
	hp, _ := strconv.Atoi(os.Getenv("BEZISYNC_TEST_STARROCKS_HTTP_PORT"))
	s, err := config.Load(func(k string) (string, bool) {
		if k == "BEZISYNC_SSRF_ALLOW_CIDRS" {
			return `["127.0.0.0/8", "172.16.0.0/12", "10.0.0.0/8"]`, true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	config.Set(s)
	t.Cleanup(config.Reset)
	ctx := context.Background()
	conn := connectors.Resolved{Type: "starrocks", Config: map[string]any{"host": host, "query_port": qp, "http_port": hp, "database": "bz_live_check",
		"user": "root", "replicas": 1}, Secrets: map[string]any{}}
	snk, _ := connectors.GetSink("starrocks")
	report, err := snk.Check(ctx, conn, nil)
	if err != nil || !report.OK {
		t.Fatalf("%+v %v", report, err)
	}
	db, err := sql.Open("mysql", "root@tcp("+net.JoinHostPort(host, strconv.Itoa(qp))+")/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, q := range []string{"CREATE TABLE IF NOT EXISTS bz_live_check.orders (id INT NOT NULL, note VARCHAR(20)) PRIMARY KEY (id) DISTRIBUTED BY HASH (id) PROPERTIES (\"replication_num\" = \"1\")",
		"INSERT INTO bz_live_check.orders VALUES (1, 'it''s here')"} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := snk.Tail(ctx, conn, "bz_live_check.orders", 10, 10*time.Second)
	if err != nil || len(rows) != 1 || rows[0].Value.(map[string]any)["note"] != "it's here" {
		t.Fatalf("%+v %v", rows, err)
	}
	if rows, err := snk.Tail(ctx, conn, "bz_live_check.missing", 10, 10*time.Second); err != nil || len(rows) != 0 {
		t.Fatalf("a missing table has no rows: %v %v", rows, err)
	}
	dest := map[string]string{"shop.orders": "bz_live_check.orders"}
	for _, want := range []string{"removed", "not_found"} {
		res, err := snk.CleanupSink(ctx, conn, connectors.CompileContext{}, dest)
		if err != nil || len(res) != 1 || res[0].Outcome != want {
			t.Fatalf("%+v %v", res, err)
		}
	}
	_, _ = db.ExecContext(ctx, "DROP DATABASE IF EXISTS bz_live_check FORCE")
	// A user without rights is told what to grant.
	bad := conn
	bad.Config = map[string]any{"host": host, "query_port": qp, "http_port": hp, "database": "bz_live_check", "user": "nobody", "replicas": 1}
	if report, _ := snk.Check(ctx, bad, nil); report.OK {
		t.Fatal("an unknown user signed in")
	}
}
