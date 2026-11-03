package connectors

import (
	"context"
	"os"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/config"
)

// TestIcebergLive runs the checks, prepare, a tail and cleanup against a real catalog when
// BEZISYNC_TEST_ICEBERG_URI is set (e.g. RustFS S3 Tables: http://127.0.0.1:29000/iceberg).
func TestIcebergLive(t *testing.T) {
	uri := os.Getenv("BEZISYNC_TEST_ICEBERG_URI")
	if uri == "" {
		t.Skip("BEZISYNC_TEST_ICEBERG_URI not set")
	}
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
	conn := Resolved{Type: "iceberg", Provider: "rustfs", Config: map[string]any{"catalog_type": "rest", "uri": uri,
		"warehouse": os.Getenv("BEZISYNC_TEST_ICEBERG_WAREHOUSE"), "namespace": "bz_live", "auth": "sigv4", "signing_name": "s3",
		"region": "us-east-1", "storage": "s3", "s3_endpoint": os.Getenv("BEZISYNC_TEST_ICEBERG_S3"), "path_style": true,
		"access_key_id": os.Getenv("BEZISYNC_TEST_ICEBERG_KEY")}, Secrets: map[string]any{"secret_access_key": os.Getenv("BEZISYNC_TEST_ICEBERG_SECRET")}}
	s3 := &icebergSink{BaseSink{Title: "Apache Iceberg"}}
	ctx := context.Background()
	prepared, err := s3.PrepareSink(ctx, conn, CompileContext{}, nil)
	if err != nil || len(prepared) != 1 || prepared[0].Outcome == "failed" {
		t.Fatalf("prepare: %+v %v", prepared, err)
	}
	report, err := s3.Check(ctx, conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range report.Checks {
		t.Logf("%s %s: %s %s", c.Status, c.Code, c.Message, c.Technical)
	}
	if !report.OK {
		t.Fatal("checks failed")
	}
	rows, err := s3.Tail(ctx, conn, "bz_live.missing_table", 5, 0)
	if err != nil || len(rows) != 0 {
		t.Fatalf("tail of a table not created yet: %v %v", rows, err)
	}
}
