package connectors

import (
	"context"
	"os"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

// TestIcebergFlinkColumnsLive reads a table's columns and adds columns at its end through a real
// Iceberg REST catalog (ADR 0012) when BEZISYNC_TEST_ICEBERG_REST is set (e.g. the e2e or
// playground catalog: http://127.0.0.1:18181, warehouse BEZISYNC_TEST_ICEBERG_REST_WAREHOUSE).
// It works in its own namespace and drops what it creates.
func TestIcebergFlinkColumnsLive(t *testing.T) {
	uri := os.Getenv("BEZISYNC_TEST_ICEBERG_REST")
	if uri == "" {
		t.Skip("BEZISYNC_TEST_ICEBERG_REST not set")
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
	conn := Resolved{Type: "iceberg", Provider: "self_hosted", Config: map[string]any{"catalog_type": "rest", "uri": uri,
		"warehouse": os.Getenv("BEZISYNC_TEST_ICEBERG_REST_WAREHOUSE"), "namespace": "bz_live_layout", "auth": "none", "storage": "s3",
		"s3_endpoint": "http://127.0.0.1:1", "path_style": true}}
	ctx := context.Background()
	sink := &icebergSink{BaseSink{Title: "Apache Iceberg"}}
	if _, err := sink.PrepareSink(ctx, conn, CompileContext{}, nil); err != nil {
		t.Fatal(err)
	}
	cfg, err := icebergCfg(conn)
	if err != nil {
		t.Fatal(err)
	}
	cat, ctx, err := cfg.open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.DropNamespace(context.Background(), table.Identifier{"bz_live_layout"}) })
	ident := table.Identifier{"bz_live_layout", "orders"}
	_, _ = icebergDrop(ctx, cat, ident)
	schema := iceberg.NewSchemaWithIdentifiers(0, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "total", Type: iceberg.DecimalTypeOf(10, 2)},
		iceberg.NestedField{ID: 3, Name: "__op", Type: iceberg.PrimitiveTypes.String})
	if _, err := cat.CreateTable(ctx, ident, schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = icebergDrop(context.Background(), cat, ident) })

	cols, exists, err := sink.FlinkTableColumns(ctx, conn, "bz_live_layout.orders")
	if err != nil || !exists || len(cols) != 3 {
		t.Fatal(cols, exists, err)
	}
	if cols[0].Name != "id" || cols[0].Type != "INT" || cols[0].Nullable || !cols[0].Key || cols[1].Type != "DECIMAL(10, 2)" || cols[2].Name != "__op" {
		t.Fatalf("%+v", cols)
	}
	ran, err := sink.AddFlinkColumns(ctx, conn, "bz_live_layout.orders", []domain.FlinkColumn{{Name: "test_field", Type: "VARCHAR"}, {Name: "n", Type: "BIGINT"}})
	if err != nil || len(ran) != 2 {
		t.Fatal(ran, err)
	}
	cols, _, err = sink.FlinkTableColumns(ctx, conn, "bz_live_layout.orders")
	if err != nil || len(cols) != 5 || cols[2].Name != "__op" || cols[3].Name != "test_field" || cols[4].Name != "n" || cols[4].Type != "BIGINT" || !cols[4].Nullable {
		t.Fatalf("%+v %v", cols, err)
	}
	if _, exists, err := sink.FlinkTableColumns(ctx, conn, "bz_live_layout.nope"); exists || err != nil {
		t.Fatal(exists, err)
	}
}
