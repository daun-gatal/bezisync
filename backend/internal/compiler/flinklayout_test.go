package compiler_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

// ADR 0012: the columns each table writes, in the order the spec writes them, for the layout
// planner: source columns (under their written names), step columns, the destination's own.
func TestFlinkProduced(t *testing.T) {
	flinkConfig(t)
	spec := flinkSpec(t, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}}},
		"sink":      map[string]any{"naming": "{schema}_{table}"},
		"flink_cdc": map[string]any{"new_columns": "send"},
		"keys":      map[string]any{"shop.orders": []any{"id"}},
		"transforms": []any{
			map[string]any{"type": "unwrap"},
			map[string]any{"type": "add_field", "id": "addf1", "name": "f1", "value": map[string]any{"kind": "static", "text": "one"}},
			map[string]any{"type": "computed", "id": "comp1", "table": "shop.orders", "name": "doubled", "expression": "UPPER(`total`)"},
		}})
	soft := testSink
	soft.Config = map[string]any{"soft": true}
	produced, destinations, err := compiler.FlinkProduced(flinkCtx(spec), mysqlConn, soft, map[string][]string{"shop.orders": {"id", "total"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if destinations["shop.orders"] != "lake.shop_orders" {
		t.Fatal(destinations)
	}
	got := produced["shop.orders"]
	var names []string
	for _, c := range got {
		names = append(names, c.Name)
	}
	if !reflect.DeepEqual(names, []string{"id", "total", "f1", "doubled", "__op"}) {
		t.Fatal(names)
	}
	if !got[0].Key || got[1].Key || got[2].Type != "VARCHAR" || got[4].Type != "VARCHAR" {
		t.Fatalf("%+v", got)
	}
	if got[3].Type != "" || got[3].AddType != "VARCHAR" || !got[3].AddCast {
		t.Fatalf("a computed column is typed by Flink CDC, cast when added: %+v", got[3])
	}
}

// L3/L4: a stored layout writes the table column by column in its order: a retired column as an
// empty value of its type, a new computed one cast, the rest as the spec writes them; a table
// without a stored layout compiles as before.
func TestCompileFlinkFromLayout(t *testing.T) {
	flinkConfig(t)
	spec := flinkSpec(t, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders", "shop.items"}}},
		"sink":      map[string]any{"naming": "{schema}_{table}"},
		"flink_cdc": map[string]any{"new_columns": "send"},
		"transforms": []any{
			map[string]any{"type": "unwrap"},
			map[string]any{"type": "add_field", "id": "addtf", "tables": []any{"shop.orders"}, "name": "test_field", "value": map[string]any{"kind": "static", "text": "x"}},
			map[string]any{"type": "computed", "id": "comp1", "table": "shop.orders", "name": "doubled", "expression": "UPPER(`total`)"},
		}})
	soft := testSink
	soft.Config = map[string]any{"soft": true}
	in := secrets
	in.Columns = map[string][]string{"shop.orders": {"id", "total"}, "shop.items": {"id", "sku"}}
	before, err := compiler.CompileFlink(flinkCtx(spec), mysqlConn, soft, "acme/flink-cdc:3.7.0", in)
	if err != nil {
		t.Fatal(err)
	}
	in.Layout = map[string][]domain.LayoutColumn{"lake.shop_orders": {
		{Name: "id"}, {Name: "total"}, {Name: "gone", Type: "VARCHAR", Retired: true}, {Name: "__op", Type: "VARCHAR"},
		{Name: "test_field", Type: "VARCHAR"}, {Name: "doubled", Type: "VARCHAR", Cast: true}}}
	got, err := compiler.CompileFlink(flinkCtx(spec), mysqlConn, soft, "acme/flink-cdc:3.7.0", in)
	if err != nil {
		t.Fatal(err)
	}
	want := "projection: \"`id`, `total`, CAST(NULL AS VARCHAR) AS `gone`, __data_event_type__ AS `__op`, 'x' AS `test_field`, CAST((CASE WHEN `total` IS NULL"
	if !strings.Contains(got.Pipeline, want) || !strings.Contains(got.Pipeline, "END) AS VARCHAR) AS `doubled`\"") {
		t.Fatal(got.Pipeline)
	}
	// shop.items has no stored layout: written as the spec says, the same as before.
	if !strings.Contains(got.Pipeline, "projection: \"*, __data_event_type__ AS `__op`\"") || !strings.Contains(before.Pipeline, "projection: \"*, __data_event_type__ AS `__op`\"") {
		t.Fatal(got.Pipeline)
	}
	// A column the layout needs empty but can't type is refused, never guessed.
	in.Layout["lake.shop_orders"] = append(in.Layout["lake.shop_orders"], domain.LayoutColumn{Name: "untyped", Retired: true})
	if _, err := compiler.CompileFlink(flinkCtx(spec), mysqlConn, soft, "acme/flink-cdc:3.7.0", in); code(err) != "flink_layout_type" {
		t.Fatal(err)
	}
}

// L12: two tables routed into one destination both write its stored layout: what a table doesn't
// have is written empty.
func TestCompileFlinkLayoutForARoutedDestination(t *testing.T) {
	flinkConfig(t)
	spec := flinkSpec(t, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders_2025", "shop.orders_2026"}}},
		"sink":      map[string]any{"naming": "{schema}_{table}"},
		"flink_cdc": map[string]any{"new_columns": "send"},
		"transforms": []any{map[string]any{"type": "route", "id": "route1", "routes": []any{
			map[string]any{"match": "shop.orders_*", "to": "orders"}}}}})
	in := secrets
	in.Columns = map[string][]string{"shop.orders_2025": {"id", "total"}, "shop.orders_2026": {"id", "total", "channel"}}
	in.Layout = map[string][]domain.LayoutColumn{"lake.orders": {{Name: "id"}, {Name: "total"}, {Name: "channel", Type: "VARCHAR"}}}
	got, err := compiler.CompileFlink(flinkCtx(spec), mysqlConn, testSink, "acme/flink-cdc:3.7.0", in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Pipeline, "projection: \"`id`, `total`, CAST(NULL AS VARCHAR) AS `channel`\"") ||
		!strings.Contains(got.Pipeline, "projection: \"`id`, `total`, `channel`\"") {
		t.Fatal(got.Pipeline)
	}
}
