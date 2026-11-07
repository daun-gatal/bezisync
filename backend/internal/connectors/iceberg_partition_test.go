package connectors

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

func partitionSpec(tables []string, by map[string]any, transforms ...domain.Transform) domain.PipelineSpec {
	spec := domain.DefaultSpec()
	spec.Source.Tables.Include = tables
	spec.Sink.Options = map[string]any{"write_mode": "upsert", "partitioning": "none", "partition_by": by}
	spec.Transforms = transforms
	return spec
}

func field(col, tr string, n int) map[string]any {
	return map[string]any{"column": col, "transform": tr, "n": n}
}

// Partition by on Flink CDC: partition.key per destination, Iceberg's transforms, the rules the
// schema can't say (T19), and a route that merges tables asking for different partitioning.
func TestIcebergPartitionBy(t *testing.T) {
	rest := map[string]any{"catalog_type": "rest", "uri": "https://cat.example/api", "warehouse": "s3://lake/", "namespace": "lake", "auth": "token", "storage": "vended"}
	compile := func(spec domain.PipelineSpec) (*FlinkBlock, error) {
		return (&icebergSink{}).FlinkSink(Resolved{Type: "iceberg", Config: rest, Secrets: map[string]any{"token": "x"}}, CompileContext{ShortID: "0123456789ab", Spec: spec, Engine: "flink-cdc"})
	}
	spec := partitionSpec([]string{"shop.orders", "shop.items"}, map[string]any{
		"shop.orders": []any{field("created_at", "day", 0), field("id", "bucket", 16)},
		"shop.items":  []any{field("sku", "truncate", 4), field("region", "identity", 0)},
	})
	b, err := compile(spec)
	if err != nil {
		t.Fatal(err)
	}
	if got := b.Options.Get("partition.key"); got != "lake.cdc_shop_items:truncate[4](sku),region;lake.cdc_shop_orders:day(created_at),bucket[16](id)" {
		t.Fatal(got)
	}
	if b, _ := compile(partitionSpec([]string{"shop.orders"}, map[string]any{})); b.Options.Get("partition.key") != "" {
		t.Fatal("partition.key without partitioning")
	}
	for name, c := range map[string]struct {
		by   map[string]any
		want string
	}{
		"T19: no buckets":       {map[string]any{"shop.orders": []any{field("id", "bucket", 0)}}, "1 to 1024"},
		"T19: too many buckets": {map[string]any{"shop.orders": []any{field("id", "bucket", 1025)}}, "1 to 1024"},
		"truncate of 0":         {map[string]any{"shop.orders": []any{field("sku", "truncate", 0)}}, "at least 1"},
		"a number for day":      {map[string]any{"shop.orders": []any{field("created_at", "day", 3)}}, "takes no number"},
		"twice":                 {map[string]any{"shop.orders": []any{field("id", "identity", 0), field("id", "identity", 0)}}, "listed twice"},
		"not captured":          {map[string]any{"shop.refunds": []any{field("id", "identity", 0)}}, "not captured"},
		"an unknown transform":  {map[string]any{"shop.orders": []any{field("id", "week", 0)}}, "Input should be"},
		"an expression":         {map[string]any{"shop.orders": []any{field("id) ;x", "identity", 0)}}, "pattern"},
		"five fields":           {map[string]any{"shop.orders": []any{field("a", "identity", 0), field("b", "identity", 0), field("c", "identity", 0), field("d", "identity", 0), field("e", "identity", 0)}}, "at most 4"},
	} {
		_, err := compile(partitionSpec([]string{"shop.orders"}, c.by))
		if err == nil || !strings.Contains(problemDetail(err), c.want) {
			t.Errorf("%s: %v", name, problemDetail(err))
		}
	}
	// Tables a route merges must ask for the same partitioning.
	route := domain.Transform{Type: "route", Routes: []domain.Route{{Match: "shop.orders_*", To: "orders"}}}
	merged := partitionSpec([]string{"shop.orders_eu", "shop.orders_us"}, map[string]any{
		"shop.orders_eu": []any{field("id", "bucket", 8)}, "shop.orders_us": []any{field("id", "bucket", 16)}}, route)
	if _, err := compile(merged); err == nil || !strings.Contains(err.Error(), "different partitioning") {
		t.Fatal(err)
	}
	same := partitionSpec([]string{"shop.orders_eu", "shop.orders_us"}, map[string]any{
		"shop.orders_eu": []any{field("id", "bucket", 8)}, "shop.orders_us": []any{field("id", "bucket", 8)}}, route)
	if b, err := compile(same); err != nil || b.Options.Get("partition.key") != "lake.orders:bucket[8](id)" {
		t.Fatal(err, b)
	}
	// Debezium's build partitions by its own option.
	if _, _, err := (&icebergSink{}).CompileSink(Resolved{Type: "iceberg", Config: rest, Secrets: map[string]any{"token": "x"}}, CompileContext{ShortID: "0123456789ab", Spec: spec}); err == nil ||
		!strings.Contains(err.Error(), "Partition by is a Flink CDC option") {
		t.Fatal(err)
	}
}

// T17: a partition column the rows don't carry (left out, renamed away) or of a kind the
// transform can't take blocks the deploy; renamed, added and computed columns count.
func TestIcebergPartitionColumns(t *testing.T) {
	cols := []Column{{Name: "id", Type: "integer"}, {Name: "status", Type: "text"}, {Name: "created_at", Type: "timestamp without time zone"}, {Name: "notes", Type: "text"}}
	tables := []TableInfo{{Schema: "shop", Name: "orders", Columns: cols}}
	run := func(by []any, columns map[string]any, steps ...domain.Transform) []checks.Check {
		spec := partitionSpec([]string{"shop.orders"}, map[string]any{"shop.orders": by}, steps...)
		if columns != nil {
			spec.Source.Columns = map[string]domain.ColumnRules{"shop.orders": {Exclude: []string{"notes"}, Mask: []string{}}}
		}
		opts, err := SinkOptions("iceberg", spec)
		if err != nil {
			t.Fatal(err)
		}
		return partitionChecks(context.Background(), nil, icebergConfig{}, CompileContext{Spec: spec},
			PipelineFacts{Tables: tables, Options: opts, FlinkCDC: true, SourceType: "postgresql"})
	}
	ok := run([]any{field("created_at", "day", 0), field("id", "bucket", 4), field("status", "truncate", 2)}, nil)
	if len(ok) != 1 || ok[0].Status != checks.Pass {
		t.Fatal(ok)
	}
	for name, c := range map[string]struct {
		by      []any
		columns map[string]any
		steps   []domain.Transform
		want    string
	}{
		"left out":          {[]any{field("notes", "identity", 0)}, map[string]any{}, nil, "no column notes"},
		"renamed away":      {[]any{field("status", "identity", 0)}, nil, []domain.Transform{{Type: "rename", Renames: []domain.Rename{{From: "status", To: "state"}}}}, "no column status"},
		"day of text":       {[]any{field("status", "day", 0)}, nil, nil, "needs a date or a timestamp"},
		"hour of a number":  {[]any{field("id", "hour", 0)}, nil, nil, "needs a timestamp"},
		"truncate of times": {[]any{field("created_at", "truncate", 3)}, nil, nil, "truncate[3](created_at) needs text"},
	} {
		got := run(c.by, c.columns, c.steps...)
		if len(got) != 1 || !got[0].Blocking || !strings.Contains(got[0].Message, c.want) {
			t.Errorf("%s: %+v", name, got)
		}
	}
	renamed := run([]any{field("state", "identity", 0)}, nil, domain.Transform{Type: "rename", Renames: []domain.Rename{{From: "status", To: "state"}}})
	computed := run([]any{field("label", "identity", 0)}, nil, domain.Transform{Type: "computed", Table: "shop.orders", Name: "label", Function: "upper", Args: []domain.ComputedArg{{Column: "status"}}})
	if renamed[0].Status != checks.Pass || computed[0].Status != checks.Pass {
		t.Fatal(renamed, computed)
	}
}

// problemDetail is a problem's detail and its field errors, for matching.
func problemDetail(err error) string {
	var p *problem.Problem
	if !errors.As(err, &p) {
		return fmt.Sprint(err)
	}
	out := p.Detail
	for _, e := range p.Errors {
		out += " | " + fmt.Sprint(e["msg"], e["message"])
	}
	return out
}

// ADR 0005 R4: kinds as the engine writes them, Iceberg's transform table, and the engine's own
// columns.
func TestIcebergPartitionWrittenKinds(t *testing.T) {
	cols := []Column{{Name: "id", Type: "integer"}, {Name: "flag", Type: "boolean"}, {Name: "score", Type: "double precision"},
		{Name: "total", Type: "numeric(10,2)"}, {Name: "ref", Type: "uuid"}, {Name: "at", Type: "time without time zone"}}
	tables := []TableInfo{{Schema: "shop", Name: "orders", Columns: cols}}
	run := func(flink bool, by []any, mutate func(*domain.PipelineSpec)) checks.Check {
		spec := partitionSpec([]string{"shop.orders"}, map[string]any{"shop.orders": by})
		if mutate != nil {
			mutate(&spec)
		}
		opts, err := SinkOptions("iceberg", spec)
		if err != nil {
			t.Fatal(err)
		}
		got := partitionChecks(context.Background(), nil, icebergConfig{}, CompileContext{Spec: spec}, PipelineFacts{Tables: tables, Options: opts, FlinkCDC: flink, SourceType: "postgresql"})
		if len(got) != 1 {
			t.Fatalf("%v", got)
		}
		return got[0]
	}
	for name, c := range map[string]struct {
		flink  bool
		by     []any
		mutate func(*domain.PipelineSpec)
		want   string // "" passes
	}{
		"E15 bucket of a boolean":             {false, []any{field("flag", "bucket", 4)}, nil, "can't take true/false"},
		"E15 bucket of a float":               {true, []any{field("score", "bucket", 4)}, nil, "can't take a floating-point number"},
		"identity of a boolean":               {false, []any{field("flag", "identity", 0)}, nil, ""},
		"truncate of a decimal":               {false, []any{field("total", "truncate", 10)}, nil, ""},
		"decimals sent as double":             {false, []any{field("total", "truncate", 10)}, func(s *domain.PipelineSpec) { s.Values.Decimals = "double" }, "written as a floating-point number"},
		"decimals as double, Flink CDC":       {true, []any{field("total", "truncate", 10)}, func(s *domain.PipelineSpec) { s.Values.Decimals = "double" }, ""},
		"uuid is text":                        {false, []any{field("ref", "truncate", 4)}, nil, ""},
		"time of day on Debezium is a number": {false, []any{field("at", "hour", 0)}, nil, "written as a whole number"},
		"time of day on Flink CDC":            {true, []any{field("at", "bucket", 4)}, nil, ""},
		"R3 time of the change on Debezium":   {false, []any{field("__source_ts_ms", "day", 0)}, nil, ""},
		"E19 no time of the change on Flink":  {true, []any{field("__source_ts_ms", "day", 0)}, nil, "no column __source_ts_ms"},
		"X4 one column, two transforms":       {false, []any{field("__source_ts_ms", "day", 0), field("__source_ts_ms", "hour", 0)}, nil, ""},
		"chained renames": {true, []any{field("c", "identity", 0)}, func(s *domain.PipelineSpec) {
			s.Transforms = []domain.Transform{{Type: "rename", Renames: []domain.Rename{{From: "id", To: "b"}}}, {Type: "rename", Renames: []domain.Rename{{From: "b", To: "c"}}}}
		}, ""},
	} {
		got := run(c.flink, c.by, c.mutate)
		switch {
		case c.want == "" && got.Status != checks.Pass:
			t.Errorf("%s: %s", name, got.Message)
		case c.want != "" && (!got.Blocking || !strings.Contains(got.Message, c.want)):
			t.Errorf("%s: %s", name, got.Message)
		}
	}
}

// fakeTables is a catalog with some tables, and an error for the others.
type fakeTables struct {
	tables map[string]*table.Table
	err    error
}

func (f fakeTables) LoadTable(_ context.Context, id table.Identifier) (*table.Table, error) {
	if t, ok := f.tables[strings.Join(id, ".")]; ok {
		return t, nil
	}
	return nil, f.err
}

// R7 (E11, E21): only "no such table" means the pipeline creates it; any other catalog error, and
// a source that couldn't be read, leave the checks unverified, never passed.
func TestIcebergPartitionFailsClosed(t *testing.T) {
	spec := partitionSpec([]string{"shop.orders"}, map[string]any{"shop.orders": []any{field("id", "bucket", 8)}})
	opts, err := SinkOptions("iceberg", spec)
	if err != nil {
		t.Fatal(err)
	}
	tables := []TableInfo{{Schema: "shop", Name: "orders", Columns: []Column{{Name: "id", Type: "integer"}}}}
	facts := PipelineFacts{Tables: tables, Options: opts, FlinkCDC: true, SourceType: "postgresql", Destinations: map[string]string{"shop.orders": "lake.cdc_shop_orders"}}
	find := func(cs []checks.Check, code string) *checks.Check {
		for i := range cs {
			if cs[i].Code == code {
				return &cs[i]
			}
		}
		return nil
	}
	missing := partitionChecks(context.Background(), fakeTables{err: fmt.Errorf("load: %w", catalog.ErrNoSuchTable)}, icebergConfig{}, CompileContext{Spec: spec}, facts)
	if find(missing, "iceberg_partition_existing") != nil {
		t.Fatalf("a table to create has nothing to compare: %v", missing)
	}
	denied := partitionChecks(context.Background(), fakeTables{err: errors.New("403 forbidden")}, icebergConfig{}, CompileContext{Spec: spec}, facts)
	if c := find(denied, "iceberg_partition_existing"); c == nil || c.Status != checks.Warn || !strings.Contains(c.Message, "403 forbidden") {
		t.Fatalf("an unreadable table: %+v", c)
	}
	facts.Tables = nil
	blind := partitionChecks(context.Background(), nil, icebergConfig{}, CompileContext{Spec: spec}, facts)
	if c := find(blind, "iceberg_partition_column"); c == nil || c.Status != checks.Warn {
		t.Fatalf("no discovery: %+v", c)
	}
}
