package connectors

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
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
}

// ADR 0005 R1, R2: Partition by on Debezium, as the build's per-table partition-by keyed by the
// event's destination; precedence over the automatic default; routes.
func TestIcebergPartitionByDebezium(t *testing.T) {
	rest := map[string]any{"catalog_type": "rest", "uri": "https://cat.example/api", "warehouse": "s3://lake/", "namespace": "lake", "auth": "token", "storage": "vended"}
	compile := func(spec domain.PipelineSpec, opts map[string]any) (map[string]string, error) {
		for k, v := range opts {
			spec.Sink.Options[k] = v
		}
		spec.Sink.Naming = "{schema}_{table}"
		props, _, err := (&icebergSink{}).CompileSink(Resolved{Type: "iceberg", Config: rest, Secrets: map[string]any{"token": "x"}}, CompileContext{ShortID: "0123456789ab", Spec: spec})
		if err != nil {
			return nil, err
		}
		out := map[string]string{}
		for k, v := range props {
			if after, ok := strings.CutPrefix(k, "debezium.sink.iceberg.partition-by."); ok {
				out[after] = v
			}
		}
		return out, nil
	}
	long := strings.Repeat("c", 63)
	spec := partitionSpec([]string{"shop.orders", "shop.items", "shop.events"}, map[string]any{
		"shop.orders": []any{field("created_at", "day", 0), field("id", "bucket", 16)},
		"shop.items":  []any{field("SKU", "truncate", 4), field("region", "identity", 0), field(long, "year", 0), field("at", "hour", 0)},
	})
	got, err := compile(spec, map[string]any{"partitioning": "auto", "partitions": map[string]any{"shop.orders": "id", "shop.events": "id"}})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"shop_orders": `day(created_at),bucket(id\,16)`,
		"shop_items":  `truncate(SKU\,4),region,year(` + long + `),hour(at)`, // X3: names verbatim, case kept
		"shop_events": `bucket(id\,16)`,                                      // unlisted: the automatic default
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("partition-by: %v, want %v", got, want)
	}
	// E8: partitioning none turns the default off, not Partition by.
	got, _ = compile(spec, map[string]any{"partitions": map[string]any{"shop.events": "id"}})
	if _, ok := got["shop_events"]; !ok {
		t.Fatal("auto by default")
	}
	got, _ = compile(spec, map[string]any{"partitioning": "none", "partitions": map[string]any{"shop.events": "id"}})
	if _, ok := got["shop_events"]; ok || got["shop_orders"] == "" {
		t.Fatalf("none: %v", got)
	}
	// E16: tables routed together list the same fields, keyed by the route's destination.
	route := domain.Transform{Type: "route", Routes: []domain.Route{{Match: "shop.orders_*", To: "orders"}}}
	merged := partitionSpec([]string{"shop.orders_eu", "shop.orders_us"}, map[string]any{
		"shop.orders_eu": []any{field("id", "bucket", 8)}, "shop.orders_us": []any{field("id", "bucket", 16)}}, route)
	if _, err := compile(merged, nil); err == nil || !strings.Contains(err.Error(), "different partitioning") {
		t.Fatal(err)
	}
	same := partitionSpec([]string{"shop.orders_eu", "shop.orders_us"}, map[string]any{
		"shop.orders_eu": []any{field("id", "bucket", 8)}, "shop.orders_us": []any{field("id", "bucket", 8)}}, route)
	if got, err := compile(same, nil); err != nil || fmt.Sprint(got) != fmt.Sprint(map[string]string{"orders": `bucket(id\,8)`}) {
		t.Fatal(err, got)
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
		"E15 bucket of a boolean":              {false, []any{field("flag", "bucket", 4)}, nil, "can't take true/false"},
		"E15 bucket of a float":                {true, []any{field("score", "bucket", 4)}, nil, "can't take a floating-point number"},
		"identity of a boolean":                {false, []any{field("flag", "identity", 0)}, nil, ""},
		"a PostgreSQL NUMERIC on Debezium":     {false, []any{field("total", "truncate", 10)}, nil, "total can't be partitioned"},
		"a PostgreSQL NUMERIC on Flink CDC":    {true, []any{field("total", "truncate", 10)}, nil, ""},
		"decimals sent as text":                {false, []any{field("total", "truncate", 10)}, func(s *domain.PipelineSpec) { s.Values.Decimals = "string" }, ""},
		"decimals sent as double":              {false, []any{field("total", "truncate", 10)}, func(s *domain.PipelineSpec) { s.Values.Decimals = "double" }, "written as a floating-point number"},
		"decimals as double, Flink CDC":        {true, []any{field("total", "truncate", 10)}, func(s *domain.PipelineSpec) { s.Values.Decimals = "double" }, ""},
		"uuid: no truncate on Debezium":        {false, []any{field("ref", "truncate", 4)}, nil, "written as a UUID"},
		"uuid: buckets on Debezium":            {false, []any{field("ref", "bucket", 4)}, nil, ""},
		"uuid is text on Flink CDC":            {true, []any{field("ref", "truncate", 4)}, nil, ""},
		"time of day on Debezium is a number":  {false, []any{field("at", "hour", 0)}, nil, "written as a whole number"},
		"time of day on Flink CDC":             {true, []any{field("at", "bucket", 4)}, nil, ""},
		"R3 time of the change on Debezium":    {false, []any{field("__source_ts_ms", "day", 0)}, nil, ""},
		"E19 no time of the change on Flink":   {true, []any{field("__source_ts_ms", "day", 0)}, nil, "no column __source_ts_ms"},
		"X4 two time transforms of one column": {false, []any{field("__source_ts_ms", "day", 0), field("__source_ts_ms", "hour", 0)}, nil, "redundant"},
		"a time and a bucket of one column":    {false, []any{field("__source_ts_ms", "day", 0), field("__source_ts_ms", "bucket", 4)}, nil, ""},
		"chained renames on Debezium": {false, []any{field("c", "identity", 0)}, func(s *domain.PipelineSpec) {
			s.Transforms = []domain.Transform{{Type: "rename", Renames: []domain.Rename{{From: "id", To: "b"}}}, {Type: "rename", Renames: []domain.Rename{{From: "b", To: "c"}}}}
		}, ""},
		"Flink CDC renames once": {true, []any{field("c", "identity", 0)}, func(s *domain.PipelineSpec) {
			s.Transforms = []domain.Transform{{Type: "rename", Renames: []domain.Rename{{From: "id", To: "b"}}}, {Type: "rename", Renames: []domain.Rename{{From: "b", To: "c"}}}}
		}, "no column c"},
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
	if t, ok := f.tables[id[len(id)-1]]; ok {
		return t, nil
	}
	if f.err == nil {
		return nil, catalog.ErrNoSuchTable
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

// liveTable is an Iceberg table with columns id (identity when identity is true), created_at and
// status, partitioned by the given transforms of those columns.
func liveTable(t *testing.T, identity bool, parts ...[2]string) *table.Table {
	t.Helper()
	var ids []int
	if identity {
		ids = []int{1}
	}
	sc := iceberg.NewSchemaWithIdentifiers(0, ids,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "created_at", Type: iceberg.PrimitiveTypes.TimestampTz},
		iceberg.NestedField{ID: 3, Name: "status", Type: iceberg.PrimitiveTypes.String})
	col := map[string]int{"id": 1, "created_at": 2, "status": 3}
	var fields []iceberg.PartitionField
	for i, p := range parts {
		tr, err := iceberg.ParseTransform(p[0])
		if err != nil {
			t.Fatal(err)
		}
		fields = append(fields, iceberg.PartitionField{SourceIDs: []int{col[p[1]]}, FieldID: 1000 + i, Name: fmt.Sprintf("p%d", i), Transform: tr})
	}
	spec := iceberg.NewPartitionSpec(fields...)
	meta, err := table.NewMetadata(sc, &spec, table.UnsortedSortOrder, "s3://lake/t", nil)
	if err != nil {
		t.Fatal(err)
	}
	return table.New(table.Identifier{"lake", "cdc_shop_orders"}, meta, "s3://lake/t/metadata.json", nil, nil)
}

// ADR 0005 R5: on an upserted table every partition column is one the upsert deletes by — for a
// table to create, its key; for one that exists, its identifier fields.
func TestIcebergPartitionKey(t *testing.T) {
	cols := []Column{{Name: "id", Type: "integer"}, {Name: "tenant_id", Type: "integer"}, {Name: "created_at", Type: "timestamp with time zone"}, {Name: "status", Type: "text"}}
	orders := TableInfo{Schema: "shop", Name: "orders", PrimaryKey: []string{"id"}, Columns: cols}
	keyless := TableInfo{Schema: "shop", Name: "orders", Columns: cols}
	missing := fakeTables{err: catalog.ErrNoSuchTable}
	type tc struct {
		flink  bool
		info   TableInfo
		opts   map[string]any
		mutate func(*domain.PipelineSpec)
		cat    icebergTables
		source string
		want   string // "" passes, "-" no check, else a blocking message part; "warn:" prefix a warning
	}
	by := func(fields ...any) map[string]any {
		return map[string]any{"partition_by": map[string]any{"shop.orders": fields}}
	}
	with := func(m map[string]any, kv ...any) map[string]any {
		out := map[string]any{}
		for k, v := range m {
			out[k] = v
		}
		for i := 0; i < len(kv); i += 2 {
			out[kv[i].(string)] = kv[i+1]
		}
		return out
	}
	recorded := func(col string) map[string]any {
		return map[string]any{"partitions": map[string]any{"shop.orders": col}}
	}
	cases := map[string]tc{
		"E1 automatic bucket of the key":             {info: orders, opts: recorded("id"), cat: missing, want: ""},
		"E2 recorded keyless, now keyed":             {info: orders, opts: recorded(""), cat: missing, want: "shop.orders by day(__source_ts_ms) (its key: id)"},
		"stale recorded key":                         {info: orders, opts: recorded("status"), cat: missing, want: "bucket[16](status)"},
		"E3 change log by the day of the change":     {info: orders, opts: with(recorded("id"), "write_mode", "append"), cat: missing, want: ""},
		"E4 listed bucket of the key":                {info: orders, opts: by(field("id", "bucket", 8)), cat: missing, want: ""},
		"E5 listed day of a non-key column":          {info: orders, opts: by(field("created_at", "day", 0)), cat: missing, want: "day(created_at) (its key: id)"},
		"E6 change log by any column":                {info: orders, opts: with(by(field("status", "identity", 0)), "write_mode", "append"), cat: missing, want: ""},
		"E7 keyless upsert is appended":              {info: keyless, opts: by(field("created_at", "day", 0)), cat: missing, want: ""},
		"E8 none turns the automatic default off":    {info: orders, opts: with(recorded(""), "partitioning", "none"), cat: missing, want: "-"},
		"E12 Flink CDC bucket of the key":            {flink: true, info: orders, opts: by(field("id", "bucket", 4)), cat: missing, want: ""},
		"E13 Flink CDC day of a non-key column":      {flink: true, info: orders, opts: by(field("created_at", "day", 0)), cat: missing, want: "day(created_at)"},
		"Flink CDC has no automatic default":         {flink: true, info: orders, opts: recorded(""), cat: missing, want: "-"},
		"E22 a leading key column and the key":       {info: orders, opts: by(field("tenant_id", "identity", 0), field("id", "bucket", 16)), mutate: func(s *domain.PipelineSpec) { s.Keys = map[string][]string{"shop.orders": {"tenant_id", "id"}} }, cat: missing, want: ""},
		"chosen key columns replace the primary key": {info: orders, opts: by(field("id", "bucket", 16)), mutate: func(s *domain.PipelineSpec) { s.Keys = map[string][]string{"shop.orders": {"tenant_id"}} }, cat: missing, want: "(its key: tenant_id)"},
		"Debezium ignores chosen keys it lacks":      {info: orders, opts: by(field("id", "bucket", 16)), mutate: func(s *domain.PipelineSpec) { s.Keys = map[string][]string{"shop.orders": {"nope"}} }, cat: missing, want: ""},
		"E20 a renamed key on Debezium: appended": {info: orders, opts: by(field("order_id", "bucket", 8)), mutate: func(s *domain.PipelineSpec) {
			s.Transforms = []domain.Transform{{Type: "rename", Renames: []domain.Rename{{From: "id", To: "order_id"}}}}
		}, cat: missing, want: ""},
		"a renamed key on Flink CDC": {flink: true, info: orders, opts: by(field("order_id", "bucket", 8)), mutate: func(s *domain.PipelineSpec) {
			s.Transforms = []domain.Transform{{Type: "rename", Renames: []domain.Rename{{From: "id", To: "order_id"}}}}
		}, cat: missing, want: ""},
		"E17 a routed table's automatic default": {info: orders, opts: recorded(""), mutate: func(s *domain.PipelineSpec) {
			s.Transforms = []domain.Transform{{Type: "route", Routes: []domain.Route{{Match: "shop.orders", To: "all"}}}}
		}, cat: missing, want: "-"},
		"E18 MongoDB":                              {info: orders, opts: recorded(""), source: "mongodb", cat: missing, want: "-"},
		"E10 live table off its identity":          {info: orders, opts: recorded("id"), cat: fakeTables{tables: map[string]*table.Table{"cdc_shop_orders": liveTable(t, true, [2]string{"day", "created_at"})}}, want: "may already hold duplicate rows"},
		"E14 live Flink table off its identity":    {flink: true, info: orders, opts: by(field("created_at", "day", 0)), cat: fakeTables{tables: map[string]*table.Table{"cdc_shop_orders": liveTable(t, true, [2]string{"day", "created_at"})}}, want: "identity id"},
		"live table bucketed by its identity":      {info: orders, opts: recorded("id"), cat: fakeTables{tables: map[string]*table.Table{"cdc_shop_orders": liveTable(t, true, [2]string{"bucket[16]", "id"})}}, want: ""},
		"X9 live table without identifier fields":  {info: orders, opts: recorded("id"), cat: fakeTables{tables: map[string]*table.Table{"cdc_shop_orders": liveTable(t, false, [2]string{"day", "created_at"})}}, want: ""},
		"live table, change log":                   {info: orders, opts: with(recorded("id"), "write_mode", "append"), cat: fakeTables{tables: map[string]*table.Table{"cdc_shop_orders": liveTable(t, true, [2]string{"day", "created_at"})}}, want: ""},
		"live table: what is asked doesn't matter": {info: orders, opts: by(field("created_at", "day", 0)), cat: fakeTables{tables: map[string]*table.Table{"cdc_shop_orders": liveTable(t, true, [2]string{"bucket[16]", "id"})}}, want: ""},
		"E11 unreadable table":                     {info: orders, opts: recorded("id"), cat: fakeTables{err: errors.New("403")}, want: "warn:Couldn't read"},
		"no catalog: checked as new":               {info: orders, opts: by(field("created_at", "day", 0)), want: "day(created_at)"},
	}
	for name, c := range cases {
		spec := partitionSpec([]string{"shop.orders"}, nil)
		spec.Sink.Options = map[string]any{"write_mode": "upsert"}
		for k, v := range c.opts {
			spec.Sink.Options[k] = v
		}
		if c.mutate != nil {
			c.mutate(&spec)
		}
		opts, err := SinkOptions("iceberg", spec)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		source := c.source
		if source == "" {
			source = "postgresql"
		}
		facts := PipelineFacts{Tables: []TableInfo{c.info}, Options: opts, FlinkCDC: c.flink, SourceType: source, Destinations: map[string]string{"shop.orders": "lake.cdc_shop_orders"}}
		got := partitionKeyChecks(context.Background(), c.cat, icebergConfig{}, CompileContext{Spec: spec}, facts)
		switch {
		case c.want == "-":
			if len(got) != 0 {
				t.Errorf("%s: %+v", name, got)
			}
		case c.want == "":
			if len(got) != 1 || got[0].Status != checks.Pass {
				t.Errorf("%s: %+v", name, got)
			}
		case strings.HasPrefix(c.want, "warn:"):
			if len(got) != 1 || got[0].Status != checks.Warn || !strings.Contains(got[0].Message, strings.TrimPrefix(c.want, "warn:")) {
				t.Errorf("%s: %+v", name, got)
			}
		default:
			if len(got) != 1 || !got[0].Blocking || !strings.Contains(got[0].Message, c.want) {
				t.Errorf("%s: %+v", name, got)
			}
		}
	}
	// E21: no discovery, partitioning asked for: unverified.
	spec := partitionSpec([]string{"shop.orders"}, map[string]any{"shop.orders": []any{field("id", "bucket", 8)}})
	opts, _ := SinkOptions("iceberg", spec)
	got := partitionKeyChecks(context.Background(), missing, icebergConfig{}, CompileContext{Spec: spec}, PipelineFacts{Options: opts, SourceType: "postgresql"})
	if len(got) != 1 || got[0].Status != checks.Warn {
		t.Fatalf("no discovery: %+v", got)
	}
}

// iceberg_key_columns: on Debezium a key column renamed or left out of the row drops out of the
// table's identity.
func TestIcebergKeyColumns(t *testing.T) {
	cols := []Column{{Name: "tenant_id", Type: "integer"}, {Name: "id", Type: "integer"}, {Name: "note", Type: "text"}}
	info := TableInfo{Schema: "shop", Name: "orders", PrimaryKey: []string{"tenant_id", "id"}, Columns: cols}
	rename := func(from, to string) domain.Transform {
		return domain.Transform{Type: "rename", Renames: []domain.Rename{{From: from, To: to}}}
	}
	for name, c := range map[string]struct {
		steps   []domain.Transform
		exclude []string
		want    string
	}{
		"kept":          {nil, nil, ""},
		"one renamed":   {[]domain.Transform{rename("id", "order_id")}, nil, "fail:shop.orders (id)"},
		"one left out":  {nil, []string{"tenant_id"}, "fail:shop.orders (tenant_id)"},
		"all renamed":   {[]domain.Transform{rename("id", "a"), rename("tenant_id", "b")}, nil, "warn:appended"},
		"other renamed": {[]domain.Transform{rename("note", "memo")}, nil, ""},
	} {
		spec := partitionSpec([]string{"shop.orders"}, nil, c.steps...)
		if c.exclude != nil {
			spec.Source.Columns = map[string]domain.ColumnRules{"shop.orders": {Exclude: c.exclude, Mask: []string{}}}
		}
		got := icebergKeyColumnChecks(spec, PipelineFacts{Tables: []TableInfo{info}, SourceType: "postgresql"})
		switch {
		case c.want == "":
			if len(got) != 0 {
				t.Errorf("%s: %+v", name, got)
			}
		case strings.HasPrefix(c.want, "fail:"):
			if len(got) != 1 || !got[0].Blocking || !strings.Contains(got[0].Message, strings.TrimPrefix(c.want, "fail:")) {
				t.Errorf("%s: %+v", name, got)
			}
		default:
			if len(got) != 1 || got[0].Status != checks.Warn || !strings.Contains(got[0].Message, strings.TrimPrefix(c.want, "warn:")) {
				t.Errorf("%s: %+v", name, got)
			}
		}
	}
}

// R8: MongoDB collections have no fixed columns to partition by.
func TestIcebergPartitionMongo(t *testing.T) {
	spec := partitionSpec([]string{"shop.orders"}, map[string]any{"shop.orders": []any{field("_id", "bucket", 8)}})
	opts, _ := SinkOptions("iceberg", spec)
	got := partitionChecks(context.Background(), nil, icebergConfig{}, CompileContext{Spec: spec}, PipelineFacts{Options: opts, SourceType: "mongodb"})
	if len(got) != 1 || !got[0].Blocking || !strings.Contains(got[0].Message, "MongoDB") {
		t.Fatal(got)
	}
}
