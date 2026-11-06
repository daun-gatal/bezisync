package compiler_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/contract"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// compileSpec compiles a spec (JSON) against the corpus's source and sink connections.
func compileSpec(t *testing.T, source, sink, specJSON string) (*compiler.Compiled, error) {
	t.Helper()
	var fx compileFixture
	contract.Load(t, "compile", &fx)
	s, err := config.Load(func(k string) (string, bool) {
		if k == "BEZISYNC_STATE_DATABASE_URL" {
			return "postgresql://state:pw@state-db:5432/bzstate", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	config.Set(s)
	t.Cleanup(config.Reset)
	raw, err := pyjson.DecodeGeneric([]byte(specJSON))
	if err != nil {
		t.Fatal(err)
	}
	v := &val.V{}
	spec, ok := domain.ParseSpec(v, nil, raw)
	if !ok {
		t.Fatalf("invalid spec: %v", v.Errors)
	}
	ctx := connectors.CompileContext{PipelineID: fx.PipelineID, ShortID: "3f2c1d4e5b6a", Name: "orders-cdc", Spec: spec}
	return compiler.Compile(ctx, resolved(t, fx.Sources[source]), resolved(t, fx.Sinks[sink]), "")
}

func TestAdvancedCannotTouchTheTransformChain(t *testing.T) {
	for _, key := range []string{
		"debezium.transforms",
		"debezium.transforms.bzmeta.type",
		"debezium.transforms.bzroute.replacement",
		"debezium.transforms.mine.type",
		"debezium.predicates",
		"debezium.predicates.bzmeta.pattern",
	} {
		_, err := compileSpec(t, "pg", "kafka", `{"source": {"tables": {"include": ["public.orders"]}}, "advanced": {"`+key+`": "x"}}`)
		var p *problem.Problem
		if !errors.As(err, &p) || p.Code != "managed_property" {
			t.Errorf("%s: got %v, want managed_property", key, err)
			continue
		}
		if len(p.Errors) != 1 || !strings.Contains(p.Errors[0]["message"].(string), "managed by BeziSync") {
			t.Errorf("%s: errors %v", key, p.Errors)
		}
	}
	// Look-alikes stay allowed.
	for _, key := range []string{"debezium.transformsx", "debezium.source.transforms.x"} {
		if _, err := compileSpec(t, "pg", "kafka", `{"source": {"tables": {"include": ["public.orders"]}}, "advanced": {"`+key+`": "x"}}`); err != nil {
			t.Errorf("%s: %v", key, err)
		}
	}
}

const orders = `"source": {"tables": {"include": ["public.orders", "public.audit_log"]}}`

func TestStepsCompileInOrderWithScopes(t *testing.T) {
	got, err := compileSpec(t, "pg", "kafka", `{`+orders+`, "transforms": [
	  {"type": "filter", "id": "k7f2", "tables": ["public.orders"], "operations": ["insert", "update"],
	   "rules": [{"field": "status", "op": "in", "values": ["paid", "it's"]}]},
	  {"type": "unwrap", "id": "u1a0"},
	  {"type": "rename", "id": "r9c1", "tables": ["public.orders"], "renames": [{"from": "cust_id", "to": "customer_id"}, {"from": "a", "to": "b"}]},
	  {"type": "add_field", "id": "a3d4", "name": "source_system", "value": {"kind": "static", "text": "orders-eu"}},
	  {"type": "add_field", "id": "a3d5", "name": "pipeline", "value": {"kind": "pipeline"}},
	  {"type": "flatten", "id": "f001", "delimiter": "."},
	  {"type": "custom", "id": "c81e", "class": "com.acme.Smt", "tables": ["public.audit_log", "public.orders"], "properties": {"field": "total"}}]}`)
	if err != nil {
		t.Fatal(err)
	}
	p := got.Properties
	want := map[string]string{
		"debezium.transforms":                     "bzmeta,t_k7f2,t_u1a0,t_r9c1,t_a3d4,t_a3d5,t_f001,t_c81e,bzroute",
		"debezium.predicates":                     "bzmeta,p_t_r9c1,p_t_c81e",
		"debezium.transforms.t_k7f2.type":         "io.debezium.transforms.Filter",
		"debezium.transforms.t_k7f2.language":     "jsr223.groovy",
		"debezium.transforms.t_k7f2.topic.regex":  `bz_3f2c1d4e5b6a\.public\.orders`,
		"debezium.transforms.t_u1a0.type":         "io.debezium.transforms.ExtractNewRecordState",
		"debezium.transforms.t_r9c1.type":         "org.apache.kafka.connect.transforms.ReplaceField$Value",
		"debezium.transforms.t_r9c1.renames":      "cust_id:customer_id,a:b",
		"debezium.transforms.t_r9c1.predicate":    "p_t_r9c1",
		"debezium.predicates.p_t_r9c1.type":       "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
		"debezium.predicates.p_t_r9c1.pattern":    `bz_3f2c1d4e5b6a\.public\.orders`,
		"debezium.transforms.t_a3d4.static.field": "source_system",
		"debezium.transforms.t_a3d4.static.value": "orders-eu",
		"debezium.transforms.t_a3d5.static.value": "orders-cdc",
		"debezium.transforms.t_f001.type":         "org.apache.kafka.connect.transforms.Flatten$Value",
		"debezium.transforms.t_f001.delimiter":    ".",
		"debezium.transforms.t_c81e.type":         "com.acme.Smt",
		"debezium.transforms.t_c81e.field":        "total",
		"debezium.predicates.p_t_c81e.pattern":    `(?:bz_3f2c1d4e5b6a\.public\.audit_log|bz_3f2c1d4e5b6a\.public\.orders)`,
		"debezium.predicates.bzmeta.type":         "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
		"debezium.transforms.bzroute.type":        "org.apache.kafka.connect.transforms.RegexRouter",
	}
	for k, v := range want {
		if p[k] != v {
			t.Errorf("%s = %q, want %q", k, p[k], v)
		}
	}
	for _, k := range []string{"debezium.transforms.t_a3d4.predicate", "debezium.transforms.t_k7f2.predicate"} {
		if _, ok := p[k]; ok {
			t.Errorf("%s set", k)
		}
	}
	cond := p["debezium.transforms.t_k7f2.condition"]
	for _, part := range []string{"(value.op in ['c', 'u'])", `(g(r, 'status') == 'paid')`, `(g(r, 'status') == 'it\'s')`, "def r = (value.after ?: value.before)"} {
		if !strings.Contains(cond, part) {
			t.Errorf("condition lacks %s:\n%s", part, cond)
		}
	}
	if !strings.Contains(got.Render(), `debezium.transforms.t_k7f2.condition=({ s, f -> s != null`) {
		t.Error("condition not rendered")
	}
}

func TestFilterConditionShapes(t *testing.T) {
	cases := []struct{ step, want string }{
		{`{"type": "filter", "operations": ["delete", "snapshot"]}`, "(value.op in ['d', 'r']) && true"},
		{`{"type": "filter", "rules": [{"field": "total", "op": "gte", "value": 10.5}]}`,
			"true && ((n(g(r, 'total')) != null && n(g(r, 'total')).compareTo(new BigDecimal('10.5')) >= 0))"},
		{`{"type": "filter", "match": "any", "rules": [{"field": "a", "op": "is_null"}, {"field": "b", "op": "is_true"}]}`,
			"true && ((g(r, 'a') == null) || (g(r, 'b') == true))"},
		{`{"type": "filter", "rules": [{"field": "a", "op": "not_in", "values": [1, "x", false]}]}`,
			"!((n(g(r, 'a')) != null && n(g(r, 'a')).compareTo(new BigDecimal('1')) == 0) || (g(r, 'a') == 'x') || (g(r, 'a') == false))"},
		{`{"type": "filter", "rules": [{"field": "a", "op": "ne", "value": "back\\slash"}]}`, `!(g(r, 'a') == 'back\\slash')`},
		{`{"type": "filter", "rules": [{"field": "a$b", "op": "lt", "value": -2e3}]}`, "compareTo(new BigDecimal('-2e3')) < 0"},
	}
	for _, c := range cases {
		spec := mustStep(t, c.step)
		if got := compiler.FilterCondition(spec); !strings.Contains(got, c.want) {
			t.Errorf("%s:\n got %s\nwant …%s…", c.step, got, c.want)
		}
	}
}

func mustStep(t *testing.T, step string) domain.Transform {
	t.Helper()
	raw, err := pyjson.DecodeGeneric([]byte(`{"transforms": [` + step + `]}`))
	if err != nil {
		t.Fatal(err)
	}
	v := &val.V{}
	spec, ok := domain.ParseSpec(v, nil, raw)
	if !ok {
		t.Fatalf("%s: %v", step, v.Errors)
	}
	return spec.Transforms[0]
}

func TestValueFormatsAndKeysPerSource(t *testing.T) {
	got, err := compileSpec(t, "pg", "kafka", `{`+orders+`, "values": {"decimals": "double", "timestamps": "isostring", "binary": "hex"},
	  "keys": {"public.audit_log": ["event_id", "at"], "public.orders": ["id"]}}`)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range map[string]string{
		"debezium.source.decimal.handling.mode": "double",
		"debezium.source.time.precision.mode":   "isostring",
		"debezium.source.binary.handling.mode":  "hex",
		"debezium.source.message.key.columns":   `public\.orders:id;public\.audit_log:event_id,at`, // JSONB key order
	} {
		if got.Properties[k] != v {
			t.Errorf("%s = %q, want %q", k, got.Properties[k], v)
		}
	}
	// Connector defaults are left unset.
	got, err = compileSpec(t, "pg", "kafka", `{`+orders+`, "values": {"decimals": "precise", "timestamps": "epoch", "binary": "bytes"}}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"debezium.source.decimal.handling.mode", "debezium.source.time.precision.mode", "debezium.source.binary.handling.mode"} {
		if _, ok := got.Properties[k]; ok {
			t.Errorf("%s set for the default", k)
		}
	}
	if _, err := compileSpec(t, "mysql", "kafka", `{"source": {"tables": {"include": ["shop.orders"]}}, "values": {"timestamps": "isostring"}, "keys": {"shop.orders": ["id"]}}`); err != nil {
		t.Errorf("mysql: %v", err)
	}
	refusals := []struct{ source, spec, want string }{
		{"mssql", `{"source": {"tables": {"include": ["dbo.orders"]}}, "values": {"timestamps": "isostring"}}`, "Timestamps as ISO-8601 text are not available for SQL Server."},
		{"mssql", `{"source": {"tables": {"include": ["dbo.orders"]}}, "keys": {"dbo.orders": ["id"]}}`, "Choosing message key columns is not available for SQL Server."},
		{"mongo", `{"source": {"tables": {"include": ["shop.orders"]}}, "values": {"decimals": "double"}}`, "Decimals as numbers are not available for MongoDB."},
		{"mongo", `{"source": {"tables": {"include": ["shop.orders"]}}, "transforms": [{"type": "filter", "rules": [{"field": "a", "op": "is_null"}]}, {"type": "unwrap"}]}`,
			"Filtering on column values is not available for MongoDB; filter on operations instead."},
	}
	for _, c := range refusals {
		_, err := compileSpec(t, c.source, "kafka", c.spec)
		var p *problem.Problem
		if !errors.As(err, &p) || p.Code != "transform_unsupported" || p.Detail != c.want {
			t.Errorf("%s %s: got %v, want %q", c.source, c.spec, err, c.want)
		}
	}
	// MongoDB may still filter on operations.
	if _, err := compileSpec(t, "mongo", "kafka", `{"source": {"tables": {"include": ["shop.orders"]}}, "transforms": [{"type": "filter", "operations": ["insert"]}, {"type": "unwrap"}]}`); err != nil {
		t.Errorf("mongo operations: %v", err)
	}
}

func TestSQLServerTopicsCarryTheDatabase(t *testing.T) {
	got, err := compileSpec(t, "mssql", "kafka", `{"source": {"tables": {"include": ["dbo.orders"]}}, "transforms": [{"type": "filter", "operations": ["insert"]}, {"type": "unwrap"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	if r := got.Properties["debezium.transforms.t0filter.topic.regex"]; r != `bz_3f2c1d4e5b6a\.[^.]+\.dbo\.orders` {
		t.Errorf("topic.regex %q", r)
	}
}

// T1, T2: a soft delete makes the unwrap step mark deletes (no tombstone), adding one where the
// pipeline has none; the step itself compiles to nothing.
func TestSoftDeleteDebezium(t *testing.T) {
	with, err := compileSpec(t, "pg", "kafka", `{`+orders+`, "transforms": [{"type": "unwrap", "id": "u1a0", "delete_handling": "drop"}, {"type": "soft_delete", "id": "sd01"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	if with.Properties["debezium.transforms.t_u1a0.delete.tombstone.handling.mode"] != "rewrite" || strings.Contains(with.Properties["debezium.transforms"], "sd01") {
		t.Fatal(with.Properties["debezium.transforms"], with.Properties["debezium.transforms.t_u1a0.delete.tombstone.handling.mode"])
	}
	without, err := compileSpec(t, "pg", "kafka", `{`+orders+`, "transforms": [{"type": "soft_delete", "id": "sd01"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	chain := strings.Split(without.Properties["debezium.transforms"], ",")
	var unwrap string
	for _, step := range chain {
		if without.Properties["debezium.transforms."+step+".type"] == "io.debezium.transforms.ExtractNewRecordState" {
			unwrap = step
		}
	}
	if unwrap == "" || without.Properties["debezium.transforms."+unwrap+".delete.tombstone.handling.mode"] != "rewrite" {
		t.Fatal(without.Properties)
	}
	// Without the step nothing changes.
	plain, _ := compileSpec(t, "pg", "kafka", `{`+orders+`, "transforms": [{"type": "unwrap", "id": "u1a0", "delete_handling": "drop"}]}`)
	if plain.Properties["debezium.transforms.t_u1a0.delete.tombstone.handling.mode"] != "drop" {
		t.Fatal(plain.Properties)
	}
}
