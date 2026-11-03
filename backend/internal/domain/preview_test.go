package domain_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

var ordersColumns = []domain.PreviewColumn{
	{Name: "id", Type: "bigint"}, {Name: "cust_id", Type: "bigint"}, {Name: "status", Type: "text"},
	{Name: "total", Type: "numeric"}, {Name: "vip", Type: "boolean"}, {Name: "created_at", Type: "timestamp without time zone"},
	{Name: "blob", Type: "bytea"}, {Name: "addr", Type: "jsonb"},
}

func preview(t *testing.T, spec, op string, mutate ...func(*domain.PreviewInput)) domain.PreviewResult {
	t.Helper()
	full := "full"
	in := domain.PreviewInput{SourceType: "postgresql", Pipeline: "orders-cdc", Prefix: "bz_3f2c1d4e5b6a", Spec: mustSpec(t, spec),
		Table: "public.orders", Operation: op, Columns: ordersColumns, PrimaryKey: []string{"id"}, ReplicaIdentity: &full}
	for _, m := range mutate {
		m(&in)
	}
	return domain.Preview(in)
}

func asJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

const ordersTables = `"source": {"tables": {"include": ["public.orders", "public.audit_log"]}}`

// Field order and encodings were read from Debezium Server 3.7.0 (PostgreSQL → Kafka, JSON)
// with this exact chain.
func TestPreviewMatchesDebezium(t *testing.T) {
	res := preview(t, `{`+ordersTables+`, "transforms": [
	  {"type": "filter", "id": "keep", "tables": ["public.orders"], "operations": ["insert", "update", "delete", "snapshot"], "match": "any",
	   "rules": [{"field": "status", "op": "in", "values": ["paid"]}, {"field": "total", "op": "gte", "value": 1000}]},
	  {"type": "unwrap", "id": "shape", "delete_handling": "rewrite", "add_fields": ["op"]},
	  {"type": "rename", "id": "ren1", "tables": ["public.orders"], "renames": [{"from": "cust_id", "to": "customer_id"}, {"from": "nope", "to": "never"}]},
	  {"type": "add_field", "id": "add1", "name": "source_system", "value": {"kind": "static", "text": "orders-eu"}},
	  {"type": "add_field", "id": "add2", "name": "pipeline", "value": {"kind": "pipeline"}},
	  {"type": "flatten", "id": "flt1"}],
	 "values": {"decimals": "string", "timestamps": "isostring", "binary": "hex"}}`, "snapshot")
	if res.Outcome != "delivered" || res.Destination != "cdc.public.orders" {
		t.Fatalf("%+v", res)
	}
	want := `{"key":{"id":1042},"value":{"id":1042,"customer_id":88,"status":"paid","total":"129.90","vip":false,` +
		`"created_at":"2026-10-30T09:14:02.118Z","blob":"abcdef","addr":"{\"city\": \"Lyon\"}","__deleted":"false","__op":"r",` +
		`"source_system":"orders-eu","pipeline":"orders-cdc"}}`
	if got := asJSON(t, res.Output); got != want {
		t.Errorf("output\n got %s\nwant %s", got, want)
	}
	if res.Steps[2].Note != "No field nope at this point: nothing to rename." {
		t.Errorf("rename note %q", res.Steps[2].Note)
	}
	// Connector defaults, as Debezium 3.7.0 writes them (a negative decimal included).
	res = preview(t, `{`+ordersTables+`, "transforms": [{"type": "unwrap", "delete_handling": "drop", "add_fields": []}]}`, "insert",
		func(in *domain.PreviewInput) {
			in.Columns = []domain.PreviewColumn{{Name: "id", Type: "bigint"}, {Name: "total", Type: "numeric"}, {Name: "created_at", Type: "timestamp without time zone"},
				{Name: "day", Type: "date"}, {Name: "t", Type: "time without time zone"}, {Name: "blob", Type: "bytea"}}
		})
	if got := asJSON(t, res.Output.Value); got != `{"id":1042,"total":"Mr4=","created_at":1793351642118000,"day":20756,"t":33242118000,"blob":"q83v"}` {
		t.Errorf("defaults %s", got)
	}
}

func TestPreviewFiltersLikeTheGeneratedScript(t *testing.T) {
	spec := `{` + ordersTables + `, "transforms": [{"type": "filter", "operations": ["insert"], "rules": [{"field": "status", "op": "eq", "value": "paid"}]}, {"type": "unwrap"}]}`
	if res := preview(t, spec, "insert"); res.Outcome != "delivered" {
		t.Errorf("insert: %s", res.Outcome)
	}
	res := preview(t, spec, "update")
	if res.Outcome != "dropped" || res.Steps[0].Status != "dropped" || res.Steps[1].Status != "skipped" || res.Output != nil {
		t.Errorf("update: %+v", res)
	}
	// Deletes are judged on the row before; PostgreSQL without REPLICA IDENTITY FULL keeps only the key.
	keepDeletes := `{` + ordersTables + `, "transforms": [{"type": "filter", "rules": [{"field": "status", "op": "eq", "value": "paid"}]}, {"type": "unwrap"}]}`
	if res := preview(t, keepDeletes, "delete"); res.Outcome != "delivered" || !strings.Contains(asJSON(t, res.Output.Value), `"__deleted":"true"`) {
		t.Errorf("delete full: %+v", res)
	}
	res = preview(t, keepDeletes, "delete", func(in *domain.PreviewInput) { in.ReplicaIdentity = nil })
	if res.Outcome != "dropped" || !strings.Contains(strings.Join(res.Notes, " "), "only the key columns") {
		t.Errorf("delete default identity: %+v", res)
	}
	// Numbers: decimals as text still compare as numbers; a missing column is null.
	num := func(rule string) string {
		return `{` + ordersTables + `, "values": {"decimals": "string"}, "transforms": [{"type": "filter", "rules": [` + rule + `]}, {"type": "unwrap"}]}`
	}
	for rule, want := range map[string]string{
		`{"field": "total", "op": "gte", "value": 129.9}`:        "delivered",
		`{"field": "total", "op": "gt", "value": 129.9}`:         "dropped",
		`{"field": "total", "op": "eq", "value": 129.90}`:        "delivered",
		`{"field": "status", "op": "gt", "value": 1}`:            "dropped",
		`{"field": "missing", "op": "is_null"}`:                  "delivered",
		`{"field": "missing", "op": "eq", "value": "x"}`:         "dropped",
		`{"field": "vip", "op": "is_false"}`:                     "delivered",
		`{"field": "id", "op": "not_in", "values": [1042, "x"]}`: "dropped",
		`{"field": "id", "op": "ne", "value": 1}`:                "delivered",
		`{"field": "cust_id", "op": "in", "values": [88]}`:       "delivered",
	} {
		if got := preview(t, num(rule), "insert").Outcome; got != want {
			t.Errorf("%s: %s, want %s", rule, got, want)
		}
	}
}

func TestPreviewShapesAndScopes(t *testing.T) {
	// Tombstones pass through field steps; drop removes deletes.
	res := preview(t, `{`+ordersTables+`, "transforms": [{"type": "unwrap", "delete_handling": "tombstone"}, {"type": "add_field", "name": "x", "value": {"kind": "static", "text": "y"}}]}`, "delete")
	if res.Outcome != "delivered" || res.Output.Value != nil || asJSON(t, res.Output.Key) != `{"id":1042}` {
		t.Errorf("tombstone: %+v", res)
	}
	if res := preview(t, `{`+ordersTables+`, "transforms": [{"type": "unwrap", "delete_handling": "drop"}]}`, "delete"); res.Outcome != "dropped" {
		t.Errorf("drop: %s", res.Outcome)
	}
	// Scoped to another table: skipped.
	res = preview(t, `{`+ordersTables+`, "transforms": [{"type": "unwrap"}, {"type": "rename", "tables": ["public.audit_log"], "renames": [{"from": "status", "to": "s"}]}]}`, "insert")
	if res.Steps[1].Status != "skipped" || !strings.Contains(asJSON(t, res.Output.Value), `"status"`) {
		t.Errorf("scope: %+v", res.Steps)
	}
	// Envelope without a shape step; keys from spec.keys; no key without a primary key.
	res = preview(t, `{`+ordersTables+`, "transforms": [], "keys": {"public.orders": ["cust_id", "status"]}}`, "update")
	if got := asJSON(t, res.Output); !strings.Contains(got, `"key":{"cust_id":88,"status":"paid"}`) || !strings.Contains(got, `"before":{"id":1042,"cust_id":88,"status":"pending"`) ||
		!strings.Contains(got, `"op":"u"`) || !strings.Contains(got, `"source":{"version":"3.7.0.Final","connector":"postgresql"`) {
		t.Errorf("envelope: %s", got)
	}
	res = preview(t, `{`+ordersTables+`, "keys": {"public.orders": ["nope"]}}`, "insert")
	if got := asJSON(t, res.Output.Key); got != `{"id":1042}` {
		t.Errorf("missing key column falls back to the primary key: %s", got)
	}
	if res := preview(t, `{`+ordersTables+`}`, "insert", func(in *domain.PreviewInput) { in.PrimaryKey = nil }); res.Output.Key != nil {
		t.Errorf("key without primary key: %v", res.Output.Key)
	}
	// Column rules apply before any step.
	res = preview(t, `{"source": {"tables": {"include": ["public.orders"]}, "columns": {"public.orders": {"mask": ["status"], "exclude": ["blob"]}}}}`, "insert")
	if got := asJSON(t, res.Output.Value); strings.Contains(got, `"blob"`) || strings.Contains(got, `"paid"`) || !strings.Contains(got, `"status":"`) {
		t.Errorf("column rules: %s", got)
	}
}

func TestPreviewStopsAtWhatItCannotSimulate(t *testing.T) {
	res := preview(t, `{`+ordersTables+`, "transforms": [{"type": "unwrap"}, {"type": "custom", "class": "a.B"}, {"type": "flatten"}]}`, "insert")
	if res.Outcome != "unknown" || res.Output != nil || res.Steps[1].Status != "unknown" || res.Steps[2].Status != "unknown" || res.Fields != nil {
		t.Errorf("%+v", res)
	}
	res = preview(t, `{`+ordersTables+`, "transforms": [{"type": "filter", "condition": "true"}, {"type": "unwrap"}]}`, "insert")
	if res.Outcome != "unknown" {
		t.Errorf("raw filter: %s", res.Outcome)
	}
}

func TestPreviewCatchesWhatWouldStopTheConnector(t *testing.T) {
	for spec, want := range map[string]string{
		`{"transforms": [{"type": "unwrap"}, {"type": "add_field", "name": "status", "value": {"kind": "static", "text": "x"}}]}`:          "already has a field status",
		`{"transforms": [{"type": "unwrap"}, {"type": "rename", "renames": [{"from": "cust_id", "to": "status"}]}]}`:                       "Two fields would be called status",
		`{"transforms": [{"type": "unwrap", "add_fields": ["op"]}, {"type": "add_field", "name": "__op", "value": {"kind": "pipeline"}}]}`: "already has a field __op",
	} {
		res := preview(t, `{`+ordersTables+`, `+spec[1:], "insert")
		if res.Outcome != "error" || !strings.Contains(asJSON(t, res.Steps), want) {
			t.Errorf("%s: %+v", spec, res)
		}
		if last := res.Steps[len(res.Steps)-1]; last.Status != "error" {
			t.Errorf("%s: last step %+v", spec, last)
		}
	}
}

func TestPreviewMongoDB(t *testing.T) {
	res := preview(t, `{"source": {"tables": {"include": ["shop.customers"]}}, "transforms": [{"type": "unwrap"}, {"type": "flatten"}]}`, "insert",
		func(in *domain.PreviewInput) {
			in.SourceType, in.Table, in.PrimaryKey = "mongodb", "shop.customers", []string{"_id"}
			in.Columns = []domain.PreviewColumn{{Name: "_id", Type: "ObjectId"}, {Name: "name", Type: "str"}, {Name: "address", Type: "dict"}}
		})
	if got := asJSON(t, res.Output); got != `{"key":{"id":"6720f1a2b3c4d5e6f7a8b9c0"},"value":{"_id":"6720f1a2b3c4d5e6f7a8b9c0","name":"Ana Lima","address_city":"Lyon","address_zip":"69001","__deleted":"false","__op":"c","__source_ts_ms":1793351642118}}` {
		t.Errorf("mongo %s", got)
	}
	if in := asJSON(t, res.Input.Value); !strings.Contains(in, `"after":"{\"_id\":`) {
		t.Errorf("mongo envelope carries JSON text: %s", in)
	}
}

// Read from Debezium Server 3.7.0 (PostgreSQL, no transforms).
func TestPreviewEnvelopeFieldsMatchDebezium(t *testing.T) {
	res := preview(t, `{`+ordersTables+`, "transforms": []}`, "snapshot")
	env := res.Output.Value.(interface{ Keys() []string })
	if got := strings.Join(env.Keys(), ","); got != "before,after,source,transaction,op,ts_ms,ts_us,ts_ns" {
		t.Errorf("envelope %s", got)
	}
	src := asJSON(t, res.Output.Value)
	if !strings.Contains(src, `"source":{"version":"3.7.0.Final","connector":"postgresql","name":"bz_3f2c1d4e5b6a","ts_ms":1793351642118,"snapshot":"true","db":"app","sequence":`) ||
		!strings.Contains(src, `"schema":"public","table":"orders","txId":771,"lsn":22497448,"xmin":null,"origin":null,"origin_lsn":null}`) {
		t.Errorf("source %s", src)
	}
}
