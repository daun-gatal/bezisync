package domain_test

import (
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

func findings(t *testing.T, spec string, tables map[string]domain.TableColumns) []string {
	t.Helper()
	var out []string
	for _, f := range domain.TransformFindings("debezium", "postgresql", "orders-cdc", mustSpec(t, spec), tables) {
		out = append(out, f.Code+": "+f.Message)
	}
	return out
}

func TestTransformFindings(t *testing.T) {
	tables := map[string]domain.TableColumns{
		"public.orders":    {Columns: ordersColumns, PrimaryKey: []string{"id"}, KeyKnown: true},
		"public.audit_log": {Columns: []domain.PreviewColumn{{Name: "event_id", Type: "bigint"}, {Name: "msg", Type: "text"}}, KeyKnown: true},
		"public.ignored":   {Columns: ordersColumns},
	}
	got := findings(t, `{`+ordersTables+`, "transforms": [
	  {"type": "filter", "tables": ["public.orders"], "rules": [{"field": "status", "op": "gt", "value": 3}, {"field": "nope", "op": "is_null"},
	     {"field": "id", "op": "eq", "value": "1042"}, {"field": "vip", "op": "eq", "value": 1}, {"field": "total", "op": "in", "values": ["1.5"]}]},
	  {"type": "unwrap"},
	  {"type": "rename", "renames": [{"from": "cust_id", "to": "customer_id"}]},
	  {"type": "add_field", "name": "msg", "value": {"kind": "static", "text": "x"}}],
	 "keys": {"public.audit_log": ["at"], "public.orders": ["id", "gone"]}}`, tables)
	want := []string{
		"transform_column_missing: public.audit_log, step 3 (rename): No field cust_id at this point: nothing to rename.",
		"transform_field_conflict: public.audit_log, step 4 (add field): The event already has a field msg; the connector would stop with an error.",
		"transform_key_column_missing: public.audit_log has no column at, so its message key has no key at all.",
		"transform_rule_type: public.orders, step 1 (filter): status is text, but gt compares numbers; it never matches.",
		"transform_column_missing: public.orders, step 1 (filter) checks nope, which this table does not send; the rule sees an empty value.",
		"transform_rule_type: public.orders, step 1 (filter): id is bigint; compare it with numbers, not text.",
		"transform_rule_type: public.orders, step 1 (filter): vip is boolean, so comparing it with a number never matches.",
		"transform_rule_type: public.orders, step 1 (filter): total is numeric; compare it with numbers, not text.",
		"transform_key_column_missing: public.orders has no column gone, so its message key uses only the other key columns.",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got\n%s", strings.Join(got, "\n"))
	}
	// Text decimals compare fine as text; masked and excluded columns are called out.
	got = findings(t, `{"source": {"tables": {"include": ["public.orders"]}, "columns": {"public.orders": {"mask": ["status"], "exclude": ["vip"]}}},
	  "values": {"decimals": "string"}, "transforms": [{"type": "filter", "rules": [{"field": "total", "op": "eq", "value": "1.50"},
	  {"field": "status", "op": "eq", "value": "paid"}, {"field": "vip", "op": "is_true"}]}, {"type": "unwrap"}]}`, tables)
	want = []string{
		"transform_rule_type: public.orders, step 1 (filter) checks status, which is masked: the rule compares the hashed value.",
		"transform_column_missing: public.orders, step 1 (filter) checks vip, which this table does not send; the rule sees an empty value.",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got\n%s", strings.Join(got, "\n"))
	}
	// Unknown primary keys (the controller's schema read) do not claim "no key at all".
	got = findings(t, `{`+ordersTables+`, "keys": {"public.audit_log": ["at"]}}`, map[string]domain.TableColumns{"public.audit_log": tables["public.audit_log"]})
	if len(got) != 1 || !strings.HasSuffix(got[0], "has no key at all.") {
		t.Fatal(got)
	}
	got = findings(t, `{`+ordersTables+`, "keys": {"public.audit_log": ["at"]}}`,
		map[string]domain.TableColumns{"public.audit_log": {Columns: tables["public.audit_log"].Columns}})
	if len(got) != 1 || !strings.HasSuffix(got[0], "falls back to the primary key, if the table has one.") {
		t.Fatal(got)
	}
	// Tables without columns are not judged.
	if got := findings(t, `{`+ordersTables+`, "transforms": [{"type": "unwrap"}, {"type": "rename", "renames": [{"from": "a", "to": "b"}]}]}`,
		map[string]domain.TableColumns{"public.orders": {}}); len(got) != 0 {
		t.Fatal(got)
	}
}

func TestTransformChecks(t *testing.T) {
	spec := mustSpec(t, `{`+ordersTables+`, "transforms": [{"type": "unwrap"}, {"type": "custom", "class": "com.acme.Smt"},
	  {"type": "custom", "class": "org.apache.kafka.connect.transforms.ValueToKey"}]}`)
	var findings []domain.TransformFinding
	for i := range 7 {
		findings = append(findings, domain.TransformFinding{Code: "transform_column_missing", Message: "m" + string(rune('0'+i)) + "."})
	}
	findings = append(findings, domain.TransformFinding{Code: "transform_field_conflict", Fail: true, Message: "boom."})
	got := domain.TransformChecks(spec, findings)
	if len(got) != 3 {
		t.Fatalf("%+v", got)
	}
	if got[0].Code != "transform_column_missing" || got[0].Status != "warn" || got[0].Message != "m0. m1. m2. m3. m4. (and 2 more)" {
		t.Errorf("%+v", got[0])
	}
	if got[1].Code != "transform_field_conflict" || got[1].Status != "fail" || !got[1].Blocking || got[1].Fix == nil {
		t.Errorf("%+v", got[1])
	}
	if got[2].Code != "transform_custom_class_unknown" || !strings.Contains(got[2].Message, "step 2 (custom) (com.acme.Smt)") ||
		strings.Contains(got[2].Message, "ValueToKey") {
		t.Errorf("%+v", got[2])
	}
}
