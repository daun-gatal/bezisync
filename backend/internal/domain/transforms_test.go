package domain_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

func mustSpec(t *testing.T, s string) domain.PipelineSpec {
	t.Helper()
	raw, err := pyjson.DecodeGeneric([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	v := &val.V{}
	spec, ok := domain.ParseSpec(v, nil, raw)
	if !ok {
		t.Fatalf("%s: %v", s, v.Errors)
	}
	return spec
}

func mustPolicies(t *testing.T, s string) domain.ProjectPolicies {
	t.Helper()
	raw, err := pyjson.DecodeGeneric([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	v := &val.V{}
	p, ok := domain.ParsePolicies(v, nil, raw)
	if !ok {
		t.Fatalf("%s: %v", s, v.Errors)
	}
	return p
}

func TestScriptedFilterNeedsThePolicy(t *testing.T) {
	spec := mustSpec(t, `{"transforms": [{"type": "unwrap"}, {"type": "filter", "condition": "value.op == 'c'"}]}`)
	got := domain.EvaluatePolicies(mustPolicies(t, `{}`), spec, nil, "postgresql", "kafka", 0)
	if len(got) != 1 || got[0].Code != "policy_scripted_transform" || !got[0].Blocking {
		t.Fatalf("got %+v", got)
	}
	if !strings.Contains(got[0].Message, "step 2 (filter)") || got[0].Fix == nil || got[0].Fix.Who != "Admin" {
		t.Errorf("message %q fix %v", got[0].Message, got[0].Fix)
	}
	allowed := domain.EvaluatePolicies(mustPolicies(t, `{"allow_scripted_transforms": true}`), spec, nil, "postgresql", "kafka", 0)
	if len(allowed) != 1 || allowed[0].Code != "policy_ok" {
		t.Fatalf("allowed: %+v", allowed)
	}
	// An empty or missing condition is not a script.
	for _, s := range []string{`{"transforms": [{"type": "filter"}]}`, `{"transforms": [{"type": "filter", "condition": ""}]}`} {
		if got := domain.EvaluatePolicies(mustPolicies(t, `{}`), mustSpec(t, s), nil, "postgresql", "kafka", 0); got[0].Code != "policy_ok" {
			t.Errorf("%s: %+v", s, got)
		}
	}
}

func TestScriptedPolicyDumpsOnlyWhenSet(t *testing.T) {
	off, _ := json.Marshal(mustPolicies(t, `{"allow_scripted_transforms": false}`))
	if strings.Contains(string(off), "allow_scripted_transforms") {
		t.Errorf("default dumped: %s", off)
	}
	on, _ := json.Marshal(mustPolicies(t, `{"allow_scripted_transforms": true}`))
	if !strings.HasSuffix(string(on), `"require_approval":false,"allow_scripted_transforms":true}`) &&
		!strings.HasSuffix(string(on), `"require_approval": false, "allow_scripted_transforms": true}`) {
		t.Errorf("set: %s", on)
	}
	v := &val.V{}
	if _, ok := domain.ParsePolicies(v, nil, map[string]any{"allow_scripted_transforms": "yes please"}); ok {
		t.Error("accepted a non-boolean")
	}
}

const tables = `"source": {"tables": {"include": ["public.orders", "public.audit_log"]}}`

func TestTypedTransformsRoundTrip(t *testing.T) {
	in := `{` + tables + `,
	  "transforms": [
	    {"type": "filter", "id": "k7f2", "tables": ["public.orders"], "operations": ["insert", "update"],
	     "rules": [{"field": "status", "op": "in", "values": ["paid", "shipped"]}, {"field": "total", "op": "gte", "value": 10.5},
	               {"field": "note", "op": "is_null"}, {"field": "vip", "op": "eq", "value": true}]},
	    {"type": "unwrap", "id": "u1a0", "delete_handling": "drop"},
	    {"type": "rename", "id": "r9c1", "tables": ["public.orders"], "renames": [{"from": "cust_id", "to": "customer_id"}]},
	    {"type": "add_field", "id": "a3d4", "name": "source_system", "value": {"kind": "static", "text": "orders-eu"}},
	    {"type": "add_field", "id": "a3d5", "name": "pipeline", "value": {"kind": "pipeline"}},
	    {"type": "flatten", "id": "f001"},
	    {"type": "custom", "id": "c81e", "class": "com.acme.smt.CurrencyNormalizer", "properties": {"field": "total", "target.currency": "EUR"}}
	  ],
	  "values": {"decimals": "double", "timestamps": "isostring"},
	  "keys": {"public.audit_log": ["event_id"]}}`
	spec := mustSpec(t, in)
	if spec.Transforms[0].Match != "all" || spec.Transforms[5].Delimiter != "_" || spec.Values.Binary != "" {
		t.Fatalf("defaults: %+v", spec.Transforms)
	}
	b, _ := json.Marshal(spec)
	again := mustSpec(t, string(b))
	if !again.Equal(spec) || domain.SpecHash(again) != domain.SpecHash(spec) {
		t.Fatalf("round trip changed the spec:\n%s", b)
	}
	clone := spec.Clone()
	clone.Transforms[0].Rules[0].Values[0] = "changed"
	clone.Transforms[2].Renames[0].To = "x"
	clone.Transforms[6].Properties["field"] = "x"
	clone.Keys["public.audit_log"][0] = "x"
	if !spec.Equal(mustSpec(t, string(b))) {
		t.Fatal("Clone shares state with the original")
	}
	for _, want := range []string{`"values":{"decimals":"double","timestamps":"isostring"}`, `"keys":{"public.audit_log":["event_id"]}`,
		`{"type":"rename","id":"r9c1","tables":["public.orders"],"renames":[{"from":"cust_id","to":"customer_id"}]}`,
		`"match":"all","rules":[{"field":"status","op":"in","values":["paid","shipped"]},{"field":"total","op":"gte","value":10.5}`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("dump lacks %s:\n%s", want, b)
		}
	}
}

func TestPythonEraSpecsStillParse(t *testing.T) {
	// Stored versions are read with MustSpec: anything the Python releases accepted must parse.
	for _, s := range []string{
		`{"transforms": [{"type": "unwrap"}, {"type": "unwrap"}]}`,
		`{"transforms": [{"type": "outbox"}, {"type": "filter", "condition": "x"}, {"type": "unwrap"}, {"type": "filter"}]}`,
		`{"transforms": [{"type": "unwrap", "add_fields": ["anything.goes"], "delete_handling": "tombstone"}]}`,
	} {
		mustSpec(t, s)
	}
}

func TestPythonEraSpecsDumpUnchanged(t *testing.T) {
	// No new key appears unless it is used: the stored hashes of every existing version hold.
	spec := mustSpec(t, `{"transforms": [{"type": "unwrap"}, {"type": "filter", "condition": "x"}, {"type": "outbox"}]}`)
	b, _ := json.Marshal(spec)
	steps, _ := json.Marshal(spec.Dump().Plain()["transforms"])
	if string(steps) != `[{"add_fields":["op","source.ts_ms"],"condition":null,"delete_handling":"rewrite","type":"unwrap"},`+
		`{"add_fields":["op","source.ts_ms"],"condition":"x","delete_handling":"rewrite","type":"filter"},`+
		`{"add_fields":["op","source.ts_ms"],"condition":null,"delete_handling":"rewrite","type":"outbox"}]` {
		t.Errorf("steps: %s", steps)
	}
	for _, key := range []string{`"values"`, `"keys"`} {
		if strings.Contains(string(b), key) {
			t.Errorf("%s dumped for a Python-era spec: %s", key, b)
		}
	}
	if domain.StepName(1, spec.Transforms[1]) != "t1filter" {
		t.Error(domain.StepName(1, spec.Transforms[1]))
	}
}

func TestTransformValidation(t *testing.T) {
	cases := []struct{ spec, want string }{
		{`{"transforms": [{"type": "rename", "renames": [{"from": "a", "to": "b"}]}]}`, "needs the row-after-change event shape before it"},
		{`{"transforms": [{"type": "unwrap"}, {"type": "filter", "operations": ["insert"]}]}`, "move this step above the event shape (step 1)"},
		{`{"transforms": [{"type": "filter", "operations": ["insert"], "condition": "x"}]}`, "either operations and rules, or a raw condition"},
		{`{"transforms": [{"type": "filter", "operations": []}]}`, "needs at least one operation or rule"},
		{`{"transforms": [{"type": "filter", "operations": ["insert", "insert"]}]}`, "operations must not repeat"},
		{`{"transforms": [{"type": "filter", "operations": ["upsert"]}]}`, "Input should be 'insert', 'update', 'delete' or 'snapshot'"},
		{`{"transforms": [{"type": "filter", "condition": "x", "tables": ["public.orders"]}], ` + tables + `}`, "only filters built from operations and rules"},
		{`{"transforms": [{"type": "filter", "match": "any", "operations": ["insert"]}]}`, "match needs rules"},
		{`{"transforms": [{"type": "filter", "rules": [{"field": "a", "op": "gt", "value": "10"}]}]}`, "must be a number for this operator"},
		{`{"transforms": [{"type": "filter", "rules": [{"field": "a", "op": "in", "values": []}]}]}`, "List should have at least 1 item"},
		{`{"transforms": [{"type": "filter", "rules": [{"field": "a", "op": "eq"}]}]}`, "Field required"},
		{`{"transforms": [{"type": "filter", "rules": [{"field": "a", "op": "is_null", "value": 1}]}]}`, "is_null takes no value"},
		{`{"transforms": [{"type": "filter", "rules": [{"field": "a", "op": "eq", "value": "${BZ_SRC_PASSWORD}"}]}]}`, `must not contain "${"`},
		{`{"transforms": [{"type": "filter", "rules": [{"field": "a'b", "op": "is_null"}]}]}`, "String should match pattern"},
		{`{"transforms": [{"type": "filter", "rules": [{"field": "a", "op": "eq", "value": "x\ny"}]}]}`, "must not contain control characters"},
		{`{"transforms": [{"type": "unwrap"}, {"type": "rename", "renames": [{"from": "a", "to": "a"}]}]}`, "cannot be renamed to itself"},
		{`{"transforms": [{"type": "unwrap"}, {"type": "rename", "renames": [{"from": "a", "to": "b"}, {"from": "c", "to": "b"}]}]}`, "two fields would be called b"},
		{`{"transforms": [{"type": "unwrap"}, {"type": "rename", "renames": [{"from": "a", "to": "b c"}]}]}`, "String should match pattern"},
		{`{"transforms": [{"type": "unwrap"}, {"type": "rename", "renames": []}]}`, "List should have at least 1 item"},
		{`{"transforms": [{"type": "unwrap"}, {"type": "add_field", "name": "x", "value": {"kind": "static", "text": "${HOME}"}}]}`, `must not contain "${"`},
		{`{"transforms": [{"type": "unwrap"}, {"type": "add_field", "name": "x", "value": {"kind": "static"}}]}`, "Field required"},
		{`{"transforms": [{"type": "unwrap"}, {"type": "add_field", "name": "x", "value": {"kind": "pipeline", "text": "y"}}]}`, "needs no text"},
		{`{"transforms": [{"type": "unwrap"}, {"type": "add_field", "name": "x", "value": {"kind": "time"}}]}`, "Input should be 'static' or 'pipeline'"},
		{`{"transforms": [{"type": "unwrap"}, {"type": "flatten", "delimiter": "-"}]}`, "Input should be '_' or '.'"},
		{`{"transforms": [{"type": "unwrap"}, {"type": "custom", "class": "Filter"}]}`, "String should match pattern"},
		{`{"transforms": [{"type": "unwrap"}, {"type": "custom", "class": "a.B", "properties": {"type": "x"}}]}`, "type is set by BeziSync"},
		{`{"transforms": [{"type": "unwrap"}, {"type": "custom", "class": "a.B", "properties": {"predicate": "x"}}]}`, "predicate is set by BeziSync"},
		{`{"transforms": [{"type": "unwrap"}, {"type": "custom", "class": "a.B", "properties": {"x": "${BZ_STATE_PASSWORD}"}}]}`, `must not contain "${"`},
		{`{"transforms": [{"type": "unwrap"}, {"type": "custom", "class": "a.B", "properties": {"X=1": "y"}}]}`, "String should match pattern"},
		{`{"transforms": [{"type": "rename", "delete_handling": "drop", "renames": [{"from": "a", "to": "b"}]}]}`, "not used by rename steps"},
		{`{"transforms": [{"type": "unwrap", "tables": ["public.orders"]}], ` + tables + `}`, "unwrap steps apply to every table"},
		{`{"transforms": [{"type": "unwrap"}, {"type": "flatten", "tables": ["public.nope"]}], ` + tables + `}`, "public.nope is not captured"},
		{`{"transforms": [{"type": "unwrap"}, {"type": "flatten", "tables": []}]}`, "leave tables out"},
		{`{"transforms": [{"type": "unwrap", "id": "abcd"}, {"type": "flatten", "id": "abcd"}]}`, "step id abcd is used twice"},
		{`{"transforms": [{"type": "unwrap", "id": "AB"}]}`, "String should match pattern"},
		{`{"keys": {"public.nope": ["id"]}, ` + tables + `}`, "public.nope is not captured"},
		{`{"keys": {"public.orders": []}, ` + tables + `}`, "List should have at least 1 item"},
		{`{"keys": {"public.orders": ["id", "id"]}, ` + tables + `}`, "key columns must not repeat"},
		{`{"values": {"decimals": "float"}}`, "Input should be 'precise', 'double' or 'string'"},
		{`{"values": {"zone": "UTC"}}`, "Extra inputs are not permitted"},
	}
	for _, c := range cases {
		raw, err := pyjson.DecodeGeneric([]byte(c.spec))
		if err != nil {
			t.Fatalf("%s: %v", c.spec, err)
		}
		v := &val.V{}
		if _, ok := domain.ParseSpec(v, nil, raw); ok {
			t.Errorf("%s: accepted, want %q", c.spec, c.want)
			continue
		}
		var msgs []string
		for _, e := range v.Errors {
			msgs = append(msgs, e.Message)
		}
		if !strings.Contains(strings.Join(msgs, " | "), c.want) {
			t.Errorf("%s:\n got %v\nwant %q", c.spec, msgs, c.want)
		}
	}
}

func TestTransformPolicies(t *testing.T) {
	spec := mustSpec(t, `{"transforms": [{"type": "unwrap"}, {"type": "flatten"}, {"type": "custom", "class": "a.B"},
	  {"type": "custom", "class": "io.debezium.transforms.Filter", "properties": {"language": "jsr223.groovy", "condition": "true"}}]}`)
	got := domain.EvaluatePolicies(mustPolicies(t, `{"allowed_transforms": ["unwrap"]}`), spec, nil, "postgresql", "kafka", 0)
	var codes []string
	for _, c := range got {
		codes = append(codes, c.Code+": "+c.Message)
	}
	want := []string{
		"policy_transform_type: This project does not allow step 2 (flatten).",
		"policy_custom_transform: 2 steps run(s) a custom transform class (step 3 (custom), step 4 (custom)); this project does not allow custom transforms.",
		"policy_scripted_transform: One step run(s) a script you wrote (step 4 (custom)); this project does not allow scripted transforms.",
	}
	if strings.Join(codes, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got\n%s", strings.Join(codes, "\n"))
	}
	all := domain.EvaluatePolicies(mustPolicies(t, `{"allow_custom_transforms": true, "allow_scripted_transforms": true}`), spec, nil, "postgresql", "kafka", 0)
	if len(all) != 1 || all[0].Code != "policy_ok" {
		t.Fatalf("%+v", all)
	}
	v := &val.V{}
	if _, ok := domain.ParsePolicies(v, nil, map[string]any{"allowed_transforms": []any{"custom"}}); ok {
		t.Error("custom is governed by allow_custom_transforms, not the type list")
	}
	b, _ := json.Marshal(mustPolicies(t, `{"allowed_transforms": [], "allow_custom_transforms": true}`))
	if !strings.Contains(string(b), `"allow_custom_transforms":true,"allowed_transforms":[]`) {
		t.Errorf("%s", b)
	}
}

func TestTruncateStep(t *testing.T) {
	parse := func(columns map[string]any, steps ...any) (domain.PipelineSpec, string) {
		v := &val.V{}
		s, _ := domain.ParseSpec(v, nil, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"public.orders"}}, "columns": columns},
			"transforms": steps})
		var msgs []string
		for _, e := range v.Errors {
			msgs = append(msgs, e.Message)
		}
		return s, strings.Join(msgs, "|")
	}
	col := func(table, column string, n int) map[string]any {
		return map[string]any{"table": table, "column": column, "length": n}
	}
	step := func(cols ...any) map[string]any {
		return map[string]any{"type": "truncate", "id": "tr01", "columns": cols}
	}
	s, errs := parse(map[string]any{}, step(col("public.orders", "notes", 100)))
	if errs != "" || len(s.Transforms) != 1 || s.Transforms[0].Truncate[0] != (domain.Truncation{Table: "public.orders", Column: "notes", Length: 100}) {
		t.Fatal(errs)
	}
	b, _ := json.Marshal(s)
	if !strings.Contains(string(b), `{"type":"truncate","id":"tr01","columns":[{"table":"public.orders","column":"notes","length":100}]}`) {
		t.Fatal(string(b))
	}
	if c := s.Clone(); &c.Transforms[0].Truncate[0] == &s.Transforms[0].Truncate[0] {
		t.Fatal("clone shares truncations")
	}
	for name, c := range map[string]struct {
		columns map[string]any
		step    any
		want    string
	}{
		"T6: masked":         {map[string]any{"public.orders": map[string]any{"mask": []any{"notes"}, "exclude": []any{}}}, step(col("public.orders", "notes", 5)), "is masked"},
		"left out":           {map[string]any{"public.orders": map[string]any{"mask": []any{}, "exclude": []any{"notes"}}}, step(col("public.orders", "notes", 5)), "left out"},
		"not captured":       {map[string]any{}, step(col("public.refunds", "notes", 5)), "not captured"},
		"twice":              {map[string]any{}, step(col("public.orders", "notes", 5), col("public.orders", "notes", 9)), "truncated twice"},
		"no columns":         {map[string]any{}, map[string]any{"type": "truncate", "columns": []any{}}, "at least 1 item"},
		"length 0":           {map[string]any{}, step(col("public.orders", "notes", 0)), "greater than or equal to 1"},
		"tables not allowed": {map[string]any{}, map[string]any{"type": "truncate", "tables": []any{"public.orders"}, "columns": []any{col("public.orders", "notes", 5)}}, "every table"},
	} {
		if _, errs := parse(c.columns, c.step); !strings.Contains(errs, c.want) {
			t.Errorf("%s: %s", name, errs)
		}
	}
	// The preview cuts text, and only text.
	spec, _ := parse(map[string]any{}, step(col("public.orders", "notes", 3)))
	res := domain.Preview(domain.PreviewInput{SourceType: "postgresql", Pipeline: "p", Spec: spec, Table: "public.orders", Operation: "insert",
		Columns: []domain.PreviewColumn{{Name: "id", Type: "integer"}, {Name: "notes", Type: "text"}}, PrimaryKey: []string{"id"}})
	if !strings.Contains(res.Steps[0].Note, "notes to 3 characters") {
		t.Fatal(res.Steps)
	}
}

func TestRouteStep(t *testing.T) {
	for pattern, cases := range map[string]map[string]bool{
		"shop.orders_*": {"shop.orders_eu": true, "shop.orders_": true, "shop.orders": false, "crm.orders_eu": false},
		"*.orders":      {"shop.orders": true, "crm.orders": true, "shop.orders_eu": false},
		"shop.*_eu":     {"shop.orders_eu": true, "shop.eu": false, "shop.x_eu_y": false},
		"shop.orders":   {"shop.orders": true, "shop.orders2": false},
		// A middle part never takes the end's characters; the ends never overlap.
		"p*s*s": {"public.sales": true, "public.orders": false, "ps": false, "pss": true},
		"a*a":   {"a": false, "aa": true},
		"*":     {"shop.x": true},
	} {
		for table, want := range cases {
			if got := domain.GlobMatch(pattern, table); got != want {
				t.Errorf("%s ~ %s: %v", pattern, table, got)
			}
		}
	}
	parse := func(steps ...any) (domain.PipelineSpec, string) {
		v := &val.V{}
		s, _ := domain.ParseSpec(v, nil, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders_eu", "shop.orders_us", "shop.refunds"}}},
			"transforms": steps})
		var msgs []string
		for _, e := range v.Errors {
			msgs = append(msgs, e.Message)
		}
		return s, strings.Join(msgs, "|")
	}
	route := func(rules ...any) map[string]any {
		return map[string]any{"type": "route", "id": "rt01", "routes": rules}
	}
	rule := func(match, to string) map[string]any { return map[string]any{"match": match, "to": to} }
	s, errs := parse(route(rule("shop.orders_*", "shop.orders_all")))
	if errs != "" {
		t.Fatal(errs)
	}
	if got := domain.RouteTargets(s); len(got) != 2 || got["shop.orders_eu"] != "shop.orders_all" || got["shop.refunds"] != "" {
		t.Fatal(got)
	}
	b, _ := json.Marshal(s)
	if !strings.Contains(string(b), `{"type":"route","id":"rt01","routes":[{"match":"shop.orders_*","to":"shop.orders_all"}]}`) {
		t.Fatal(string(b))
	}
	// T10: a table two routes match.
	if _, errs := parse(route(rule("shop.orders_*", "a"), rule("shop.*_eu", "b"))); !strings.Contains(errs, "shop.orders_eu matches more than one route") {
		t.Fatal(errs)
	}
	for _, bad := range []map[string]any{rule("orders", "x"), rule("shop.orders_*", "${X}"), rule("shop.orders_*", "a b")} {
		if _, errs := parse(route(bad)); errs == "" {
			t.Errorf("%v accepted", bad)
		}
	}
	// The preview says where the table goes.
	res := domain.Preview(domain.PreviewInput{SourceType: "postgresql", Pipeline: "p", Spec: s, Table: "shop.orders_us", Operation: "insert",
		Columns: []domain.PreviewColumn{{Name: "id", Type: "integer"}}, PrimaryKey: []string{"id"}})
	if res.Destination != "shop.orders_all" {
		t.Fatal(res.Destination)
	}
}
