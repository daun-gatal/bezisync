package domain_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

// ADR 0008: an expression is stored as its canonical text, dumped only when set.
func TestExpressionSteps(t *testing.T) {
	s, errs := parseComputed(map[string]any{},
		map[string]any{"type": "computed", "id": "cc01", "table": "public.orders", "name": "label", "expression": "upper(status) || '-'"},
		map[string]any{"type": "filter", "id": "ff01", "tables": []any{"public.orders"}, "expression": "total > 10 and status <> 'x'"})
	if errs != "" {
		t.Fatal(errs)
	}
	b, _ := json.Marshal(s)
	b = []byte(strings.NewReplacer(`\u003e`, ">", `\u003c`, "<", `\u0026`, "&").Replace(string(b)))
	for _, want := range []string{
		`{"type":"computed","id":"cc01","table":"public.orders","name":"label","expression":"UPPER(` + "`status`" + `) || '-'"}`,
		`"id":"ff01","tables":["public.orders"],"expression":"` + "`total`" + ` > 10 AND ` + "`status`" + ` <> 'x'"}`,
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("no %s in %s", want, b)
		}
	}
	if !s.Transforms[1].Typed() || s.Transforms[1].Stage() != 0 {
		t.Error("an expression filter is a typed keep-events filter")
	}
	// Specs without expressions dump as before (no "expression" key).
	plain, _ := parseComputed(map[string]any{}, computed("upper", colArg("status")))
	if pb, _ := json.Marshal(plain); strings.Contains(string(pb), "expression") {
		t.Fatal(string(pb))
	}
}

func TestExpressionRefusals(t *testing.T) {
	masked := map[string]any{"public.orders": map[string]any{"mask": []any{"email"}, "exclude": []any{"card"}}}
	cases := []struct {
		step any
		want string
	}{
		{map[string]any{"type": "computed", "table": "public.orders", "name": "x", "expression": "upper(email)"}, "email is masked"},
		{map[string]any{"type": "computed", "table": "public.orders", "name": "x", "expression": "card || 'x'"}, "card is left out"},
		{map[string]any{"type": "computed", "table": "public.orders", "name": "x", "expression": "my_udf(a)"}, "isn't a function"},
		{map[string]any{"type": "computed", "table": "public.orders", "name": "x", "expression": "a; drop"}, "(at \";\", character 2)"},
		{map[string]any{"type": "computed", "table": "public.orders", "name": "x", "expression": "upper(a)", "function": "upper"}, "either an expression or a function"},
		{map[string]any{"type": "filter", "expression": "ts > now()"}, "can't use NOW"},
		{map[string]any{"type": "filter", "expression": "a = 1", "rules": []any{map[string]any{"field": "a", "op": "is_null"}}}, "either an expression"},
		{map[string]any{"type": "filter", "expression": "a = 1", "match": "any"}, "match is for rules"},
	}
	for _, c := range cases {
		_, errs := parseComputed(masked, c.step)
		if !strings.Contains(errs, c.want) {
			t.Errorf("%v: got %q, want %q", c.step, errs, c.want)
		}
	}
}

// The new typed functions take their new argument fields, dumped only when set.
func TestComputedNewArguments(t *testing.T) {
	s, errs := parseComputed(map[string]any{},
		computed("round", colArg("total"), map[string]any{"integer": 2}),
		map[string]any{"type": "computed", "id": "cc02", "table": "public.orders", "name": "scaled", "function": "multiply", "args": []any{colArg("total"), map[string]any{"decimal": "-1.5"}}},
		map[string]any{"type": "computed", "id": "cc03", "table": "public.orders", "name": "d", "function": "parse_date", "args": []any{colArg("day"), textArg("dd/MM/yyyy")}})
	if errs != "" {
		t.Fatal(errs)
	}
	b, _ := json.Marshal(s)
	for _, want := range []string{`"args":[{"column":"total"},{"integer":2}]`, `"args":[{"column":"total"},{"decimal":"-1.5"}]`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("no %s in %s", want, b)
		}
	}
	for _, bad := range []map[string]any{
		computed("round", colArg("total"), map[string]any{"integer": 19}),
		computed("multiply", colArg("total"), map[string]any{"decimal": "01.5"}),
		computed("multiply", colArg("total"), map[string]any{"decimal": "1e5"}),
		computed("parse_date", colArg("day"), textArg("yyyy")),
		computed("replace", colArg("s"), textArg("${x}"), textArg("y")),
	} {
		if _, errs := parseComputed(map[string]any{}, bad); errs == "" {
			t.Errorf("%v: accepted", bad)
		}
	}
}

// Preflight (transform_expression): columns the table lacks, kinds, mixed kinds, and a column
// Flink CDC reads as a function.
func TestExpressionFindings(t *testing.T) {
	info := map[string]domain.TableColumns{"public.orders": {Columns: []domain.PreviewColumn{
		{Name: "id", Type: "bigint"}, {Name: "total", Type: "numeric(10,2)"}, {Name: "status", Type: "text"}, {Name: "now", Type: "text"}}}}
	s, errs := parseComputed(map[string]any{},
		map[string]any{"type": "computed", "table": "public.orders", "name": "a", "expression": "upper(stauts)"},
		map[string]any{"type": "computed", "table": "public.orders", "name": "b", "expression": "upper(total)"},
		map[string]any{"type": "computed", "table": "public.orders", "name": "c", "function": "first_set", "args": []any{colArg("id"), colArg("total")}},
		map[string]any{"type": "computed", "table": "public.orders", "name": "d", "function": "upper", "args": []any{colArg("now")}},
		map[string]any{"type": "filter", "expression": "id = total"},
		map[string]any{"type": "computed", "table": "public.orders", "name": "ok", "expression": "cast(id as double) * total"})
	if errs != "" {
		t.Fatal(errs)
	}
	got := domain.TransformFindings("postgresql", "p", s, info)
	msgs := map[int]string{}
	for _, f := range got {
		if f.Code == "transform_expression" {
			msgs[f.Step] = f.Message
		}
	}
	for step, want := range map[int]string{0: "no column stauts", 1: "needs text", 2: "mix whole numbers and decimals", 3: "compiles as its NOW function", 4: "write CAST"} {
		if !strings.Contains(msgs[step], want) {
			t.Errorf("step %d: got %q, want %q", step, msgs[step], want)
		}
	}
	if _, bad := msgs[5]; bad {
		t.Errorf("step 5 is valid: %s", msgs[5])
	}
}

// The frontend mirrors ComputedFunctions (COMPUTED_FUNCTIONS in transforms/model.ts); both test
// suites read backend/testdata/computed_functions.json (UPDATE_GOLDEN=1 rewrites it).
func TestComputedFunctionVectors(t *testing.T) {
	type fn struct {
		Args  []string `json:"args"`
		Min   int      `json:"min,omitempty"`
		Max   int      `json:"max,omitempty"`
		Out   string   `json:"out"`
		Group string   `json:"group"`
	}
	all := map[string]fn{}
	for k, f := range domain.ComputedFunctions {
		all[k] = fn{Args: f.Args, Min: f.Min, Max: f.Max, Out: f.Out, Group: f.Group}
	}
	got, _ := json.MarshalIndent(map[string]any{"functions": all, "date_patterns": domain.DatePatterns, "parse_date_patterns": domain.ParseDatePatterns}, "", "  ")
	got = append(got, '\n')
	path := filepath.Join("..", "..", "testdata", "computed_functions.json")
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(path, got, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path) //nolint:gosec // test data
	if err != nil || string(want) != string(got) {
		t.Fatalf("computed_functions.json differs from domain.ComputedFunctions (UPDATE_GOLDEN=1 rewrites it): %v", err)
	}
}
