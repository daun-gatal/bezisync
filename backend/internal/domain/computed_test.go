package domain_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

func parseComputed(columns map[string]any, steps ...any) (domain.PipelineSpec, string) {
	v := &val.V{}
	s, _ := domain.ParseSpec(v, nil, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"public.orders"}}, "columns": columns},
		"transforms": steps})
	var msgs []string
	for _, e := range v.Errors {
		msgs = append(msgs, e.Message)
	}
	return s, strings.Join(msgs, "|")
}

func computed(fn string, args ...any) map[string]any {
	return map[string]any{"type": "computed", "id": "cc01", "table": "public.orders", "name": "label", "function": fn, "args": args}
}

func colArg(c string) map[string]any  { return map[string]any{"column": c} }
func textArg(s string) map[string]any { return map[string]any{"text": s} }
func numArg(n int) map[string]any     { return map[string]any{"number": n} }

func TestComputedStep(t *testing.T) {
	s, errs := parseComputed(map[string]any{}, computed("concat", colArg("status"), textArg("-"), colArg("id")))
	if errs != "" {
		t.Fatal(errs)
	}
	b, _ := json.Marshal(s)
	if !strings.Contains(string(b), `{"type":"computed","id":"cc01","table":"public.orders","name":"label","function":"concat","args":[{"column":"status"},{"text":"-"},{"column":"id"}]}`) {
		t.Fatal(string(b))
	}
	c := s.Clone()
	if c.Transforms[0].Args[1].Text == s.Transforms[0].Args[1].Text {
		t.Fatal("clone shares arguments")
	}
	if got := domain.ComputedInputs(s.Transforms[0]); strings.Join(got, ",") != "status,id" {
		t.Fatal(got)
	}
	masked := map[string]any{"public.orders": map[string]any{"mask": []any{"email"}, "exclude": []any{"notes"}}}
	for name, tc := range map[string]struct {
		columns map[string]any
		step    any
		want    string
	}{
		"T14: a masked input":       {masked, computed("upper", colArg("email")), "is masked"},
		"a left-out input":          {masked, computed("upper", colArg("notes")), "left out"},
		"not captured":              {map[string]any{}, map[string]any{"type": "computed", "table": "public.refunds", "name": "x", "function": "upper", "args": []any{colArg("a")}}, "not captured"},
		"an unknown function":       {map[string]any{}, computed("eval", colArg("a")), "Input should be"},
		"too few arguments":         {map[string]any{}, computed("concat", colArg("a")), "at least 2"},
		"too many arguments":        {map[string]any{}, computed("upper", colArg("a"), colArg("b")), "at most 1"},
		"a literal where a column":  {map[string]any{}, computed("upper", textArg("x")), "Field required"},
		"both a column and a text":  {map[string]any{}, computed("concat", map[string]any{"column": "a", "text": "b"}, colArg("c")), "one of them"},
		"a variable in a text":      {map[string]any{}, computed("concat", colArg("a"), textArg("${BZ_SRC_PASSWORD}")), "${"},
		"a line break in a text":    {map[string]any{}, computed("concat", colArg("a"), textArg("a\nb")), "control characters"},
		"a pattern not offered":     {map[string]any{}, computed("date_format", colArg("created_at"), textArg("yyyy-'Q'")), "Input should be"},
		"a start of 0":              {map[string]any{}, computed("substr", colArg("notes"), numArg(0), numArg(3)), "greater than or equal to 1"},
		"a bad name":                {map[string]any{}, map[string]any{"type": "computed", "table": "public.orders", "name": "1x", "function": "upper", "args": []any{colArg("a")}}, "pattern"},
		"an expression as a column": {map[string]any{}, computed("upper", colArg("a) OR (1=1")), "pattern"},
		"tables not allowed":        {map[string]any{}, map[string]any{"type": "computed", "tables": []any{"public.orders"}, "table": "public.orders", "name": "x", "function": "upper", "args": []any{colArg("a")}}, "every table"},
	} {
		if _, errs := parseComputed(tc.columns, tc.step); !strings.Contains(errs, tc.want) {
			t.Errorf("%s: %s", name, errs)
		}
	}
}

// The preview shows the computed value on the sample row; findings flag an input that is gone or
// of the wrong kind (T15), and a name the table already has (T13).
func TestComputedPreviewAndFindings(t *testing.T) {
	cols := []domain.PreviewColumn{{Name: "id", Type: "integer"}, {Name: "status", Type: "text"}, {Name: "created_at", Type: "timestamp without time zone"}}
	run := func(step map[string]any) (domain.PreviewResult, []domain.TransformFinding) {
		s, errs := parseComputed(map[string]any{}, step)
		if errs != "" {
			t.Fatal(errs)
		}
		res := domain.Preview(domain.PreviewInput{SourceType: "postgresql", Pipeline: "p", Spec: s, Table: "public.orders", Operation: "insert",
			Columns: cols, PrimaryKey: []string{"id"}})
		f := domain.TransformFindings("postgresql", "p", s, map[string]domain.TableColumns{"public.orders": {Columns: cols, PrimaryKey: []string{"id"}, KeyKnown: true}})
		return res, f
	}
	res, f := run(computed("date_format", colArg("created_at"), textArg("yyyy-MM")))
	if len(f) != 0 || !strings.Contains(string(mustJSON(t, res.Output)), `"label":"2026-10"`) {
		t.Fatal(f, string(mustJSON(t, res.Output)))
	}
	if _, f := run(computed("upper", colArg("id"))); len(f) != 1 || f[0].Code != "transform_computed_input" || !strings.Contains(f[0].Message, "id is integer, and Upper case needs text") {
		t.Fatal(f)
	}
	if _, f := run(computed("upper", colArg("gone"))); len(f) != 1 || !strings.Contains(f[0].Message, "no longer has") {
		t.Fatal(f)
	}
	clash := computed("to_text", colArg("id"))
	clash["name"] = "status"
	if _, f := run(clash); len(f) != 1 || f[0].Code != "transform_field_conflict" {
		t.Fatal(f)
	}
	checks := domain.TransformChecks(domain.PipelineSpec{}, []domain.TransformFinding{{Code: "transform_computed_input", Fail: true, Message: "x"}})
	if len(checks) != 1 || !checks[0].Blocking {
		t.Fatal(checks)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
