package domain_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/expr"
)

// ADR 0010: a Flink CDC key names a source column (written under its renamed name) or a column a
// step adds; a source column the table writes wins over a step's column of the same name.
func TestFlinkKey(t *testing.T) {
	const tables = `"source": {"tables": {"include": ["public.orders", "public.items"]}, "columns": {"public.orders": {"exclude": ["note", "old"]}}}`
	const steps = `"transforms": [{"type": "unwrap"},
	  {"type": "rename", "renames": [{"from": "cust_id", "to": "customer_id"}]},
	  {"type": "add_field", "name": "tenant", "value": {"kind": "static", "text": "acme"}},
	  {"type": "add_field", "tables": ["public.items"], "name": "only_items", "value": {"kind": "pipeline"}},
	  {"type": "computed", "table": "public.orders", "name": "status_up", "function": "upper", "args": [{"column": "status"}]},
	  {"type": "computed", "table": "public.orders", "name": "old", "function": "lower", "args": [{"column": "status"}]}]`
	cols := []string{"id", "cust_id", "status", "note", "old"}
	cases := []struct {
		name    string
		keys    string
		columns []string
		want    string // the written names, from
		err     string
	}{
		{"source columns, renamed", `["id", "cust_id"]`, cols, "id:column customer_id:column", ""},
		{"an added field", `["id", "tenant"]`, cols, "id:column tenant:added", ""},
		{"a computed column", `["status_up"]`, cols, "status_up:computed", ""},
		{"a step column replacing a left-out source column", `["old"]`, cols, "old:computed", ""},
		{"a left-out source column", `["note"]`, cols, "", "public.orders: the key column note is left out of the table. Keep it, or pick another key column under Transform."},
		{"an added field of another table", `["only_items"]`, cols, "", "public.orders writes no column only_items to key its rows by: pick the key columns again under Transform."},
		{"a column nobody writes", `["gone"]`, cols, "", "public.orders writes no column gone to key its rows by: pick the key columns again under Transform."},
		{"a rename's new name is that column (as compiled before)", `["customer_id"]`, cols, "customer_id:column", ""},
		// Columns not read: every name as it is, through the renames (the compile before ADR 0010).
		{"columns unknown", `["cust_id", "gone", "tenant"]`, nil, "customer_id:column gone:column tenant:added", ""},
		{"columns unknown, left out", `["note"]`, nil, "", "public.orders: the key column note is left out of the table. Keep it, or pick another key column under Transform."},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			spec := mustSpec(t, `{`+tables+`, `+steps+`, "keys": {"public.orders": `+c.keys+`}}`)
			got, err := domain.FlinkKey(spec, "public.orders", c.columns)
			if c.err != "" {
				if err == nil || err.Error() != c.err {
					t.Fatalf("got %v, want error %q", err, c.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			s := ""
			for i, k := range got {
				if i > 0 {
					s += " "
				}
				s += k.Written + ":" + k.From
			}
			if s != c.want {
				t.Fatalf("got %q, want %q", s, c.want)
			}
		})
	}
}

// On Flink CDC a key column the table doesn't write is a blocking finding; one a step adds is fine.
// Debezium keeps its message-key finding (a step's field is not in the key it builds at the source).
func TestFlinkKeyFindings(t *testing.T) {
	tables := map[string]domain.TableColumns{"public.orders": {Columns: ordersColumns, PrimaryKey: []string{"id"}, KeyKnown: true}}
	spec := mustSpec(t, `{`+ordersTables+`, "transforms": [
	  {"type": "computed", "table": "public.orders", "name": "status_up", "function": "upper", "args": [{"column": "status"}]}],
	 "keys": {"public.orders": ["id", "status_up"]}}`)
	if got := domain.TransformFindings("flink-cdc", "postgresql", "p", spec, tables); len(got) != 0 {
		t.Fatalf("a computed key column: %+v", got)
	}
	spec.Keys["public.orders"] = []string{"id", "gone"}
	got := domain.TransformFindings("flink-cdc", "postgresql", "p", spec, tables)
	if len(got) != 1 || got[0].Code != "transform_key_column_unknown" || !got[0].Fail {
		t.Fatalf("a key column nobody writes: %+v", got)
	}
	checks := domain.TransformChecks(spec, got)
	if len(checks) != 1 || checks[0].Status != "fail" || !checks[0].Blocking || checks[0].Title != "Transforms: primary keys" {
		t.Fatalf("check: %+v", checks)
	}
	spec.Keys["public.orders"] = []string{"status_up"}
	got = domain.TransformFindings("debezium", "postgresql", "p", spec, tables)
	if len(got) != 1 || got[0].Code != "transform_key_column_missing" || got[0].Fail {
		t.Fatalf("Debezium: %+v", got)
	}
}

// ADR 0010 K4: what a computed key column must be, and that a key with a step column keeps the
// whole primary key (otherwise rows would share a key).
func TestFlinkKeyProblems(t *testing.T) {
	info := domain.TableColumns{PrimaryKey: []string{"id"}, KeyKnown: true, Columns: []domain.PreviewColumn{
		{Name: "id", Type: "bigint"}, {Name: "code", Type: "text"}, {Name: "note", Type: "text", Nullable: true}, {Name: "made", Type: "date"}}}
	base := `"source": {"tables": {"include": ["public.t"]}}, "transforms": [{"type": "unwrap"},
	  {"type": "add_field", "name": "tenant", "value": {"kind": "static", "text": "acme"}},
	  {"type": "truncate", "columns": [{"table": "public.t", "column": "code", "length": 3}]},
	  {"type": "computed", "table": "public.t", "name": "c", %s}]`
	cases := []struct{ step, keys, want string }{
		{`"function": "upper", "args": [{"column": "made"}]`, `["id", "tenant"]`, ""},
		{`"function": "year", "args": [{"column": "made"}]`, `["id", "c"]`, ""},
		{`"function": "year", "args": [{"column": "made"}]`, `["c"]`, "must keep the whole primary key (id)"},
		{`"function": "year", "args": [{"column": "made"}]`, `["tenant"]`, "must keep the whole primary key (id)"},
		{`"function": "upper", "args": [{"column": "note"}]`, `["id", "c"]`, "it can be empty"},
		{`"function": "join_with", "args": [{"text": "-"}, {"column": "note"}, {"column": "code"}]`, `["id", "c"]`, "it can be empty"},
		{`"function": "to_integer", "args": [{"column": "code"}]`, `["id", "c"]`, "it can be empty"},
		{`"function": "first_set", "args": [{"column": "code"}, {"column": "note"}]`, `["id", "c"]`, "its kind follows the columns"},
		{`"function": "to_float", "args": [{"column": "code"}]`, `["id", "c"]`, "a number; a computed key column"},
		{"\"expression\": \"IFNULL(`note`, 'x')\"", `["id", "c"]`, "its kind follows the columns"},
		{"\"expression\": \"CAST(IFNULL(`note`, 'x') AS STRING)\"", `["id", "c"]`, "it can be empty"},
		{"\"expression\": \"`code` || '-' || CAST(`id` AS STRING)\"", `["id", "c"]`, ""},
		{"\"expression\": \"CAST(NOW() AS STRING)\"", `["id", "c"]`, "changes each time"},
		{`"function": "year", "args": [{"column": "made"}]`, `["id", "code"]`, "cut to 3 characters"},
	}
	for _, c := range cases {
		spec := mustSpec(t, "{"+fmt.Sprintf(base, c.step)+`, "keys": {"public.t": `+c.keys+"}}")
		got := strings.Join(domain.FlinkKeyProblems("postgresql", spec, "public.t", info), " | ")
		if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
			t.Errorf("%s %s: got %q, want %q", c.step, c.keys, got, c.want)
		}
	}
}

// The wizard mirrors which computed columns can be key columns (keyChoices in transforms/model.ts):
// the kind the compile reads (without the table's columns) and whether the value is never empty
// when every column input is NOT NULL, none is, or only the first is. Both suites read
// backend/testdata/key_columns.json (UPDATE_GOLDEN=1 rewrites it).
func TestKeyColumnVectors(t *testing.T) {
	type vector struct {
		Kind  string `json:"kind"`
		All   bool   `json:"all"`
		None  bool   `json:"none"`
		First bool   `json:"first"`
	}
	out := map[string]vector{}
	for name, f := range domain.ComputedFunctions {
		n := f.Min
		if n == 0 {
			n = len(f.Args)
		}
		step := domain.Transform{Type: "computed", Table: "public.t", Name: "k", Function: name}
		var cols []string
		for i := range n {
			slot := f.Args[min(i, len(f.Args)-1)]
			switch slot {
			case domain.ArgNumber:
				step.Args = append(step.Args, domain.ComputedArg{Number: ptr(1)})
			case domain.ArgDigits:
				step.Args = append(step.Args, domain.ComputedArg{Integer: ptr(2)})
			case domain.ArgPattern, domain.ArgDatePattern:
				step.Args = append(step.Args, domain.ComputedArg{Text: ptr("yyyy-MM-dd")})
			case domain.ArgLit:
				step.Args = append(step.Args, domain.ComputedArg{Text: ptr("-")})
			default:
				c := "c" + strconv.Itoa(i)
				cols = append(cols, c)
				step.Args = append(step.Args, domain.ComputedArg{Column: c})
			}
		}
		tree := domain.ComputedTree(step)
		res, err := expr.Check(tree, expr.Env{}, expr.UseColumn)
		if err != nil {
			t.Fatal(name, err)
		}
		out[name] = vector{Kind: string(res.Kind),
			All:   expr.NeverNull(tree, func(string) bool { return true }),
			None:  expr.NeverNull(tree, func(string) bool { return false }),
			First: expr.NeverNull(tree, func(c string) bool { return len(cols) > 0 && c == cols[0] })}
	}
	got, _ := json.MarshalIndent(map[string]any{"key_kinds": domain.KeyKinds, "functions": out}, "", "  ")
	got = append(got, '\n')
	path := filepath.Join("..", "..", "testdata", "key_columns.json")
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(path, got, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path) //nolint:gosec // test data
	if err != nil || string(want) != string(got) {
		t.Fatalf("key_columns.json differs from the code (UPDATE_GOLDEN=1 rewrites it): %v", err)
	}
}
func ptr[T any](v T) *T { return &v }

// A source key column that can be empty is a warning (the probe: Flink CDC writes it as ” on
// Iceberg, so those rows share a key); the primary key's own columns never are.
func TestFlinkKeyWarnings(t *testing.T) {
	info := domain.TableColumns{PrimaryKey: []string{"id"}, Columns: []domain.PreviewColumn{{Name: "id", Type: "bigint", Nullable: true}, {Name: "note", Type: "text", Nullable: true}}}
	spec := mustSpec(t, `{"source": {"tables": {"include": ["public.t"]}}, "keys": {"public.t": ["id", "note"]}}`)
	got := domain.FlinkKeyWarnings(spec, "public.t", info)
	if len(got) != 1 || !strings.Contains(got[0], "the key column note can be empty") {
		t.Fatal(got)
	}
	f := domain.TransformFindings("flink-cdc", "postgresql", "p", spec, map[string]domain.TableColumns{"public.t": info})
	if len(f) != 1 || f[0].Code != "transform_key_column_nullable" || f[0].Fail {
		t.Fatalf("%+v", f)
	}
}
