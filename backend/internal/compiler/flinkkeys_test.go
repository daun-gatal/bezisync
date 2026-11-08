package compiler_test

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
)

// ADR 0010: a Flink CDC primary key names any column the table writes: a source column (written
// under its renamed name), an added field or a computed column; one the table doesn't write is
// refused before anything deploys. Keys of source columns compile as before, a rename's new name
// included (it was written as it is).
func TestCompileFlinkKeysFromSteps(t *testing.T) {
	flinkConfig(t)
	specWith := func(keys []any) map[string]any {
		return map[string]any{
			"source": map[string]any{"tables": map[string]any{"include": []any{"public.orders"}},
				"columns": map[string]any{"public.orders": map[string]any{"mask": []any{}, "exclude": []any{"card"}}}},
			"transforms": []any{
				map[string]any{"type": "unwrap"},
				map[string]any{"type": "rename", "id": "renamer", "renames": []any{map[string]any{"from": "total", "to": "amount"}}},
				map[string]any{"type": "add_field", "id": "addten", "name": "tenant", "value": map[string]any{"kind": "static", "text": "acme"}},
				map[string]any{"type": "computed", "id": "cc01", "table": "public.orders", "name": "status_up", "function": "upper", "args": []any{map[string]any{"column": "status"}}},
			},
			"keys":      map[string]any{"public.orders": keys},
			"flink_cdc": map[string]any{"new_columns": "send"},
		}
	}
	in := secrets
	in.Columns = map[string][]string{"public.orders": {"id", "status", "total", "card"}}
	in.Startup = map[string]any{"mode": "latest-offset"}
	keysOf := func(keys []any) (string, error) {
		got, err := compiler.CompileFlink(flinkCtx(flinkSpec(t, specWith(keys))), pgConn, testSink, "acme/flink-cdc:3.7.0", in)
		if err != nil {
			return "", err
		}
		var doc struct {
			Transform []map[string]string `yaml:"transform"`
		}
		if err := yaml.Unmarshal([]byte(got.Pipeline), &doc); err != nil || len(doc.Transform) != 1 {
			t.Fatal(err, got.Pipeline)
		}
		return doc.Transform[0]["primary-keys"], nil
	}
	for keys, want := range map[string]string{"id,total": "id,amount", "id,tenant": "id,tenant", "status_up": "status_up", "tenant,status_up,id": "tenant,status_up,id", "id,amount": "id,amount"} {
		var list []any
		for _, k := range strings.Split(keys, ",") {
			list = append(list, k)
		}
		got, err := keysOf(list)
		if err != nil || got != want {
			t.Fatalf("%s: got %q, %v; want %q", keys, got, err, want)
		}
	}
	for keys, want := range map[string]string{
		"card": "public.orders: the key column card is left out of the table.",
		"gone": "public.orders writes no column gone to key its rows by",
	} {
		_, err := keysOf([]any{keys})
		if code(err) != "column_unknown" || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: %v", keys, err)
		}
	}
}
