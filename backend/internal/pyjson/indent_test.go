package pyjson

import (
	"os"
	"strings"
	"testing"
)

// frontend/openapi.json was written by json.dumps(indent=2, sort_keys=True).
func TestDumpsIndentMatchesPython(t *testing.T) {
	raw, err := os.ReadFile("../api/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	v, err := DecodeGeneric(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DumpsIndent(v, 2)
	if err != nil {
		t.Fatal(err)
	}
	if want := strings.TrimSuffix(string(raw), "\n"); got != want {
		for i := range min(len(got), len(want)) {
			if got[i] != want[i] {
				t.Fatalf("differs at %d: %q vs %q", i, got[max(0, i-40):min(len(got), i+40)], want[max(0, i-40):min(len(want), i+40)])
			}
		}
		t.Fatalf("length %d vs %d", len(got), len(want))
	}
	if s, _ := DumpsIndent(map[string]any{"b": []any{}, "a": map[string]any{}, "é": "ü"}, 2); s != "{\n  \"a\": {},\n  \"b\": [],\n  \"\\u00e9\": \"\\u00fc\"\n}" {
		t.Fatal(s)
	}
}
