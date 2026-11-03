// Package contract loads the fixtures captured from the Python implementation
// (scripts/contract/capture.py) for the cross-implementation tests.
package contract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// Dir is backend-go/testdata/contract.
func Dir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "testdata", "contract")
}

// Load decodes one fixture file into v, failing the test when it is missing.
func Load(t testing.TB, name string, v any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(Dir(), name+".json")) //nolint:gosec // test fixtures
	if err != nil {
		t.Fatalf("contract fixture %s: %v", name, err)
	}
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("contract fixture %s: %v", name, err)
	}
}
