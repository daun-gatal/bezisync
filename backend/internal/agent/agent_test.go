package agent

import (
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/runtime"
)

// On Kubernetes the agent passes on whether Flink CDC pipelines can run (Services).
func TestLocalTestCarriesFlinkCDC(t *testing.T) {
	got := localTest(runtime.TestResult{OK: true, Message: "ok", Details: map[string]any{"flink_cdc": false, "cpu_allocatable": 4}})
	if v, ok := got.Get("flink_cdc"); !ok || v != false || got.Len() != 3 {
		t.Fatalf("%v", got)
	}
	if _, ok := localTest(runtime.TestResult{OK: true, Details: map[string]any{"cpu_allocatable": 4}}).Get("flink_cdc"); ok {
		t.Fatal("Docker says nothing about Services")
	}
}
