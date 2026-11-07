package agent

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

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

// runningDriver observes every workload as running and answers every HTTP probe.
type runningDriver struct{ runtime.Driver }

func (runningDriver) Observe(context.Context, string) (runtime.Observation, error) {
	return runtime.Observation{Phase: "running", Ready: true}, nil
}

func (runningDriver) HTTPGet(context.Context, string, int, string, time.Duration) (int, string, error) {
	return 200, "{}", nil
}

func (runningDriver) Apply(context.Context, runtime.Workload) error { return nil }

func (runningDriver) ListWorkloads(context.Context, string) ([]string, error) { return nil, nil }

func (runningDriver) Test(context.Context) runtime.TestResult { return runtime.TestResult{OK: true} }

func (runningDriver) Capacity(context.Context) map[string]any { return nil }

// Report reads the desired workloads under the lock while Converge replaces them (Known issue
// 12; this test fails under -race without the fix).
func TestReportWhileConverging(t *testing.T) {
	a := NewWithDriver("http://x", "docker", runningDriver{}, http.DefaultClient)
	ctx := context.Background()
	desired := []any{map[string]any{"short_id": "abc123def456", "name": "p", "image": "i"}}
	a.Converge(ctx, desired)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 200 {
			a.Converge(ctx, desired)
		}
	}()
	go func() {
		defer wg.Done()
		for range 200 {
			a.Report(ctx)
		}
	}()
	wg.Wait()
	if _, ok := a.Report(ctx).Get("workloads"); !ok {
		t.Fatal("no workloads in the report")
	}
}
