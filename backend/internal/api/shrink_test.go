package api_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

// reachableLake adds an Iceberg destination whose REST catalog answers (its namespace exists), so
// the controller can deploy to it.
func reachableLake(t *testing.T, e *pipelineEnv) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/v1/config"):
			_, _ = w.Write([]byte(`{"defaults": {}, "overrides": {}}`))
		case strings.HasSuffix(r.URL.Path, "/v1/namespaces"):
			_, _ = w.Write([]byte(`{"namespaces": [["cdc"]]}`))
		case strings.HasSuffix(r.URL.Path, "/v1/namespaces/cdc"):
			_, _ = w.Write([]byte(`{"namespace": ["cdc"], "properties": {}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error": {"message": "not found", "type": "NoSuchTableException", "code": 404}}`))
		}
	}))
	t.Cleanup(srv.Close)
	return e.admin.post("/api/v1/projects/"+e.pid+"/connections", map[string]any{"kind": "sink", "type": "iceberg", "name": "reachable-lake",
		"config": map[string]any{"catalog_type": "rest", "uri": srv.URL + "/catalog", "warehouse": "s3://lake/warehouse"}}).expect(201).Str("id")
}

// helperStatus is a Flink CDC helper's status: running, streaming or copying, with its last save.
func helperStatus(streaming bool, savedID int64, savedAt time.Time) string {
	saved := ""
	if savedID > 0 {
		saved = fmt.Sprintf(`, "last_stored": {"id": %d, "kind": "checkpoint", "reason": "periodic", "taken_at": %q}`, savedID, savedAt.UTC().Format(time.RFC3339Nano))
	}
	return fmt.Sprintf(`{"phase": "running", "job_state": "RUNNING", "streaming": %t, "taskmanagers": 1, "taskmanagers_wanted": 1%s}`, streaming, saved)
}

// R25–R34: a custom cluster with a streaming slot size shrinks once its first copy is done and a
// position was saved after streaming began (the controller, audited), restarting with the
// streaming shape, and grows back before copying again.
func TestFlinkClusterShrinksAfterItsCopy(t *testing.T) {
	e := newPipelineEnv(t)
	enableFlink(t, e.admin)
	lake := reachableLake(t, e)
	e.driver.Cap = map[string]any{"cpu_allocatable": 64, "memory_allocatable": 256 << 30, "node_cpu_max": 4, "node_memory_max": 8 << 30}
	base := "/api/v1/projects/" + e.pid + "/pipelines"
	cluster := map[string]any{"parallelism": 6, "slot": map[string]any{"cpu": "1", "memory": "2Gi"}, "streaming_slot": map[string]any{"cpu": "0.5", "memory": "512Mi"},
		"job_manager": map[string]any{"cpu": "1", "memory": "1Gi"}}
	p := e.admin.post(base, map[string]any{"name": "lake-shrink", "source_id": e.ids.Source, "sink_id": lake, "runtime_id": e.ids.Runtime, "engine": "flink-cdc",
		"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}, "policies": map[string]any{"resources": "custom"},
			"flink_cdc": map[string]any{"cluster": cluster}}}).expect(201)
	u := base + "/" + p.Str("id")
	e.admin.post(u+"/deploy", map[string]any{"force_reason": "fake source in tests"}).expect(200)
	shape := func() map[string]any { return e.admin.get(u).At("artifacts", "flink_shape").(map[string]any) }
	tms := func() int {
		for _, w := range e.driver.Workloads {
			if w.Flink != nil {
				return w.Flink.TaskManagers
			}
		}
		return -1
	}
	// The copy shape: 6 slots of 1 CPU and 2 GiB on 4-CPU, 8-GiB nodes → 2 TaskManagers.
	e.driver.FlinkStatus = helperStatus(false, 0, time.Time{})
	e.tick()
	if s := shape(); s["phase"] != "copy" || tms() != 2 {
		t.Fatal(s, tms())
	}
	// Still copying: nothing. Streaming: noted, and a save from before doesn't count.
	e.tick()
	e.driver.FlinkStatus = helperStatus(true, 7, time.Now().Add(-time.Hour))
	e.tick()
	if s := shape(); s["phase"] != "copy" || s["streaming_since"] == nil {
		t.Fatal(s)
	}
	e.tick()
	if s := shape(); s["phase"] != "copy" {
		t.Fatal("shrank without a save after streaming began", s)
	}
	// A save after streaming began: it shrinks, once, audited as the controller.
	e.driver.FlinkStatus = helperStatus(true, 8, time.Now().Add(time.Second))
	e.tick()
	if s := shape(); s["phase"] != "streaming" || s["copy_done_save"] != float64(8) {
		t.Fatal(s)
	}
	e.tick()
	if tms() != 1 {
		t.Fatal("the streaming shape (6 × 0.5 CPU, 512 MiB) fits one TaskManager", tms())
	}
	var actor, summary string
	var n int
	if err := testdb.Pool(t).QueryRow(context.Background(), `SELECT count(*), max(actor_label), max(summary) FROM events WHERE kind = 'pipeline.resized' AND resource_id = $1`,
		p.Str("id")).Scan(&n, &actor, &summary); err != nil || n != 1 || actor != "controller" || !strings.Contains(summary, "1 TaskManager × 6 slots") {
		t.Fatal(n, actor, summary, err)
	}
	// Restoring a position saved during the copy grows it back first (R28).
	short := shortOf(p.Str("id"))
	ids := savePositions(t, short, 2)
	exec(t, `UPDATE pipelines SET artifacts = jsonb_set(artifacts, '{flink_shape,copy_done_save}', '`+fmt.Sprint(ids[1])+`') WHERE id = '`+p.Str("id")+`'`)
	e.admin.post(u+"/pause", nil).expect(200)
	e.driver.Phase = "stopped"
	e.admin.post(u+fmt.Sprintf("/positions/%d/restore", ids[0]), map[string]any{"confirm": "lake-shrink"}).expect(200)
	if s := shape(); s["phase"] != "copy" || s["streaming_since"] != nil {
		t.Fatal(s)
	}
	// Start over: changes from now copy nothing and keep the streaming shape; copying every table
	// again grows it back (R27).
	setStreaming := func() {
		exec(t, `UPDATE pipelines SET artifacts = jsonb_set(artifacts, '{flink_shape,phase}', '"streaming"') WHERE id = '`+p.Str("id")+`'`)
	}
	setStreaming()
	e.admin.post(u+"/offsets/reset", map[string]any{"mode": "skip_to_now", "confirm": "lake-shrink"}).expect(200)
	if s := shape(); s["phase"] != "streaming" {
		t.Fatal(s)
	}
	e.admin.post(u+"/offsets/reset", map[string]any{"mode": "resnapshot", "confirm": "lake-shrink"}).expect(200)
	if s := shape(); s["phase"] != "copy" {
		t.Fatal(s)
	}
	// A redeploy that copies nothing new keeps a shrunk cluster shrunk; one adding tables grows it
	// back (R29).
	setStreaming()
	e.admin.post(u+"/resume", nil).expect(200)
	cur := e.admin.get(u).expect(200)
	spec := map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}, "policies": map[string]any{"resources": "custom", "latency_target_seconds": 90},
		"flink_cdc": map[string]any{"cluster": cluster}}
	e.admin.patch(u, map[string]any{"spec": spec}, ifMatch(cur.At("version"))...).expect(200)
	e.admin.post(u+"/deploy", map[string]any{"force_reason": "fake source in tests"}).expect(200)
	if s := shape(); s["phase"] != "streaming" {
		t.Fatal("a redeploy without new tables grew it back", s)
	}
	cur = e.admin.get(u).expect(200)
	spec["source"] = map[string]any{"tables": map[string]any{"include": []string{"shop.orders", "shop.refunds"}}}
	e.admin.patch(u, map[string]any{"spec": spec}, ifMatch(cur.At("version"))...).expect(200)
	e.admin.post(u+"/deploy", map[string]any{"force_reason": "fake source in tests"}).expect(200)
	if s := shape(); s["phase"] != "copy" {
		t.Fatal(s)
	}
}

// R31: a streaming shape that fails before it saves goes back to the copy shape; after three
// such failures BeziSync stops shrinking. R32: the "suggest" policy only offers Shrink now.
func TestFlinkShrinkFailsBackAndSuggest(t *testing.T) {
	e := newPipelineEnv(t)
	enableFlink(t, e.admin)
	lake := reachableLake(t, e)
	e.driver.Cap = map[string]any{"cpu_allocatable": 64, "memory_allocatable": 256 << 30, "node_cpu_max": 8, "node_memory_max": 32 << 30}
	e.admin.put("/api/v1/projects/"+e.pid+"/policies", map[string]any{"shrink_after_copy": "suggest"}).expect(200)
	if got := e.admin.get("/api/v1/projects/" + e.pid + "/policies").expect(200); got.Str("shrink_after_copy") != "suggest" {
		t.Fatal(string(got.Raw))
	}
	base := "/api/v1/projects/" + e.pid + "/pipelines"
	p := e.admin.post(base, map[string]any{"name": "lake-suggest", "source_id": e.ids.Source, "sink_id": lake, "runtime_id": e.ids.Runtime, "engine": "flink-cdc",
		"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}, "policies": map[string]any{"resources": "custom"},
			"flink_cdc": map[string]any{"cluster": map[string]any{"parallelism": 4, "slot": map[string]any{"cpu": "1", "memory": "2Gi"},
				"streaming_slot": map[string]any{"cpu": "0.5", "memory": "1Gi"}, "job_manager": map[string]any{"cpu": "1", "memory": "1Gi"}}}}}).expect(201)
	u := base + "/" + p.Str("id")
	e.admin.post(u+"/deploy", map[string]any{"force_reason": "fake source in tests"}).expect(200)
	shape := func() map[string]any { return e.admin.get(u).At("artifacts", "flink_shape").(map[string]any) }
	if r := e.admin.post(u+"/cluster:shrink", nil).expect(409); r.Code() != "not_ready" {
		t.Fatal(string(r.Raw))
	}
	e.driver.FlinkStatus = helperStatus(true, 3, time.Now().Add(-time.Minute))
	e.tick()
	e.driver.FlinkStatus = helperStatus(true, 4, time.Now().Add(time.Second))
	e.tick()
	if s := shape(); s["phase"] != "copy" || s["shrink_ready"] != true {
		t.Fatal("suggest never shrinks by itself", s)
	}
	e.admin.post(u+"/cluster:shrink", nil).expect(200)
	if s := shape(); s["phase"] != "streaming" {
		t.Fatal(s)
	}
	// The streaming shape fails without saving: back, three times, then no more.
	for attempt := 1; attempt <= 3; attempt++ {
		e.driver.FlinkStatus = `{"phase": "failed", "error": "TaskManager OOMKilled", "error_code": "job_failed"}`
		for i := 0; i < 3 && shape()["phase"] == "streaming"; i++ {
			e.tick()
		}
		s := shape()
		if s["phase"] != "copy" || s["shrink_failures"] != float64(attempt) {
			t.Fatal(attempt, s)
		}
		if attempt < 3 {
			// Copies, streams, saves, and is shrunk again (Shrink now).
			e.driver.FlinkStatus = helperStatus(true, int64(10*attempt), time.Now().Add(-time.Minute))
			e.tick()
			e.driver.FlinkStatus = helperStatus(true, int64(10*attempt+1), time.Now().Add(time.Second))
			e.tick()
			e.admin.post(u+"/cluster:shrink", nil).expect(200)
		}
	}
	if s := shape(); s["shrink_off"] != true {
		t.Fatal(shape())
	}
	e.driver.FlinkStatus = helperStatus(true, 40, time.Now().Add(time.Second))
	e.tick()
	e.tick()
	if s := shape(); s["phase"] != "copy" || s["shrink_ready"] == true {
		t.Fatal("stopped trying", s)
	}
	var n int
	_ = testdb.Pool(t).QueryRow(context.Background(), `SELECT count(*) FROM events WHERE kind = 'pipeline.resize_failed' AND resource_id = $1`, p.Str("id")).Scan(&n)
	if n != 3 {
		t.Fatal(n)
	}
}
