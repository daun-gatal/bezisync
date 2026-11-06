package api_test

import (
	"context"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

// A custom Flink CDC cluster: saved and refused per engine, checked against the runtime's nodes
// (Unknown when it doesn't say), and compiled to the packed shape.
func TestFlinkCustomCluster(t *testing.T) {
	e := newPipelineEnv(t)
	enableFlink(t, e.admin)
	lake := icebergSink(t, e)
	base := "/api/v1/projects/" + e.pid + "/pipelines"
	cluster := map[string]any{"parallelism": 6, "slot": map[string]any{"cpu": "1", "memory": "2Gi"}, "job_manager": map[string]any{"cpu": "1", "memory": "1Gi"}}
	spec := func(policies map[string]any, flink map[string]any) map[string]any {
		s := map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}, "policies": policies}
		if flink != nil {
			s["flink_cdc"] = flink
		}
		return s
	}
	body := map[string]any{"name": "lake-custom", "source_id": e.ids.Source, "sink_id": lake, "runtime_id": e.ids.Runtime, "engine": "flink-cdc",
		"spec": spec(map[string]any{"resources": "custom", "custom_resources": map[string]any{"cpu": "1", "memory": "1Gi"}}, nil)}
	// Flink CDC is sized by its cluster, never by a Debezium container size.
	if r := e.admin.post(base, body).expect(422); r.Code() != "engine_settings" || !strings.Contains(r.Str("detail"), "flink_cdc.cluster") {
		t.Fatal(string(r.Raw))
	}
	// And a Debezium pipeline can't have a cluster.
	deb := map[string]any{"name": "deb-custom", "source_id": e.ids.Source, "sink_id": e.ids.Sink, "spec": spec(map[string]any{"resources": "custom"}, map[string]any{"cluster": cluster})}
	if r := e.admin.post(base, deb).expect(422); r.Code() != "engine_settings" {
		t.Fatal(string(r.Raw))
	}
	body["spec"] = spec(map[string]any{"resources": "custom"}, map[string]any{"cluster": cluster})
	p := e.admin.post(base, body).expect(201)
	if p.At("spec", "flink_cdc", "cluster", "shape") != "auto" || p.At("spec", "flink_cdc", "cluster", "parallelism") != float64(6) {
		t.Fatal(string(p.Raw))
	}
	u := base + "/" + p.Str("id")
	fit := func() map[string]any {
		for _, s := range e.admin.post(u+"/preflight", nil).expect(200).List("sections") {
			for _, c := range at(t, s, "report", "checks").([]any) {
				if at(t, c, "code") == "flink_cluster_fit" {
					return c.(map[string]any)
				}
			}
		}
		return nil
	}
	// The fake runtime doesn't report its nodes: unknown, never passed.
	if c := fit(); c == nil || c["status"] != "unknown" || c["blocking"] != false {
		t.Fatal(c)
	}
	e.driver.Cap = map[string]any{"cpu_allocatable": 16, "memory_allocatable": 32 << 30, "node_cpu_max": 4, "node_memory_max": 8 << 30}
	if c := fit(); c == nil || c["status"] != "pass" || !strings.Contains(c["message"].(string), "2 TaskManagers × 3 slots") {
		t.Fatal(c)
	}
	cfg := e.admin.get(u + "/config").expect(200).Str("properties")
	if !strings.Contains(cfg, `parallelism: "6"`) {
		t.Fatal(cfg)
	}
	// One slot bigger than any node blocks the deploy.
	cur := e.admin.get(u).expect(200)
	big := map[string]any{"parallelism": 2, "slot": map[string]any{"cpu": "4", "memory": "2Gi"}, "job_manager": map[string]any{"cpu": "1", "memory": "1Gi"}}
	e.driver.Cap = map[string]any{"cpu_allocatable": 16, "memory_allocatable": 32 << 30, "node_cpu_max": 2, "node_memory_max": 8 << 30}
	e.admin.patch(u, map[string]any{"spec": spec(map[string]any{"resources": "custom"}, map[string]any{"cluster": big})}, ifMatch(cur.At("version"))...).expect(200)
	if c := fit(); c == nil || c["status"] != "fail" || c["blocking"] != true {
		t.Fatal(c)
	}
	if r := e.admin.post(u+"/deploy", map[string]any{"force_reason": "we know what we are doing"}).expect(409); !strings.Contains(string(r.Raw), "cannot be skipped") {
		t.Fatal(string(r.Raw))
	}
	// A manual shape saves as written.
	cur = e.admin.get(u).expect(200)
	manual := map[string]any{"parallelism": 4, "slot": map[string]any{"cpu": "1", "memory": "1Gi"}, "job_manager": map[string]any{"cpu": "1", "memory": "1Gi"},
		"shape": "manual", "task_managers": map[string]any{"count": 2, "slots": 2, "cpu": "2", "memory": "3Gi"}}
	got := e.admin.patch(u, map[string]any{"spec": spec(map[string]any{"resources": "custom"}, map[string]any{"cluster": manual})}, ifMatch(cur.At("version"))...).expect(200)
	if got.At("spec", "flink_cdc", "cluster", "task_managers", "count") != float64(2) {
		t.Fatal(string(got.Raw))
	}
}

// Preflight measures the runtime's capacity when it runs (and keeps it): no measurement is
// Unknown, and a namespace quota the pipeline doesn't fit blocks, even forced.
func TestCapacityMeasuredAtPreflight(t *testing.T) {
	e := newPipelineEnv(t)
	p := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	u := "/api/v1/projects/" + e.pid + "/pipelines/" + p.Str("id")
	capacity := func() map[string]any {
		for _, s := range e.admin.post(u+"/preflight", nil).expect(200).List("sections") {
			for _, c := range at(t, s, "report", "checks").([]any) {
				if at(t, c, "code") == "runtime_capacity" {
					if at(t, s, "key") != "runtime" {
						t.Fatal("not in the runtime section")
					}
					return c.(map[string]any)
				}
			}
		}
		return nil
	}
	e.driver.Cap = map[string]any{}
	if c := capacity(); c == nil || c["status"] != "unknown" || c["blocking"] != false {
		t.Fatal(c)
	}
	e.driver.Cap = map[string]any{"cpu_allocatable": 8, "memory_allocatable": 16 << 30, "quota": true, "quota_cpu_free": 0.25, "quota_memory_free": 8 << 30}
	if c := capacity(); c == nil || c["status"] != "fail" || c["blocking"] != true || !strings.Contains(c["message"].(string), "ResourceQuota leaves 0.25 CPU") {
		t.Fatal(c)
	}
	if r := e.admin.post(u+"/deploy", map[string]any{"force_reason": "we know what we are doing"}).expect(409); !strings.Contains(string(r.Raw), "cannot be skipped") {
		t.Fatal(string(r.Raw))
	}
	// What preflight measured is the runtime's capacity now.
	var stored float64
	if err := testdb.Pool(t).QueryRow(context.Background(), `SELECT (capacity->>'quota_cpu_free')::float FROM runtimes WHERE id = $1`, e.ids.Runtime).Scan(&stored); err != nil || stored != 0.25 {
		t.Fatal(stored, err)
	}
	e.driver.Cap = map[string]any{"cpu_allocatable": 8, "memory_allocatable": 16 << 30, "quota": false}
	if c := capacity(); c == nil || c["status"] != "pass" || !strings.Contains(c["message"].(string), "no ResourceQuota") {
		t.Fatal(c)
	}
}
