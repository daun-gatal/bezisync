package api_test

import (
	"strings"
	"testing"
)

// icebergSink adds an Iceberg destination (both engines can write it).
func icebergSink(t *testing.T, e *pipelineEnv) string {
	t.Helper()
	return e.admin.post("/api/v1/projects/"+e.pid+"/connections", map[string]any{"kind": "sink", "type": "iceberg", "name": "lake",
		"config": map[string]any{"catalog_type": "rest", "uri": "http://127.0.0.1:9/catalog", "warehouse": "s3://lake/warehouse"}}).expect(201).Str("id")
}

// enableFlink registers a Flink CDC image, which turns the engine on.
func enableFlink(t *testing.T, admin *client) {
	t.Helper()
	admin.post("/api/v1/debezium-catalog", map[string]any{"image": "registry.example/bezisync-flink-cdc:3.7.0-2.2.1", "engine": "flink-cdc"}).expect(201)
}

func TestFlinkCDCOffUntilAnImageIsRegistered(t *testing.T) {
	e := newPipelineEnv(t)
	lake := icebergSink(t, e)
	engines := e.admin.get("/api/v1/engines").expect(200)
	l := engines.List()
	if len(l) != 2 || at(t, l[0], "id") != "debezium" || at(t, l[0], "available") != true || at(t, l[1], "id") != "flink-cdc" ||
		at(t, l[1], "available") != false || !strings.Contains(at(t, l[1], "reason").(string), "Engine versions") || at(t, l[1], "default_image") != nil {
		t.Fatal(string(engines.Raw))
	}
	// What each engine runs, and Flink CDC's settings and sizes, for the wizard.
	if at(t, l[0], "sources") != nil || at(t, l[0], "except_sinks").([]any)[0] != "starrocks" || len(at(t, l[1], "sources").([]any)) != 3 ||
		len(at(t, l[1], "sinks").([]any)) != 4 || len(at(t, l[1], "settings").([]any)) == 0 || at(t, l[1], "sizes", "medium", "parallelism") != float64(4) ||
		at(t, l[0], "settings") != nil {
		t.Fatal(string(engines.Raw))
	}
	if c := e.admin.get("/api/v1/debezium-catalog?engine=flink-cdc").expect(200); c.At("default_image") != nil || len(c.List("entries")) != 0 {
		t.Fatal(string(c.Raw))
	}
	body := map[string]any{"name": "lake", "source_id": e.ids.Source, "sink_id": lake, "engine": "flink-cdc"}
	r := e.admin.post("/api/v1/projects/"+e.pid+"/pipelines", body).expect(422)
	if r.Code() != "engine_unavailable" || !strings.Contains(r.Str("detail"), "Engine versions") {
		t.Fatal(string(r.Raw))
	}
	// Iceberg falls back to Debezium while Flink CDC is off.
	delete(body, "engine")
	if p := e.admin.post("/api/v1/projects/"+e.pid+"/pipelines", body).expect(201); p.Str("engine") != "debezium" {
		t.Fatal(string(p.Raw))
	}
	enableFlink(t, e.admin)
	on := e.admin.get("/api/v1/engines").expect(200).List()
	if at(t, on[1], "available") != true || at(t, on[1], "reason") != nil || at(t, on[1], "default_image") != "registry.example/bezisync-flink-cdc:3.7.0-2.2.1" {
		t.Fatal(on)
	}
	body["name"], body["engine"] = "lake2", "flink-cdc"
	e.admin.post("/api/v1/projects/"+e.pid+"/pipelines", body).expect(201)
}

func TestPipelineEngineDefaultsAndCaps(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	if pipe.Str("engine") != "debezium" || pipe.At("caps", "edit_position") != true || pipe.At("caps", "saved_positions") != false ||
		pipe.Str("caps", "why", "saved_positions") == "" {
		t.Fatal(string(pipe.Raw))
	}
	// A draft with no destination yet starts on Debezium too.
	body := map[string]any{"name": "no-sink", "source_id": e.ids.Source}
	if p := e.admin.post("/api/v1/projects/"+e.pid+"/pipelines", body).expect(201); p.Str("engine") != "debezium" {
		t.Fatal(string(p.Raw))
	}
}

func TestPipelineEngineChoice(t *testing.T) {
	e := newPipelineEnv(t)
	enableFlink(t, e.admin)
	lake := icebergSink(t, e)
	base := "/api/v1/projects/" + e.pid + "/pipelines"
	body := map[string]any{"name": "lake-flink", "source_id": e.ids.Source, "sink_id": lake, "runtime_id": e.ids.Runtime, "engine": "flink-cdc",
		"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
			"flink_cdc": map[string]any{"commit_seconds": 30}}}
	flink := e.admin.post(base, body).expect(201)
	caps := flink.At("caps").(map[string]any)
	if flink.Str("engine") != "flink-cdc" || caps["saved_positions"] != true || caps["edit_position"] != false || caps["start_from_time"] != false ||
		!strings.Contains(flink.Str("caps", "why", "start_from_time"), "PostgreSQL") || flink.At("spec", "flink_cdc", "commit_seconds") != float64(30) {
		t.Fatal(string(flink.Raw))
	}

	// Flink CDC can't write Kafka; asking for it is refused with the pair named.
	bad := map[string]any{"name": "bad", "source_id": e.ids.Source, "sink_id": e.ids.Sink, "engine": "flink-cdc"}
	r := e.admin.post(base, bad).expect(422)
	if r.Code() != "engine_unsupported" || !strings.Contains(r.Str("detail"), "PostgreSQL") || !strings.Contains(r.Str("detail"), "Kafka") {
		t.Fatal(string(r.Raw))
	}
	if r := e.admin.post(base, map[string]any{"name": "bad2", "source_id": e.ids.Source, "engine": "spark"}).expect(422); !strings.Contains(string(r.Raw), "engine") {
		t.Fatal(string(r.Raw))
	}
	// Flink CDC settings on a Debezium pipeline are refused, and the reverse.
	deb := map[string]any{"name": "deb", "source_id": e.ids.Source, "sink_id": lake, "spec": map[string]any{"flink_cdc": map[string]any{"new_columns": "send"}}}
	if r := e.admin.post(base, deb).expect(422); r.Code() != "engine_settings" {
		t.Fatal(string(r.Raw))
	}
	img := "quay.io/debezium/server:3.7.0.Final"
	fl := map[string]any{"name": "fl", "source_id": e.ids.Source, "sink_id": lake, "engine": "flink-cdc", "spec": map[string]any{"debezium_image": img}}
	if r := e.admin.post(base, fl).expect(422); r.Code() != "engine_settings" {
		t.Fatal(string(r.Raw))
	}

	// Moving a Flink CDC draft to a destination Flink CDC can't write moves it to Debezium,
	// unless its spec holds Flink CDC settings.
	url := base + "/" + flink.Str("id")
	if r := e.admin.patch(url, map[string]any{"sink_id": e.ids.Sink}, ifMatch(flink.At("version"))...).expect(422); r.Code() != "engine_settings" {
		t.Fatal(string(r.Raw))
	}
	cleared := e.admin.patch(url, map[string]any{"sink_id": e.ids.Sink, "spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}}},
		ifMatch(flink.At("version"))...).expect(200)
	if cleared.Str("engine") != "debezium" {
		t.Fatal(string(cleared.Raw))
	}
	// Back to Iceberg: it stays on Debezium unless Flink CDC is asked for.
	back := e.admin.patch(url, map[string]any{"sink_id": lake}, ifMatch(cleared.At("version"))...).expect(200)
	if back.Str("engine") != "debezium" {
		t.Fatal(string(back.Raw))
	}
	asked := e.admin.patch(url, map[string]any{"engine": "flink-cdc"}, ifMatch(back.At("version"))...).expect(200)
	if asked.Str("engine") != "flink-cdc" {
		t.Fatal(string(asked.Raw))
	}
}

func TestPipelineEngineFixedAfterDeploy(t *testing.T) {
	e := newPipelineEnv(t)
	enableFlink(t, e.admin)
	lake := icebergSink(t, e)
	pipe := createPipeline(t, e.admin, e.pid, ids{Source: e.ids.Source, Sink: lake, Runtime: e.ids.Runtime}, "shop.orders")
	exec(t, `UPDATE pipelines SET deployed_version_id = current_version_id WHERE id = '`+pipe.Str("id")+`'`)
	url := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	r := e.admin.patch(url, map[string]any{"engine": "flink-cdc"}, ifMatch(pipe.At("version"))...).expect(409)
	if r.Code() != "engine_after_deploy" {
		t.Fatal(string(r.Raw))
	}
	// Asking for the engine it already has is fine.
	e.admin.patch(url, map[string]any{"engine": "debezium"}, ifMatch(pipe.At("version"))...).expect(200)
}

func TestAllowedEnginesPolicy(t *testing.T) {
	e := newPipelineEnv(t)
	enableFlink(t, e.admin)
	lake := icebergSink(t, e)
	pol := e.admin.put("/api/v1/projects/"+e.pid+"/policies", map[string]any{"allowed_engines": []string{"debezium"}}).expect(200)
	if l := pol.List("allowed_engines"); len(l) != 1 || l[0] != "debezium" {
		t.Fatal(string(pol.Raw))
	}
	body := map[string]any{"name": "lake", "source_id": e.ids.Source, "sink_id": lake, "engine": "flink-cdc"}
	r := e.admin.post("/api/v1/projects/"+e.pid+"/pipelines", body).expect(422)
	if r.Code() != "policy_violation" || !strings.Contains(r.Str("detail"), "Flink CDC") {
		t.Fatal(string(r.Raw))
	}
	if r := e.admin.put("/api/v1/projects/"+e.pid+"/policies", map[string]any{"allowed_engines": []string{"spark"}}).expect(422); !strings.Contains(string(r.Raw), "allowed_engines") {
		t.Fatal(string(r.Raw))
	}
	// Unset, it is not returned (stored policies stay byte-identical).
	if p := e.admin.put("/api/v1/projects/"+e.pid+"/policies", map[string]any{}).expect(200); p.At("allowed_engines") != nil || strings.Contains(string(p.Raw), "allowed_engines") {
		t.Fatal(string(p.Raw))
	}
}

func TestCatalogPerEngine(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	deb := admin.get("/api/v1/debezium-catalog").expect(200)
	if deb.Str("engine") != "debezium" || !strings.HasPrefix(deb.Str("default_image"), "quay.io/debezium/server:") {
		t.Fatal(string(deb.Raw))
	}
	fl := admin.get("/api/v1/debezium-catalog?engine=flink-cdc").expect(200)
	if fl.Str("engine") != "flink-cdc" || fl.At("default_image") != nil {
		t.Fatal(string(fl.Raw))
	}
	admin.get("/api/v1/debezium-catalog?engine=spark").expect(422)
	added := admin.post("/api/v1/debezium-catalog", map[string]any{"image": "registry.example/flink-cdc:custom", "engine": "flink-cdc", "is_default": true}).expect(201)
	if added.Str("engine") != "flink-cdc" || added.Str("default_image") != "registry.example/flink-cdc:custom" {
		t.Fatal(string(added.Raw))
	}
	// The Flink CDC default does not move Debezium's.
	if again := admin.get("/api/v1/debezium-catalog").expect(200); again.Str("default_image") != deb.Str("default_image") {
		t.Fatal(string(again.Raw))
	}
	for _, e := range admin.get("/api/v1/debezium-catalog").List("entries") {
		if e.(map[string]any)["image"] == "registry.example/flink-cdc:custom" {
			t.Fatal("a Flink CDC image is listed among Debezium images")
		}
	}
}

// StarRocks is written by Flink CDC only: offered, and connectable, once the engine is on. A
// PostgreSQL pipeline to it compiles to a Flink CDC pipeline whose secrets stay out of the file.
func TestStarRocksNeedsFlinkCDC(t *testing.T) {
	e := newPipelineEnv(t)
	types := func() any {
		for _, d := range e.admin.get("/api/v1/projects/" + e.pid + "/connector-types").expect(200).List() {
			if at(t, d, "type") == "starrocks" {
				return d
			}
		}
		t.Fatal("no StarRocks")
		return nil
	}
	if d := types(); at(t, d, "available") != false || !strings.Contains(at(t, d, "unavailable_reason").(string), "Engine versions") {
		t.Fatal(d)
	}
	body := map[string]any{"kind": "sink", "type": "starrocks", "name": "olap",
		"config": map[string]any{"host": "starrocks.internal", "user": "loader", "database": "lake", "replicas": 1}, "secrets": map[string]any{"password": "sr-s3cret"}}
	if r := e.admin.post("/api/v1/projects/"+e.pid+"/connections", body).expect(422); r.Code() != "engine_unavailable" {
		t.Fatal(string(r.Raw))
	}
	enableFlink(t, e.admin)
	if d := types(); at(t, d, "available") != true || at(t, d, "engines").([]any)[0] != "flink-cdc" {
		t.Fatal(d)
	}
	olap := e.admin.post("/api/v1/projects/"+e.pid+"/connections", body).expect(201).Str("id")
	// Debezium can't write it; the engine defaults to Flink CDC.
	p := e.admin.post("/api/v1/projects/"+e.pid+"/pipelines", map[string]any{"name": "orders-olap", "source_id": e.ids.Source, "sink_id": olap,
		"runtime_id": e.ids.Runtime, "spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}}}).expect(201)
	if p.Str("engine") != "flink-cdc" {
		t.Fatal(string(p.Raw))
	}
	cfg := e.admin.get("/api/v1/projects/" + e.pid + "/pipelines/" + p.Str("id") + "/config").expect(200)
	yaml := cfg.Str("properties")
	for _, want := range []string{`type: "starrocks"`, `load-url: "starrocks.internal:8030"`, "password: ${BZ_SINK_PASSWORD}", `sink-table: "lake.cdc_shop_orders"`,
		"projection: \"CASE WHEN TRUE THEN `id` END AS `id`, `total`\"" /* id is serial: its default is left out */, `replication_num: "1"`} {
		if !strings.Contains(yaml, want) {
			t.Errorf("config lacks %s:\n%s", want, yaml)
		}
	}
	if strings.Contains(string(cfg.Raw), "sr-s3cret") || cfg.At("destinations", "shop.orders") != "lake.cdc_shop_orders" {
		t.Fatal(string(cfg.Raw))
	}
}
