package api_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/daun-gatal/bezisync/backend/internal/controller"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

// kafkaCluster starts an in-memory Kafka cluster and returns one bootstrap address.
func kafkaCluster(t *testing.T) string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()[0]
}

// shop creates sample tables readable by a canary role and returns the role.
func shop(t *testing.T) string {
	t.Helper()
	role := canaryRole(t)
	exec(t, `CREATE SCHEMA IF NOT EXISTS shop`)
	exec(t, `CREATE TABLE IF NOT EXISTS shop.orders (id serial primary key, total int)`)
	exec(t, `CREATE TABLE IF NOT EXISTS shop.refunds (id serial primary key, total int)`)
	exec(t, `GRANT USAGE ON SCHEMA shop TO `+role)
	exec(t, `GRANT SELECT ON ALL TABLES IN SCHEMA shop TO `+role)
	// Registered after canaryRole's cleanup, so it runs first (LIFO).
	t.Cleanup(func() {
		exec(t, `DROP SCHEMA IF EXISTS shop CASCADE`)
		exec(t, `DROP SCHEMA IF EXISTS bezisync CASCADE`)
		exec(t, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'bz_%' AND NOT active`)
		exec(t, `DROP PUBLICATION IF EXISTS dummy`)
	})
	return role
}

type ids struct{ Source, Sink, Runtime string }

func (i ids) body() map[string]any {
	return map[string]any{"source_id": i.Source, "sink_id": i.Sink, "runtime_id": i.Runtime}
}

func setupProject(t *testing.T, admin *client, projectID, role, bootstrap string) ids {
	t.Helper()
	base := "/api/v1/projects/" + projectID
	target, _ := pgTarget(t)
	target["user"] = role
	src := admin.post(base+"/connections", map[string]any{"kind": "source", "type": "postgresql", "name": "shop-pg", "config": target,
		"secrets": map[string]any{"password": canary}}).expect(201)
	sink := admin.post(base+"/connections", map[string]any{"kind": "sink", "type": "kafka", "name": "events",
		"config": map[string]any{"bootstrap_servers": bootstrap}}).expect(201)
	rt := admin.post(base+"/runtimes", map[string]any{"name": "k8s", "kind": "kubernetes", "config": map[string]any{"namespace": "cdc"}}).expect(201)
	if rt.At("last_test", "ok") != true {
		t.Fatal(string(rt.Raw))
	}
	return ids{src.Str("id"), sink.Str("id"), rt.Str("id")}
}

func createPipeline(t *testing.T, admin *client, projectID string, i ids, tables ...string) *resp {
	t.Helper()
	body := i.body()
	body["name"], body["owner"] = "orders-cdc", "payments"
	body["spec"] = map[string]any{"source": map[string]any{"tables": map[string]any{"include": tables}}}
	return admin.post("/api/v1/projects/"+projectID+"/pipelines", body).expect(201)
}

type pipelineEnv struct {
	srv    *httptest.Server
	admin  *client
	pid    string
	driver *fakeDriver
	ids    ids
	ctrl   *controller.Controller
}

func newPipelineEnv(t *testing.T) *pipelineEnv {
	t.Helper()
	srv := server(t)
	driver := installFake(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	role := shop(t)
	return &pipelineEnv{srv: srv, admin: admin, pid: pid, driver: driver, ids: setupProject(t, admin, pid, role, kafkaCluster(t)), ctrl: controller.New()}
}

func (e *pipelineEnv) tick()    { e.ctrl.TickOnce(context.Background()) }
func (e *pipelineEnv) runJobs() { e.ctrl.RunJobs(context.Background(), 100) }

func ifMatch(v any) []string { return []string{"if-match", fmt.Sprintf(`"%v"`, v)} }

func TestFullLifecycleWithController(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	if pipe.Str("lifecycle") != "draft" || pipe.At("current_version") != float64(1) || pipe.At("deployed_version") != nil {
		t.Fatal(string(pipe.Raw))
	}
	base := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	pre := admin.post(base+"/preflight", nil).expect(200)
	if pre.At("ok") != true || !strings.Contains(pre.Str("properties"), `shop\\.orders`) || strings.Contains(pre.Str("properties"), canary) {
		t.Fatal(string(pre.Raw))
	}
	deployed := admin.post(base+"/deploy", map[string]any{}).expect(200)
	if deployed.At("deployed_version") != float64(1) || deployed.Str("desired_state") != "running" {
		t.Fatal(string(deployed.Raw))
	}
	e.tick()
	status := admin.get(base).expect(200)
	if status.Str("lifecycle") != "streaming" || status.Str("health") != "healthy" {
		t.Fatalf("%s", status.Raw)
	}
	if len(e.driver.Workloads) != 1 {
		t.Fatal(e.driver.Workloads)
	}
	heartbeat := false
	for _, a := range status.List("artifacts", "source") {
		heartbeat = heartbeat || at(t, a, "name") == "bezisync.heartbeat"
	}
	if !heartbeat {
		t.Fatal(string(status.Raw))
	}
	// Adding a table creates v2 and explains the effect.
	res := admin.patch(base, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders", "shop.refunds"}}}}},
		ifMatch(status.At("version"))...).expect(200)
	if res.At("current_version") != float64(2) || res.At("has_undeployed_changes") != true {
		t.Fatal(string(res.Raw))
	}
	pre2 := admin.post(base+"/preflight", nil).expect(200)
	if added := pre2.List("change", "tables_added"); len(added) != 1 || added[0] != "shop.refunds" {
		t.Fatal(string(pre2.Raw))
	}
	admin.post(base+"/deploy", map[string]any{}).expect(200)
	e.tick()
	hashes := map[string]bool{}
	for _, h := range e.driver.Applied {
		hashes[h] = true
	}
	if len(hashes) != 2 {
		t.Fatalf("applied %v", e.driver.Applied)
	}
	if s := admin.post(base+"/pause", nil).expect(200).Str("desired_state"); s != "paused" {
		t.Fatal(s)
	}
	e.tick()
	if l := admin.get(base).Str("lifecycle"); l != "paused" {
		t.Fatal(l)
	}
	admin.post(base+"/resume", nil).expect(200)
	e.tick()
	if l := admin.get(base).Str("lifecycle"); l != "streaming" {
		t.Fatal(l)
	}
	versions := admin.get(base + "/versions").expect(200).List()
	if len(versions) != 2 || at(t, versions[0], "number") != float64(2) || at(t, versions[0], "deployed") != true {
		t.Fatal(versions)
	}
	cfg := admin.get(base + "/config").expect(200)
	found := false
	for _, k := range cfg.List("secret_env") {
		found = found || k == "BZ_SRC_PASSWORD"
	}
	if cfg.At("version") != float64(2) || !found {
		t.Fatal(string(cfg.Raw))
	}
	e.driver.LogsText = "connecting with " + canary + "x"
	if logs := admin.get(base + "/logs").expect(200); strings.Contains(string(logs.Raw), canary) {
		t.Fatal(string(logs.Raw))
	}
	// Delete runs cleanup and removes source artifacts.
	if l := admin.del(base + "?drop_destinations=true").expect(200).Str("lifecycle"); l != "deleting" {
		t.Fatal(l)
	}
	e.runJobs()
	final := admin.get(base).expect(200)
	if final.Str("lifecycle") != "deleted" {
		t.Fatal(string(final.Raw))
	}
	outcomes := map[string]bool{}
	for _, r := range final.List("artifacts", "cleanup") {
		outcomes[fmt.Sprint(at(t, r, "kind"), "/", at(t, r, "outcome"))] = true
	}
	if !outcomes["table/removed"] || !outcomes["topic/removed"] {
		t.Fatal(final.List("artifacts", "cleanup"))
	}
	if l := admin.get("/api/v1/projects/" + e.pid + "/pipelines").expect(200).List(); len(l) != 0 {
		t.Fatal(l)
	}
	reg, err := db.Scalar[*string](context.Background(), testdb.Pool(t), `SELECT to_regclass('bezisync.signal_' || $1)::text`,
		strings.ReplaceAll(pipe.Str("id"), "-", "")[:12])
	if err != nil || reg != nil {
		t.Fatal(reg, err)
	}
}

func TestFailedPreflightBlocksDeployUnlessForcedWithReason(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.missing_table")
	base := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	res := e.admin.post(base+"/deploy", map[string]any{}).expect(409)
	if res.Code() != "preflight_failed" {
		t.Fatal(string(res.Raw))
	}
	found := false
	for _, s := range res.List("preflight", "sections") {
		for _, c := range at(t, s, "report", "checks").([]any) {
			found = found || at(t, c, "code") == "pg_tables_exist"
		}
	}
	if !found {
		t.Fatal(string(res.Raw))
	}
	e.admin.post(base+"/deploy", map[string]any{"force_reason": "short"}).expect(422)
	e.admin.post(base+"/deploy", map[string]any{"force_reason": "Table is created by the app migration at startup"}).expect(200)
	forced := false
	for _, ev := range e.admin.get("/api/v1/projects/" + e.pid + "/events?audit_only=true").List("items") {
		if at(t, ev, "kind") == "pipeline.deployed_forced" {
			skipped, _ := at(t, ev, "details", "skipped_checks").([]any)
			forced = forced || len(skipped) > 0
		}
	}
	if !forced {
		t.Fatal("no forced deploy event")
	}
}

func TestRuntimeFailureBlocksEvenForcedDeploys(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	e.driver.OK = false
	e.admin.post("/api/v1/projects/"+e.pid+"/pipelines/"+pipe.Str("id")+"/deploy", map[string]any{"force_reason": "I know what I am doing here"}).expect(409)
}

func TestCrashingConnectorIsFailedWithDiagnosis(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	base := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	e.admin.post(base+"/deploy", map[string]any{}).expect(200)
	e.driver.Phase = "restarting"
	e.driver.LogsText = "ERROR Login failed for user 'cdc'"
	exec(t, fmt.Sprintf(`UPDATE pipelines SET deployed_at = deployed_at - interval '10 minutes' WHERE id = '%s'`, pipe.Str("id")))
	e.tick()
	status := e.admin.get(base).expect(200)
	if status.Str("lifecycle") != "failed" || status.Str("health") != "down" || status.Str("status", "reasons", 0, "code") != "source_auth" {
		t.Fatal(string(status.Raw))
	}
}

// A health check that gives no answer is shown as that, with the runtime's answer, while the
// metrics keep arriving (it used to read "no metrics").
func TestUnansweredHealthCheckSaysWhy(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	base := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	e.admin.post(base+"/deploy", map[string]any{}).expect(200)
	e.driver.Health = [2]any{404, "Not Found"}
	exec(t, fmt.Sprintf(`UPDATE pipelines SET deployed_at = deployed_at - interval '10 minutes' WHERE id = '%s'`, pipe.Str("id")))
	e.tick()
	status := e.admin.get(base).expect(200)
	if status.Str("lifecycle") != "streaming" || status.Str("health") != "unknown" || status.Str("status", "summary") != "Running — health unknown." ||
		status.Str("status", "reasons", 0, "code") != "no_health" ||
		status.Str("status", "reasons", 0, "message") != "Running, but its health check gave no answer: Debezium Server answered HTTP 404 on port 8080." {
		t.Fatal(string(status.Raw))
	}
}

// A finished one-table copy reads "1 table, none remaining": Debezium also counts the signal
// table, which it never copies.
func TestSnapshotCountsLeaveOutTheSignalTable(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	base := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	e.admin.post(base+"/deploy", map[string]any{}).expect(200)
	e.driver.Metrics = healthyMetrics + `debezium_metrics_SnapshotCompleted{context="snapshot",name="x",plugin="postgres"} 1.0
debezium_metrics_TotalTableCount{context="snapshot",name="x",plugin="postgres"} 2.0
debezium_metrics_RemainingTableCount{context="snapshot",name="x",plugin="postgres"} 1.0
`
	e.tick()
	status := e.admin.get(base).expect(200)
	if status.At("status", "metrics", "snapshot_tables_total") != 1.0 || status.At("status", "metrics", "snapshot_tables_remaining") != 0.0 {
		t.Fatal(string(status.Raw))
	}
}

func TestPipelinePermissionsAndValidation(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	base := "/api/v1/projects/" + e.pid + "/pipelines"
	viewer := member(t, e.srv, e.admin, e.pid, "viewer")
	operator := member(t, e.srv, e.admin, e.pid, "operator")
	viewer.post(base+"/"+pipe.Str("id")+"/deploy", map[string]any{}).expect(403)
	operator.post(base+"/"+pipe.Str("id")+"/deploy", map[string]any{}).expect(403)
	if c := operator.post(base+"/"+pipe.Str("id")+"/pause", nil).Code(); c != "not_deployed" {
		t.Fatal(c)
	}
	viewer.get(base + "/" + pipe.Str("id")).expect(200)
	dup := e.ids.body()
	dup["name"], dup["spec"] = "orders-cdc", map[string]any{}
	e.admin.post(base, dup).expect(409)
	bad := e.ids.body()
	bad["name"], bad["spec"] = "x", map[string]any{"sink": map[string]any{"naming": "static"}}
	e.admin.post(base, bad).expect(422)
	e.admin.patch(base+"/"+pipe.Str("id"), map[string]any{"owner": "x"}, ifMatch(999)...).expect(412)
	if c := e.admin.del("/api/v1/projects/" + e.pid + "/connections/" + e.ids.Source).Code(); c != "connection_in_use" {
		t.Fatal(c)
	}
	labels := e.ids.body()
	labels["name"], labels["labels"] = "y", map[string]any{"Bad Key": "v"}
	e.admin.post(base, labels).expect(422)
	// Editing follows the same rule when the labels change (Known issue 8)...
	u := base + "/" + pipe.Str("id")
	cur := e.admin.get(u).expect(200)
	if c := e.admin.patch(u, map[string]any{"labels": map[string]any{"Bad Key": "v"}}, ifMatch(cur.At("version"))...).expect(422); !strings.Contains(string(c.Raw), "up to 20 labels") {
		t.Fatal(string(c.Raw))
	}
	many := map[string]any{}
	for i := range 21 {
		many[fmt.Sprintf("k%d", i)] = "v"
	}
	e.admin.patch(u, map[string]any{"labels": many}, ifMatch(cur.At("version"))...).expect(422)
	e.admin.patch(u, map[string]any{"labels": map[string]any{"team": strings.Repeat("x", 64)}}, ifMatch(cur.At("version"))...).expect(422)
	e.admin.patch(u, map[string]any{"labels": map[string]any{"team": strings.Repeat("x", 63)}}, ifMatch(cur.At("version"))...).expect(200)
	// ...but labels an older pipeline already holds still save unchanged.
	exec(t, `UPDATE pipelines SET labels = '{"Old Key": "v"}' WHERE id = '`+pipe.Str("id")+`'`)
	cur = e.admin.get(u).expect(200)
	e.admin.patch(u, map[string]any{"owner": "ops", "labels": map[string]any{"Old Key": "v"}}, ifMatch(cur.At("version"))...).expect(200)
}

func TestRuntimeAPI(t *testing.T) {
	srv := server(t)
	installFake(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	base := "/api/v1/projects/" + pid
	kinds := map[any]bool{}
	for _, k := range admin.get(base + "/runtime-kinds").expect(200).List() {
		kinds[at(t, k, "kind")] = true
	}
	if len(kinds) != 2 || !kinds["kubernetes"] || !kinds["docker"] {
		t.Fatal(kinds)
	}
	rt := admin.post(base+"/runtimes", map[string]any{"name": "docker-1", "kind": "docker", "config": map[string]any{},
		"secrets": map[string]any{"key_pem": "PRIVATE-KEY-CANARY"}}).expect(201)
	if l := rt.List("secret_fields_set"); len(l) != 1 || l[0] != "key_pem" || strings.Contains(string(rt.Raw), "PRIVATE-KEY-CANARY") {
		t.Fatal(string(rt.Raw))
	}
	if admin.post(base+"/runtimes/"+rt.Str("id")+"/test", nil).At("ok") != true {
		t.Fatal("test failed")
	}
	// The image download timeout: a choice, 30 minutes unless set.
	if v := rt.At("config", "image_pull_timeout"); v != "30m" {
		t.Fatalf("default image download timeout %v", v)
	}
	admin.post(base+"/runtimes", map[string]any{"name": "slow", "kind": "docker", "config": map[string]any{"image_pull_timeout": "5h"}}).expect(422)
	if v := admin.post(base+"/runtimes", map[string]any{"name": "slow", "kind": "docker", "config": map[string]any{"image_pull_timeout": "2h"}}).
		expect(201).At("config", "image_pull_timeout"); v != "2h" {
		t.Fatalf("image download timeout %v", v)
	}
	admin.post(base+"/runtimes", map[string]any{"name": "bad", "kind": "kubernetes", "config": map[string]any{"namespace": "Bad_NS"}}).expect(422)
	editor := member(t, srv, admin, pid, "editor")
	editor.post(base+"/runtimes", map[string]any{"name": "x", "kind": "docker", "config": map[string]any{}}).expect(403)
	// Renaming, cordoning and the version check.
	u := base + "/runtimes/" + rt.Str("id")
	admin.patch(u, map[string]any{"enabled": false}, ifMatch(9)...).expect(412)
	if r := admin.patch(u, map[string]any{"enabled": false}, ifMatch(1)...).expect(200); r.At("enabled") != false || r.At("version") != float64(2) {
		t.Fatal(string(r.Raw))
	}
	if n := admin.patch(u, map[string]any{"name": "docker-2"}, ifMatch(2)...).expect(200).Str("name"); n != "docker-2" {
		t.Fatal(n)
	}
	admin.del(u).expect(204)
}

func TestWizardDraftWithoutSinkThenCompletedAndRenamed(t *testing.T) {
	e := newPipelineEnv(t)
	base := "/api/v1/projects/" + e.pid + "/pipelines"
	draft := e.admin.post(base, map[string]any{"name": "draft-x", "source_id": e.ids.Source}).expect(201)
	if draft.At("sink") != nil || draft.At("runtime") != nil {
		t.Fatal(string(draft.Raw))
	}
	u := base + "/" + draft.Str("id")
	if c := e.admin.post(u+"/preflight", nil).Code(); c != "incomplete_draft" {
		t.Fatal(c)
	}
	res := e.admin.patch(u, map[string]any{"name": "orders-to-kafka", "sink_id": e.ids.Sink, "runtime_id": e.ids.Runtime,
		"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}}}, ifMatch(draft.At("version"))...).expect(200)
	if res.Str("name") != "orders-to-kafka" || res.Str("sink", "id") != e.ids.Sink || res.At("current_version") != float64(1) {
		t.Fatal(string(res.Raw))
	}
	e.admin.post(u+"/deploy", map[string]any{}).expect(200)
	if c := e.admin.patch(u, map[string]any{"name": "renamed"}, ifMatch(e.admin.get(u).At("version"))...).Code(); c != "rename_after_deploy" {
		t.Fatal(c)
	}
}

func TestSnapshotRequestValidatesTables(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	u := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	if c := e.admin.post(u+"/snapshots", map[string]any{"tables": []string{"shop.orders"}}).Code(); c != "not_running" {
		t.Fatal(c)
	}
	e.admin.post(u+"/deploy", map[string]any{}).expect(200)
	e.admin.post(u+"/snapshots", map[string]any{"tables": []string{"shop.nope"}}).expect(422)
	e.admin.post(u+"/snapshots", map[string]any{"tables": []string{"shop.orders"}}).expect(202)
	e.admin.post(u+"/snapshots", map[string]any{"tables": []string{}}).expect(422)
}

var _ = time.Second
