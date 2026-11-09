package api_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"software.sslmate.com/src/go-pkcs12"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/flinkhelper"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

// flinkPipeline creates a deployed, paused Flink CDC pipeline (Iceberg destination) whose cluster
// is gone, as after a pause.
func flinkPipeline(t *testing.T, e *pipelineEnv) (*resp, string) {
	t.Helper()
	enableFlink(t, e.admin)
	lake := icebergSink(t, e)
	body := map[string]any{"name": "lake-flink", "source_id": e.ids.Source, "sink_id": lake, "runtime_id": e.ids.Runtime, "engine": "flink-cdc",
		"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}}}
	p := e.admin.post("/api/v1/projects/"+e.pid+"/pipelines", body).expect(201)
	exec(t, `UPDATE pipelines SET deployed_version_id = current_version_id, desired_state = 'paused' WHERE id = '`+p.Str("id")+`'`)
	return p, "/api/v1/projects/" + e.pid + "/pipelines/" + p.Str("id")
}

// savePositions stores n checkpoints for a pipeline the way its helper does.
func savePositions(t *testing.T, shortID string, n int) []int64 {
	t.Helper()
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(db.NormalizeURL(config.Get().EffectiveStateDatabaseURL()))
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	store, err := flinkhelper.OpenStore(ctx, cfg, shortID, base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	dir := t.TempDir()
	var ids []int64
	for i := 1; i <= n; i++ {
		rel := filepath.Join("cp", "job", "chk-"+string(rune('0'+i)))
		if err := os.MkdirAll(filepath.Join(dir, rel), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, rel, "_metadata"), []byte(rel), 0o600); err != nil {
			t.Fatal(err)
		}
		s, err := store.Save(ctx, "checkpoint", "periodic", int64(i), dir, rel)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, s.ID)
	}
	t.Cleanup(func() { _ = services.DropSavedPositions(context.Background(), shortID) })
	return ids
}

func shortOf(id string) string { return strings.ReplaceAll(id, "-", "")[:12] }

func TestFlinkSecretsAreGeneratedOnceAndSealed(t *testing.T) {
	e := newPipelineEnv(t)
	pipe, _ := flinkPipeline(t, e)
	ctx := context.Background()
	q := testdb.Pool(t)
	p, err := db.Find[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE id = $1`, pipe.Str("id"))
	if err != nil || p == nil || p.EngineSecretsEnc != nil {
		t.Fatal(err, p)
	}
	first, err := services.FlinkSecretsOf(ctx, q, p)
	if err != nil {
		t.Fatal(err)
	}
	again, err := db.Find[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE id = $1`, pipe.Str("id"))
	if err != nil || again.EngineSecretsEnc == nil || strings.Contains(*again.EngineSecretsEnc, first.KeystorePassword) {
		t.Fatal("secrets were not sealed")
	}
	second, err := services.FlinkSecretsOf(ctx, q, again)
	if err != nil || *second != *first {
		t.Fatal("secrets changed on the second read")
	}
	if key, err := base64.StdEncoding.DecodeString(first.StateKey); err != nil || len(key) != 32 {
		t.Fatal("state key")
	}
	raw, err := base64.StdEncoding.DecodeString(first.Keystore)
	if err != nil {
		t.Fatal(err)
	}
	_, cert, err := pkcs12.Decode(raw, first.KeystorePassword)
	if err != nil || cert.Subject.CommonName != "bezisync-flink-"+p.ShortID() || cert.NotAfter.Before(time.Now().AddDate(10, 0, 0)) {
		t.Fatal(err, cert)
	}
	if _, _, err := pkcs12.Decode(raw, "wrong"); err == nil {
		t.Fatal("the keystore opened with a wrong password")
	}
}

func TestSavedPositionsListRestoreAndStartOver(t *testing.T) {
	e := newPipelineEnv(t)
	pipe, url := flinkPipeline(t, e)
	short := shortOf(pipe.Str("id"))
	// Never started: no positions, no error.
	if l := e.admin.get(url + "/positions").expect(200).List("positions"); len(l) != 0 {
		t.Fatal(l)
	}
	ids := savePositions(t, short, 3)
	l := e.admin.get(url + "/positions").expect(200).List("positions")
	if len(l) != 3 || at(t, l[0], "id") != float64(ids[2]) || at(t, l[0], "current") != true || at(t, l[0], "kind") != "checkpoint" {
		t.Fatal(l)
	}
	// Restoring needs the name typed, a paused pipeline and a stopped cluster.
	restore := url + "/positions/" + strconv.FormatInt(ids[0], 10) + "/restore"
	e.admin.post(restore, map[string]any{"confirm": "nope"}).expect(422)
	e.driver.mu.Lock()
	e.driver.Workloads[short] = runtime.Workload{ShortID: short, Running: true}
	e.driver.mu.Unlock()
	if r := e.admin.post(restore, map[string]any{"confirm": "lake-flink"}).expect(409); r.Code() != "still_running" {
		t.Fatal(string(r.Raw))
	}
	e.driver.mu.Lock()
	delete(e.driver.Workloads, short)
	e.driver.mu.Unlock()
	e.admin.post(url+"/positions/999999/restore", map[string]any{"confirm": "lake-flink"}).expect(404)
	// The copy keeps the column generation its job ran with (ADR 0013).
	exec(t, `UPDATE debezium_state.flink_state_`+short+` SET generation = 3 WHERE id = `+strconv.FormatInt(ids[0], 10))
	got := e.admin.post(restore, map[string]any{"confirm": "lake-flink"}).expect(200)
	if got.At("position", "id").(float64) <= float64(ids[2]) || !strings.Contains(got.Str("position", "reason"), "restored from #") {
		t.Fatal(string(got.Raw))
	}
	var generation int
	if err := testdb.Pool(t).QueryRow(context.Background(), `SELECT generation FROM debezium_state.flink_state_`+short+` WHERE id = $1`, int64(got.At("position", "id").(float64))).Scan(&generation); err != nil || generation != 3 {
		t.Fatal(generation, err)
	}
	read := e.admin.get(url).expect(200)
	if read.At("artifacts", "flink_restore_after") != got.At("position", "id").(float64)-1 {
		t.Fatal(read.At("artifacts"))
	}
	l = e.admin.get(url + "/positions").expect(200).List("positions")
	if at(t, l[0], "current") != true || at(t, l[1], "current") != false {
		t.Fatal("only the restored copy is current", l)
	}

	// Start over: every position is set aside and the next start copies every table again.
	reset := url + "/offsets/reset"
	r := e.admin.post(reset, map[string]any{"mode": "resnapshot", "confirm": "lake-flink"}).expect(200)
	if r.Str("mode") != "resnapshot" {
		t.Fatal(string(r.Raw))
	}
	read = e.admin.get(url).expect(200)
	if read.At("artifacts", "flink_startup", "mode") != "initial" || read.At("artifacts", "flink_restore_after") != got.At("position", "id") {
		t.Fatal(read.At("artifacts"))
	}
	for _, p := range e.admin.get(url + "/positions").expect(200).List("positions") {
		if at(t, p, "current") != false {
			t.Fatal("a position survived Start over", p)
		}
	}
	// A PostgreSQL slot can't go back in time.
	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	if r := e.admin.post(reset, map[string]any{"mode": "from_time", "from_time": past, "confirm": "lake-flink"}).expect(422); r.Code() != "engine_unsupported" ||
		!strings.Contains(r.Str("detail"), "PostgreSQL") {
		t.Fatal(string(r.Raw))
	}
	e.admin.post(reset, map[string]any{"mode": "from_time", "confirm": "lake-flink"}).expect(422)
	// Running pipelines are refused.
	exec(t, `UPDATE pipelines SET desired_state = 'running' WHERE id = '`+pipe.Str("id")+`'`)
	if r := e.admin.post(reset, map[string]any{"mode": "skip_to_now", "confirm": "lake-flink"}).expect(409); r.Code() != "not_paused" {
		t.Fatal(string(r.Raw))
	}
}

func TestDebeziumHasNoSavedPositions(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	url := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	if l := e.admin.get(url + "/positions").expect(200).List("positions"); len(l) != 0 {
		t.Fatal(l)
	}
	exec(t, `UPDATE pipelines SET deployed_version_id = current_version_id, desired_state = 'paused' WHERE id = '`+pipe.Str("id")+`'`)
	if r := e.admin.post(url+"/positions/1/restore", map[string]any{"confirm": "orders-cdc"}).expect(409); r.Code() != "engine_unsupported" {
		t.Fatal(string(r.Raw))
	}
	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	if r := e.admin.post(url+"/offsets/reset", map[string]any{"mode": "from_time", "from_time": past, "confirm": "orders-cdc"}).expect(422); r.Code() != "engine_unsupported" {
		t.Fatal(string(r.Raw))
	}
}

// Positions set aside and savepoints beyond the newest ten expire after a week, unless kept.
func TestSavedPositionsExpire(t *testing.T) {
	e := newPipelineEnv(t)
	pipe, url := flinkPipeline(t, e)
	short := shortOf(pipe.Str("id"))
	ids := savePositions(t, short, 3)
	e.admin.post(url+"/offsets/reset", map[string]any{"mode": "resnapshot", "confirm": "lake-flink"}).expect(200) // sets all three aside
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(db.NormalizeURL(config.Get().EffectiveStateDatabaseURL()))
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	table := "debezium_state.flink_state_" + short
	// Twelve savepoints above the floor.
	for i := range 12 {
		if _, err := pool.Exec(ctx, `INSERT INTO `+table+` (kind, reason, flink_id, path, size, sha256, payload) VALUES ('savepoint', 'stop', $1, 'sp/x', 1, '', '\x00')`, i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE `+table+` SET kept = true WHERE id = $1`, ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := services.ExpireSavedPositions(ctx, testdb.Pool(t), time.Now()); err != nil {
		t.Fatal(err)
	}
	count := func() (n int) {
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&n)
		return n
	}
	if n := count(); n != 15 {
		t.Fatalf("nothing is a week old yet: %d rows", n)
	}
	if _, err := pool.Exec(ctx, `UPDATE `+table+` SET taken_at = now() - interval '8 days'`); err != nil {
		t.Fatal(err)
	}
	removed, err := services.ExpireSavedPositions(ctx, testdb.Pool(t), time.Now())
	if err != nil || removed != 4 {
		t.Fatalf("removed %d, %v", removed, err)
	}
	var keptLeft, setAside, savepoints int
	_ = pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE kept), count(*) FILTER (WHERE id <= $1), count(*) FILTER (WHERE kind = 'savepoint') FROM `+table, ids[2]).
		Scan(&keptLeft, &setAside, &savepoints)
	if keptLeft != 1 || setAside != 1 || savepoints != 10 {
		t.Fatalf("kept %d, set aside %d, savepoints %d", keptLeft, setAside, savepoints)
	}
}

// A Flink CDC pipeline's preflight has its own section: keys and whole-row changes on the
// PostgreSQL source, the cluster's size against the runtime.
func TestFlinkPreflight(t *testing.T) {
	e := newPipelineEnv(t)
	_, url := flinkPipeline(t, e)
	pre := e.admin.post(url+"/preflight", nil).expect(200)
	var engineSection bool
	for _, s := range pre.List("sections") {
		engineSection = engineSection || at(t, s, "key") == "engine"
	}
	identity := checkOf(t, pre, "flink_replica_identity")
	if !engineSection || identity == nil || at(t, identity, "status") != "fail" || at(t, identity, "blocking") != true ||
		at(t, identity, "fix", "code") != `ALTER TABLE "shop"."orders" REPLICA IDENTITY FULL;` {
		t.Fatal(string(pre.Raw))
	}
	if c := checkOf(t, pre, "flink_primary_key"); c == nil || at(t, c, "status") != "pass" {
		t.Fatal(c)
	}
	exec(t, `ALTER TABLE shop.orders REPLICA IDENTITY FULL`)
	pre = e.admin.post(url+"/preflight", nil).expect(200)
	if c := checkOf(t, pre, "flink_replica_identity"); c == nil || at(t, c, "status") != "pass" {
		t.Fatal(c)
	}
	// The image is judged by the Flink CDC catalogue it is in.
	if c := checkOf(t, pre, "debezium_image_unlisted"); c != nil {
		t.Fatal(c)
	}
	// The capacity check counts the whole cluster.
	if c := checkOf(t, pre, "runtime_capacity"); c == nil || !strings.Contains(at(t, c, "message").(string), "this pipeline asks for 1.5 CPU and 3072 MiB (a JobManager and 1 TaskManager)") {
		t.Fatal(c)
	}
}

// A Flink CDC pipeline deploys its engine's image, never the Debezium default.
func TestFlinkPipelinesResolveTheirEngineImage(t *testing.T) {
	e := newPipelineEnv(t)
	pipe, _ := flinkPipeline(t, e)
	ctx := context.Background()
	q := testdb.Pool(t)
	p, err := db.Find[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE id = $1`, pipe.Str("id"))
	if err != nil {
		t.Fatal(err)
	}
	spec, err := services.SpecOf(ctx, q, p.CurrentVersionID)
	if err != nil {
		t.Fatal(err)
	}
	image, err := services.ResolvePipelineImage(ctx, q, p, *spec, nil, "iceberg")
	if err != nil || image != "registry.example/bezisync-flink-cdc:3.7.0-2.2.1" {
		t.Fatal(image, err)
	}
	// The Debezium versions view and its upgrades leave it alone.
	usage := e.admin.get("/api/v1/projects/" + e.pid + "/debezium-images").expect(200)
	if strings.Contains(string(usage.Raw), pipe.Str("id")) {
		t.Fatal(string(usage.Raw))
	}
}

// A Flink CDC pipeline's logs are read per component.
func TestFlinkComponentLogs(t *testing.T) {
	e := newPipelineEnv(t)
	_, url := flinkPipeline(t, e)
	if l := e.admin.get(url + "/logs?component=taskmanager-2").expect(200).List("lines"); len(l) != 1 || l[0] != "logs of taskmanager-2" {
		t.Fatal(l)
	}
	e.admin.get(url + "/logs?component=sidecar").expect(422)
	e.admin.get(url + "/logs?component=taskmanager-0").expect(422)
	deb := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	if r := e.admin.get("/api/v1/projects/" + e.pid + "/pipelines/" + deb.Str("id") + "/logs?component=jobmanager").expect(503); !strings.Contains(r.Str("detail"), "Only Flink CDC") {
		t.Fatal(string(r.Raw))
	}
}

// A deployed Flink CDC pipeline keeps its readers while it has a saved position to restore; after
// Start over they can change. A forced deploy can't skip it.
func TestFlinkReadersChangeNeedsStartOver(t *testing.T) {
	e := newPipelineEnv(t)
	pipe, url := flinkPipeline(t, e)
	short := shortOf(pipe.Str("id"))
	exec(t, `UPDATE pipeline_versions SET sealed_at = now() WHERE pipeline_id = '`+pipe.Str("id")+`'`) // deployed: an edit is a new version
	readers := func() map[string]any {
		for _, s := range e.admin.post(url+"/preflight", nil).expect(200).List("sections") {
			for _, c := range at(t, s, "report", "checks").([]any) {
				if at(t, c, "code") == "flink_readers_changed" {
					if at(t, s, "key") != "config" {
						t.Fatal("not in the config section")
					}
					return c.(map[string]any)
				}
			}
		}
		return nil
	}
	if c := readers(); c != nil {
		t.Fatal("unchanged readers say nothing", c)
	}
	cur := e.admin.get(url).expect(200)
	medium := map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}, "policies": map[string]any{"resources": "medium"}}
	e.admin.patch(url, map[string]any{"spec": medium}, ifMatch(cur.At("version"))...).expect(200)
	// Never started: nothing to restore.
	if c := readers(); c == nil || c["status"] != "pass" {
		t.Fatal(c)
	}
	savePositions(t, short, 2)
	c := readers()
	if c == nil || c["status"] != "fail" || c["blocking"] != true || !strings.Contains(c["message"].(string), "runs 2 readers and this version asks for 4") {
		t.Fatal(c)
	}
	if r := e.admin.post(url+"/deploy", map[string]any{"force_reason": "we know what we are doing"}).expect(409); !strings.Contains(string(r.Raw), "cannot be skipped") {
		t.Fatal(string(r.Raw))
	}
	e.admin.post(url+"/offsets/reset", map[string]any{"mode": "resnapshot", "confirm": "lake-flink"}).expect(200)
	if c := readers(); c == nil || c["status"] != "pass" {
		t.Fatal(c)
	}
}

// Flink CDC can't copy one table again while the others stream: a snapshot request is refused,
// and a snapshot job queued for one is dropped instead of sending a Debezium signal (Known issue 10).
func TestFlinkRefusesSnapshots(t *testing.T) {
	e := newPipelineEnv(t)
	p, url := flinkPipeline(t, e)
	exec(t, `UPDATE pipelines SET desired_state = 'running', lifecycle = 'streaming' WHERE id = '`+p.Str("id")+`'`)
	r := e.admin.post(url+"/snapshots", map[string]any{"tables": []string{"shop.orders"}}).expect(422)
	if r.Code() != "snapshot_unsupported" || !strings.Contains(r.Str("detail"), "Flink CDC can't copy one table again") {
		t.Fatal(string(r.Raw))
	}
	id := stuckJob(t, p.Str("id"), "snapshot", 0, 0)
	exec(t, `UPDATE jobs SET state = 'queued', locked_at = NULL, locked_by = NULL WHERE id = '`+id.String()+`'`)
	e.runJobs()
	if j := jobRow(t, id); j.State != "done" || j.Result["skipped"] == nil {
		t.Fatalf("%s %v %v", j.State, j.Result, j.Error)
	}
}

// ADR 0013: a deploy that changes the columns a Flink CDC pipeline with a saved position writes
// switches columns at that position: preflight says so (no Start over), the controller raises the
// column generation the helper gets, records what the job now writes, and audits it; a deploy that
// keeps the columns resumes as before.
func TestFlinkColumnSwitchAtTheSavedPosition(t *testing.T) {
	e := newPipelineEnv(t)
	enableFlink(t, e.admin)
	exec(t, `ALTER TABLE shop.orders REPLICA IDENTITY FULL`)
	tables := map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}
	send := map[string]any{"new_columns": "send"} // new source columns flow live
	body := map[string]any{"name": "events-flink", "source_id": e.ids.Source, "sink_id": e.ids.Sink, "runtime_id": e.ids.Runtime, "engine": "flink-cdc",
		"spec": map[string]any{"source": tables, "flink_cdc": send}}
	pipe := e.admin.post("/api/v1/projects/"+e.pid+"/pipelines", body).expect(201)
	url := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	short := shortOf(pipe.Str("id"))
	e.admin.post(url+"/deploy", map[string]any{}).expect(200)
	ctx := context.Background()
	q := testdb.Pool(t)
	load := func() *models.Pipeline {
		p, err := db.Find[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE id = $1`, pipe.Str("id"))
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	settings := func() flinkhelper.Settings {
		var s flinkhelper.Settings
		w, ok := e.driver.Workloads[short]
		if !ok {
			t.Fatal("no workload", e.driver.Workloads)
		}
		if err := json.Unmarshal([]byte(w.Flink.Config["settings.json"]), &s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	// The first start: nothing saved, planned fresh.
	e.tick()
	if s := settings(); !s.FreshStart || s.Generation != 0 {
		t.Fatalf("%+v", s)
	}
	savePositions(t, short, 2)
	deploy := func(steps []any) *resp {
		cur := e.admin.get(url).expect(200)
		e.admin.patch(url, map[string]any{"spec": map[string]any{"source": tables, "flink_cdc": send, "transforms": steps}}, ifMatch(cur.At("version"))...).expect(200)
		return e.admin.post(url+"/preflight", nil).expect(200)
	}
	added := []any{map[string]any{"type": "unwrap", "id": "shape"}, map[string]any{"type": "add_field", "id": "add1", "name": "region", "value": map[string]any{"kind": "static", "text": "eu"}}}
	pre := deploy(added)
	c := checkOf(t, pre, "flink_columns_changed")
	if c == nil || at(t, c, "status") != "pass" || at(t, c, "blocking") == true ||
		!strings.Contains(at(t, c, "message").(string), "shop.orders adds region") || !strings.Contains(at(t, c, "message").(string), "switches columns at its saved position") {
		t.Fatal(c)
	}
	// A source column added after the saved position: the restored job hasn't read it, so the switch
	// doesn't write it (Flink CDC would fail on a column it can't find).
	exec(t, `ALTER TABLE shop.orders ADD COLUMN note text`)
	t.Cleanup(func() { exec(t, `ALTER TABLE shop.orders DROP COLUMN IF EXISTS note`) })
	first := load().DeployedVersionID.String()
	e.admin.post(url+"/deploy", map[string]any{}).expect(200)
	if w := load().Artifacts[services.ArtifactWritten]; w != first {
		t.Fatalf("flink_written = %v, want the version the job ran (%s)", w, first)
	}
	e.tick()
	p := load()
	layout, _ := p.Artifacts[services.ArtifactLayout].(map[string]any)
	if fmt.Sprint(layout["generation"]) != "1" || p.Artifacts[services.ArtifactWritten] != p.DeployedVersionID.String() || p.Artifacts[services.ArtifactFreshStart] != nil {
		t.Fatalf("artifacts %v", p.Artifacts)
	}
	if s := settings(); s.FreshStart || s.Generation != 1 || s.Restore != "latest" {
		t.Fatalf("%+v", s)
	}
	if cols := fmt.Sprint(p.Artifacts[services.ArtifactColumns]); strings.Contains(cols, "note") || !strings.Contains(cols, "total") {
		t.Fatal(cols)
	}
	var n int
	if err := q.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind = 'pipeline.columns_switched' AND resource_id = $1`, p.ID).Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	// A version that keeps the columns resumes in the same generation.
	same := []any{added[0], map[string]any{"type": "add_field", "id": "add1", "name": "region", "value": map[string]any{"kind": "static", "text": "us"}}}
	if c := checkOf(t, deploy(same), "flink_columns_changed"); c != nil {
		t.Fatal("unchanged columns say nothing", c)
	}
	e.admin.post(url+"/deploy", map[string]any{}).expect(200)
	e.tick()
	if s := settings(); s.Generation != 1 {
		t.Fatalf("%+v", s)
	}
	// Taking the field away again is another switch.
	deploy(added[:1])
	e.admin.post(url+"/deploy", map[string]any{}).expect(200)
	e.tick()
	if s := settings(); s.Generation != 2 {
		t.Fatalf("%+v", s)
	}
}

// ADR 0015: a restored job that names a source column it hasn't read yet (column_not_read) has it
// held back: the controller drops it from the columns the pipeline writes, the compile changes, and
// the job starts again; the next deploy writes it again.
func TestFlinkHoldsColumnsTheJobHasntRead(t *testing.T) {
	e := newPipelineEnv(t)
	enableFlink(t, e.admin)
	exec(t, `ALTER TABLE shop.orders REPLICA IDENTITY FULL`)
	tables := map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}
	body := map[string]any{"name": "held-flink", "source_id": e.ids.Source, "sink_id": e.ids.Sink, "runtime_id": e.ids.Runtime, "engine": "flink-cdc",
		"spec": map[string]any{"source": tables, "flink_cdc": map[string]any{"new_columns": "hold"}}}
	pipe := e.admin.post("/api/v1/projects/"+e.pid+"/pipelines", body).expect(201)
	url := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	short := shortOf(pipe.Str("id"))
	exec(t, `ALTER TABLE shop.orders ADD COLUMN note text`)
	t.Cleanup(func() { exec(t, `ALTER TABLE shop.orders DROP COLUMN IF EXISTS note`) })
	e.admin.post(url+"/deploy", map[string]any{}).expect(200)
	e.tick()
	hash := func() string {
		e.driver.mu.Lock()
		defer e.driver.mu.Unlock()
		return e.driver.Workloads[short].ConfigHash
	}
	before := hash()
	e.driver.mu.Lock()
	e.driver.FlinkStatus = `{"phase": "failed", "error_code": "column_not_read", "error": "names columns", "unread_columns": ["note"]}`
	e.driver.mu.Unlock()
	e.tick()
	ctx := context.Background()
	q := testdb.Pool(t)
	p, err := db.Find[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE id = $1`, pipe.Str("id"))
	if err != nil {
		t.Fatal(err)
	}
	if cols := fmt.Sprint(p.Artifacts[services.ArtifactColumns]); strings.Contains(cols, "note") || !strings.Contains(cols, "total") {
		t.Fatal(cols)
	}
	if held := fmt.Sprint(p.Artifacts[services.ArtifactHeldUnread]); !strings.Contains(held, "note") {
		t.Fatal(held)
	}
	var n int
	if err := q.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind = 'pipeline.columns_held' AND resource_id = $1`, p.ID).Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	e.driver.mu.Lock()
	e.driver.FlinkStatus = ""
	e.driver.mu.Unlock()
	e.tick()
	if after := hash(); after == before || after == "" {
		t.Fatalf("config hash %q → %q: the job wasn't started again without the column", before, after)
	}
	if cfg := e.admin.get(url + "/config").expect(200).Str("properties"); strings.Contains(cfg, "note") {
		t.Fatal(cfg)
	}
}
