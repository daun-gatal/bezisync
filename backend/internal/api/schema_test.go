package api_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/controller"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

// withConfig changes settings for one test.
func withConfig(t *testing.T, change func(s *config.Settings)) {
	t.Helper()
	s := *config.Get()
	change(&s)
	config.Set(&s)
	t.Cleanup(config.Reset)
}

func conditionActive(t *testing.T, pipelineID, kind string) bool {
	t.Helper()
	p, err := db.Find[models.Pipeline](context.Background(), testdb.Pool(t), `SELECT * FROM pipelines WHERE id = $1`, pipelineID)
	if err != nil || p == nil {
		t.Fatal(p, err)
	}
	return services.Evaluate(kind, p, map[string]any{}, db.Now()).Active
}

func TestSchemaChangesAreTrackedAlertedAndReviewed(t *testing.T) {
	withConfig(t, func(s *config.Settings) { s.SchemaCheckSeconds = 0 })
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	u := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	e.admin.post(u+"/deploy", map[string]any{}).expect(200)
	e.tick()
	e.tick()
	base := e.admin.get(u + "/schema").expect(200)
	var names []string
	for _, c := range base.List("tables", "shop.orders") {
		names = append(names, at(t, c, "name").(string))
	}
	if base.At("tracked") != true || strings.Join(names, ",") != "id,total" || len(base.List("changes")) != 0 || base.At("pending_breaking") != float64(0) {
		t.Fatal(string(base.Raw))
	}
	exec(t, `ALTER TABLE shop.orders ADD COLUMN note text`)
	exec(t, `ALTER TABLE shop.orders ALTER COLUMN total TYPE bigint`)
	e.tick()
	after := e.admin.get(u + "/schema").expect(200)
	kinds := map[string]bool{}
	for _, c := range after.List("changes") {
		kinds[fmt.Sprint(at(t, c, "kind"), "/", at(t, c, "column"), "/", at(t, c, "breaking"))] = true
	}
	if len(kinds) != 2 || !kinds["column_added/note/false"] || !kinds["type_changed/total/true"] || after.At("pending_breaking") != float64(1) {
		t.Fatal(string(after.Raw))
	}
	if n := e.admin.get(u).At("status", "schema_pending_breaking"); n != float64(1) {
		t.Fatal(n)
	}
	e.tick()
	if n := e.admin.get(u).At("status", "schema_pending_breaking"); n != float64(1) {
		t.Fatal(n)
	}
	if !conditionActive(t, pipe.Str("id"), "schema_breaking") {
		t.Fatal("schema_breaking not active")
	}
	reviewed := e.admin.post(u+"/schema/review", nil).expect(200)
	if reviewed.At("pending_breaking") != float64(0) {
		t.Fatal(string(reviewed.Raw))
	}
	for _, c := range reviewed.List("changes") {
		if at(t, c, "reviewed_at") == nil {
			t.Fatal(c)
		}
	}
	if conditionActive(t, pipe.Str("id"), "schema_breaking") {
		t.Fatal("schema_breaking still active")
	}
}

func TestTelemetryIsAnonymousAndWeekly(t *testing.T) {
	e := newPipelineEnv(t)
	createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	preview := e.admin.get("/api/v1/system/telemetry").expect(200)
	if preview.At("enabled") != false || preview.At("payload", "pipelines", "by_source", "postgresql") != float64(1) {
		t.Fatal(string(preview.Raw))
	}
	body, _ := json.Marshal(preview.At("payload"))
	for _, secret := range []string{"orders-cdc", "shop-pg", "shop.orders", "admin@", canary} {
		if strings.Contains(string(body), secret) {
			t.Fatal("telemetry contains", secret)
		}
	}
	member(t, e.srv, e.admin, e.pid, "viewer").get("/api/v1/system/telemetry").expect(403)
	var mu sync.Mutex
	var sent []map[string]any
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m map[string]any
		_ = json.NewDecoder(r.Body).Decode(&m)
		mu.Lock()
		sent = append(sent, m)
		mu.Unlock()
	}))
	defer collector.Close()
	ctx := context.Background()
	send := func(now time.Time) bool {
		var ok bool
		if err := db.Tx(ctx, func(tx pgx.Tx) error {
			var err error
			ok, err = services.MaybeSendTelemetry(ctx, tx, now)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	now := db.Now()
	if send(now) {
		t.Fatal("sent without opt-in")
	}
	withConfig(t, func(s *config.Settings) { s.TelemetryEnabled = true })
	if send(now) {
		t.Fatal("sent without a collector")
	}
	withConfig(t, func(s *config.Settings) { s.TelemetryURL = collector.URL + "/v1" })
	if !send(now) || send(now.Add(72*time.Hour)) || !send(now.Add(8*24*time.Hour)) {
		t.Fatal("weekly schedule broken")
	}
	if len(sent) != 2 || sent[0]["install_id"] == nil || sent[0]["install_id"] != sent[1]["install_id"] {
		t.Fatal(sent)
	}
}

func TestMasterKeyRotation(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	created := admin.post("/api/v1/projects/"+pid+"/connections", map[string]any{"kind": "source", "type": "postgresql", "name": "rot",
		"secrets": map[string]any{"password": canary}, "config": map[string]any{"host": "db", "port": 5432, "database": "x", "user": "u", "ssl_mode": "disable"}}).expect(201)
	token := admin.post("/api/v1/auth/tokens", map[string]any{"name": "ci"}).expect(201).Str("token")
	anon := newClient(t, srv)
	old := config.Get().MasterKey
	withConfig(t, func(s *config.Settings) { s.MasterKey, s.PreviousMasterKeys = newMasterKey(), old })
	admin.get("/api/v1/auth/me").expect(200)
	anon.get("/api/v1/auth/me", "authorization", "Bearer "+token).expect(200)
	ctx := context.Background()
	rotate := func() map[string]int {
		var counts map[string]int
		if err := db.Tx(ctx, func(tx pgx.Tx) error {
			var err error
			counts, _, err = services.RotateKeys(ctx, tx)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return counts
	}
	if rotate()["connections"] < 1 {
		t.Fatal("nothing rotated")
	}
	withConfig(t, func(s *config.Settings) { s.PreviousMasterKeys = "" })
	c, err := db.Find[models.Connection](ctx, testdb.Pool(t), `SELECT * FROM connections WHERE id = $1`, created.Str("id"))
	if err != nil {
		t.Fatal(err)
	}
	stored, err := services.StoredSecrets(c)
	if err != nil || stored["password"] != canary {
		t.Fatal(stored, err)
	}
	admin.get("/api/v1/auth/me").expect(200)
	anon.get("/api/v1/auth/me", "authorization", "Bearer "+token).expect(200)
	if n := rotate()["connections"]; n != 0 {
		t.Fatal(n)
	}
}

func TestDeliveryCanaryEndToEnd(t *testing.T) {
	withConfig(t, func(s *config.Settings) { s.CanaryIntervalSeconds = 0 })
	prevPoll, prevTail := controller.CanaryPoll, controller.CanaryTailTimeout
	controller.CanaryPoll, controller.CanaryTailTimeout = 0, 500*time.Millisecond
	t.Cleanup(func() { controller.CanaryPoll, controller.CanaryTailTimeout = prevPoll, prevTail })
	srv := server(t)
	installFake(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	role := shop(t)
	bootstrap := kafkaCluster(t)
	ids := setupProject(t, admin, pid, role, bootstrap)
	ctrl := controller.New()
	ctx := context.Background()
	pipe := createPipeline(t, admin, pid, ids, "shop.orders")
	u := "/api/v1/projects/" + pid + "/pipelines/" + pipe.Str("id")
	admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}},
		"policies": map[string]any{"delivery_canary": true}}}, ifMatch(pipe.At("version"))...).expect(200)
	pre := admin.post(u+"/preflight", nil).expect(200)
	props := pre.Str("properties")
	short := regexp.MustCompile(`slot\.name=bz_(\w+)`).FindStringSubmatch(props)[1]
	include := regexp.MustCompile(`table\.include\.list=(.*)`).FindStringSubmatch(props)[1]
	topic, _ := pre.At("destinations", "bezisync.canary_"+short).(string)
	if !strings.Contains(include, "canary_"+short) || !strings.Contains(props, "(?!canary_)") || topic == "" {
		t.Fatal(props, string(pre.Raw))
	}
	admin.post(u+"/deploy", map[string]any{}).expect(200)
	ctrl.TickOnce(ctx)
	ctrl.TickOnce(ctx)
	token := admin.get(u).Str("status", "canary", "pending", "token")
	n, err := db.Scalar[int64](ctx, testdb.Pool(t), fmt.Sprintf(`SELECT count(*) FROM bezisync.canary_%s WHERE id = $1`, short), token)
	if err != nil || n != 1 {
		t.Fatal(n, err)
	}
	// Debezium would deliver the canary row to its topic; produce it there.
	producer, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	value, _ := json.Marshal(map[string]any{"id": token, "sent_at": pytime.ISOFormat(db.Now())})
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	ctrl.TickOnce(ctx)
	c := admin.get(u)
	if c.At("status", "canary", "ok") != true || c.At("status", "canary", "verified_at") == nil || c.At("status", "canary", "latency_ms").(float64) < 0 ||
		c.Str("status", "canary", "pending", "token") == token {
		t.Fatal(string(c.Raw))
	}
	withConfig(t, func(s *config.Settings) { s.CanaryTimeoutSeconds = 0 })
	ctrl.TickOnce(ctx)
	ctrl.TickOnce(ctx)
	c = admin.get(u)
	if c.At("status", "canary", "ok") != false || c.At("status", "canary", "pending") == nil {
		t.Fatal(string(c.Raw))
	}
	if !conditionActive(t, pipe.Str("id"), "delivery_unverified") {
		t.Fatal("delivery_unverified not active")
	}
	admin.del(u).expect(200)
	ctrl.RunJobs(ctx, 100)
	reg, err := db.Scalar[*string](ctx, testdb.Pool(t), fmt.Sprintf(`SELECT to_regclass('bezisync.canary_%s')::text`, short))
	if err != nil || reg != nil {
		t.Fatal(reg, err)
	}
}

func TestCanaryIsOffForUnsupportedPipelines(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	pre := e.admin.post("/api/v1/projects/"+e.pid+"/pipelines/"+pipe.Str("id")+"/preflight", nil).expect(200)
	include := regexp.MustCompile(`table\.include\.list=(.*)`).FindStringSubmatch(pre.Str("properties"))[1]
	if strings.Contains(include, "canary_") {
		t.Fatal(include)
	}
	for d := range pre.At("destinations").(map[string]any) {
		if strings.Contains(d, "canary") {
			t.Fatal(d)
		}
	}
}

func newMasterKey() string {
	raw := make([]byte, 32)
	_, _ = rand.Read(raw)
	return base64.StdEncoding.EncodeToString(raw)
}
