package api_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

type hookRequest struct {
	Header http.Header
	Body   []byte
}

type hookSink struct {
	*httptest.Server
	mu   sync.Mutex
	reqs []hookRequest
}

func newHookSink(t *testing.T) *hookSink {
	s := &hookSink{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.reqs = append(s.reqs, hookRequest{r.Header.Clone(), b})
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *hookSink) requests() []hookRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]hookRequest{}, s.reqs...)
}

func evaluateProject(t *testing.T, pid string, at time.Time) []string {
	t.Helper()
	var states []string
	ctx := context.Background()
	err := db.Tx(ctx, func(tx pgx.Tx) error {
		alerts, err := services.EvaluateProject(ctx, tx, uuid.MustParse(pid), at)
		for _, a := range alerts {
			states = append(states, a.State)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return states
}

func TestAlertLifecycleWithWebhookDeliveryAndInbox(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	sink := newHookSink(t)
	base := "/api/v1/projects/" + e.pid
	rules := admin.get(base + "/alert-rules").expect(200).List()
	kinds := map[any]any{}
	for _, r := range rules {
		kinds[at(t, r, "kind")] = r
	}
	for _, k := range []string{"pipeline_down", "latency", "source_at_risk", "status_unknown"} {
		if kinds[k] == nil {
			t.Fatal(kinds)
		}
	}
	channel := admin.post(base+"/alert-channels", map[string]any{"name": "ops", "kind": "webhook",
		"secrets": map[string]any{"url": sink.URL + "/hook", "signing_secret": "s3cret-sign"}}).expect(201)
	if l := channel.List("secret_fields_set"); len(l) != 2 || l[0] != "signing_secret" || strings.Contains(string(channel.Raw), sink.URL) {
		t.Fatal(string(channel.Raw))
	}
	if r := admin.post(base+"/alert-channels/"+channel.Str("id")+"/test", nil).expect(200); r.At("ok") != true {
		t.Fatal(string(r.Raw))
	}
	down := at(t, kinds["pipeline_down"], "id").(string)
	admin.patch(base+"/alert-rules/"+down, map[string]any{"for_seconds": 0, "channel_ids": []string{channel.Str("id")}}).expect(200)
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	admin.post(base+"/pipelines/"+pipe.Str("id")+"/deploy", map[string]any{}).expect(200)
	status, _ := json.Marshal(map[string]any{"reasons": []any{map[string]any{"code": "source_auth", "message": "Login failed", "fix": "Rotate"}},
		"as_of": time.Now().UTC().Format(time.RFC3339Nano)})
	exec(t, fmt.Sprintf(`UPDATE pipelines SET lifecycle = 'failed', health = 'down', status = '%s' WHERE id = '%s'`, status, pipe.Str("id")))
	if s := evaluateProject(t, e.pid, db.Now()); len(s) != 1 || s[0] != "firing" {
		t.Fatal(s)
	}
	e.runJobs()
	var hooks []map[string]any
	for _, r := range sink.requests() {
		if !strings.Contains(string(r.Body), `"test": true`) {
			var m map[string]any
			_ = json.Unmarshal(r.Body, &m)
			hooks = append(hooks, m)
		}
	}
	if len(hooks) == 0 || hooks[len(hooks)-1]["state"] != "firing" || hooks[len(hooks)-1]["message"] != "Login failed" {
		t.Fatal(hooks)
	}
	last := sink.requests()[len(sink.requests())-1]
	mac := hmac.New(sha256.New, []byte("s3cret-sign"))
	mac.Write(last.Body)
	if last.Header.Get("x-bezisync-signature") != "sha256="+hex.EncodeToString(mac.Sum(nil)) {
		t.Fatal(last.Header)
	}
	alerts := admin.get(base + "/alerts").expect(200).List()
	if len(alerts) != 1 || at(t, alerts[0], "pipeline_name") != "orders-cdc" {
		t.Fatal(alerts)
	}
	id := at(t, alerts[0], "id").(string)
	member(t, e.srv, admin, e.pid, "viewer").post(base+"/alerts/"+id+"/acknowledge", nil).expect(403)
	member(t, e.srv, admin, e.pid, "operator").post(base+"/alerts/"+id+"/acknowledge", nil).expect(204)
	if s := admin.get(base+"/alerts").At(0, "state"); s != "acknowledged" {
		t.Fatal(s)
	}
	inbox := admin.get(base + "/notifications")
	if inbox.At("unread").(float64) < 1 || inbox.Str("items", 0, "title") != "orders-cdc is down" {
		t.Fatal(string(inbox.Raw))
	}
	exec(t, fmt.Sprintf(`UPDATE pipelines SET lifecycle = 'streaming', health = 'healthy' WHERE id = '%s'`, pipe.Str("id")))
	if s := evaluateProject(t, e.pid, db.Now()); len(s) != 1 || s[0] != "resolved" {
		t.Fatal(s)
	}
	e.runJobs()
	reqs := sink.requests()
	var lastHook map[string]any
	_ = json.Unmarshal(reqs[len(reqs)-1].Body, &lastHook)
	if lastHook["state"] != "resolved" {
		t.Fatal(lastHook)
	}
	if l := admin.get(base + "/alerts").List(); len(l) != 0 {
		t.Fatal(l)
	}
	if l := admin.get(base + "/alerts?state=resolved").List(); len(l) != 1 {
		t.Fatal(l)
	}
}

func TestPendingAlertsWaitForDurationAndVanishSilently(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	e.admin.post("/api/v1/projects/"+e.pid+"/pipelines/"+pipe.Str("id")+"/deploy", map[string]any{}).expect(200)
	e.admin.get("/api/v1/projects/" + e.pid + "/alert-rules").expect(200)
	exec(t, fmt.Sprintf(`UPDATE pipelines SET lifecycle = 'failed', health = 'down' WHERE id = '%s'`, pipe.Str("id")))
	if s := evaluateProject(t, e.pid, db.Now()); len(s) != 0 {
		t.Fatal(s)
	}
	states, err := db.Select[struct {
		State string `db:"state"`
	}](context.Background(), testdb.Pool(t), `SELECT state FROM alerts`)
	if err != nil || len(states) != 1 || states[0].State != "pending" {
		t.Fatal(states, err)
	}
	exec(t, fmt.Sprintf(`UPDATE pipelines SET lifecycle = 'streaming', health = 'healthy' WHERE id = '%s'`, pipe.Str("id")))
	if s := evaluateProject(t, e.pid, db.Now()); len(s) != 0 {
		t.Fatal(s)
	}
	if l := e.admin.get("/api/v1/projects/" + e.pid + "/alerts?state=all").List(); len(l) != 0 {
		t.Fatal(l)
	}
	if s := evaluateProject(t, e.pid, db.Now().Add(5*time.Minute)); len(s) != 0 {
		t.Fatal(s)
	}
}

func TestChannelValidationAndPermissions(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	base := "/api/v1/projects/" + pid + "/alert-channels"
	admin.post(base, map[string]any{"name": "x", "kind": "slack", "secrets": map[string]any{"webhook_url": "http://insecure"}}).expect(422)
	admin.post(base, map[string]any{"name": "x", "kind": "email", "config": map[string]any{"recipients": []string{"not-an-email"}}}).expect(422)
	member(t, srv, admin, pid, "editor").post(base, map[string]any{"name": "x", "kind": "slack", "secrets": map[string]any{"webhook_url": "https://hooks.slack.com/x"}}).expect(403)
	md := admin.post(base, map[string]any{"name": "meta", "kind": "webhook", "secrets": map[string]any{"url": "http://169.254.169.254/latest"}}).expect(201)
	res := admin.post(base+"/"+md.Str("id")+"/test", nil).expect(200)
	if res.At("ok") != false || !strings.Contains(res.Str("message"), "not allowed") {
		t.Fatal(string(res.Raw))
	}
	// Deleting a channel unhooks it from rules.
	rules := admin.get("/api/v1/projects/" + pid + "/alert-rules").List()
	rule := at(t, rules[0], "id").(string)
	admin.patch("/api/v1/projects/"+pid+"/alert-rules/"+rule, map[string]any{"channel_ids": []string{md.Str("id")}}).expect(200)
	admin.del(base + "/" + md.Str("id")).expect(204)
	for _, r := range admin.get("/api/v1/projects/" + pid + "/alert-rules").List() {
		if at(t, r, "id") == rule && len(at(t, r, "channel_ids").([]any)) != 0 {
			t.Fatal(r)
		}
	}
	admin.patch("/api/v1/projects/"+pid+"/alert-rules/"+rule, map[string]any{"channel_ids": []string{uuid.NewString()}}).expect(404)
	if c := admin.post("/api/v1/projects/"+pid+"/alert-rules", map[string]any{"name": "r", "kind": "bogus"}).Code(); c != "unknown_rule_kind" {
		t.Fatal(c)
	}
	created := admin.post("/api/v1/projects/"+pid+"/alert-rules", map[string]any{"name": "slow", "kind": "latency", "params": map[string]any{"threshold_seconds": 30}}).expect(201)
	if created.Str("description") == "" || created.At("for_seconds") != float64(120) {
		t.Fatal(string(created.Raw))
	}
	admin.del("/api/v1/projects/" + pid + "/alert-rules/" + created.Str("id")).expect(204)
	if k := admin.get("/api/v1/projects/" + pid + "/alert-channel-kinds").List(); len(k) != 4 {
		t.Fatal(k)
	}
}
