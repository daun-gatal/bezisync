package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

// discardEnv is a deployed pipeline (v1) and helpers to edit, discard and read it.
type discardEnv struct {
	*pipelineEnv
	base, u string
}

func newDiscardEnv(t *testing.T) *discardEnv {
	t.Helper()
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	d := &discardEnv{pipelineEnv: e, base: "/api/v1/projects/" + e.pid, u: "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")}
	e.admin.post(d.u+"/deploy", map[string]any{}).expect(200)
	e.tick()
	return d
}

func (d *discardEnv) edit(t *testing.T, c *client, tables ...string) *resp {
	t.Helper()
	return c.patch(d.u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": tables}}}},
		ifMatch(d.admin.get(d.u).At("version"))...).expect(200)
}

func (d *discardEnv) discard(c *client) *resp {
	return c.post(d.u+"/changes:discard", nil, ifMatch(d.admin.get(d.u).At("version"))...)
}

// auditOf is the details of a pipeline's audit events of a kind, oldest first.
func auditOf(t *testing.T, kind string) []map[string]any {
	t.Helper()
	rows, err := db.Select[struct {
		Details db.Map `db:"details"`
	}](context.Background(), testdb.Pool(t), `SELECT details FROM events WHERE kind = $1 ORDER BY occurred_at`, kind)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]map[string]any, len(rows))
	for i, r := range rows {
		out[i] = r.Details
	}
	return out
}

func jobCount(t *testing.T) int64 {
	t.Helper()
	n, err := db.Scalar[int64](context.Background(), testdb.Pool(t), `SELECT count(*) FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// ADR 0017 E1, E2, E5, E9, E10, E15: discard points current back at the deployed version, keeps
// every row, touches nothing that runs, and numbers never repeat.
func TestDiscardChanges(t *testing.T) {
	d := newDiscardEnv(t)
	admin := d.admin
	editor := member(t, d.srv, admin, d.pid, "editor")
	d.edit(t, admin, "shop.orders", "shop.refunds") // v2, admin's draft
	d.edit(t, editor, "shop.refunds")               // v3, the editor's own version on top
	before := admin.get(d.u).expect(200)
	if before.At("current_version") != float64(3) || before.At("has_undeployed_changes") != true {
		t.Fatal(string(before.Raw))
	}
	stale := before.At("version")
	jobs := jobCount(t)

	res := d.discard(admin).expect(200)
	if res.At("current_version") != float64(1) || res.At("deployed_version") != float64(1) || res.At("has_undeployed_changes") != false {
		t.Fatal(string(res.Raw))
	}
	if res.Str("desired_state") != before.Str("desired_state") || res.Str("lifecycle") != before.Str("lifecycle") || res.Str("deployed_image") != before.Str("deployed_image") {
		t.Fatalf("discard changed what runs: %s", res.Raw)
	}
	ba, _ := json.Marshal(before.At("artifacts"))
	ra, _ := json.Marshal(res.At("artifacts"))
	if string(ba) != string(ra) {
		t.Fatalf("artifacts changed: %s → %s", ba, ra)
	}
	if n := jobCount(t); n != jobs {
		t.Fatalf("discard queued %d jobs", n-jobs)
	}
	if inc := res.At("spec", "source", "tables", "include").([]any); len(inc) != 1 || inc[0] != "shop.orders" {
		t.Fatal(string(res.Raw))
	}
	vs := admin.get(d.u + "/versions").expect(200).List()
	if len(vs) != 3 {
		t.Fatalf("versions were deleted: %v", vs)
	}
	ev := auditOf(t, "pipeline.changes_discarded")
	if len(ev) != 1 || fmt.Sprint(ev[0]["discarded_version"]) != "3" || fmt.Sprint(ev[0]["deployed_version"]) != "1" || ev[0]["reason"] != "discarded" || ev[0]["discarded_by_author"] == "" {
		t.Fatal(ev)
	}

	// A save read before the discard is refused: the draft isn't revived (E9).
	r := admin.patch(d.u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.refunds"}}}}},
		ifMatch(stale)...).expect(412)
	if r.Code() != "version_conflict" {
		t.Fatal(string(r.Raw))
	}
	// The next change is a new version past every number used (E10).
	if res = d.edit(t, admin, "shop.refunds"); res.At("current_version") != float64(4) {
		t.Fatal(string(res.Raw))
	}

	// After a rollback, the newer version above deployed can be discarded too (E5).
	admin.post(d.u+"/deploy", map[string]any{}).expect(200) // v4 deployed
	d.tick()
	admin.post(d.u+"/deploy", map[string]any{"version": 1}).expect(200) // roll back to v1
	d.tick()
	if p := admin.get(d.u).expect(200); p.At("current_version") != float64(4) || p.At("deployed_version") != float64(1) {
		t.Fatal(string(p.Raw))
	}
	if res = d.discard(editor).expect(200); res.At("current_version") != float64(1) || res.At("has_undeployed_changes") != false {
		t.Fatal(string(res.Raw))
	}
}

// E6, E7, E8, E13: refusals.
func TestDiscardRefusals(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	draft := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	du := "/api/v1/projects/" + e.pid + "/pipelines/" + draft.Str("id")
	if r := admin.post(du+"/changes:discard", nil, ifMatch(draft.At("version"))...).expect(409); r.Code() != "never_deployed" {
		t.Fatal(string(r.Raw))
	}
	admin.post(du+"/deploy", map[string]any{}).expect(200)
	e.tick()
	cur := admin.get(du).expect(200)
	if r := admin.post(du+"/changes:discard", nil, ifMatch(cur.At("version"))...).expect(409); r.Code() != "nothing_to_discard" {
		t.Fatal(string(r.Raw))
	}
	admin.patch(du, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.refunds"}}}}},
		ifMatch(cur.At("version"))...).expect(200)
	if r := admin.post(du+"/changes:discard", nil).expect(428); r.Code() != "precondition_required" {
		t.Fatal(string(r.Raw))
	}
	if r := admin.post(du+"/changes:discard", nil, ifMatch(cur.At("version"))...).expect(412); r.Code() != "version_conflict" {
		t.Fatal(string(r.Raw))
	}
	now := admin.get(du).expect(200).At("version")
	member(t, e.srv, admin, e.pid, "operator").post(du+"/changes:discard", nil, ifMatch(now)...).expect(403)
	member(t, e.srv, admin, e.pid, "viewer").post(du+"/changes:discard", nil, ifMatch(now)...).expect(403)
	other := admin.post("/api/v1/projects", map[string]any{"slug": "elsewhere", "name": "Elsewhere"}).expect(201).Str("id")
	admin.post("/api/v1/projects/"+other+"/pipelines/"+draft.Str("id")+"/changes:discard", nil, ifMatch(now)...).expect(404)
	if p := admin.get(du).expect(200); p.At("current_version") != float64(2) {
		t.Fatalf("a refused discard changed the pipeline: %s", p.Raw)
	}
}

// E3, E4, E14: a pending request blocks the discard; once it is cancelled, the sealed version can
// be set aside, its request stays, and no approval is asked for the discard itself.
func TestDiscardAfterApproval(t *testing.T) {
	d := newDiscardEnv(t)
	admin := d.admin
	admin.put(d.base+"/policies", map[string]any{"require_approval": true}).expect(200)
	d.edit(t, admin, "shop.orders", "shop.refunds")
	cr := admin.post(d.u+"/deploy", map[string]any{}).expect(202).Str("pending_change", "id")
	r := d.discard(admin).expect(409)
	if r.Code() != "pending_change_request" || r.Str("change_request_id") != cr || r.At("version") != float64(2) {
		t.Fatal(string(r.Raw))
	}
	admin.post(d.base+"/change-requests/"+cr+"/cancel", nil).expect(200)
	if res := d.discard(admin).expect(200); res.At("current_version") != float64(1) || res.At("pending_change") != nil {
		t.Fatal(string(res.Raw))
	}
	n, err := db.Scalar[int64](context.Background(), testdb.Pool(t), `SELECT count(*) FROM change_requests WHERE id = $1 AND state = 'cancelled'`, cr)
	if err != nil || n != 1 {
		t.Fatal(n, err)
	}
}

// E11, E12: editing back to the deployed spec points current at the deployed version, also when
// the deploy wrote a pinned setting the edit leaves out.
func TestEditBackWithPins(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	admin.put(base+"/policies", map[string]any{"setting_limits": map[string]any{"debezium.source.poll.interval.ms": map[string]any{"pin": "500"}}}).expect(200)
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	d := &discardEnv{pipelineEnv: e, base: base, u: base + "/pipelines/" + pipe.Str("id")}
	admin.post(d.u+"/deploy", map[string]any{}).expect(200)
	e.tick()
	if p := admin.get(d.u).expect(200); p.At("spec", "tuning", "debezium.source.poll.interval.ms") != "500" {
		t.Fatal(string(p.Raw))
	}
	d.edit(t, admin, "shop.orders", "shop.refunds")
	res := d.edit(t, admin, "shop.orders") // the deployed tables, without the pinned setting
	if res.At("current_version") != float64(1) || res.At("has_undeployed_changes") != false {
		t.Fatal(string(res.Raw))
	}
	ev := auditOf(t, "pipeline.changes_discarded")
	if len(ev) != 1 || ev[0]["reason"] != "edited_back" || fmt.Sprint(ev[0]["discarded_version"]) != "2" {
		t.Fatal(ev)
	}
}
