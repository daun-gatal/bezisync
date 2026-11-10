package api_test

import (
	"context"
	"errors"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

// One change is one version: saves on a version nobody depends on yet (not deployed, not sent
// for approval, made by the same person) update it in place; anything else starts a new one.
func TestOneVersionPerChange(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := base + "/pipelines/" + pipe.Str("id")
	tables := func(ts ...string) map[string]any {
		return map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": ts}}}}
	}
	versions := func() []any { return admin.get(u + "/versions").expect(200).List() }
	save := func(c *client, body map[string]any) *resp {
		t.Helper()
		return c.patch(u, body, ifMatch(admin.get(u).At("version"))...).expect(200)
	}

	// Every wizard step saves the draft: it stays v1, holding the latest spec.
	save(admin, tables("shop.orders", "shop.refunds"))
	save(admin, tables("shop.orders"))
	res := save(admin, tables("shop.orders", "shop.refunds"))
	if res.At("current_version") != float64(1) {
		t.Fatal(string(res.Raw))
	}
	vs := versions()
	if len(vs) != 1 || at(t, vs[0], "note") != "Created" || at(t, vs[0], "draft") != true {
		t.Fatal(vs)
	}
	if inc := at(t, vs[0], "spec", "source", "tables", "include").([]any); len(inc) != 2 {
		t.Fatal(vs)
	}

	// Deploying seals v1; the next change is v2 however many saves it takes.
	admin.post(u+"/deploy", map[string]any{}).expect(200)
	e.tick()
	if vs = versions(); at(t, vs[0], "draft") != false {
		t.Fatal(vs)
	}
	if res = save(admin, tables("shop.orders")); res.At("current_version") != float64(2) {
		t.Fatal(string(res.Raw))
	}
	// Going back to exactly what is deployed points current at the deployed version again (ADR
	// 0017): nothing is left to deploy, and v2 stays in History.
	if res = save(admin, tables("shop.orders", "shop.refunds")); res.At("current_version") != float64(1) || res.At("has_undeployed_changes") != false {
		t.Fatal(string(res.Raw))
	}
	if vs = versions(); len(vs) != 2 || at(t, vs[0], "draft") != true || at(t, vs[1], "deployed") != true {
		t.Fatal(vs)
	}

	// Someone else's edit is their own version.
	editor := member(t, e.srv, admin, e.pid, "editor")
	if res = save(editor, tables("shop.orders")); res.At("current_version") != float64(3) {
		t.Fatal(string(res.Raw))
	}

	// Sending a version for approval seals it: the approver reviews exactly what was sent.
	admin.put(base+"/policies", map[string]any{"require_approval": true}).expect(200)
	cr := editor.post(u+"/deploy", map[string]any{}).expect(202)
	if cr.At("pending_change", "version") != float64(3) {
		t.Fatal(string(cr.Raw))
	}
	if res = save(editor, tables("shop.refunds")); res.At("current_version") != float64(4) {
		t.Fatal(string(res.Raw))
	}
	if vs = versions(); len(vs) != 4 {
		t.Fatal(vs)
	}
	if inc := at(t, vs[1], "spec", "source", "tables", "include").([]any); len(inc) != 1 {
		t.Fatalf("v3 changed after it was sent for approval: %v", vs[1])
	}
}

// A deploy seals exactly what its checks ran on: if the draft was saved again in between (an
// autosave racing the Deploy button), sealing refuses instead of shipping unchecked changes.
func TestSealRefusesADraftSavedAfterItsChecks(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	ctx := context.Background()
	pool := testdb.Pool(t)
	checked, err := db.Find[models.PipelineVersion](ctx, pool, `SELECT v.* FROM pipeline_versions v JOIN pipelines p ON p.current_version_id = v.id WHERE p.id = $1`, pipe.Str("id"))
	if err != nil || checked == nil {
		t.Fatal(err)
	}
	u := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	e.admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders", "shop.refunds"}}}}},
		ifMatch(pipe.At("version"))...).expect(200)
	var pe *problem.Problem
	if err := services.SealVersion(ctx, pool, checked); !errors.As(err, &pe) || pe.Code != "version_changed" {
		t.Fatal(err)
	}
	// Once sealed, the next save starts a new version.
	fresh, _ := db.Find[models.PipelineVersion](ctx, pool, `SELECT * FROM pipeline_versions WHERE id = $1`, checked.ID)
	if err := services.SealVersion(ctx, pool, fresh); err != nil {
		t.Fatal(err)
	}
	res := e.admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}}},
		ifMatch(e.admin.get(u).At("version"))...).expect(200)
	if res.At("current_version") != float64(2) {
		t.Fatal(string(res.Raw))
	}
}
