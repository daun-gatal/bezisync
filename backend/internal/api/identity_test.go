package api_test

import (
	"context"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

func slugs(t *testing.T, projects []any) []string {
	var out []string
	for _, p := range projects {
		out = append(out, at(t, p, "slug").(string))
	}
	return out
}

func hasPerm(t *testing.T, project any, perm string) bool {
	for _, p := range at(t, project, "permissions").([]any) {
		if p == perm {
			return true
		}
	}
	return false
}

func TestBootstrapCreatesAdminAndDefaultProject(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	me := admin.get("/api/v1/auth/me").expect(200)
	if me.Str("user", "email") != adminEmail || me.At("user", "is_superuser") != true {
		t.Fatalf("me: %s", me.Raw)
	}
	if s := slugs(t, me.List("projects")); len(s) != 1 || s[0] != "default" {
		t.Fatalf("projects %v", s)
	}
	if !hasPerm(t, me.At("projects", 0), "pipeline.deploy") {
		t.Fatal("admin lacks pipeline.deploy")
	}
	events := admin.get("/api/v1/projects/" + me.Str("projects", 0, "id") + "/events?audit_only=true").expect(200).List("items")
	found := false
	for _, e := range events {
		found = found || at(t, e, "summary") == "Created project Default"
	}
	if !found {
		t.Fatal("no project.created event")
	}
}

func TestLoginRejectsBadPasswordWithoutRevealingWhichField(t *testing.T) {
	srv := server(t)
	adminClient(t, srv)
	res := newClient(t, srv).post("/api/v1/auth/login", map[string]any{"email": adminEmail, "password": "nope"}).expect(401)
	if res.Code() != "invalid_credentials" || !strings.HasPrefix(res.Header.Get("content-type"), "application/problem+json") {
		t.Fatalf("%v %s", res.Header, res.Raw)
	}
	if res.Header.Get("x-request-id") != res.Str("request_id") {
		t.Fatal("request id mismatch")
	}
}

func TestLoginIsRateLimited(t *testing.T) {
	srv := server(t)
	adminClient(t, srv)
	anon := newClient(t, srv)
	var last int
	for range 12 {
		last = anon.post("/api/v1/auth/login", map[string]any{"email": "x@example.com", "password": "bad"}).Status
	}
	if last != 429 {
		t.Fatalf("last status %d", last)
	}
}

func TestUnsafeRequestsNeedCSRFHeader(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	res := admin.post("/api/v1/projects", map[string]any{"slug": "x", "name": "X"}, "x-csrf-token", "").expect(403)
	if res.Code() != "csrf_failed" {
		t.Fatal(string(res.Raw))
	}
}

func TestUnauthenticatedRequestsGet401(t *testing.T) {
	srv := server(t)
	newClient(t, srv).get("/api/v1/auth/me").expect(401)
}

func TestLogoutInvalidatesSession(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	admin.post("/api/v1/auth/logout", nil).expect(204)
	admin.get("/api/v1/auth/me").expect(401)
}

func TestProjectCRUDAndAudit(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := admin.post("/api/v1/projects", map[string]any{"slug": "prod", "name": "Production"}).expect(201).Str("id")
	admin.post("/api/v1/projects", map[string]any{"slug": "prod", "name": "Again"}).expect(409)
	admin.post("/api/v1/projects", map[string]any{"slug": "Bad Slug", "name": "x"}).expect(422)
	if d := admin.patch("/api/v1/projects/"+pid, map[string]any{"description": "Customer-facing"}).expect(200).Str("description"); d != "Customer-facing" {
		t.Fatal(d)
	}
	kinds, err := db.Select[struct {
		Kind string `db:"kind"`
	}](context.Background(), testdb.Pool(t), `SELECT DISTINCT kind FROM events`)
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, k := range kinds {
		have[k.Kind] = true
	}
	for _, k := range []string{"project.created", "project.updated", "auth.login"} {
		if !have[k] {
			t.Fatalf("missing event %s in %v", k, have)
		}
	}
}

func TestViewerCannotManageAndOutsidersSee404(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	viewer := member(t, srv, admin, pid, "viewer")
	viewer.get("/api/v1/projects/" + pid).expect(200)
	viewer.patch("/api/v1/projects/"+pid, map[string]any{"name": "hijack"}).expect(403)
	other := admin.post("/api/v1/projects", map[string]any{"slug": "secret", "name": "Secret"}).expect(201).Str("id")
	viewer.get("/api/v1/projects/" + other).expect(404)
	me := viewer.get("/api/v1/auth/me")
	if s := slugs(t, me.List("projects")); len(s) != 1 || s[0] != "default" {
		t.Fatal(s)
	}
	if hasPerm(t, me.At("projects", 0), "pipeline.edit") {
		t.Fatal("viewer can edit")
	}
}

func TestAdminsOpenEveryProjectWithoutMembership(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	id := admin.post("/api/v1/projects", map[string]any{"slug": "fresh", "name": "Fresh"}).expect(201).Str("id")
	if l := admin.get("/api/v1/projects/" + id + "/members").expect(200).List(); len(l) != 0 {
		t.Fatal(l)
	}
	for _, p := range admin.get("/api/v1/auth/me").List("projects") {
		if at(t, p, "slug") == "fresh" && !hasPerm(t, p, "project.manage") {
			t.Fatal("admin cannot manage fresh")
		}
	}
}

func TestMemberUpsertCreatesUserAndChangesRole(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	editor := member(t, srv, admin, pid, "editor", "ed@example.com")
	if !hasPerm(t, editor.get("/api/v1/auth/me").At("projects", 0), "pipeline.edit") {
		t.Fatal("editor cannot edit")
	}
	admin.put("/api/v1/projects/"+pid+"/members", map[string]any{"email": "ed@example.com", "role": "viewer"}).expect(200)
	if hasPerm(t, editor.get("/api/v1/auth/me").At("projects", 0), "pipeline.edit") {
		t.Fatal("viewer can still edit")
	}
	members := admin.get("/api/v1/projects/" + pid + "/members").List()
	if len(members) != 1 || at(t, members[0], "email") != "ed@example.com" {
		t.Fatal(members)
	}
}

func TestEventsFeedIsPaginated(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	for i := range 5 {
		admin.patch("/api/v1/projects/"+pid, map[string]any{"description": "d" + string(rune('0'+i))}).expect(200)
	}
	first := admin.get("/api/v1/projects/" + pid + "/events?limit=2").expect(200)
	cursor := first.Str("next_cursor")
	if len(first.List("items")) != 2 || cursor == "" {
		t.Fatal(string(first.Raw))
	}
	second := admin.get("/api/v1/projects/" + pid + "/events?limit=2&cursor=" + cursor).expect(200)
	seen := map[any]bool{}
	for _, e := range first.List("items") {
		seen[at(t, e, "id")] = true
	}
	for _, e := range second.List("items") {
		if seen[at(t, e, "id")] {
			t.Fatal("pages overlap")
		}
	}
	admin.get("/api/v1/projects/" + pid + "/events?cursor=garbage").expect(400)
}

func TestMeUpdateSwitchesUIMode(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	if m := admin.patch("/api/v1/auth/me", map[string]any{"ui_mode": "expert"}).expect(200).Str("user", "ui_mode"); m != "expert" {
		t.Fatal(m)
	}
}

func TestInactiveUserCannotLogin(t *testing.T) {
	srv := server(t)
	adminClient(t, srv)
	if _, err := testdb.Pool(t).Exec(context.Background(), `UPDATE users SET is_active = FALSE WHERE email = $1`, adminEmail); err != nil {
		t.Fatal(err)
	}
	newClient(t, srv).post("/api/v1/auth/login", map[string]any{"email": adminEmail, "password": adminPassword}).expect(401)
}

func TestHealthEndpoints(t *testing.T) {
	srv := server(t)
	anon := newClient(t, srv)
	if s := anon.get("/healthz").expect(200).Str("status"); s != "ok" {
		t.Fatal(s)
	}
	if s := anon.get("/readyz").expect(200).Str("status"); s != "ready" {
		t.Fatal(s)
	}
	if anon.get("/api/v1/system/info").expect(200).Str("version") == "" {
		t.Fatal("no version")
	}
	anon.get("/api/v1/nope").expect(404)
	anon.do("PUT", "/healthz", nil).expect(405)
}

// The server's licences are public plain text, like the OpenAPI document: the same text ships
// in every image and release archive.
func TestLicensesArePublicPlainText(t *testing.T) {
	srv := server(t)
	r := newClient(t, srv).get("/api/v1/system/licenses").expect(200)
	if ct := r.Header.Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Fatalf("content type %q", ct)
	}
	for _, want := range []string{"BeziSync: LICENSE", "Apache License", "github.com/go-chi/chi/v5"} {
		if !strings.Contains(string(r.Raw), want) {
			t.Errorf("licences lack %q", want)
		}
	}
}
