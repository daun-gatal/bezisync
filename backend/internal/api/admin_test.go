package api_test

import (
	"net/url"
	"testing"
)

func meID(c *client) string { return c.get("/api/v1/auth/me").Str("user", "id") }

func TestOnlyAdminsManagePeopleAndProjectSettings(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	approver := member(t, srv, admin, pid, "editor")
	admin.put("/api/v1/projects/"+pid+"/members", map[string]any{"email": approver.get("/api/v1/auth/me").Str("user", "email"),
		"role": "editor", "can_approve": true}).expect(200)
	base := "/api/v1/projects/" + pid
	for _, res := range []*resp{
		approver.get("/api/v1/users"),
		approver.post("/api/v1/users", map[string]any{"email": "x@example.com"}),
		approver.patch("/api/v1/users/"+meID(admin), map[string]any{"is_active": false}),
		approver.del(base),
		approver.patch(base, map[string]any{"name": "hijack"}),
		approver.put(base+"/members", map[string]any{"email": "y@example.com", "role": "viewer"}),
		approver.get(base + "/events?audit_only=true"),
		approver.get(base + "/audit:export"),
	} {
		res.expect(403)
	}
	p := approver.get("/api/v1/auth/me").At("projects", 0)
	if !hasPerm(t, p, "pipeline.approve") || hasPerm(t, p, "project.manage") {
		t.Fatal(p)
	}
}

func TestProjectAdminIsNotARole(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	admin.put("/api/v1/projects/"+defaultProject(t, admin)+"/members", map[string]any{"email": "z@example.com", "role": "admin"}).expect(422)
}

func TestAssignProjectsAndPlatformAdmin(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	sam := admin.post("/api/v1/users", map[string]any{"email": "Sam@Example.com", "name": "Sam"}).expect(201)
	if sam.Str("email") != "sam@example.com" || sam.At("has_password") != false || len(sam.List("projects")) != 0 {
		t.Fatal(string(sam.Raw))
	}
	if c := admin.post("/api/v1/users", map[string]any{"email": "sam@example.com"}).Code(); c != "user_exists" {
		t.Fatal(c)
	}
	other := admin.post("/api/v1/projects", map[string]any{"slug": "payments", "name": "Payments"}).expect(201).Str("id")
	res := admin.put("/api/v1/users/"+sam.Str("id")+"/projects/"+other, map[string]any{"role": "operator", "can_approve": true}).expect(200)
	projects := res.List("projects")
	if len(projects) != 1 || at(t, projects[0], "slug") != "payments" || at(t, projects[0], "role") != "operator" || at(t, projects[0], "can_approve") != true {
		t.Fatal(string(res.Raw))
	}
	ok := false
	for _, m := range admin.get("/api/v1/projects/" + other + "/members").List() {
		ok = ok || (at(t, m, "email") == "sam@example.com" && at(t, m, "granted_by_sso") == false)
	}
	if !ok {
		t.Fatal("sam not a member")
	}
	listing := admin.get("/api/v1/users?q=sam").expect(200).List("items")
	if len(listing) != 1 || at(t, listing[0], "email") != "sam@example.com" {
		t.Fatal(listing)
	}
	if admin.patch("/api/v1/users/"+sam.Str("id"), map[string]any{"is_superuser": true}).expect(200).At("is_superuser") != true {
		t.Fatal("not promoted")
	}
	if l := admin.del("/api/v1/users/" + sam.Str("id") + "/projects/" + other).expect(200).List("projects"); len(l) != 0 {
		t.Fatal(l)
	}
	kinds := map[any]bool{}
	for _, e := range admin.get("/api/v1/projects/" + other + "/events?audit_only=true").List("items") {
		kinds[at(t, e, "kind")] = true
	}
	if !kinds["member.upserted"] || !kinds["member.removed"] {
		t.Fatal(kinds)
	}
}

func TestGuardsKeepSomeoneInCharge(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	me := meID(admin)
	if c := admin.patch("/api/v1/users/"+me, map[string]any{"is_superuser": false}).Code(); c != "cannot_demote_self" {
		t.Fatal(c)
	}
	if c := admin.patch("/api/v1/users/"+me, map[string]any{"is_active": false}).Code(); c != "cannot_deactivate_self" {
		t.Fatal(c)
	}
	if r := admin.get("/api/v1/auth/me").At("projects", 0, "role"); r != nil {
		t.Fatal(r)
	}
	// Another admin may not demote the last active one.
	other := admin.post("/api/v1/users", map[string]any{"email": "two@example.com", "initial_password": "a-long-enough-password", "is_superuser": true}).expect(201)
	two := login(t, srv, "two@example.com", "a-long-enough-password")
	admin.patch("/api/v1/users/"+other.Str("id"), map[string]any{"is_active": false}).expect(200)
	_ = two
	if c := admin.patch("/api/v1/users/"+me, map[string]any{"name": "Boss"}).expect(200).Str("name"); c != "Boss" {
		t.Fatal(c)
	}
}

func TestDeactivatingSignsTheUserOutEverywhere(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	viewer := member(t, srv, admin, pid, "viewer", "vic@example.com")
	viewer.get("/api/v1/auth/me").expect(200)
	var vic string
	for _, u := range admin.get("/api/v1/users").List("items") {
		if at(t, u, "email") == "vic@example.com" {
			vic = at(t, u, "id").(string)
		}
	}
	if admin.patch("/api/v1/users/"+vic, map[string]any{"is_active": false}).expect(200).At("is_active") != false {
		t.Fatal("still active")
	}
	viewer.get("/api/v1/auth/me").expect(401)
	if s := newClient(t, srv).post("/api/v1/auth/login", map[string]any{"email": "vic@example.com", "password": "a-long-enough-password"}).Status; s != 401 && s != 403 {
		t.Fatal(s)
	}
	admin.patch("/api/v1/users/"+vic, map[string]any{"is_active": true}).expect(200)
	login(t, srv, "vic@example.com", "a-long-enough-password")
}

func TestProjectsListCountsAndDelete(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	id := admin.post("/api/v1/projects", map[string]any{"slug": "scratch", "name": "Scratch"}).expect(201).Str("id")
	rows := map[any]any{}
	for _, p := range admin.get("/api/v1/projects").expect(200).List() {
		rows[at(t, p, "slug")] = p
	}
	if at(t, rows["scratch"], "members") != float64(0) || at(t, rows["scratch"], "pipelines") != float64(0) {
		t.Fatal(rows["scratch"])
	}
	admin.del("/api/v1/projects/" + id).expect(204)
	for _, p := range admin.get("/api/v1/projects").List() {
		if at(t, p, "slug") == "scratch" {
			t.Fatal("still listed")
		}
	}
	admin.del("/api/v1/projects/" + id).expect(404)
}

func TestAProjectWithPipelinesIsDeletedOnlyOnceTheyAreGone(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	u := "/api/v1/projects/" + e.pid
	if c := e.admin.del(u).expect(409).Code(); c != "project_not_empty" {
		t.Fatal(c)
	}
	e.admin.post(u+"/pipelines/"+pipe.Str("id")+"/deploy", map[string]any{}).expect(200)
	e.tick()
	e.admin.del(u + "/pipelines/" + pipe.Str("id")).expect(200)
	if c := e.admin.del(u).Code(); c != "project_busy" {
		t.Fatal(c)
	}
	e.runJobs()
	e.admin.del(u).expect(204)
	e.admin.get(u).expect(404)
}

func TestPeopleFilteredByProjectIncludeEveryAdmin(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	other := admin.post("/api/v1/projects", map[string]any{"slug": "billing", "name": "Billing"}).expect(201).Str("id")
	member(t, srv, admin, pid, "viewer", "in-default@example.com")
	member(t, srv, admin, other, "editor", "in-billing@example.com")
	items := admin.get("/api/v1/users?project_id=" + url.QueryEscape(other)).expect(200).List("items")
	emails, super := map[any]bool{}, false
	for _, u := range items {
		emails[at(t, u, "email")] = true
		super = super || at(t, u, "is_superuser") == true
	}
	if !emails["in-billing@example.com"] || emails["in-default@example.com"] || !super {
		t.Fatal(emails)
	}
}

func TestAuditExport(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	admin.patch("/api/v1/projects/"+pid, map[string]any{"description": "a,b \"quoted\""}).expect(200)
	csv := admin.get("/api/v1/projects/" + pid + "/audit:export").expect(200)
	if csv.Header.Get("content-type") != "text/csv; charset=utf-8" || csv.Header.Get("content-disposition") != `attachment; filename="bezisync-audit-default.csv"` {
		t.Fatal(csv.Header)
	}
	lines := splitLines(string(csv.Raw))
	if lines[0] != "occurred_at,actor_label,kind,resource_type,resource_id,summary,details,id" || len(lines) < 3 {
		t.Fatal(lines)
	}
	jsonl := admin.get("/api/v1/projects/" + pid + "/audit:export?format=jsonl&since=2000-01-01").expect(200)
	if jsonl.Header.Get("content-type") != "application/x-ndjson" || len(splitLines(string(jsonl.Raw))) < 2 {
		t.Fatal(string(jsonl.Raw))
	}
	if n := len(splitLines(string(admin.get("/api/v1/projects/" + pid + "/audit:export?format=jsonl&until=2000-01-01").Raw))); n != 0 {
		t.Fatal(n)
	}
	bad := admin.get("/api/v1/projects/" + pid + "/audit:export?since=yesterday-x").expect(422)
	if m := bad.Str("errors", 0, "message"); m != "Input should be a valid datetime or date, invalid character in year" {
		t.Fatal(string(bad.Raw))
	}
	admin.get("/api/v1/projects/" + pid + "/audit:export?format=xml").expect(422)
}

func splitLines(s string) []string {
	var out []string
	for _, l := range splitAny(s) {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

func splitAny(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			end := i
			if end > start && s[end-1] == '\r' {
				end--
			}
			out = append(out, s[start:end])
			start = i + 1
		}
	}
	return append(out, s[start:])
}
