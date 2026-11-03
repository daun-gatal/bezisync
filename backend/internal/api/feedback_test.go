package api_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

func sendFeedback(c *client, projectID string, overrides map[string]any) *resp {
	body := map[string]any{"kind": "bug", "message": "The deploy button stays disabled.", "page": "/p/default/pipelines"}
	for k, v := range overrides {
		body[k] = v
	}
	return c.post("/api/v1/projects/"+projectID+"/feedback", body, "user-agent", "Mozilla/5.0 (Macintosh) Chrome/131.0")
}

func TestAnyoneInAProjectSendsFeedbackAndAdminsHearAboutIt(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	viewer := member(t, srv, admin, pid, "viewer")
	sent := sendFeedback(viewer, pid, map[string]any{"kind": "security", "message": "  <script>alert(1)</script>  "}).expect(201)
	if sent.Str("message") != "<script>alert(1)</script>" || sent.Str("page") != "/p/default/pipelines" ||
		!strings.HasPrefix(sent.Str("user_agent"), "Mozilla/5.0") || !strings.HasPrefix(sent.Str("email"), "viewer-") || sent.At("resolved_at") != nil {
		t.Fatal(string(sent.Raw))
	}
	var notes []any
	for _, n := range admin.get("/api/v1/projects/" + pid + "/notifications").expect(200).List("items") {
		if at(t, n, "kind") == "feedback_received" {
			notes = append(notes, n)
		}
	}
	if len(notes) != 1 || at(t, notes[0], "title") != "New security report from "+sent.Str("email") ||
		!strings.HasSuffix(at(t, notes[0], "link").(string), "/settings/feedback") {
		t.Fatal(notes)
	}
	if l := viewer.get("/api/v1/projects/" + pid + "/notifications").List("items"); len(l) != 0 {
		t.Fatal(l)
	}
	var events []any
	for _, e := range admin.get("/api/v1/projects/" + pid + "/events?audit_only=true").List("items") {
		if at(t, e, "kind") == "feedback.sent" {
			events = append(events, e)
		}
	}
	if len(events) != 1 {
		t.Fatal(events)
	}
	if raw := admin.get("/api/v1/projects/" + pid + "/events?audit_only=true").Raw; strings.Contains(string(raw), "script") {
		t.Fatal("message reached the audit log")
	}
	// Read-all clears the unread count.
	if admin.get("/api/v1/projects/"+pid+"/notifications").At("unread") != float64(1) {
		t.Fatal("unread")
	}
	admin.post("/api/v1/projects/"+pid+"/notifications/read-all", nil).expect(204)
	if admin.get("/api/v1/projects/"+pid+"/notifications").At("unread") != float64(0) {
		t.Fatal("still unread")
	}
}

func TestFeedbackIsValidated(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	editor := member(t, srv, admin, pid, "editor")
	sendFeedback(editor, pid, map[string]any{"message": "   "}).expect(422)
	sendFeedback(editor, pid, map[string]any{"message": strings.Repeat("x", 5001)}).expect(422)
	sendFeedback(editor, pid, map[string]any{"kind": "praise"}).expect(422)
	if p := sendFeedback(editor, pid, map[string]any{"page": "//evil.example/x"}).expect(201).Str("page"); p != "" {
		t.Fatal(p)
	}
	if p := sendFeedback(editor, pid, map[string]any{"page": "https://evil.example/x"}).expect(201).Str("page"); p != "" {
		t.Fatal(p)
	}
}

func TestFeedbackNeedsAccessToTheProject(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	other := admin.post("/api/v1/projects", map[string]any{"slug": "other", "name": "Other"}).expect(201).Str("id")
	editor := member(t, srv, admin, pid, "editor")
	sendFeedback(editor, other, nil).expect(404)
}

func TestFeedbackIsRateLimitedPerPerson(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	editor := member(t, srv, admin, pid, "editor")
	for range 10 {
		sendFeedback(editor, pid, nil).expect(201)
	}
	res := sendFeedback(editor, pid, nil).expect(429)
	if !strings.Contains(res.Str("detail"), "this hour") {
		t.Fatal(string(res.Raw))
	}
	sendFeedback(member(t, srv, admin, pid, "viewer"), pid, nil).expect(201)
}

func TestAPITokensCannotSendFeedback(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	token := admin.post("/api/v1/auth/tokens", map[string]any{"name": "ci", "project_id": pid}).expect(201).Str("token")
	res := newClient(t, srv).post("/api/v1/projects/"+pid+"/feedback", map[string]any{"kind": "idea", "message": "hi"}, "authorization", "Bearer "+token).expect(403)
	if res.Code() != "session_required" {
		t.Fatal(res.Code())
	}
}

func TestOnlyAdminsReadAndResolveFeedback(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	editor := member(t, srv, admin, pid, "editor")
	bug := sendFeedback(editor, pid, nil).expect(201).Str("id")
	sendFeedback(editor, pid, map[string]any{"kind": "idea", "message": "Show the properties before deploy."}).expect(201)
	editor.get("/api/v1/feedback").expect(403)
	editor.patch("/api/v1/feedback/"+bug, map[string]any{"resolved": true}).expect(403)
	page := admin.get("/api/v1/feedback").expect(200)
	kinds := func(r *resp) []any {
		var out []any
		for _, i := range r.List("items") {
			out = append(out, at(t, i, "kind"))
		}
		return out
	}
	if page.At("open") != float64(2) || page.At("resolved") != float64(0) || len(kinds(page)) != 2 || kinds(page)[0] != "idea" ||
		page.Str("items", 0, "project_name") == "" {
		t.Fatal(string(page.Raw))
	}
	if k := kinds(admin.get("/api/v1/feedback?kind=bug")); len(k) != 1 || k[0] != "bug" {
		t.Fatal(k)
	}
	res := admin.patch("/api/v1/feedback/"+bug, map[string]any{"resolved": true}).expect(200)
	if res.Str("resolved_by") != adminEmail {
		t.Fatal(string(res.Raw))
	}
	page = admin.get("/api/v1/feedback")
	if page.At("open") != float64(1) || page.At("resolved") != float64(1) || len(kinds(page)) != 1 {
		t.Fatal(string(page.Raw))
	}
	if l := admin.get("/api/v1/feedback?status=resolved").List("items"); len(l) != 1 || at(t, l[0], "id") != bug {
		t.Fatal(l)
	}
	if l := admin.get("/api/v1/feedback?status=all").List("items"); len(l) != 2 {
		t.Fatal(l)
	}
	res = admin.patch("/api/v1/feedback/"+bug, map[string]any{"resolved": false}).expect(200)
	if res.At("resolved_at") != nil || res.At("resolved_by") != nil {
		t.Fatal(string(res.Raw))
	}
	ev := map[any]bool{}
	for _, e := range admin.get("/api/v1/projects/" + pid + "/events?audit_only=true").List("items") {
		ev[at(t, e, "kind")] = true
	}
	if !ev["feedback.resolved"] || !ev["feedback.reopened"] {
		t.Fatal(ev)
	}
	admin.patch("/api/v1/feedback/00000000-0000-0000-0000-000000000000", map[string]any{"resolved": true}).expect(404)
	admin.get("/api/v1/feedback?status=bogus").expect(422)
}

func TestMutedKindsAreNotDelivered(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	me := admin.get("/api/v1/auth/me")
	if l := me.List("user", "muted_notifications"); len(l) != 0 {
		t.Fatal(l)
	}
	res := admin.patch("/api/v1/auth/me", map[string]any{"muted_notifications": []string{"alert_resolved", "alert_resolved"}}).expect(200)
	if l := res.List("user", "muted_notifications"); len(l) != 1 || l[0] != "alert_resolved" {
		t.Fatal(l)
	}
	admin.patch("/api/v1/auth/me", map[string]any{"muted_notifications": []string{"nope"}}).expect(422)
	ctx := context.Background()
	q := testdb.Pool(t)
	user := uuid.MustParse(me.Str("user", "id"))
	project := uuid.MustParse(me.Str("projects", 0, "id"))
	notify := func(kind string) int {
		n, err := services.Notify(ctx, q, []uuid.UUID{user}, services.Notification{ProjectID: project, Kind: kind, Title: "t"})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if notify("alert_resolved") != 0 || notify("alert_fired") != 1 {
		t.Fatal("muting ignored")
	}
	kinds, err := db.Select[struct {
		Kind string `db:"kind"`
	}](ctx, q, `SELECT kind FROM notifications WHERE user_id = $1`, user)
	if err != nil || len(kinds) != 1 || kinds[0].Kind != "alert_fired" {
		t.Fatal(kinds, err)
	}
	admin.patch("/api/v1/auth/me", map[string]any{"muted_notifications": []string{}}).expect(200)
	if notify("alert_resolved") != 1 {
		t.Fatal("unmuted kind not delivered")
	}
}
