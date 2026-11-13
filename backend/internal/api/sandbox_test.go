package api_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/security"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

const sandboxTeamPassword = "lab-team-password-1"

// ensureSandbox runs EnsureSandbox in its own transaction, as migrate does.
func ensureSandbox(t *testing.T) (bool, error) {
	t.Helper()
	ctx := context.Background()
	var created bool
	err := db.Tx(ctx, func(tx pgx.Tx) error {
		var err error
		created, err = services.EnsureSandbox(ctx, tx)
		return err
	})
	return created, err
}

// sandboxSettings configures the base Sandbox plus the extras given by edit.
func sandboxSettings(t *testing.T, edit func(*config.Settings)) {
	t.Helper()
	s := *config.Get()
	s.SandboxSourceHost, s.SandboxSourcePassword, s.SandboxKafkaBootstrap, s.SandboxDockerNetwork = "demo-shop", "demo-pw", "kafka:9092", "bezisync-demo"
	if edit != nil {
		edit(&s)
	}
	config.Set(&s)
	t.Cleanup(config.Reset)
}

func allSandboxExtras(s *config.Settings) {
	s.SandboxMySQLHost, s.SandboxMySQLPassword = "lab-mysql", "mysql-cdc-pw"
	s.SandboxLegacyHost, s.SandboxLegacyPassword = "lab-legacy", "legacy-pw"
	s.SandboxWarehouseHost, s.SandboxWarehousePassword = "lab-warehouse", "warehouse-pw"
	s.SandboxHTTPURL = "http://lab-http-echo:8080/events"
	s.SandboxTeamPassword = sandboxTeamPassword
}

type sandboxConn struct {
	Name       string  `db:"name"`
	Kind       string  `db:"kind"`
	Type       string  `db:"type"`
	Managed    bool    `db:"managed"`
	SecretsEnc *string `db:"secrets_enc"`
}

func sandboxConnections(t *testing.T) []sandboxConn {
	t.Helper()
	conns, err := db.Select[sandboxConn](context.Background(), testdb.Pool(t),
		`SELECT c.name, c.kind, c.type, c.managed, c.secrets_enc FROM connections c JOIN projects p ON p.id = c.project_id
		 WHERE p.slug = 'sandbox' ORDER BY c.name`)
	if err != nil {
		t.Fatal(err)
	}
	return conns
}

func sandboxTemplates(t *testing.T) map[string]string {
	t.Helper()
	rows, err := db.Select[struct {
		Name string `db:"name"`
		Sink string `db:"sink"`
	}](context.Background(), testdb.Pool(t), `SELECT t.name, c.name AS sink FROM pipeline_templates t JOIN connections c ON c.id = t.sink_id
		JOIN projects p ON p.id = t.project_id WHERE p.slug = 'sandbox'`)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, r := range rows {
		out[r.Name] = r.Sink
	}
	return out
}

func sandboxProjectID(t *testing.T) string {
	t.Helper()
	id, err := db.Scalar[string](context.Background(), testdb.Pool(t), `SELECT id::text FROM projects WHERE slug = 'sandbox'`)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// E1: without the extra settings the Sandbox is what it always was.
func TestSandboxWithoutExtras(t *testing.T) {
	srv := server(t)
	adminClient(t, srv)
	sandboxSettings(t, nil)
	if created, err := ensureSandbox(t); err != nil || !created {
		t.Fatal(created, err)
	}
	conns := sandboxConnections(t)
	if len(conns) != 2 || conns[0].Name != "demo-kafka" || conns[1].Name != "demo-shop" {
		t.Fatal(conns)
	}
	if tpls := sandboxTemplates(t); len(tpls) != 1 || tpls["demo-shop-to-kafka"] != "demo-kafka" {
		t.Fatal(tpls)
	}
	users, err := db.Scalar[int64](context.Background(), testdb.Pool(t), `SELECT count(*) FROM users`)
	if err != nil || users != 1 {
		t.Fatal("expected only the admin", users, err)
	}
}

// E2, E3, E7, E10: every extra, created once, encrypted, and teammates with their roles.
func TestSandboxExtras(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	sandboxSettings(t, allSandboxExtras)
	if created, err := ensureSandbox(t); err != nil || !created {
		t.Fatal(created, err)
	}
	conns := sandboxConnections(t)
	want := []string{"demo-http/sink/http", "demo-kafka/sink/kafka", "demo-mysql/source/mysql", "demo-shop/source/postgresql",
		"demo-warehouse/sink/jdbc", "legacy-postgres/source/postgresql"}
	if len(conns) != len(want) {
		t.Fatal(conns)
	}
	for i, c := range conns {
		if got := c.Name + "/" + c.Kind + "/" + c.Type; got != want[i] || !c.Managed {
			t.Fatalf("connection %d: got %s managed=%v, want %s", i, got, c.Managed, want[i])
		}
		if c.SecretsEnc != nil {
			for _, pw := range []string{"mysql-cdc-pw", "legacy-pw", "warehouse-pw", "demo-pw"} {
				if strings.Contains(*c.SecretsEnc, pw) {
					t.Fatalf("%s: secret stored in clear", c.Name)
				}
			}
		} else if c.Name != "demo-http" && c.Name != "demo-kafka" {
			t.Fatalf("%s: password not stored", c.Name)
		}
	}
	tpls := sandboxTemplates(t)
	if len(tpls) != 3 || tpls["demo-shop-to-warehouse"] != "demo-warehouse" || tpls["demo-shop-to-http"] != "demo-http" {
		t.Fatal(tpls)
	}

	// Each teammate signs in with the team password and holds exactly its role in the Sandbox.
	projectID := sandboxProjectID(t)
	for _, m := range services.SandboxTeam {
		c := login(t, srv, m.Email, sandboxTeamPassword)
		role := ""
		for _, p := range c.get("/api/v1/auth/me").expect(200).List("projects") {
			if at(t, p, "id") == projectID {
				role, _ = at(t, p, "role").(string)
			}
		}
		if role != m.Role {
			t.Fatalf("%s: role %q, want %q", m.Email, role, m.Role)
		}
	}
	// A Viewer can see pipelines but not change them.
	viewer := login(t, srv, "viewer@bezisync.local", sandboxTeamPassword)
	viewer.get("/api/v1/projects/" + projectID + "/connections").expect(200)
	tplID, err := db.Scalar[string](context.Background(), testdb.Pool(t), `SELECT id::text FROM pipeline_templates WHERE name = 'demo-shop-to-kafka'`)
	if err != nil {
		t.Fatal(err)
	}
	viewer.post("/api/v1/projects/"+projectID+"/pipelines", map[string]any{"name": "viewer-try", "source_id": connectionID(t, "demo-shop"),
		"template_id": tplID}).expect(403)
	audits, err := db.Scalar[int64](context.Background(), testdb.Pool(t), `SELECT count(*) FROM events WHERE kind = 'member.upserted'`)
	if err != nil || audits != 3 {
		t.Fatal("member.upserted audits", audits, err)
	}

	// Created once: a second run, or a run after a visitor deleted a piece, changes nothing.
	admin.del("/api/v1/projects/" + projectID + "/connections/" + connectionID(t, "demo-mysql")).expect(204)
	if created, err := ensureSandbox(t); err != nil || created {
		t.Fatal("second run created", created, err)
	}
	if n := len(sandboxConnections(t)); n != 5 {
		t.Fatal("deleted connection came back or others changed", n)
	}
}

func connectionID(t *testing.T, name string) string {
	t.Helper()
	id, err := db.Scalar[string](context.Background(), testdb.Pool(t), `SELECT id::text FROM connections WHERE name = $1`, name)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// E4: a teammate whose email is taken is left as it is; the others are still created.
func TestSandboxTeamEmailTaken(t *testing.T) {
	srv := server(t)
	adminClient(t, srv)
	// Inserted directly: the API refuses .local addresses for new users, but an install may
	// already hold one (the bootstrap admin is admin@bezisync.local).
	hash, err := security.HashPassword("their-own-password-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := testdb.Pool(t).Exec(context.Background(), `INSERT INTO users (id, email, name, password_hash, is_superuser, is_active, ui_mode,
		muted_notifications, created_at, updated_at) VALUES (gen_random_uuid(), 'editor@bezisync.local', 'Someone Else', $1, false, true, 'guided',
		'[]', now(), now())`, hash); err != nil {
		t.Fatal(err)
	}
	sandboxSettings(t, func(s *config.Settings) { s.SandboxTeamPassword = sandboxTeamPassword })
	if created, err := ensureSandbox(t); err != nil || !created {
		t.Fatal(created, err)
	}
	login(t, srv, "editor@bezisync.local", "their-own-password-1")
	c := newClient(t, srv)
	c.post("/api/v1/auth/login", map[string]any{"email": "editor@bezisync.local", "password": sandboxTeamPassword}).expect(401)
	members, err := db.Scalar[int64](context.Background(), testdb.Pool(t),
		`SELECT count(*) FROM memberships m JOIN users u ON u.id = m.user_id JOIN projects p ON p.id = m.project_id
		 WHERE p.slug = 'sandbox' AND u.email = 'editor@bezisync.local'`)
	if err != nil || members != 0 {
		t.Fatal("existing user got a sandbox membership", members, err)
	}
	login(t, srv, "viewer@bezisync.local", sandboxTeamPassword)
}

// E5, E6: a weak team password or an invalid extra fails the whole run; nothing is half-created.
func TestSandboxRefusesBadExtras(t *testing.T) {
	cases := map[string]func(*config.Settings){
		"weak team password":  func(s *config.Settings) { s.SandboxTeamPassword = "password" },
		"short team password": func(s *config.Settings) { s.SandboxTeamPassword = "short-pw" },
		"bad port":            func(s *config.Settings) { s.SandboxMySQLHost, s.SandboxMySQLPort = "lab-mysql", 70000 },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			srv := server(t)
			adminClient(t, srv)
			sandboxSettings(t, edit)
			if created, err := ensureSandbox(t); err == nil || created {
				t.Fatal("accepted", created, err)
			}
			projects, err := db.Scalar[int64](context.Background(), testdb.Pool(t), `SELECT count(*) FROM projects WHERE is_sandbox IS TRUE`)
			if err != nil || projects != 0 {
				t.Fatal("a sandbox was left behind", projects, err)
			}
		})
	}
}

func TestSandboxTeamPasswordProblem(t *testing.T) {
	for pw, ok := range map[string]bool{"admin": false, "password": false, "eleven-char": false, sandboxTeamPassword: true,
		strings.Repeat("x", 1024): true, strings.Repeat("x", 1025): false} {
		if got := services.SandboxTeamPasswordProblem(pw) == ""; got != ok {
			t.Errorf("%q: usable=%v, want %v", pw, got, ok)
		}
	}
}
