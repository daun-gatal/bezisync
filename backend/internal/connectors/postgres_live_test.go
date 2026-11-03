package connectors_test

import (
	"context"
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/contract"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

// The fixture database the Python reports were captured against (scripts/contract,
// section pg_live). Creating it is idempotent so the test can run anywhere.
const pgFixture = `
CREATE TABLE IF NOT EXISTS public.orders (id bigserial PRIMARY KEY, email text, total numeric NOT NULL);
CREATE TABLE IF NOT EXISTS public.nokey (a int, b text);
CREATE TABLE IF NOT EXISTS public.ident (id int PRIMARY KEY);
ALTER TABLE public.ident REPLICA IDENTITY NOTHING;
CREATE SCHEMA IF NOT EXISTS app;
CREATE TABLE IF NOT EXISTS app.users (user_id uuid PRIMARY KEY, phone_number text, birth_date date);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='limited') THEN CREATE ROLE limited LOGIN PASSWORD 'limited-pw'; END IF; END $$;
GRANT SELECT ON public.orders TO limited;`

func setupPGFixture(t *testing.T) (string, int) {
	t.Helper()
	admin := testdb.AdminURL()
	if admin == "" {
		t.Skip("BEZISYNC_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	var exists bool
	_ = c.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname='contract_src')").Scan(&exists)
	if !exists {
		if _, err := c.Exec(ctx, "CREATE DATABASE contract_src"); err != nil {
			t.Fatal(err)
		}
	}
	u, _ := url.Parse(admin)
	u.Path = "/contract_src"
	src, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close(ctx) }()
	if _, err := src.Exec(ctx, pgFixture); err != nil {
		t.Fatal(err)
	}
	port := 5432
	if p := u.Port(); p != "" {
		_ = json.Unmarshal([]byte(p), &port)
	}
	return u.Hostname(), port
}

var latencyRe = regexp.MustCompile(` in \d+ ms\.`)

func normalizeReport(t *testing.T, raw []byte, ignoreTechnical bool) string {
	t.Helper()
	g, err := pyjson.DecodeGeneric(raw)
	if err != nil {
		t.Fatal(err)
	}
	m := g.(map[string]any)
	delete(m, "duration_ms")
	delete(m, "checked_at")
	for _, c := range m["checks"].([]any) {
		chk := c.(map[string]any)
		chk["message"] = latencyRe.ReplaceAllString(chk["message"].(string), " in N ms.")
		chk["message"] = regexp.MustCompile(`Connected to [^ ]+:\d+`).ReplaceAllString(chk["message"].(string), "Connected to HOST")
		if ignoreTechnical || chk["status"] == "fail" && strings.HasPrefix(chk["code"].(string), "pg_login") {
			delete(chk, "technical")
		}
	}
	s, _ := pyjson.Dumps(m)
	return s
}

func TestPostgresCheckMatchesPythonLive(t *testing.T) {
	host, port := setupPGFixture(t)
	var fx struct {
		Cases []struct {
			Label   string         `json:"label"`
			Config  map[string]any `json:"config"`
			Secrets map[string]any `json:"secrets"`
			Tables  []string       `json:"tables"`
			Report  json.RawMessage
		} `json:"cases"`
		Discovery json.RawMessage `json:"discovery"`
	}
	contract.Load(t, "pg_live", &fx)
	s, err := config.Load(func(k string) (string, bool) {
		if k == "BEZISYNC_SSRF_ALLOW_CIDRS" {
			return `["127.0.0.0/8", "172.16.0.0/12", "10.0.0.0/8"]`, true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	config.Set(s)
	t.Cleanup(config.Reset)
	src, _ := connectors.GetSource("postgresql")
	ctx := context.Background()
	for _, c := range fx.Cases {
		c.Config["host"], c.Config["port"] = host, port
		conn := connectors.Resolved{Type: "postgresql", Provider: "self_hosted", Config: c.Config, Secrets: c.Secrets}
		report, err := src.Check(ctx, conn, c.Tables)
		if err != nil {
			t.Fatalf("%s: %v", c.Label, err)
		}
		got, _ := json.Marshal(report)
		if a, b := normalizeReport(t, got, false), normalizeReport(t, c.Report, false); a != b {
			t.Errorf("%s:\n got %s\nwant %s", c.Label, a, b)
		}
	}
	disc, err := src.Discover(ctx, connectors.Resolved{Type: "postgresql", Config: map[string]any{
		"host": host, "port": port, "database": "contract_src", "user": "bezisync", "ssl_mode": "disable",
	}, Secrets: map[string]any{"password": "bezisync"}})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := json.Marshal(disc)
	gg, _ := pyjson.DecodeGeneric(got)
	wg, _ := pyjson.DecodeGeneric(fx.Discovery)
	delete(gg.(map[string]any), "discovered_at")
	delete(wg.(map[string]any), "discovered_at")
	// Sizes and row estimates depend on the server's statistics; compare the shape.
	for _, d := range []any{gg, wg} {
		for _, tb := range d.(map[string]any)["tables"].([]any) {
			delete(tb.(map[string]any), "bytes_estimate")
			delete(tb.(map[string]any), "rows_estimate")
		}
	}
	a, _ := pyjson.Dumps(gg)
	b, _ := pyjson.Dumps(wg)
	if a != b {
		t.Errorf("discovery\n got %s\nwant %s", a, b)
	}
}
