// Package testdb gives tests a real, migrated PostgreSQL database.
//
// Point BEZISYNC_TEST_DATABASE_URL at a server where the user may create databases
// (scripts/dev-db.sh starts one). Each test package gets its own fresh database; tests
// call Reset to start from empty tables.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/logging"
	"github.com/daun-gatal/bezisync/backend/internal/migrate"
	"github.com/daun-gatal/bezisync/backend/internal/ratelimit"
)

// Admin credentials bootstrapped in tests.
const (
	AdminEmail    = "admin@example.com"
	AdminPassword = "correct-horse-battery"
)

// AdminURL is the server URL tests create databases on.
func AdminURL() string {
	return db.NormalizeURL(os.Getenv("BEZISYNC_TEST_DATABASE_URL"))
}

// Main is a TestMain helper: it creates a database, migrates it, configures the process
// to use it, runs the tests and drops the database. Without BEZISYNC_TEST_DATABASE_URL it
// fails, like the Python suite.
func Main(m *testing.M, env map[string]string) int {
	admin := AdminURL()
	if admin == "" {
		fmt.Fprintln(os.Stderr, "Set BEZISYNC_TEST_DATABASE_URL (see scripts/dev-db.sh)")
		return 2
	}
	ctx := context.Background()
	suffix := make([]byte, 5)
	_, _ = rand.Read(suffix)
	name := "bz_test_" + hex.EncodeToString(suffix)
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		fmt.Fprintln(os.Stderr, "test database:", err)
		return 2
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, name)); err != nil {
		fmt.Fprintln(os.Stderr, "create test database:", err)
		return 2
	}
	defer func() {
		db.Close()
		_, _ = conn.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS "%s" WITH (FORCE)`, name))
		_ = conn.Close(ctx)
	}()
	u, _ := url.Parse(admin)
	u.Path = "/" + name
	dbURL := u.String()
	vars := map[string]string{
		"BEZISYNC_DATABASE_URL":             dbURL,
		"BEZISYNC_ENVIRONMENT":              "test",
		"BEZISYNC_BOOTSTRAP_ADMIN_EMAIL":    AdminEmail,
		"BEZISYNC_BOOTSTRAP_ADMIN_PASSWORD": AdminPassword,
		"BEZISYNC_LOG_JSON":                 "false",
	}
	for k, v := range env {
		vars[k] = v
	}
	for k, v := range vars {
		_ = os.Setenv(k, v)
	}
	config.Reset()
	logging.Configure()
	if err := migrate.Up(ctx, dbURL); err != nil {
		fmt.Fprintln(os.Stderr, "migrate test database:", err)
		return 2
	}
	return m.Run()
}

// URL is the current test database URL.
func URL() string { return db.NormalizeURL(config.Get().DatabaseURL) }

// Pool returns the process pool on the test database.
func Pool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	p, err := db.Pool(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// Reset empties every application table.
func Reset(t testing.TB) {
	t.Helper()
	ctx := context.Background()
	p := Pool(t)
	tables, err := db.Select[struct {
		Name string `db:"tablename"`
	}](ctx, p, `SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'goose_db_version'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) == 0 {
		return
	}
	names := make([]string, len(tables))
	for i, tb := range tables {
		names[i] = `"` + tb.Name + `"`
	}
	if _, err := p.Exec(ctx, "TRUNCATE "+strings.Join(names, ", ")+" RESTART IDENTITY CASCADE"); err != nil {
		t.Fatal(err)
	}
	ratelimit.Login.Clear()
	ratelimit.Check.Clear()
	ratelimit.Feedback.Clear()
}
