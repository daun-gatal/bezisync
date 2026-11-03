package migrate_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/migrate"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

func freshDB(t *testing.T) string {
	t.Helper()
	admin := testdb.AdminURL()
	if admin == "" {
		t.Skip("BEZISYNC_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	suffix := make([]byte, 5)
	_, _ = rand.Read(suffix)
	name := "bz_mig_" + hex.EncodeToString(suffix)
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, name)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS "%s" WITH (FORCE)`, name))
		_ = conn.Close(ctx)
	})
	u, _ := url.Parse(admin)
	u.Path = "/" + name
	return u.String()
}

// columns describes the schema as rows of "table.column type null default" plus
// constraints and indexes, independent of dump formatting.
func describe(t *testing.T, dbURL string) string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var b strings.Builder
	queries := []string{
		`SELECT table_name||'.'||column_name||' '||data_type||coalesce('('||character_maximum_length||')','')||' '||is_nullable||' '||coalesce(column_default,'')
		 FROM information_schema.columns WHERE table_schema='public' AND table_name NOT IN ('alembic_version','goose_db_version') ORDER BY 1`,
		`SELECT conrelid::regclass||' '||conname||' '||pg_get_constraintdef(oid) FROM pg_constraint
		 WHERE connamespace='public'::regnamespace AND conrelid::regclass::text NOT IN ('alembic_version','goose_db_version') ORDER BY 1`,
		`SELECT indexdef FROM pg_indexes WHERE schemaname='public' AND tablename NOT IN ('alembic_version','goose_db_version') ORDER BY 1`,
	}
	for _, q := range queries {
		rows, err := conn.Query(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatal(err)
			}
			b.WriteString(s + "\n")
		}
		rows.Close()
	}
	return b.String()
}

func TestBaselineEqualsAlembicHead(t *testing.T) {
	alembicURL := os.Getenv("BEZISYNC_ALEMBIC_DATABASE_URL")
	goURL := freshDB(t)
	if err := migrate.Up(context.Background(), goURL); err != nil {
		t.Fatal(err)
	}
	got := describe(t, goURL)
	if !strings.Contains(got, "pipelines.deployed_image character varying(300)") {
		t.Fatalf("baseline looks incomplete:\n%s", got)
	}
	if alembicURL == "" {
		// Without a Python-migrated database to compare to, check the dump is stable.
		if os.Getenv("UPDATE_GOLDEN") != "" {
			if err := os.WriteFile("testdata/schema.txt", []byte(got), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		golden, err := os.ReadFile("testdata/schema.txt")
		if err != nil {
			t.Fatal(err)
		}
		if got != string(golden) {
			t.Fatalf("schema drifted from testdata/schema.txt:\n%s", got)
		}
		return
	}
	want := describe(t, alembicURL)
	if got != want {
		t.Fatalf("goose baseline differs from Alembic head\n--- goose\n%s\n--- alembic\n%s", got, want)
	}
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile("testdata/schema.txt", []byte(got), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAdoptsAlembicDatabaseWithoutDDL(t *testing.T) {
	dbURL := freshDB(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	// A database Alembic brought to head: its schema (the baseline) plus the marker table.
	// Adoption must stamp the baseline, not run it again (that would fail: the tables exist),
	// and later migrations then run as usual.
	baseline, err := os.ReadFile("sql/00001_baseline.sql")
	if err != nil {
		t.Fatal(err)
	}
	up, _, _ := strings.Cut(string(baseline), "-- +goose Down")
	for _, s := range []string{
		up,
		`CREATE TABLE alembic_version (version_num varchar(32) PRIMARY KEY)`,
		`INSERT INTO alembic_version VALUES ('` + migrate.AlembicHead + `')`,
	} {
		if _, err := conn.Exec(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	if err := migrate.Up(ctx, dbURL); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='sso_providers'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("migrations after the baseline did not run")
	}
	// The baseline is stamped; later migrations then run as usual.
	var stamped bool
	if err := conn.QueryRow(ctx, `SELECT coalesce(bool_or(version_id = 1), false) FROM goose_db_version WHERE is_applied`).Scan(&stamped); err != nil {
		t.Fatal(err)
	}
	if !stamped {
		t.Fatal("the baseline was not stamped")
	}
	// Idempotent.
	if err := migrate.Up(ctx, dbURL); err != nil {
		t.Fatal(err)
	}
}

func TestRefusesOlderAlembicRevision(t *testing.T) {
	dbURL := freshDB(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, s := range []string{
		`CREATE TABLE alembic_version (version_num varchar(32) PRIMARY KEY)`,
		`INSERT INTO alembic_version VALUES ('0017')`,
	} {
		if _, err := conn.Exec(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	err = migrate.Up(ctx, dbURL)
	if err == nil || !regexp.MustCompile(`0017`).MatchString(err.Error()) {
		t.Fatalf("expected a refusal naming revision 0017, got %v", err)
	}
}
