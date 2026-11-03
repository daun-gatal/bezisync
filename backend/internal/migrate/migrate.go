// Package migrate applies the database schema with goose (embedded SQL migrations).
//
// 00001_baseline.sql is the schema exactly as the last Alembic revision (0018) left it.
// A database Alembic already brought to 0018 is stamped with the baseline without running
// any DDL; an older Alembic revision is refused (upgrade with the previous release first);
// an empty database gets the baseline.
package migrate

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"log/slog"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver for goose
	"github.com/pressly/goose/v3"

	"github.com/daun-gatal/bezisync/backend/internal/db"
)

//go:embed sql/*.sql
var files embed.FS

// LockID serialises concurrent `bezisync migrate` runs ("bezi"), shared with the Python
// implementation so the two never migrate at the same time.
const LockID = 0x62657A69

// AlembicHead is the last Alembic revision; the goose baseline equals its schema.
const AlembicHead = "0018"

const baselineVersion = 1

// Up brings the database at url to the latest schema.
func Up(ctx context.Context, url string) error {
	conn, err := sql.Open("pgx", db.NormalizeURL(url))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	// One physical session for everything: the advisory lock is session-scoped.
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", LockID); err != nil {
		return err
	}
	defer func() { _, _ = conn.ExecContext(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", LockID) }()

	if err := adoptAlembic(ctx, conn); err != nil {
		return err
	}
	goose.SetBaseFS(files)
	goose.SetLogger(gooseLogger{})
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.UpContext(ctx, conn, "sql")
}

// adoptAlembic stamps the baseline on databases Alembic created.
func adoptAlembic(ctx context.Context, c *sql.DB) error {
	var hasAlembic bool
	if err := c.QueryRowContext(ctx, `SELECT to_regclass('public.alembic_version') IS NOT NULL`).Scan(&hasAlembic); err != nil {
		return err
	}
	if !hasAlembic {
		return nil
	}
	var hasGoose bool
	if err := c.QueryRowContext(ctx, `SELECT to_regclass('public.goose_db_version') IS NOT NULL`).Scan(&hasGoose); err != nil {
		return err
	}
	if hasGoose {
		return nil
	}
	var rev string
	err := c.QueryRowContext(ctx, `SELECT version_num FROM alembic_version`).Scan(&rev)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if rev != AlembicHead {
		return fmt.Errorf("the database is at Alembic revision %q; upgrade it with BeziSync v0.1.1 (revision %s) before running this version", rev, AlembicHead)
	}
	tx, err := c.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	stmts := []string{
		`CREATE TABLE goose_db_version (
			id serial PRIMARY KEY,
			version_id bigint NOT NULL,
			is_applied boolean NOT NULL,
			tstamp timestamp NOT NULL DEFAULT now()
		)`,
		`INSERT INTO goose_db_version (version_id, is_applied) VALUES (0, true)`,
	}
	for _, s := range stmts {
		if _, err := tx.ExecContext(ctx, s); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO goose_db_version (version_id, is_applied) VALUES ($1, true)`, baselineVersion); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	slog.Info("adopted the Alembic schema as the goose baseline", "alembic_revision", rev)
	return nil
}

type gooseLogger struct{}

func (gooseLogger) Fatalf(format string, v ...any) { slog.Error(fmt.Sprintf(format, v...)) }
func (gooseLogger) Printf(format string, v ...any) { slog.Info(fmt.Sprintf(format, v...)) }
