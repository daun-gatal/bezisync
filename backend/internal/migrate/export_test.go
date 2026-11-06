package migrate

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"

	"github.com/daun-gatal/bezisync/backend/internal/db"
)

// GooseTo moves the database at url to version (up or down), for data-migration tests.
func GooseTo(ctx context.Context, url string, version int64, down bool) error {
	conn, err := sql.Open("pgx", db.NormalizeURL(url))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	goose.SetBaseFS(files)
	goose.SetLogger(gooseLogger{})
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	if down {
		return goose.DownToContext(ctx, conn, "sql", version)
	}
	return goose.UpToContext(ctx, conn, "sql", version)
}
