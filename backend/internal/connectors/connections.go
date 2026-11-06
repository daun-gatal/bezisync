package connectors

import (
	"context"
	"strconv"
	"time"
)

// ConnectionCounter is a source that can say how many connections it allows and how many are
// open: what snapshot threads and readers count against. Sources without it are Unknown.
type ConnectionCounter interface {
	Connections(ctx context.Context, conn Resolved) (limit, used int, err error)
}

// Connections reads PostgreSQL's max_connections (less the superuser reserve) and the open
// connections.
func (s *postgresSource) Connections(ctx context.Context, conn Resolved) (int, int, error) {
	pg, done, err := pgConnect(ctx, conn, 5*time.Second)
	if err != nil {
		return 0, 0, err
	}
	defer done()
	var limit, reserved, used int
	err = pg.QueryRow(ctx, `SELECT current_setting('max_connections')::int, current_setting('superuser_reserved_connections')::int,
		(SELECT count(*) FROM pg_stat_activity)::int`).Scan(&limit, &reserved, &used)
	if err != nil {
		return 0, 0, err
	}
	return limit - reserved, used, nil
}

// Connections reads MySQL's max_connections and Threads_connected.
func (s *mysqlSource) Connections(ctx context.Context, conn Resolved) (int, int, error) {
	c, done, err := myConnect(ctx, conn, 5*time.Second)
	if err != nil {
		return 0, 0, err
	}
	defer done()
	var name, limit, used string
	if err := c.QueryRowContext(ctx, `SHOW VARIABLES LIKE 'max_connections'`).Scan(&name, &limit); err != nil {
		return 0, 0, err
	}
	if err := c.QueryRowContext(ctx, `SHOW STATUS LIKE 'Threads_connected'`).Scan(&name, &used); err != nil {
		return 0, 0, err
	}
	l, err := strconv.Atoi(limit)
	if err != nil {
		return 0, 0, err
	}
	u, err := strconv.Atoi(used)
	return l, u, err
}
