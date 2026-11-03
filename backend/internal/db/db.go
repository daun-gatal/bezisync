// Package db owns the PostgreSQL connection pool, transactions and JSON column types.
//
// The schema is the one Alembic created (frozen as the goose baseline); queries are plain
// SQL scanned into the structs in internal/models.
package db

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
)

// Querier is satisfied by the pool, a connection and a transaction.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var (
	poolMu sync.Mutex
	pool   *pgxpool.Pool
)

// NormalizeURL turns a SQLAlchemy URL (postgresql+psycopg://…) into a libpq URL.
func NormalizeURL(raw string) string {
	for _, prefix := range []string{"postgresql+psycopg://", "postgresql+psycopg2://", "postgres+psycopg://"} {
		if strings.HasPrefix(raw, prefix) {
			return "postgresql://" + strings.TrimPrefix(raw, prefix)
		}
	}
	return raw
}

// Pool returns the process pool, connecting on first use.
func Pool(ctx context.Context) (*pgxpool.Pool, error) {
	poolMu.Lock()
	defer poolMu.Unlock()
	if pool != nil {
		return pool, nil
	}
	cfg, err := pgxpool.ParseConfig(NormalizeURL(config.Get().DatabaseURL))
	if err != nil {
		return nil, fmt.Errorf("BEZISYNC_DATABASE_URL: %w", err)
	}
	// Same budget as the Python engine (pool_size 10 + max_overflow 20).
	cfg.MaxConns = 30
	cfg.MinConns = 0
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	pool = p
	return pool, nil
}

// MustPool is Pool for process start-up paths.
func MustPool(ctx context.Context) *pgxpool.Pool {
	p, err := Pool(ctx)
	if err != nil {
		panic(err)
	}
	return p
}

// SetPool replaces the process pool (tests).
func SetPool(p *pgxpool.Pool) {
	poolMu.Lock()
	defer poolMu.Unlock()
	pool = p
}

// Close closes the process pool.
func Close() {
	poolMu.Lock()
	defer poolMu.Unlock()
	if pool != nil {
		pool.Close()
		pool = nil
	}
}

// Tx runs fn in a transaction: commit on success, roll back on error or panic.
func Tx(ctx context.Context, fn func(pgx.Tx) error) error {
	p, err := Pool(ctx)
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, p, fn)
}

// Now is the database-independent UTC clock (Python utcnow), truncated to microseconds
// like PostgreSQL timestamps so values compare equal after a round trip.
func Now() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}

// NewID returns a random UUID (uuid4).
func NewID() uuid.UUID {
	return uuid.New()
}

// ErrNoRows is returned when a lookup finds nothing.
var ErrNoRows = pgx.ErrNoRows

// IsNoRows reports whether err means "not found".
func IsNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

// IsUniqueViolation reports a unique-constraint violation (optionally a named one).
func IsUniqueViolation(err error, constraint ...string) bool {
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "23505" {
		return false
	}
	if len(constraint) == 0 {
		return true
	}
	for _, c := range constraint {
		if pg.ConstraintName == c {
			return true
		}
	}
	return false
}

// Get scans exactly one row into a T (columns matched by name to `db` tags).
func Get[T any](ctx context.Context, q Querier, sql string, args ...any) (T, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		var zero T
		return zero, err
	}
	return pgx.CollectExactlyOneRow(rows, pgx.RowToStructByNameLax[T])
}

// Find is Get returning nil (no error) when nothing matches.
func Find[T any](ctx context.Context, q Querier, sql string, args ...any) (*T, error) {
	v, err := Get[T](ctx, q, sql, args...)
	if IsNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// FindScalar reads one value, or nil when no row matches.
func FindScalar[T any](ctx context.Context, q Querier, sql string, args ...any) (*T, error) {
	v, err := Scalar[T](ctx, q, sql, args...)
	if IsNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// Select scans every row into a []T.
func Select[T any](ctx context.Context, q Querier, sql string, args ...any) ([]T, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[T])
	if out == nil {
		out = []T{}
	}
	return out, err
}

// Scalar reads one value.
func Scalar[T any](ctx context.Context, q Querier, sql string, args ...any) (T, error) {
	var v T
	err := q.QueryRow(ctx, sql, args...).Scan(&v)
	return v, err
}

// JSON is a JSONB column holding any JSON value. Numbers decode as json.Number so
// integers and floats keep their exact form (stored hashes depend on it).
type JSON struct {
	V any
}

// Map is a JSONB object column; nil decodes from SQL NULL.
type Map map[string]any

// List is a JSONB array column.
type List []any

func decode(src any) (any, error) {
	var raw []byte
	switch s := src.(type) {
	case nil:
		return nil, nil
	case []byte:
		raw = s
	case string:
		raw = []byte(s)
	default:
		// pgx may already have decoded jsonb; normalise through JSON.
		b, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		raw = b
	}
	return pyjson.DecodeGeneric(raw)
}

// Scan implements sql.Scanner.
func (m *Map) Scan(src any) error {
	v, err := decode(src)
	if err != nil {
		return err
	}
	if v == nil {
		*m = nil
		return nil
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return fmt.Errorf("db.Map: column holds %T, not an object", v)
	}
	*m = obj
	return nil
}

// Value implements driver.Valuer.
func (m Map) Value() (driver.Value, error) {
	if m == nil {
		return nil, nil
	}
	return marshal(map[string]any(m))
}

// Scan implements sql.Scanner.
func (l *List) Scan(src any) error {
	v, err := decode(src)
	if err != nil {
		return err
	}
	if v == nil {
		*l = nil
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return fmt.Errorf("db.List: column holds %T, not an array", v)
	}
	*l = arr
	return nil
}

// Value implements driver.Valuer.
func (l List) Value() (driver.Value, error) {
	if l == nil {
		return "[]", nil
	}
	return marshal([]any(l))
}

// Scan implements sql.Scanner.
func (j *JSON) Scan(src any) error {
	v, err := decode(src)
	if err != nil {
		return err
	}
	j.V = v
	return nil
}

// Value implements driver.Valuer.
func (j JSON) Value() (driver.Value, error) {
	return marshal(j.V)
}

// MarshalJSON renders the wrapped value.
func (j JSON) MarshalJSON() ([]byte, error) { return json.Marshal(j.V) }

func marshal(v any) (driver.Value, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return strings.TrimRight(buf.String(), "\n"), nil
}

// ToJSON marshals v for a JSONB parameter.
func ToJSON(v any) (string, error) {
	out, err := marshal(v)
	if err != nil {
		return "", err
	}
	if out == nil {
		return "null", nil
	}
	return out.(string), nil
}

// Redact hides the password of a database URL for logs and messages.
func Redact(raw string) string {
	u, err := url.Parse(NormalizeURL(raw))
	if err != nil {
		return "(invalid URL)"
	}
	return u.Redacted()
}

// Savepoint runs fn in a nested transaction (a SAVEPOINT inside a transaction): its
// changes are rolled back alone when fn fails.
func Savepoint(ctx context.Context, q Querier, fn func(Querier) error) error {
	type beginner interface {
		Begin(ctx context.Context) (pgx.Tx, error)
	}
	b, ok := q.(beginner)
	if !ok {
		return fn(q)
	}
	tx, err := b.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}
