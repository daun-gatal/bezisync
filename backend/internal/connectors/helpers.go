package connectors

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/schemaval"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// ValidationError is a model validation failure inside connector code (Pydantic raised
// ValidationError there, which surfaced as an internal error).
type ValidationError struct{ Errors []problem.FieldError }

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Errors))
	for i, fe := range e.Errors {
		parts[i] = strings.Join(fe.Loc, ".") + ": " + fe.Message
	}
	return fmt.Sprintf("%d validation error(s): %s", len(e.Errors), strings.Join(parts, "; "))
}

// ValidateConfig is Model.model_validate(values).model_dump(mode="json").
func ValidateConfig(schema *om.Map, values map[string]any) (map[string]any, error) {
	if values == nil {
		values = map[string]any{}
	}
	v := &val.V{}
	out, ok := schemaval.Model(v, nil, schema, values)
	if !ok {
		return nil, &ValidationError{v.Errors}
	}
	return out, nil
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func pyDumpsRaw(raw []byte) (string, error) {
	g, err := pyjson.DecodeGeneric(raw)
	if err != nil {
		return "", err
	}
	return pyjson.Dumps(g)
}

// decodeJSON is json.loads on bytes (UTF-8 with replacement).
func decodeJSON(raw []byte) (any, error) {
	return pyjson.DecodeGeneric([]byte(decodeText(raw)))
}

// decodeText is bytes.decode(errors="replace").
func decodeText(raw []byte) string {
	s := string(raw)
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "�")
	}
	return s
}

type sqlQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// eachRow runs a query and calls fn per row (rows are always closed and checked).
func eachRow(ctx context.Context, q sqlQuerier, query string, args []any, fn func(*sql.Rows) error) error {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		if err := fn(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// scanStrings scans every column of the current row as nullable text.
func scanStrings(rows *sql.Rows) ([]sql.NullString, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	return vals, rows.Scan(ptrs...)
}
