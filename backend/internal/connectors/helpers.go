package connectors

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
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

// ConfigExpressions are the places in a connection's settings (not its secrets) that hold "${":
// Debezium Server expands ${…} in application.properties, and the Flink CDC helper fills ${BZ_…}
// placeholders, so a setting like "${BZ_STATE_PASSWORD}" would copy another secret to wherever
// the setting is sent. Settings never need it; it is refused when saved and when compiled.
func ConfigExpressions(cfg map[string]any) [][]string {
	var out [][]string
	var walk func(loc []string, v any)
	walk = func(loc []string, v any) {
		switch x := v.(type) {
		case string:
			if strings.Contains(x, "${") {
				out = append(out, slices.Clone(loc))
			}
		case map[string]any:
			keys := make([]string, 0, len(x))
			for k := range x {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			for _, k := range keys {
				walk(append(loc, k), x[k])
			}
		case []any:
			for i, item := range x {
				walk(append(loc, strconv.Itoa(i)), item)
			}
		}
	}
	walk(nil, cfg)
	return out
}

// RefuseConfigExpressions is ConfigExpressions as a 422 for the connection named.
func RefuseConfigExpressions(name string, cfg map[string]any) error {
	locs := ConfigExpressions(cfg)
	if len(locs) == 0 {
		return nil
	}
	errs := make([]map[string]any, len(locs))
	for i, l := range locs {
		errs[i] = map[string]any{"loc": append([]string{"config"}, l...), "message": "Settings can't contain \"${\": it would be read as a variable that can hold credentials."}
	}
	return problem.New(422, "connection_config", fmt.Sprintf("The connection %s has a setting containing \"${\", which BeziSync refuses: change it on the connection.", name)).WithErrors(errs)
}
