package services

import (
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
)

// Keyset (cursor) pagination over (time, id).

// MaxLimit is the largest page size.
const MaxLimit = 200

// ClampLimit is max(1, min(limit, MaxLimit)).
func ClampLimit(limit int) int { return max(1, min(limit, MaxLimit)) }

// EncodeCursor is base64url(json.dumps([at.isoformat(), str(id)])) without padding.
func EncodeCursor(at time.Time, id uuid.UUID) string {
	raw, _ := pyjson.Encode([]any{pytime.ISOFormat(at), id.String()}, pyjson.Options{EnsureASCII: true})
	return strings.TrimRight(base64.URLEncoding.EncodeToString([]byte(raw)), "=")
}

// DecodeCursor parses a cursor (400 invalid_cursor).
func DecodeCursor(cursor string) (time.Time, uuid.UUID, error) {
	bad := problem.New(400, "invalid_cursor", "The pagination cursor is invalid.")
	padded := cursor + strings.Repeat("=", (4-len(cursor)%4)%4)
	raw, err := base64.URLEncoding.DecodeString(padded)
	if err != nil {
		return time.Time{}, uuid.Nil, bad
	}
	g, err := pyjson.DecodeGeneric(raw)
	if err != nil {
		return time.Time{}, uuid.Nil, bad
	}
	l, ok := g.([]any)
	if !ok || len(l) != 2 {
		return time.Time{}, uuid.Nil, bad
	}
	s1, ok1 := l[0].(string)
	s2, ok2 := l[1].(string)
	if !ok1 || !ok2 {
		return time.Time{}, uuid.Nil, bad
	}
	at, err := pytime.Parse(s1)
	if err != nil {
		return time.Time{}, uuid.Nil, bad
	}
	id, err := uuid.Parse(s2)
	if err != nil {
		return time.Time{}, uuid.Nil, bad
	}
	return at, id, nil
}

// PageSQL appends the keyset condition and order to a query; returns the query and args.
// timeCol/idCol are column names; newest first unless asc.
func PageSQL(base string, args []any, timeCol, idCol, cursor string, limit int, asc bool) (string, []any, error) {
	op, dir := "<", "DESC"
	if asc {
		op, dir = ">", "ASC"
	}
	where := " WHERE "
	if strings.Contains(strings.ToUpper(base), " WHERE ") {
		where = " AND "
	}
	if cursor != "" {
		at, id, err := DecodeCursor(cursor)
		if err != nil {
			return "", nil, err
		}
		args = append(args, at, id)
		n := len(args)
		base += where + "(" + timeCol + " " + op + " $" + itoa(n-1) + " OR (" + timeCol + " = $" + itoa(n-1) + " AND " + idCol + " " + op + " $" + itoa(n) + "))"
	}
	args = append(args, ClampLimit(limit)+1)
	base += " ORDER BY " + timeCol + " " + dir + ", " + idCol + " " + dir + " LIMIT $" + itoa(len(args))
	return base, args, nil
}

func itoa(n int) string { return strconv.Itoa(n) }
