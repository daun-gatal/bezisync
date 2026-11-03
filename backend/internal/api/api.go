// Package api is the versioned REST API (mounted under /api/v1) plus the system routes.
// Handlers are thin: they authenticate, validate and call internal/services.
package api

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/mcpserver"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Router builds the HTTP handler of the API process.
func Router() http.Handler {
	r := httpapi.NewRouter()
	r.Use(httpapi.Middleware)
	if origins := config.Get().CORSOrigins; len(origins) > 0 {
		r.Use(httpapi.CORS(origins))
	}
	systemRoutes(r)
	r.Route("/api/v1", func(v1 chi.Router) {
		tokenRoutes(v1)
		authRoutes(v1)
		ssoRoutes(v1)
		ssoProviderRoutes(v1)
		samlRoutes(v1)
		projectRoutes(v1)
		connectionRoutes(v1)
		runtimeRoutes(v1)
		manifestRoutes(v1)
		pipelineRoutes(v1)
		streamRoutes(v1)
		alertRoutes(v1)
		imageRoutes(v1)
		secretStoreRoutes(v1)
		copyRoutes(v1)
		approvalRoutes(v1)
		templateRoutes(v1)
		agentRoutes(v1)
		userRoutes(v1)
		catalogRoutes(v1)
		feedbackRoutes(v1)
		transformRoutes(v1)
		mcpRoutes(v1)
		// Tools call this router in process, as the caller.
		v1.Post("/mcp", mcpserver.Handler(r).ServeHTTP)
	})
	oauthDiscoveryRoutes(r)
	return r
}

func pool(ctx context.Context) db.Querier { return db.MustPool(ctx) }

// principal authenticates the request (token usage is persisted immediately).
func principal(r *http.Request) (*auth.Principal, error) {
	return auth.Current(r.Context(), pool(r.Context()), r)
}

func superuser(r *http.Request) (*auth.Principal, error) {
	p, err := principal(r)
	if err != nil {
		return nil, err
	}
	return p, auth.RequireSuperuser(p)
}

// project resolves {project_id} for the caller (unauthorised and unknown look the same).
func project(r *http.Request) (*auth.ProjectAccess, error) {
	id, err := httpapi.PathUUID(r, "project_id")
	if err != nil {
		return nil, err
	}
	p, err := principal(r)
	if err != nil {
		return nil, err
	}
	return auth.Project(r.Context(), pool(r.Context()), p, id)
}

var errDryRun = errors.New("dry run")

// dryTx runs fn in a transaction that is rolled back when dry is set.
func dryTx(ctx context.Context, dry bool, fn func(q db.Querier) error) error {
	err := tx(ctx, func(q db.Querier) error {
		if err := fn(q); err != nil {
			return err
		}
		if dry {
			return errDryRun
		}
		return nil
	})
	if errors.Is(err, errDryRun) {
		return nil
	}
	return err
}

// tx runs fn in a transaction (handlers that write).
func tx(ctx context.Context, fn func(q db.Querier) error) error {
	return db.Tx(ctx, func(t pgx.Tx) error { return fn(t) })
}

func pathID(r *http.Request, name string) (uuid.UUID, error) { return httpapi.PathUUID(r, name) }

// body decodes and validates a JSON request body with a model parser.
func body[T any](r *http.Request, parse func(v *val.V, o *val.Object) T) (T, error) {
	raw, p := httpapi.Body(r, true)
	if p != nil {
		var zero T
		return zero, p
	}
	v := &val.V{}
	o := v.Body(raw)
	out := parse(v, o)
	if p := v.Problem(); p != nil {
		var zero T
		return zero, p
	}
	return out, nil
}

// optBody decodes an optional JSON body (nil, false when none was sent).
func optBody[T any](r *http.Request, parse func(v *val.V, o *val.Object) T) (T, bool, error) {
	var zero T
	raw, p := httpapi.Body(r, false)
	if p != nil {
		return zero, false, p
	}
	if raw == nil {
		return zero, false, nil
	}
	v := &val.V{}
	o := v.Body(raw)
	out := parse(v, o)
	if p := v.Problem(); p != nil {
		return zero, false, p
	}
	return out, true, nil
}

// optStrList reads an optional list[str] with a maximum length (0 = unbounded).
func optStrList(v *val.V, o *val.Object, key string, max int) []string {
	f := o.Get(key)
	if !f.Present || f.IsNull() {
		return nil
	}
	l, ok := v.StrList(f.Loc, f.Raw)
	if !ok {
		return nil
	}
	if max > 0 && !v.ListLen(f.Loc, len(l), 0, max) {
		return nil
	}
	if l == nil {
		l = []string{}
	}
	return l
}

// paginate runs a keyset page query (services.PageSQL) and returns the rows with the
// next cursor; key returns a row's (time, id) cursor columns.
func paginate[T any](ctx context.Context, q db.Querier, base string, args []any, timeCol, idCol, cursor string, limit int,
	asc bool, key func(*T) (time.Time, uuid.UUID)) ([]T, *string, error) {
	sql, args, err := services.PageSQL(base, args, timeCol, idCol, cursor, limit, asc)
	if err != nil {
		return nil, nil, err
	}
	rows, err := db.Select[T](ctx, q, sql, args...)
	if err != nil {
		return nil, nil, err
	}
	if l := services.ClampLimit(limit); len(rows) > l {
		rows = rows[:l]
		c := services.EncodeCursor(key(&rows[l-1]))
		return rows, &c, nil
	}
	return rows, nil, nil
}

// obj is a JSON object with keys in the given order (Pydantic field order).
func obj(kv ...any) *om.Map {
	m := om.New()
	for i := 0; i+1 < len(kv); i += 2 {
		m.Set(kv[i].(string), kv[i+1])
	}
	return m
}

// optStr reads an optional string (None allowed) with constraints.
func optStr(v *val.V, o *val.Object, key string, rules val.StrRules) *string {
	f := o.Get(key)
	if !f.Present || f.IsNull() {
		return nil
	}
	s, ok := v.Constrained(f.Loc, f.Raw, rules)
	if !ok {
		return nil
	}
	return &s
}

// str reads a string with a default.
func str(v *val.V, o *val.Object, key, def string, rules val.StrRules, required bool) string {
	f := o.Get(key)
	if !f.Present {
		if required {
			f.Required()
		}
		return def
	}
	s, ok := v.Constrained(f.Loc, f.Raw, rules)
	if !ok {
		return def
	}
	return s
}

func boolean(v *val.V, o *val.Object, key string, def bool) bool {
	f := o.Get(key)
	if !f.Present {
		return def
	}
	b, ok := v.Bool(f.Loc, f.Raw)
	if !ok {
		return def
	}
	return b
}

func optBool(v *val.V, o *val.Object, key string) *bool {
	f := o.Get(key)
	if !f.Present || f.IsNull() {
		return nil
	}
	b, ok := v.Bool(f.Loc, f.Raw)
	if !ok {
		return nil
	}
	return &b
}

func optUUID(v *val.V, o *val.Object, key string) *uuid.UUID {
	f := o.Get(key)
	if !f.Present || f.IsNull() {
		return nil
	}
	id, ok := v.UUID(f.Loc, f.Raw)
	if !ok {
		return nil
	}
	return &id
}

func reqUUID(v *val.V, o *val.Object, key string) uuid.UUID {
	f := o.Get(key)
	if !f.Required() {
		return uuid.Nil
	}
	id, _ := v.UUID(f.Loc, f.Raw)
	return id
}

func dict(v *val.V, o *val.Object, key string, required bool) map[string]any {
	f := o.Get(key)
	if !f.Present {
		if required {
			f.Required()
		}
		return nil
	}
	m, _ := v.Dict(f.Loc, f.Raw)
	return m
}

func optDict(v *val.V, o *val.Object, key string) (map[string]any, bool) {
	f := o.Get(key)
	if !f.Present || f.IsNull() {
		return nil, false
	}
	m, ok := v.Dict(f.Loc, f.Raw)
	return m, ok
}

// quote is urllib.parse.quote (safe "/").
func quote(s string) string {
	var b strings.Builder
	for _, c := range []byte(s) {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.IndexByte("_.-~/", c) >= 0 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func redirect(w http.ResponseWriter, url string) {
	w.Header().Set("Location", url)
	w.WriteHeader(http.StatusFound)
}

//go:embed openapi.json
var openAPIDoc []byte

// OpenAPI is the API document (the frontend's typed client is generated from it).
func OpenAPI() []byte { return openAPIDoc }
