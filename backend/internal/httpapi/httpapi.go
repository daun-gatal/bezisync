// Package httpapi is the HTTP framework of the API: routing, middleware, RFC 9457 problems,
// JSON responses and Pydantic-style request validation (FastAPI's behaviour).
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/logging"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

type ctxKey int

const requestIDKey ctxKey = iota

// RequestID returns the request id of a request context ("" outside requests).
func RequestID(ctx context.Context) string {
	s, _ := ctx.Value(requestIDKey).(string)
	return s
}

// WithRequestID stores a request id (tests, background work).
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// Flush supports server-sent events.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the underlying writer (http.ResponseController).
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// Middleware sets the request id, security headers and access logs, and turns panics
// into 500 problems.
func Middleware(next http.Handler) http.Handler {
	log := logging.Logger("bezisync.http")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("x-request-id")
		if rid == "" {
			rid = strings.ReplaceAll(uuid.NewString(), "-", "")
		}
		r = r.WithContext(WithRequestID(r.Context(), rid))
		started := time.Now()
		h := w.Header()
		h.Set("x-request-id", rid)
		h.Set("x-content-type-options", "nosniff")
		h.Set("referrer-policy", "same-origin")
		h.Set("x-frame-options", "DENY")
		api := strings.HasPrefix(r.URL.Path, "/api/")
		if api {
			h.Set("cache-control", "no-store")
		}
		rec := &statusRecorder{ResponseWriter: w}
		defer func() {
			if rv := recover(); rv != nil {
				if err, ok := rv.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(rv)
				}
				slog.Error("unhandled error", "logger", "bezisync.core.errors", "request_id", rid,
					"exc", fmt.Sprintf("%v\n%s", rv, debug.Stack()))
				if rec.status == 0 {
					WriteProblem(rec, r, problem.New(500, "internal_error", "Something went wrong. Quote the request ID when reporting it."))
				}
			}
			if api {
				status := rec.status
				if status == 0 {
					status = http.StatusOK
				}
				ms := float64(time.Since(started).Microseconds()) / 1000
				log.Info("request", "method", r.Method, "path", r.URL.Path, "status", status,
					"ms", float64(int(ms*10+0.5))/10, "request_id", rid)
			}
		}()
		next.ServeHTTP(rec, r)
	})
}

// CORS adds credentialed CORS for the configured origins (like Starlette's middleware).
func CORS(origins []string) func(http.Handler) http.Handler {
	allowed := map[string]bool{}
	for _, o := range origins {
		allowed[o] = true
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin == "" || !(allowed[origin] || allowed["*"]) {
				next.ServeHTTP(w, r)
				return
			}
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			h.Set("Access-Control-Allow-Credentials", "true")
			h.Add("Vary", "Origin")
			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				h.Set("Access-Control-Allow-Methods", "DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT")
				if hdr := r.Header.Get("Access-Control-Request-Headers"); hdr != "" {
					h.Set("Access-Control-Allow-Headers", hdr)
				}
				h.Set("Access-Control-Max-Age", "600")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("OK"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// NewRouter builds a chi router whose 404/405 answers are problems (Starlette wording).
func NewRouter() *chi.Mux {
	r := chi.NewRouter()
	r.NotFound(func(w http.ResponseWriter, req *http.Request) {
		WriteProblem(w, req, problem.New(404, "http_404", "Not Found"))
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, req *http.Request) {
		WriteProblem(w, req, problem.New(405, "http_405", "Method Not Allowed"))
	})
	return r
}

// WriteProblem renders a problem as application/problem+json.
func WriteProblem(w http.ResponseWriter, r *http.Request, p *problem.Problem) {
	body := p.Body(RequestID(r.Context()))
	raw, _ := json.Marshal(body)
	w.Header().Set("Content-Type", "application/problem+json")
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	w.WriteHeader(p.Status)
	_, _ = w.Write(raw)
}

// Fail renders any error: problems as-is, everything else as a logged 500.
func Fail(w http.ResponseWriter, r *http.Request, err error) {
	var p *problem.Problem
	if errors.As(err, &p) {
		WriteProblem(w, r, p)
		return
	}
	slog.Error("unhandled error", "logger", "bezisync.core.errors", "request_id", RequestID(r.Context()), "exc", err.Error())
	WriteProblem(w, r, problem.New(500, "internal_error", "Something went wrong. Quote the request ID when reporting it."))
}

// JSON writes a JSON response.
func JSON(w http.ResponseWriter, status int, v any) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		panic(err)
	}
	raw := bytes.TrimRight(buf.Bytes(), "\n")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

// NoContent writes 204.
func NoContent(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }

// Handler is an API handler returning an error to render.
type Handler func(w http.ResponseWriter, r *http.Request) error

// H adapts a Handler.
func H(h Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h(w, r); err != nil {
			Fail(w, r, err)
		}
	}
}

const maxBody = 32 << 20

// Body reads a JSON request body as generic values. Missing or empty bodies with
// required=true fail with "Field required" at ["body"], malformed JSON with json_invalid.
func Body(r *http.Request, required bool) (any, *problem.Problem) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		return nil, problem.New(400, "http_400", "Could not read the request body.")
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		if required {
			v := &val.V{}
			v.Fail(val.Loc{"body"}, "missing", "Field required")
			return nil, v.Problem()
		}
		return nil, nil
	}
	g, err := pyjson.DecodeGeneric(raw)
	if err != nil {
		pos := 0
		var se *json.SyntaxError
		if errors.As(err, &se) {
			pos = int(se.Offset) - 1
			if pos < 0 {
				pos = 0
			}
		}
		v := &val.V{}
		v.Fail(val.Loc{"body", strconv.Itoa(pos)}, "json_invalid", "JSON decode error")
		return nil, v.Problem()
	}
	return g, nil
}

// PathUUID parses a UUID path parameter (422 at ["path", name] when malformed).
func PathUUID(r *http.Request, name string) (uuid.UUID, error) {
	v := &val.V{}
	id, ok := v.UUID(val.Loc{"path", name}, chi.URLParam(r, name))
	if !ok {
		return uuid.Nil, v.Problem()
	}
	return id, nil
}

// Query reads query parameters with FastAPI's validation.
type Query struct {
	r *http.Request
	v *val.V
}

// Q starts reading query parameters.
func Q(r *http.Request) *Query { return &Query{r: r, v: &val.V{}} }

func (q *Query) raw(name string) (string, bool) {
	vals, ok := q.r.URL.Query()[name]
	if !ok || len(vals) == 0 {
		return "", false
	}
	return vals[len(vals)-1], true
}

// Str reads an optional string ("" when absent).
func (q *Query) Str(name string) string {
	s, _ := q.raw(name)
	return s
}

// OptStr reads an optional string.
func (q *Query) OptStr(name string) *string {
	s, ok := q.raw(name)
	if !ok {
		return nil
	}
	return &s
}

// StrRules reads an optional constrained string.
func (q *Query) StrRules(name string, r val.StrRules) *string {
	s, ok := q.raw(name)
	if !ok {
		return nil
	}
	out, okk := q.v.Constrained(val.Loc{"query", name}, s, r)
	if !okk {
		return nil
	}
	return &out
}

// Int reads an integer with a default and bounds.
func (q *Query) Int(name string, def int, b val.Num) int {
	s, ok := q.raw(name)
	if !ok {
		return def
	}
	n, okk := q.v.IntIn(val.Loc{"query", name}, s, b)
	if !okk {
		return def
	}
	return n
}

// Bool reads a boolean with a default.
func (q *Query) Bool(name string, def bool) bool {
	s, ok := q.raw(name)
	if !ok {
		return def
	}
	b, okk := q.v.Bool(val.Loc{"query", name}, s)
	if !okk {
		return def
	}
	return b
}

// UUID reads an optional UUID.
func (q *Query) UUID(name string) *uuid.UUID {
	s, ok := q.raw(name)
	if !ok {
		return nil
	}
	id, okk := q.v.UUID(val.Loc{"query", name}, s)
	if !okk {
		return nil
	}
	return &id
}

// Literal reads one of the allowed values.
func (q *Query) Literal(name, def string, allowed ...string) string {
	s, ok := q.raw(name)
	if !ok {
		return def
	}
	out, okk := q.v.Literal(val.Loc{"query", name}, s, allowed...)
	if !okk {
		return def
	}
	return out
}

// RequiredInt reads a required integer.
func (q *Query) RequiredInt(name string) int {
	s, ok := q.raw(name)
	if !ok {
		q.v.Fail(val.Loc{"query", name}, "missing", "Field required")
		return 0
	}
	n, _ := q.v.Int(val.Loc{"query", name}, s)
	return n
}

// Required reads a required string.
func (q *Query) Required(name string) string {
	s, ok := q.raw(name)
	if !ok {
		q.v.Fail(val.Loc{"query", name}, "missing", "Field required")
	}
	return s
}

// Err is the 422 for invalid query parameters (nil when valid).
func (q *Query) Err() error {
	if p := q.v.Problem(); p != nil {
		return p
	}
	return nil
}

// ClientHost is the client address uvicorn would report: the peer, or X-Forwarded-For
// when the peer is a trusted local proxy.
func ClientHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if host == "127.0.0.1" || host == "::1" {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			return strings.TrimSpace(parts[len(parts)-1])
		}
	}
	return host
}

// Production refuses to start with a fatal misconfiguration.
func CheckProduction() error {
	if problems := config.Get().ValidateForProduction(); len(problems) > 0 {
		return fmt.Errorf("Refusing to start: %s", strings.Join(problems, "; "))
	}
	return nil
}
