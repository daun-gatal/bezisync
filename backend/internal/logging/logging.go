// Package logging configures structured logs: JSON by default (one object per line with
// ts, level, logger, msg and extra fields), so log shippers need no parsing rules.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
)

// Configure installs the process logger from settings.
func Configure() {
	s := config.Get()
	slog.SetDefault(slog.New(NewHandler(os.Stdout, s.LogJSON, ParseLevel(s.LogLevel))))
}

// ParseLevel maps Python level names to slog levels.
func ParseLevel(name string) slog.Level {
	switch strings.ToUpper(name) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARNING", "WARN":
		return slog.LevelWarn
	case "ERROR", "CRITICAL":
		return slog.LevelError
	}
	return slog.LevelInfo
}

// Logger returns a logger named like the Python module loggers ("bezisync.http"). It
// resolves the process handler when it logs, so package-level loggers created before
// Configure still follow the configured format and level.
func Logger(name string) *slog.Logger {
	return slog.New(deferred{attrs: []slog.Attr{slog.String(loggerKey, name)}})
}

// deferred hands every record to the current default handler.
type deferred struct{ attrs []slog.Attr }

func (d deferred) target() slog.Handler {
	h := slog.Default().Handler()
	if len(d.attrs) > 0 {
		h = h.WithAttrs(d.attrs)
	}
	return h
}

func (d deferred) Enabled(ctx context.Context, l slog.Level) bool { return d.target().Enabled(ctx, l) }

func (d deferred) Handle(ctx context.Context, r slog.Record) error { return d.target().Handle(ctx, r) }

func (d deferred) WithAttrs(attrs []slog.Attr) slog.Handler {
	return deferred{attrs: append(append([]slog.Attr{}, d.attrs...), attrs...)}
}

func (d deferred) WithGroup(name string) slog.Handler { return d.target().WithGroup(name) }

const loggerKey = "logger"

// Handler writes either the JSON layout or "time LEVEL logger message".
type Handler struct {
	mu    *sync.Mutex
	w     io.Writer
	json  bool
	level slog.Level
	attrs []slog.Attr
}

// NewHandler creates a handler.
func NewHandler(w io.Writer, jsonOut bool, level slog.Level) *Handler {
	return &Handler{mu: &sync.Mutex{}, w: w, json: jsonOut, level: level}
}

// Enabled implements slog.Handler.
func (h *Handler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.level }

// WithAttrs implements slog.Handler.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	c := *h
	c.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &c
}

// WithGroup implements slog.Handler (groups are flattened).
func (h *Handler) WithGroup(string) slog.Handler { return h }

func levelName(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return "ERROR"
	case l >= slog.LevelWarn:
		return "WARNING"
	case l >= slog.LevelInfo:
		return "INFO"
	}
	return "DEBUG"
}

// jsonValue keeps JSON scalars and stringifies everything else (default=str).
func jsonValue(v any) any {
	switch x := v.(type) {
	case nil, string, bool, int, int64, uint64, float64:
		return x
	case error:
		return x.Error()
	}
	return fmt.Sprint(v)
}

// Handle implements slog.Handler.
func (h *Handler) Handle(_ context.Context, r slog.Record) error {
	logger := "bezisync"
	extra := map[string]any{}
	order := []string{}
	add := func(a slog.Attr) {
		if a.Key == loggerKey {
			logger = a.Value.String()
			return
		}
		if _, seen := extra[a.Key]; !seen {
			order = append(order, a.Key)
		}
		extra[a.Key] = a.Value.Resolve().Any()
	}
	for _, a := range h.attrs {
		add(a)
	}
	r.Attrs(func(a slog.Attr) bool { add(a); return true })
	ts := r.Time
	if ts.IsZero() {
		ts = time.Now()
	}
	var line []byte
	if h.json {
		// Python's layout: ts, level, logger, msg, then extras; json.dumps(default=str).
		payload := om.New().Set("ts", ts.UTC().Format("2006-01-02T15:04:05.000000+00:00")).Set("level", levelName(r.Level)).
			Set("logger", logger).Set("msg", r.Message)
		for _, k := range order {
			payload.Set(k, jsonValue(extra[k]))
		}
		out, err := pyjson.Encode(payload, pyjson.Options{EnsureASCII: true})
		if err != nil {
			out = fmt.Sprintf(`{"level": "ERROR", "msg": %q}`, "unloggable record: "+err.Error())
		}
		b := []byte(out)
		line = append(b, '\n')
	} else {
		var sb strings.Builder
		sb.WriteString(ts.Format("2006-01-02 15:04:05,000"))
		sb.WriteString(" " + levelName(r.Level) + " " + logger + " " + r.Message)
		for _, k := range order {
			fmt.Fprintf(&sb, " %s=%v", k, extra[k])
		}
		sb.WriteByte('\n')
		line = []byte(sb.String())
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.w.Write(line)
	return err
}
