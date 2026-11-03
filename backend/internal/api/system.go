package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/security"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/version"
)

var healthCode = map[string]int{"healthy": 0, "lagging": 1, "at_risk": 2, "down": 3, "unknown": 4}

const docsPage = `<!DOCTYPE html>
<html>
<head>
<link type="text/css" rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
<title>BeziSync API - Swagger UI</title>
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
const ui = SwaggerUIBundle({url: '/api/v1/openapi.json', dom_id: '#swagger-ui', layout: 'BaseLayout', deepLinking: true,
  showExtensions: true, showCommonExtensions: true,
  presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset]})
</script>
</body>
</html>
`

func systemRoutes(r chi.Router) {
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpapi.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Get("/readyz", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		var one int
		if err := pool(r.Context()).QueryRow(r.Context(), "select 1").Scan(&one); err != nil {
			return problem.New(503, "database_unavailable", "The metadata database is not reachable.")
		}
		httpapi.JSON(w, http.StatusOK, map[string]string{"status": "ready"})
		return nil
	}))
	r.Get("/api/v1/openapi.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(openAPIDoc)))
		_, _ = w.Write(openAPIDoc)
	})
	r.Get("/api/docs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(docsPage))
	})
	r.Get("/api/v1/system/info", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		ctx := r.Context()
		image, err := services.DefaultImage(ctx, pool(ctx))
		if err != nil {
			return err
		}
		s := config.Get()
		httpapi.JSON(w, http.StatusOK, obj("version", version.Version, "environment", s.Environment, "default_debezium_image", image, "telemetry_enabled", s.TelemetryEnabled))
		return nil
	}))
	r.Get("/api/v1/system/telemetry", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		if _, err := superuser(r); err != nil {
			return err
		}
		s := config.Get()
		var url *string
		if s.TelemetryURL != "" {
			url = &s.TelemetryURL
		}
		ctx := r.Context()
		var out *om.Map
		// A transaction: the install id is persisted on first use.
		err := tx(ctx, func(q db.Querier) error {
			sent, err := services.TelemetryLastSent(ctx, q)
			if err != nil {
				return err
			}
			payload, err := services.TelemetryPayload(ctx, q)
			if err != nil {
				return err
			}
			out = obj("enabled", s.TelemetryEnabled && s.TelemetryURL != "", "url", url, "last_sent_at", pytime.P(sent), "payload", payload)
			return nil
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Get("/metrics", httpapi.H(prometheus))
}

// prometheus is the exposition of pipeline state for users' own monitoring stacks.
func prometheus(w http.ResponseWriter, r *http.Request) error {
	token := config.Get().MetricsToken
	sent := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if token == "" {
		return problem.New(404, "not_found", "Metrics are disabled. Set BEZISYNC_METRICS_TOKEN to enable them.")
	}
	if !security.ConstantTimeEqual(sent, token) {
		return problem.New(401, "invalid_token", "Missing or invalid metrics token.")
	}
	esc := func(s string) string { return strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`) }
	lines := []string{
		"# HELP bezisync_pipeline_health 0 healthy, 1 lagging, 2 at risk, 3 down, 4 unknown",
		"# TYPE bezisync_pipeline_health gauge",
		"# HELP bezisync_pipeline_running 1 when the pipeline should be running",
		"# TYPE bezisync_pipeline_running gauge",
		"# HELP bezisync_pipeline_lag_seconds Seconds behind the source",
		"# TYPE bezisync_pipeline_lag_seconds gauge",
		"# HELP bezisync_pipeline_source_retained_bytes Log retained on the source for the pipeline",
		"# TYPE bezisync_pipeline_source_retained_bytes gauge",
		"# HELP bezisync_pipeline_events_per_second Recent change rate",
		"# TYPE bezisync_pipeline_events_per_second gauge",
	}
	ctx := r.Context()
	rows, err := db.Select[struct {
		Slug         string `db:"slug"`
		Name         string `db:"name"`
		Lifecycle    string `db:"lifecycle"`
		Health       string `db:"health"`
		DesiredState string `db:"desired_state"`
		Status       db.Map `db:"status"`
	}](ctx, pool(ctx), `SELECT pr.slug, p.name, p.lifecycle, p.health, p.desired_state, p.status
		FROM projects pr JOIN pipelines p ON p.project_id = pr.id WHERE p.deleted_at IS NULL`)
	if err != nil {
		return err
	}
	for _, p := range rows {
		labels := fmt.Sprintf(`project="%s",pipeline="%s",lifecycle="%s"`, esc(p.Slug), esc(p.Name), p.Lifecycle)
		code, ok := healthCode[p.Health]
		if !ok {
			code = 4
		}
		running := 0
		if p.DesiredState == "running" {
			running = 1
		}
		lines = append(lines, fmt.Sprintf("bezisync_pipeline_health{%s} %d", labels, code),
			fmt.Sprintf("bezisync_pipeline_running{%s} %d", labels, running))
		sub := func(key string) map[string]any { m, _ := p.Status[key].(map[string]any); return m }
		if lag, ok := number(sub("metrics")["ms_behind_source"]); ok {
			lines = append(lines, fmt.Sprintf("bezisync_pipeline_lag_seconds{%s} %.3f", labels, lag/1000))
		}
		if v := sub("source_impact")["retained_bytes"]; v != nil {
			lines = append(lines, fmt.Sprintf("bezisync_pipeline_source_retained_bytes{%s} %s", labels, pyStr(v)))
		}
		if v := p.Status["events_per_second"]; v != nil {
			lines = append(lines, fmt.Sprintf("bezisync_pipeline_events_per_second{%s} %s", labels, pyStr(v)))
		}
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
	return nil
}

func number(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

// pyStr formats a JSON scalar like Python's str() of the loaded value.
func pyStr(v any) string {
	switch n := v.(type) {
	case json.Number:
		s := n.String()
		if !strings.ContainsAny(s, ".eE") {
			return s
		}
		f, _ := n.Float64()
		r, _ := pyjson.FloatRepr(f)
		return r
	case float64:
		r, _ := pyjson.FloatRepr(n)
		return r
	case bool:
		if n {
			return "True"
		}
		return "False"
	case string:
		return n
	}
	return fmt.Sprint(v)
}
