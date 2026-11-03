package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
)

// Stream timings (variables so tests can shorten them).
var (
	streamPoll = 2 * time.Second
	streamPing = 15 * time.Second
)

func streamRoutes(r chi.Router) {
	// Server-sent events: pipeline status changes, so the UI refreshes without polling.
	r.Get("/projects/{project_id}/stream", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.PipelineView); err != nil {
			return err
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			return fmt.Errorf("streaming unsupported")
		}
		projectID := a.Project.ID
		h := w.Header()
		h.Set("Content-Type", "text/event-stream; charset=utf-8")
		h.Set("Cache-Control", "no-store")
		h.Set("Connection", "keep-alive")
		h.Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		send := func(event, data string) error {
			_, err := fmt.Fprintf(w, "event: %s\r\ndata: %s\r\n\r\n", event, data)
			flusher.Flush()
			return err
		}
		ctx := r.Context()
		since := db.Now()
		if send("ready", "{}") != nil {
			return nil
		}
		poll, ping := time.NewTicker(streamPoll), time.NewTicker(streamPing)
		defer poll.Stop()
		defer ping.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ping.C:
				if _, err := fmt.Fprintf(w, ": ping - %s\r\n\r\n", time.Now().UTC().Format("2006-01-02 15:04:05.000000+00:00")); err != nil {
					return nil
				}
				flusher.Flush()
			case <-poll.C:
				rows, err := db.Select[struct {
					ID        uuid.UUID `db:"id"`
					Name      string    `db:"name"`
					Lifecycle string    `db:"lifecycle"`
					Health    string    `db:"health"`
					UpdatedAt time.Time `db:"updated_at"`
				}](ctx, pool(ctx), `SELECT id, name, lifecycle, health, updated_at FROM pipelines WHERE project_id = $1 AND updated_at > $2 ORDER BY updated_at`, projectID, since)
				if err != nil {
					if ctx.Err() == nil {
						slog.Error("stream query failed", "logger", "bezisync.api.stream", "error", err.Error())
					}
					return nil
				}
				for _, row := range rows {
					if row.UpdatedAt.After(since) {
						since = row.UpdatedAt
					}
					data, err := pyjson.Encode(om.New().Set("id", row.ID.String()).Set("name", row.Name).Set("lifecycle", row.Lifecycle).
						Set("health", row.Health).Set("updated_at", pytime.ISOFormat(row.UpdatedAt)), pyjson.Options{EnsureASCII: true})
					if err != nil {
						return err
					}
					if send("pipeline", data) != nil {
						return nil
					}
				}
			}
		}
	}))
}
