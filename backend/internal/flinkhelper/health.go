package flinkhelper

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"
)

// Health serves BeziSync's view of the pipeline:
//
//	/q/health, /q/health/ready  UP once the job runs on every TaskManager, DOWN otherwise (with why)
//	/q/health/live              UP while the helper runs
//	/bz/status                  the full Status (job, checkpoints, stored state, progress)
//
// The paths match Debezium Server's, so BeziSync probes both engines the same way.
func (m *JobManager) Health() http.Handler {
	mux := http.NewServeMux()
	health := func(w http.ResponseWriter, _ *http.Request) {
		st := m.Status()
		up := st.Phase == PhaseRunning && st.JobState == "RUNNING" && st.TaskManagers >= st.TaskManagersWanted
		status, code := "UP", http.StatusOK
		if !up {
			status, code = "DOWN", http.StatusServiceUnavailable
		}
		data := map[string]any{"phase": st.Phase, "job_state": st.JobState,
			"taskmanagers": strconv.Itoa(st.TaskManagers) + "/" + strconv.Itoa(st.TaskManagersWanted)}
		if st.Error != "" {
			data["error"] = st.Error
		}
		writeJSON(w, code, map[string]any{"status": status, "checks": []any{map[string]any{"name": "flink-cdc", "status": status, "data": data}}})
	}
	mux.HandleFunc("/q/health", health)
	mux.HandleFunc("/q/health/ready", health)
	mux.HandleFunc("/q/health/live", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "UP", "checks": []any{}})
	})
	mux.HandleFunc("/bz/status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, m.Status())
	})
	return mux
}

// ServeHealth serves Health on port until ctx ends.
func (m *JobManager) ServeHealth(ctx context.Context, port int) error {
	srv := &http.Server{Handler: m.Health(), ReadHeaderTimeout: 5 * time.Second}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
