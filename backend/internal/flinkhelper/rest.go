package flinkhelper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// REST is a client of the JobManager's REST API (bound to localhost in the container).
type REST struct {
	Base   string
	Client *http.Client
}

// NewREST returns a client of base ("http://127.0.0.1:8081").
func NewREST(base string) *REST {
	return &REST{Base: strings.TrimRight(base, "/"), Client: &http.Client{Timeout: 15 * time.Second}}
}

func (r *REST) do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, r.Base+path, rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := r.Client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("flink %s %s: HTTP %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(raw[:min(len(raw), 300)])))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

// TaskManagers is how many TaskManagers are registered.
func (r *REST) TaskManagers(ctx context.Context) (int, error) {
	var v struct {
		TaskManagers []json.RawMessage `json:"taskmanagers"`
	}
	err := r.do(ctx, http.MethodGet, "/taskmanagers", nil, &v)
	return len(v.TaskManagers), err
}

// JobRef is one job of the cluster.
type JobRef struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// Jobs lists the cluster's jobs.
func (r *REST) Jobs(ctx context.Context) ([]JobRef, error) {
	var v struct {
		Jobs []JobRef `json:"jobs"`
	}
	err := r.do(ctx, http.MethodGet, "/jobs", nil, &v)
	return v.Jobs, err
}

// Vertex is one operator chain of a job.
type Vertex struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Status  string `json:"status"`
	Metrics struct {
		ReadRecords  int64 `json:"read-records"`
		WriteRecords int64 `json:"write-records"`
	} `json:"metrics"`
}

// Job is a job's state.
type Job struct {
	ID        string   `json:"jid"`
	State     string   `json:"state"`
	StartTime int64    `json:"start-time"`
	Vertices  []Vertex `json:"vertices"`
}

// Job reads a job.
func (r *REST) Job(ctx context.Context, id string) (*Job, error) {
	var j Job
	if err := r.do(ctx, http.MethodGet, "/jobs/"+url.PathEscape(id), nil, &j); err != nil {
		return nil, err
	}
	return &j, nil
}

// Checkpoints are a job's checkpoint statistics.
type Checkpoints struct {
	Counts struct {
		Restored   int64 `json:"restored"`
		Total      int64 `json:"total"`
		InProgress int64 `json:"in_progress"`
		Completed  int64 `json:"completed"`
		Failed     int64 `json:"failed"`
	} `json:"counts"`
	Latest struct {
		Completed *struct {
			ID           int64  `json:"id"`
			ExternalPath string `json:"external_path"`
			LatestAck    int64  `json:"latest_ack_timestamp"`
			Size         int64  `json:"state_size"`
		} `json:"completed"`
		Failed *struct {
			ID             int64  `json:"id"`
			FailureMessage string `json:"failure_message"`
		} `json:"failed"`
		Restored *struct {
			ID           int64  `json:"id"`
			ExternalPath string `json:"external_path"`
			IsSavepoint  bool   `json:"is_savepoint"`
		} `json:"restored"`
	} `json:"latest"`
}

// Checkpoints reads a job's checkpoint statistics.
func (r *REST) Checkpoints(ctx context.Context, id string) (*Checkpoints, error) {
	var c Checkpoints
	if err := r.do(ctx, http.MethodGet, "/jobs/"+url.PathEscape(id)+"/checkpoints", nil, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// JobException is one entry of a job's exception history.
type JobException struct {
	Name      string `json:"exceptionName"`
	Trace     string `json:"stacktrace"`
	Timestamp int64  `json:"timestamp"`
	TaskName  string `json:"taskName"`
}

// Exceptions are a job's recent exceptions, oldest first.
func (r *REST) Exceptions(ctx context.Context, id string) ([]JobException, error) {
	var v struct {
		History struct {
			Entries []JobException `json:"entries"`
		} `json:"exceptionHistory"`
	}
	if err := r.do(ctx, http.MethodGet, "/jobs/"+url.PathEscape(id)+"/exceptions", nil, &v); err != nil {
		return nil, err
	}
	out := v.History.Entries
	sort.SliceStable(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })
	return out, nil
}

// ErrSavepointFailed is a stop-with-savepoint Flink refused or could not finish.
var ErrSavepointFailed = errors.New("stop with savepoint failed")

// StopWithSavepoint stops a job after writing a canonical savepoint under dir and returns its
// location. drain stays false: the sources must not be ended, only paused.
func (r *REST) StopWithSavepoint(ctx context.Context, id, dir string) (string, error) {
	var trig struct {
		RequestID string `json:"request-id"`
	}
	body := map[string]any{"drain": false, "formatType": "CANONICAL", "targetDirectory": dir}
	if err := r.do(ctx, http.MethodPost, "/jobs/"+url.PathEscape(id)+"/stop", body, &trig); err != nil {
		return "", err
	}
	for {
		var st struct {
			Status struct {
				ID string `json:"id"`
			} `json:"status"`
			Operation struct {
				Location     string          `json:"location"`
				FailureCause json.RawMessage `json:"failure-cause"`
			} `json:"operation"`
		}
		if err := r.do(ctx, http.MethodGet, "/jobs/"+url.PathEscape(id)+"/savepoints/"+url.PathEscape(trig.RequestID), nil, &st); err != nil {
			return "", err
		}
		if st.Status.ID == "COMPLETED" {
			if st.Operation.Location == "" {
				return "", fmt.Errorf("%w: %s", ErrSavepointFailed, firstLine(string(st.Operation.FailureCause)))
			}
			return st.Operation.Location, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Cancel cancels a job.
func (r *REST) Cancel(ctx context.Context, id string) error {
	return r.do(ctx, http.MethodPatch, "/jobs/"+url.PathEscape(id)+"?mode=cancel", nil, nil)
}

// SubtaskMetrics returns the listed metrics of a vertex aggregated over its subtasks (agg: sum,
// max, …): metric id → value.
func (r *REST) SubtaskMetrics(ctx context.Context, job, vertex string, ids []string, agg string) (map[string]float64, error) {
	q := url.Values{"get": {strings.Join(ids, ",")}, "agg": {agg}}
	var raw []map[string]any
	if err := r.do(ctx, http.MethodGet, "/jobs/"+url.PathEscape(job)+"/vertices/"+url.PathEscape(vertex)+"/subtasks/metrics?"+q.Encode(), nil, &raw); err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for _, m := range raw {
		id, _ := m["id"].(string)
		if v, ok := m[agg]; ok {
			out[id] = toFloat(v)
		}
	}
	return out, nil
}

// MetricNames lists the metric ids of a vertex.
func (r *REST) MetricNames(ctx context.Context, job, vertex string) ([]string, error) {
	var raw []struct {
		ID string `json:"id"`
	}
	if err := r.do(ctx, http.MethodGet, "/jobs/"+url.PathEscape(job)+"/vertices/"+url.PathEscape(vertex)+"/subtasks/metrics", nil, &raw); err != nil {
		return nil, err
	}
	out := make([]string, len(raw))
	for i, m := range raw {
		out[i] = m.ID
	}
	return out, nil
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	}
	return 0
}

func firstLine(s string) string {
	s = strings.Trim(s, `"`)
	if i := strings.IndexAny(s, "\n\\"); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}
