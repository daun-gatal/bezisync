package flinkhelper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// fakeJob is one job of the fake cluster: its states advance on every poll of /jobs/{id}.
type fakeJob struct {
	id        string
	states    []string // played in order; the last one sticks
	polls     int
	completed int64 // completed checkpoints
	excs      []JobException
	written   int64
	cancelled bool
}

// fakeFlink is the JobManager REST API the helper talks to.
type fakeFlink struct {
	mu          sync.Mutex
	t           *testing.T
	stateDir    string
	tms         int
	jobs        map[string]*fakeJob
	order       []string
	next        []func(j *fakeJob) // shape the next submitted jobs
	submitArgs  [][]string
	rendered    []string
	stopFail    bool
	stopCalls   int
	cancelCalls int
	streaming   bool // the binlog split exists: the first copy is done
}

// counts reads the fake's counters under its lock.
func (f *fakeFlink) counts() (stops, cancels int, submits [][]string, rendered []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopCalls, f.cancelCalls, append([][]string(nil), f.submitArgs...), append([]string(nil), f.rendered...)
}

func newFakeFlink(t *testing.T, stateDir string) (*fakeFlink, *httptest.Server) {
	f := &fakeFlink{t: t, stateDir: stateDir, tms: 2, jobs: map[string]*fakeJob{}}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeFlink) state(j *fakeJob) string {
	if j.cancelled {
		return "CANCELED"
	}
	return j.states[min(j.polls, len(j.states)-1)]
}

func (f *fakeFlink) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := r.URL.Path
	out := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	parts := strings.Split(strings.Trim(path, "/"), "/")
	switch {
	case path == "/taskmanagers":
		l := make([]any, f.tms)
		for i := range l {
			l[i] = map[string]any{"id": fmt.Sprint(i)}
		}
		out(map[string]any{"taskmanagers": l})
	case path == "/jobs":
		var l []any
		for _, id := range f.order {
			l = append(l, map[string]any{"id": id, "status": f.state(f.jobs[id])})
		}
		out(map[string]any{"jobs": l})
	case len(parts) == 2 && parts[0] == "jobs" && r.Method == http.MethodPatch:
		f.cancelCalls++
		f.jobs[parts[1]].cancelled = true
		w.WriteHeader(http.StatusAccepted)
		out(map[string]any{})
	case len(parts) == 2 && parts[0] == "jobs":
		j := f.jobs[parts[1]]
		j.polls++
		out(map[string]any{"jid": j.id, "state": f.state(j), "start-time": 1700000000000, "vertices": []any{
			map[string]any{"id": "src", "name": "Source: Flink CDC Event Source: mysql", "status": "RUNNING", "metrics": map[string]any{"write-records": j.written}},
			map[string]any{"id": "sink", "name": "PostPartition -> Sink Writer: Flink CDC Event Sink: starrocks", "status": "RUNNING",
				"metrics": map[string]any{"read-records": j.written}},
		}})
	case len(parts) == 3 && parts[2] == "checkpoints":
		j := f.jobs[parts[1]]
		var latest any
		if j.completed > 0 {
			dir := filepath.Join(f.stateDir, "cp", j.id, fmt.Sprintf("chk-%d", j.completed))
			_ = os.MkdirAll(dir, 0o750)
			_ = os.WriteFile(filepath.Join(dir, "_metadata"), []byte(dir), 0o600)
			latest = map[string]any{"id": j.completed, "external_path": "file:" + dir, "latest_ack_timestamp": 1700000060000}
		}
		out(map[string]any{"counts": map[string]any{"completed": j.completed}, "latest": map[string]any{"completed": latest}})
	case len(parts) == 3 && parts[2] == "exceptions":
		j := f.jobs[parts[1]]
		var entries []any
		for _, e := range j.excs {
			entries = append(entries, map[string]any{"exceptionName": e.Name, "stacktrace": e.Trace, "timestamp": e.Timestamp})
		}
		out(map[string]any{"exceptionHistory": map[string]any{"entries": entries}})
	case len(parts) == 3 && parts[2] == "stop":
		f.stopCalls++
		out(map[string]any{"request-id": "t1"})
	case len(parts) == 4 && parts[2] == "savepoints":
		if f.stopFail {
			out(map[string]any{"status": map[string]any{"id": "COMPLETED"}, "operation": map[string]any{"failure-cause": "Not all required tasks are currently running"}})
			return
		}
		dir := filepath.Join(f.stateDir, "sp", "savepoint-"+parts[1][:6])
		_ = os.MkdirAll(dir, 0o750)
		_ = os.WriteFile(filepath.Join(dir, "_metadata"), []byte("savepoint"), 0o600)
		f.jobs[parts[1]].cancelled = true
		out(map[string]any{"status": map[string]any{"id": "COMPLETED"}, "operation": map[string]any{"location": "file:" + dir}})
	case strings.HasSuffix(path, "/subtasks/metrics"):
		if r.URL.Query().Get("get") == "" {
			names := []any{map[string]any{"id": "Source__mysql.split.shop_orders_4.watermark.currentWatermark"},
				map[string]any{"id": "Source__mysql.split.shop_orders_9.watermark.currentWatermark"},
				map[string]any{"id": "Source__mysql.currentEmitEventTimeLag"}}
			if f.streaming {
				names = append(names, map[string]any{"id": "Source__mysql.split.binlog-split.watermark.currentWatermark"})
			}
			out(names)
			return
		}
		out([]any{map[string]any{"id": "Source__mysql.currentEmitEventTimeLag", "max": 1250.0}})
	default:
		f.t.Errorf("unexpected %s %s", r.Method, path)
		http.NotFound(w, r)
	}
}

// launcher is the fake Launcher: Submit creates a job in the fake cluster.
type launcher struct {
	f      *fakeFlink
	layout Layout
	done   chan error
}

func (l *launcher) StartJobManager(context.Context) (<-chan error, func(), error) {
	l.done = make(chan error, 1)
	return l.done, func() {}, nil
}

func (l *launcher) Submit(_ context.Context, args []string) (string, error) {
	raw, err := os.ReadFile(args[0])
	if err != nil {
		return "", err
	}
	l.f.mu.Lock()
	defer l.f.mu.Unlock()
	l.f.submitArgs = append(l.f.submitArgs, args)
	l.f.rendered = append(l.f.rendered, string(raw))
	id := fmt.Sprintf("%032x", len(l.f.order)+1)
	j := &fakeJob{id: id, states: []string{"RUNNING"}, completed: 1, written: 10}
	if len(l.f.next) > 0 {
		l.f.next[0](j)
		l.f.next = l.f.next[1:]
	}
	l.f.jobs[id] = j
	l.f.order = append(l.f.order, id)
	return "Pipeline has been submitted to cluster.\nJob ID: " + id + "\nJob Description: x\n", nil
}

// memStore is an in-memory StateStore.
type memStore struct {
	mu      sync.Mutex
	saved   []Saved
	extract []int64
	pruned  int
	fail    bool
	// generation is recorded with what Save stores (Store.Generation).
	generation int
}

func (s *memStore) Save(_ context.Context, kind, reason string, flinkID int64, stateDir, rel string) (*Saved, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return nil, ErrStateTooLarge
	}
	if _, err := os.Stat(filepath.Join(stateDir, rel, "_metadata")); err != nil {
		return nil, err
	}
	v := Saved{ID: int64(len(s.saved) + 1), Kind: kind, Reason: reason, FlinkID: flinkID, Path: rel, Generation: s.generation}
	s.saved = append(s.saved, v)
	return &v, nil
}

func (s *memStore) LatestAfter(_ context.Context, after int64) (*Saved, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.saved) == 0 || s.saved[len(s.saved)-1].ID <= after {
		return nil, nil
	}
	v := s.saved[len(s.saved)-1]
	return &v, nil
}

func (s *memStore) List(context.Context) ([]Saved, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]Saved(nil), s.saved...)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

func (s *memStore) Extract(_ context.Context, id int64, stateDir string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.extract = append(s.extract, id)
	for _, v := range s.saved {
		if v.ID == id {
			return filepath.Join(stateDir, v.Path), nil
		}
	}
	return "", fmt.Errorf("saved position %d no longer exists", id)
}

func (s *memStore) Prune(context.Context, int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruned++
	return nil
}

func (s *memStore) kinds() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, v := range s.saved {
		out = append(out, v.Kind+":"+v.Reason)
	}
	return out
}

type harness struct {
	m      *JobManager
	f      *fakeFlink
	store  *memStore
	cancel context.CancelFunc
	result chan error
}

func start(t *testing.T, connectors []string, configure func(*JobManager, *fakeFlink, *memStore)) *harness {
	t.Helper()
	l := fakeImage(t, connectors)
	if err := os.WriteFile(filepath.Join(l.ConfigDir, "pipeline.yaml"),
		[]byte("source:\n  password: ${BZ_SOURCE_PASSWORD}\n  note: ${NOT_OURS}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stateDir := t.TempDir()
	f, srv := newFakeFlink(t, stateDir)
	store := &memStore{}
	s := validSettings(connectors...)
	s.StateDir, s.StopTimeoutSeconds = stateDir, 5
	m := &JobManager{Layout: l, Settings: s, REST: NewREST(srv.URL), Store: store, Poll: 5 * time.Millisecond,
		Getenv: func(k string) string {
			if k == "BZ_SOURCE_PASSWORD" {
				return "s3cret"
			}
			return ""
		}}
	m.Launch = &launcher{f: f, layout: l}
	if configure != nil {
		configure(m, f, store)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{m: m, f: f, store: store, cancel: cancel, result: make(chan error, 1)}
	go func() { h.result <- m.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-h.result; err != nil {
			t.Log(err)
		}
	})
	return h
}

func (h *harness) waitFor(t *testing.T, what string, cond func(Status) bool) Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if st := h.m.Status(); cond(st) {
			return st
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %+v", what, h.m.Status())
	return Status{}
}

func (h *harness) stop(t *testing.T) {
	t.Helper()
	h.cancel()
	select {
	case err := <-h.result:
		if err != nil {
			t.Fatal(err)
		}
		h.result <- nil
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after SIGTERM")
	}
}

func TestJobManagerRunsStoresAndStopsWithSavepoint(t *testing.T) {
	h := start(t, []string{"mysql", "starrocks"}, nil)
	st := h.waitFor(t, "running with a stored checkpoint", func(s Status) bool { return s.Phase == PhaseRunning && s.LastStored != nil })
	if st.TaskManagers != 2 || st.TaskManagersWanted != 2 || st.Submits != 1 || st.RecordsWritten != 10 || st.CheckpointsDone != 1 || st.LastCheckpointAt == nil {
		t.Fatalf("status %+v", st)
	}
	// Progress: the highest chunk each table reached, and the source lag.
	st = h.waitFor(t, "progress", func(s Status) bool { return s.Chunks["shop_orders"] == 10 && s.LagMS != nil })
	if *st.LagMS != 1250 || st.Streaming {
		t.Fatal(*st.LagMS, st.Streaming)
	}
	// The binlog split appears once the copy is done.
	h.f.mu.Lock()
	h.f.streaming = true
	h.f.mu.Unlock()
	h.waitFor(t, "streaming", func(s Status) bool { return s.Streaming })
	// A fresh start submits without a saved position; secrets are rendered and the file removed.
	_, _, submits, rendered := h.f.counts()
	args := submits[0]
	if strings.Contains(strings.Join(args, " "), "-s ") || args[1] != "-t" || args[2] != "remote" {
		t.Fatal(args)
	}
	if r := rendered[0]; !strings.Contains(r, `password: "s3cret"`) || !strings.Contains(r, "${NOT_OURS}") {
		t.Fatal(r)
	}
	if _, err := os.Stat(args[0]); !os.IsNotExist(err) {
		t.Fatal("the rendered pipeline (with secrets) was left on disk")
	}
	// The same checkpoint is stored once, however often it is polled.
	time.Sleep(50 * time.Millisecond)
	h.store.mu.Lock()
	pruned := h.store.pruned
	h.store.mu.Unlock()
	if k := h.store.kinds(); len(k) != 1 || k[0] != "checkpoint:periodic" || pruned == 0 {
		t.Fatalf("stored %v (pruned %d)", k, pruned)
	}
	// Health is UP while the job runs.
	rec := httptest.NewRecorder()
	h.m.Health().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/q/health", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"status":"UP"`) {
		t.Fatal(rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.m.Health().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/bz/status", nil))
	if !strings.Contains(rec.Body.String(), `"phase":"running"`) {
		t.Fatal(rec.Body.String())
	}
	// SIGTERM: stop with a savepoint and store it.
	h.stop(t)
	if stops, _, _, _ := h.f.counts(); stops != 1 {
		t.Fatalf("stop calls %d", stops)
	}
	if k := h.store.kinds(); k[len(k)-1] != "savepoint:stop" {
		t.Fatalf("stored %v", k)
	}
}

func TestJobManagerRestoresTheLatestPosition(t *testing.T) {
	stateDirFile := ""
	h := start(t, []string{"postgresql", "iceberg"}, func(m *JobManager, f *fakeFlink, s *memStore) {
		m.Settings.Restore, m.Settings.AllowNonRestored = "latest", true
		dir := filepath.Join(m.Settings.StateDir, "sp", "savepoint-old")
		_ = os.MkdirAll(dir, 0o750)
		_ = os.WriteFile(filepath.Join(dir, "_metadata"), []byte("x"), 0o600)
		s.saved = []Saved{{ID: 7, Kind: "savepoint", Reason: "stop", Path: "sp/savepoint-old"}}
		stateDirFile = dir
	})
	st := h.waitFor(t, "running", func(s Status) bool { return s.Phase == PhaseRunning })
	if st.RestoredFrom == nil || st.RestoredFrom.ID != 7 || st.RestoredFrom.Kind != "savepoint" || st.RestoredFrom.Reason != "stop" {
		t.Fatalf("restored from %+v", st.RestoredFrom)
	}
	_, _, submits, _ := h.f.counts()
	args := strings.Join(submits[0], " ")
	if !strings.Contains(args, "-s file://"+stateDirFile) || !strings.Contains(args, "-cm no_claim") || !strings.Contains(args, " -n") {
		t.Fatal(args)
	}
}

// ADR 0013: a position saved before the columns changed restores without the schema
// coordinator's state; one from the current generation restores as saved; one the helper can't
// change doesn't start.
func TestJobManagerSwitchesColumnsAtAnOlderPosition(t *testing.T) {
	uid := SchemaOperatorUID("0123456789ab")
	withState := metaFile(6, metaEntry("Source", "bz-0123456789ab-source", []byte("offsets"), 1), metaEntry("SchemaOperator", uid, []byte("old schemas"), 1))
	cases := []struct {
		name            string
		saved, now      int
		metadata        []byte
		wantCoordinator bool
		wantFailed      bool
	}{
		{name: "older generation", saved: 1, now: 2, metadata: withState},
		{name: "before generations were recorded", saved: 0, now: 1, metadata: withState},
		{name: "same generation", saved: 2, now: 2, metadata: withState, wantCoordinator: true},
		{name: "metadata it can't change", saved: 0, now: 1, metadata: []byte("not flink"), wantFailed: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := ""
			h := start(t, []string{"postgresql", "iceberg"}, func(m *JobManager, _ *fakeFlink, s *memStore) {
				m.Settings.ShortID, m.Settings.Restore, m.Settings.Generation = "0123456789ab", "latest", tc.now
				dir = filepath.Join(m.Settings.StateDir, "sp", "savepoint-old")
				_ = os.MkdirAll(dir, 0o750)
				_ = os.WriteFile(filepath.Join(dir, "_metadata"), tc.metadata, 0o600)
				s.saved = []Saved{{ID: 7, Kind: "savepoint", Reason: "stop", Path: "sp/savepoint-old", Generation: tc.saved}}
				s.generation = tc.now
			})
			if tc.wantFailed {
				st := h.waitFor(t, "failed", func(s Status) bool { return s.Phase == PhaseFailed })
				if _, _, submits, _ := h.f.counts(); st.ErrorCode != "restore" || len(submits) != 0 || !strings.Contains(st.Error, "before the columns changed") {
					t.Fatalf("%+v submits %v", st, submits)
				}
				return
			}
			h.waitFor(t, "running", func(s Status) bool { return s.Phase == PhaseRunning })
			raw, _ := os.ReadFile(filepath.Join(dir, "_metadata"))
			if got := bytes.Contains(raw, []byte("old schemas")); got != tc.wantCoordinator {
				t.Fatalf("coordinator state kept = %v, want %v", got, tc.wantCoordinator)
			}
			if !bytes.Contains(raw, []byte("offsets")) {
				t.Fatal("the source's state was touched")
			}
			// What this job saves carries the current generation.
			h.waitFor(t, "a stored checkpoint", func(s Status) bool { return s.LastStored != nil })
			if g := h.m.Status().LastStored.Generation; g != tc.now {
				t.Fatalf("stored with generation %d", g)
			}
		})
	}
}

func TestJobManagerStartsFreshAfterAStartOver(t *testing.T) {
	h := start(t, []string{"mysql", "starrocks"}, func(m *JobManager, _ *fakeFlink, s *memStore) {
		m.Settings.Restore, m.Settings.RestoreAfter, m.Settings.FreshStart = "latest", 7, true
		s.saved = []Saved{{ID: 7, Kind: "savepoint", Path: "sp/old"}}
	})
	st := h.waitFor(t, "running", func(s Status) bool { return s.Phase == PhaseRunning })
	_, _, submits, _ := h.f.counts()
	if st.RestoredFrom != nil || strings.Contains(strings.Join(submits[0], " "), "-s ") {
		t.Fatalf("restored a position set aside by Start over: %+v %v", st.RestoredFrom, submits[0])
	}
}

// ADR 0012 L14/L15: with nothing to restore the helper starts a job fresh only when BeziSync
// planned that start; otherwise it stops with no_position and submits nothing.
func TestJobManagerNeverStartsFreshUnplanned(t *testing.T) {
	for name, saved := range map[string][]Saved{"nothing saved": nil, "only positions set aside": {{ID: 7, Kind: "savepoint", Path: "sp/old"}}} {
		t.Run(name, func(t *testing.T) {
			h := start(t, []string{"postgresql", "iceberg"}, func(m *JobManager, _ *fakeFlink, s *memStore) {
				m.Settings.Restore, m.Settings.RestoreAfter = "latest", 7
				s.saved = saved
			})
			st := h.waitFor(t, "failed", func(s Status) bool { return s.Phase == PhaseFailed })
			if _, _, submits, _ := h.f.counts(); st.ErrorCode != "no_position" || len(submits) != 0 || !strings.Contains(st.Error, "Start over") {
				t.Fatalf("%+v submits %v", st, submits)
			}
		})
	}
}

func TestJobManagerResubmitsAfterTheStartupRace(t *testing.T) {
	race := JobException{Name: "java.lang.RuntimeException", Trace: "java.lang.RuntimeException: Could not find schema message from SchemaRegistry for shop.orders", Timestamp: 1}
	h := start(t, []string{"mysql", "starrocks"}, func(_ *JobManager, f *fakeFlink, _ *memStore) {
		f.next = []func(*fakeJob){func(j *fakeJob) {
			j.states, j.completed, j.written, j.excs = []string{"RESTARTING"}, 0, 0, []JobException{race}
		}}
	})
	st := h.waitFor(t, "running after a resubmit", func(s Status) bool { return s.Phase == PhaseRunning && s.Submits == 2 })
	if _, cancels, _, _ := h.f.counts(); cancels != 1 || st.Error != "" {
		t.Fatalf("cancels %d, status %+v", cancels, st)
	}
}

func TestJobManagerGivesUpAfterRepeatedRaces(t *testing.T) {
	race := JobException{Trace: "Could not find schema message from SchemaRegistry for x", Timestamp: 1}
	h := start(t, []string{"mysql", "starrocks"}, func(_ *JobManager, f *fakeFlink, _ *memStore) {
		for i := 0; i <= maxResubmits; i++ {
			f.next = append(f.next, func(j *fakeJob) {
				j.states, j.completed, j.written, j.excs = []string{"RESTARTING"}, 0, 0, []JobException{race}
			})
		}
	})
	st := h.waitFor(t, "failed", func(s Status) bool { return s.Phase == PhaseFailed })
	if _, cancels, _, _ := h.f.counts(); st.ErrorCode != "schema_registry" || st.Submits != maxResubmits+1 || cancels != maxResubmits+1 {
		t.Fatalf("%+v cancels %d", st, cancels)
	}
}

func TestJobManagerStopsOnFailuresThatDontHeal(t *testing.T) {
	cases := map[string]string{
		"java.lang.IllegalStateException: Unable to coerce data record from shop.orders":   "destination_mismatch",
		"org.apache.paimon.catalog.Catalog$TableNotExistException: Table shop.orders does": "destination_missing",
		"java.lang.NullPointerException: Before data is null for UPDATE/DELETE event.":     "replica_identity",
		"TransformException: No match found for function signature MD5(<CHARACTER>)":       "transform_failed",
		// Flink CDC's Oracle reader, as the sandbox logged them (ADR 0006, E21).
		"java.lang.RuntimeException: Failed to resolve Oracle database version":                                                                                 "oracle_version",
		"io.debezium.DebeziumException: Oracle failed to re-construct redo SQL 'insert into \"SHOP\".\"ORDERS\"(\"COL 1\")'":                                    "oracle_schema_change",
		"java.lang.RuntimeException: Fail to analyze table in chunk splitter.\nCaused by: java.sql.SQLSyntaxErrorException: ORA-01031: insufficient privileges": "oracle_analyze",
		"io.debezium.DebeziumException: Failed to flush Oracle LogWriter (LGWR) buffers to disk":                                                                "oracle_flush_table",
		"java.lang.IllegalArgumentException: Cannot find any table by the option 'tables' = SHOP.ORDERS":                                                        "tables_missing",
	}
	for trace, code := range cases {
		t.Run(code, func(t *testing.T) {
			h := start(t, []string{"mysql", "starrocks"}, func(_ *JobManager, f *fakeFlink, _ *memStore) {
				f.next = []func(*fakeJob){func(j *fakeJob) {
					j.states, j.excs = []string{"RESTARTING"}, []JobException{{Trace: trace, Timestamp: 1}}
				}}
			})
			st := h.waitFor(t, "failed", func(s Status) bool { return s.Phase == PhaseFailed })
			_, cancels, _, _ := h.f.counts()
			if st.ErrorCode != code || st.Error == "" || cancels != 1 {
				t.Fatalf("%+v cancels %d", st, cancels)
			}
			rec := httptest.NewRecorder()
			h.m.Health().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/q/health", nil))
			if rec.Code != 503 || !strings.Contains(rec.Body.String(), st.Error[:20]) {
				t.Fatal(rec.Code, rec.Body.String())
			}
		})
	}
}

func TestJobManagerKeepsTheCheckpointWhenTheSavepointFails(t *testing.T) {
	h := start(t, []string{"mysql", "starrocks"}, func(_ *JobManager, f *fakeFlink, _ *memStore) { f.stopFail = true })
	h.waitFor(t, "running with a stored checkpoint", func(s Status) bool { return s.Phase == PhaseRunning && s.LastStored != nil })
	h.stop(t)
	for _, k := range h.store.kinds() {
		if k == "savepoint:stop" {
			t.Fatal("a failed savepoint was stored")
		}
	}
	if len(h.store.kinds()) != 1 {
		t.Fatal(h.store.kinds())
	}
}

func TestJobManagerReportsAnImageThatCantRunThePipeline(t *testing.T) {
	h := start(t, []string{"mysql"}, func(m *JobManager, _ *fakeFlink, _ *memStore) {
		m.Settings.Connectors = []string{"mysql", "fluss"}
	})
	st := h.waitFor(t, "config error", func(s Status) bool { return s.Phase == PhaseConfigError })
	if _, _, submits, _ := h.f.counts(); !strings.Contains(st.Error, "fluss connector") || len(submits) != 0 {
		t.Fatalf("%+v", st)
	}
	h.stop(t)
}

func TestJobManagerRecordsStoreFailures(t *testing.T) {
	h := start(t, []string{"mysql", "starrocks"}, func(_ *JobManager, _ *fakeFlink, s *memStore) { s.fail = true })
	// The store error can be recorded a moment before the phase moves on from submitting: wait
	// for both, then check the error and that the job kept running.
	st := h.waitFor(t, "store error while running", func(s Status) bool { return s.StoreError != "" && s.Phase == PhaseRunning })
	if !strings.Contains(st.StoreError, "size") {
		t.Fatalf("%+v", st)
	}
}

func TestJobManagerExitsWhenFlinkDies(t *testing.T) {
	h := start(t, []string{"mysql", "starrocks"}, nil)
	h.waitFor(t, "running", func(s Status) bool { return s.Phase == PhaseRunning })
	h.m.Launch.(*launcher).done <- fmt.Errorf("exit status 1")
	select {
	case err := <-h.result:
		if err == nil || !strings.Contains(err.Error(), "JobManager exited") {
			t.Fatal(err)
		}
		h.result <- nil
	case <-time.After(5 * time.Second):
		t.Fatal("Run kept going after the JobManager died")
	}
}

// Only whole-value placeholders are filled: a "$" inside a quoted value (a transform literal, a
// name) reaches Flink CDC as written and never names a secret.
func TestRenderSecretsOnlyWholeValues(t *testing.T) {
	tmpl := "source:\n  password: ${BZ_PW}\ntransform:\n" +
		"- source-table: \"a.b\"\n  projection: \"*, 'US$5' AS `p`, '$BZ_PW' AS `q`, '${BZ_PW}' AS `r`\"\n" +
		"  filter: \"(`c` = '$HOME')\"\nsink:\n  name: \"${BZ_PW}\"\n  user: \"x\" ${BZ_PW}\n"
	out := RenderSecrets(tmpl, func(k string) string {
		if k == "BZ_PW" {
			return "s3cr3t"
		}
		return "leaked"
	})
	if strings.Count(out, "s3cr3t") != 1 || strings.Contains(out, "leaked") || !strings.Contains(out, "  password: \"s3cr3t\"\n") {
		t.Fatalf("filled more than the whole-value placeholder:\n%s", out)
	}
	if want := strings.Replace(tmpl, "${BZ_PW}", `"s3cr3t"`, 1); out != want {
		t.Fatalf("changed other text:\n%s", out)
	}
}

// A secret can't end its YAML value or add keys, whatever it contains.
func TestRenderSecretsQuotesValues(t *testing.T) {
	tmpl := "source:\n  password: ${BZ_PW}\n  user: \"u\"\nsink:\n  token: ${BZ_EMPTY}\n"
	for _, pw := range []string{`p"w`, "a\nsink:\n  type: evil", `back\slash: {x}`, "#not-a-comment", "unicode ✓  ", "${BZ_PW}"} {
		out := RenderSecrets(tmpl, func(k string) string {
			if k == "BZ_PW" {
				return pw
			}
			return ""
		})
		var doc struct {
			Source map[string]string `yaml:"source"`
			Sink   map[string]string `yaml:"sink"`
		}
		if err := yaml.Unmarshal([]byte(out), &doc); err != nil {
			t.Fatalf("%q: %v\n%s", pw, err, out)
		}
		if doc.Source["password"] != pw || doc.Source["user"] != "u" || len(doc.Sink) != 1 || doc.Sink["token"] != "" {
			t.Fatalf("%q rendered as %+v", pw, doc)
		}
	}
}
