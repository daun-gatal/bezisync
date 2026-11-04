package flinkhelper

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// StateStore is where saved positions live (Store, or a fake in tests).
type StateStore interface {
	Save(ctx context.Context, kind, reason string, flinkID int64, stateDir, rel string) (*Saved, error)
	LatestAfter(ctx context.Context, after int64) (*Saved, error)
	List(ctx context.Context) ([]Saved, error)
	Extract(ctx context.Context, id int64, stateDir string) (string, error)
	Prune(ctx context.Context, keep int) error
}

// Phases of the JobManager helper, reported in /bz/status.
const (
	PhaseStarting     = "starting"     // preparing the classpath, starting Flink
	PhaseWaitingTMs   = "waiting"      // waiting for TaskManagers to register
	PhaseSubmitting   = "submitting"   // restoring and submitting the pipeline
	PhaseRunning      = "running"      // the job runs
	PhaseRestarting   = "restarting"   // the job restarts after a failure
	PhaseFinished     = "finished"     // a copy-only pipeline finished
	PhaseFailed       = "failed"       // the job failed and needs someone (Error says why)
	PhaseStopping     = "stopping"     // stopping with a savepoint
	PhaseConfigError  = "config_error" // the image or configuration can't run the pipeline
	PhaseResubmitting = "resubmitting" // a start-up failure: submitting again from the same point
)

// Status is what the helper knows, served at /bz/status.
type Status struct {
	Phase              string     `json:"phase"`
	Error              string     `json:"error,omitempty"`
	ErrorCode          string     `json:"error_code,omitempty"`
	JobID              string     `json:"job_id,omitempty"`
	JobState           string     `json:"job_state,omitempty"`
	JobStartedAt       *time.Time `json:"job_started_at,omitempty"`
	TaskManagers       int        `json:"taskmanagers"`
	TaskManagersWanted int        `json:"taskmanagers_wanted"`
	RestoredFrom       *Saved     `json:"restored_from,omitempty"`
	LastCheckpointAt   *time.Time `json:"last_checkpoint_at,omitempty"`
	CheckpointsDone    int64      `json:"checkpoints_completed"`
	CheckpointsFailed  int64      `json:"checkpoints_failed"`
	LastStored         *Saved     `json:"last_stored,omitempty"`
	StoreError         string     `json:"store_error,omitempty"`
	Restarts           int        `json:"restarts"`
	Submits            int        `json:"submits"`
	Exceptions         []string   `json:"exceptions,omitempty"`
	RecordsRead        int64      `json:"records_read"`
	RecordsWritten     int64      `json:"records_written"`
	LagMS              *float64   `json:"lag_ms,omitempty"`
	// Chunks counts the copy chunks each table's readers reached, keyed as Flink names its split
	// metrics: the table id with dots as underscores ("shop_orders" → 37). BeziSync matches the
	// keys to the pipeline's tables.
	Chunks map[string]int64 `json:"chunks,omitempty"`
	// Streaming: the first copy is done and changes are read from the log (the source's binlog
	// or stream split exists). Tables added later are copied while it streams.
	Streaming bool      `json:"streaming"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Launcher starts Flink's processes and the client that submits the pipeline.
type Launcher interface {
	// StartJobManager starts the Flink JobManager; done closes when it exits (with its error).
	StartJobManager(ctx context.Context) (done <-chan error, stop func(), err error)
	// Submit runs the Flink CDC client with args and returns its output.
	Submit(ctx context.Context, args []string) (string, error)
}

// JobManager runs the JobManager side of a pipeline.
type JobManager struct {
	Layout   Layout
	Settings Settings
	REST     *REST
	Store    StateStore
	Launch   Launcher
	// Getenv resolves ${BZ_…} placeholders of pipeline.yaml (os.Getenv by default).
	Getenv func(string) string
	// Poll is how often the job is watched.
	Poll time.Duration
	Now  func() time.Time

	mu     sync.Mutex
	status Status
	// lastStored is the Flink id of the newest checkpoint stored.
	lastStored int64
}

func (m *JobManager) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now().UTC()
}

// Status returns a copy of the current status.
func (m *JobManager) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.status
	if s.Chunks != nil {
		c := make(map[string]int64, len(s.Chunks))
		for k, v := range s.Chunks {
			c[k] = v
		}
		s.Chunks = c
	}
	s.Exceptions = append([]string(nil), s.Exceptions...)
	return s
}

func (m *JobManager) update(f func(*Status)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f(&m.status)
	m.status.UpdatedAt = m.now()
}

func (m *JobManager) fail(phase, code, msg string) {
	m.update(func(s *Status) { s.Phase, s.ErrorCode, s.Error = phase, code, msg })
	logf("%s: %s", code, msg)
}

// Known failures that do not heal by restarting: the job is cancelled and the reason reported.
var fatal = []struct {
	re         *regexp.Regexp
	code, text string
}{
	{regexp.MustCompile(`Unable to coerce data record`), "destination_mismatch",
		"The destination tables no longer match what this pipeline writes (a destination was renamed or changed). Start over to copy the tables again."},
	{regexp.MustCompile(`TableNotExistException|NoSuchTableException|Table does not exist|Unknown table`), "destination_missing",
		"A destination table the saved position refers to is gone. Start over to create and copy the tables again."},
	{regexp.MustCompile(`Before data is null for UPDATE/DELETE event`), "replica_identity",
		"PostgreSQL sends updates and deletes without the old row: set REPLICA IDENTITY FULL on the captured tables (preflight shows the statement), then Start over."},
	{regexp.MustCompile(`TransformException|No match found for function signature`), "transform_failed",
		"A transform can't run on Flink CDC. Change it in the Transform step, then deploy again."},
}

// The start-up race of Flink CDC's schema registry: a job that hits it before its first
// checkpoint never recovers on its own, but submitting it again works.
var schemaRace = regexp.MustCompile(`Could not find schema message from SchemaRegistry|Schema is never registered`)

// maxResubmits bounds submitting again after the start-up race.
const maxResubmits = 3

// Run runs until ctx is cancelled (SIGTERM), then stops the job with a savepoint.
func (m *JobManager) Run(ctx context.Context) error {
	if m.Poll == 0 {
		m.Poll = 5 * time.Second
	}
	if m.Getenv == nil {
		m.Getenv = os.Getenv
	}
	m.update(func(s *Status) { s.Phase, s.TaskManagersWanted = PhaseStarting, m.Settings.TaskManagers })
	if err := m.Layout.Prepare(m.Settings); err != nil {
		m.fail(PhaseConfigError, "image", err.Error())
		<-ctx.Done()
		return nil
	}
	done, stopFlink, err := m.Launch.StartJobManager(ctx)
	if err != nil {
		m.fail(PhaseConfigError, "jobmanager", "The Flink JobManager did not start: "+err.Error())
		<-ctx.Done()
		return nil
	}
	defer stopFlink()
	// The job's lifecycle runs until SIGTERM; then it is stopped with a savepoint.
	work, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	finished := make(chan struct{})
	go func() { m.lifecycle(work); close(finished) }()
	select {
	case err := <-done:
		cancelWork()
		<-finished
		return fmt.Errorf("the Flink JobManager exited: %w", err)
	case <-ctx.Done():
	}
	cancelWork()
	<-finished
	m.stop()
	return nil
}

// lifecycle waits for the cluster, submits the pipeline and watches it until ctx ends or the
// pipeline needs someone.
func (m *JobManager) lifecycle(ctx context.Context) {
	if m.waitForTaskManagers(ctx) != nil {
		return
	}
	restore, err := m.restorePoint(ctx)
	if err != nil {
		m.fail(PhaseFailed, "restore", err.Error())
		return
	}
	attempt := 0
	backoff := 30 * time.Second
	for ctx.Err() == nil {
		jobID, err := m.submit(ctx, restore)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.fail(PhaseFailed, "submit", err.Error())
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(2*backoff, 10*time.Minute)
			continue
		}
		outcome := m.watch(ctx, jobID)
		switch outcome {
		case outcomeStopped:
			return
		case outcomeRace:
			attempt++
			if attempt > maxResubmits {
				m.fail(PhaseFailed, "schema_registry",
					"Flink CDC's schema registry failed while starting the pipeline, "+strconv.Itoa(maxResubmits)+" times. Restart the pipeline; if it keeps failing, Start over.")
				return
			}
			m.update(func(s *Status) { s.Phase = PhaseResubmitting })
			logf("start-up race in Flink CDC's schema registry: submitting again (%d of %d)", attempt, maxResubmits)
			_ = m.REST.Cancel(ctx, jobID)
			m.waitGone(ctx, jobID)
		case outcomeFatal:
			_ = m.REST.Cancel(ctx, jobID)
			return
		case outcomeFailed:
			// The job used up Flink's restarts: submit again from the newest stored state, like a
			// crashed container that restarts.
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(2*backoff, 10*time.Minute)
			if restore, err = m.latestPoint(ctx); err != nil {
				m.fail(PhaseFailed, "restore", err.Error())
				return
			}
		case outcomeFinished:
			<-ctx.Done()
			return
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func (m *JobManager) waitForTaskManagers(ctx context.Context) error {
	m.update(func(s *Status) { s.Phase = PhaseWaitingTMs })
	for {
		n, err := m.REST.TaskManagers(ctx)
		if err == nil {
			m.update(func(s *Status) { s.TaskManagers = n })
			if n >= m.Settings.TaskManagers {
				return nil
			}
		}
		if !sleep(ctx, time.Second) {
			return ctx.Err()
		}
	}
}

// restorePoint resolves Settings.Restore to a directory to restore from ("" = start fresh).
func (m *JobManager) restorePoint(ctx context.Context) (string, error) {
	switch r := m.Settings.Restore; r {
	case "":
		return "", nil
	case "latest":
		return m.latestPoint(ctx)
	default:
		id, err := strconv.ParseInt(r, 10, 64)
		if err != nil {
			return "", fmt.Errorf("invalid saved position %q", r)
		}
		return m.extract(ctx, id)
	}
}

// latestPoint is the newest stored state after the Start over floor ("" when there is none: a
// first start, or a start over).
func (m *JobManager) latestPoint(ctx context.Context) (string, error) {
	latest, err := m.Store.LatestAfter(ctx, m.Settings.RestoreAfter)
	if err != nil {
		return "", fmt.Errorf("can't read the saved positions: %w", err)
	}
	if latest == nil {
		return "", nil
	}
	return m.extract(ctx, latest.ID)
}

func (m *JobManager) extract(ctx context.Context, id int64) (string, error) {
	dir, err := m.Store.Extract(ctx, id, m.Settings.StateDir)
	if err != nil {
		return "", err
	}
	from := &Saved{ID: id}
	if all, err := m.Store.List(ctx); err == nil {
		for i := range all {
			if all[i].ID == id {
				from = &all[i]
			}
		}
	}
	m.update(func(s *Status) { s.RestoredFrom = from })
	return dir, nil
}

// RenderSecrets replaces each ${BZ_…} placeholder (always a whole YAML value) with the
// variable's value as a double-quoted scalar, so no value can end the scalar or add keys.
func RenderSecrets(tmpl string, getenv func(string) string) string {
	return os.Expand(tmpl, func(name string) string {
		if strings.HasPrefix(name, "BZ_") {
			return quoteScalar(getenv(name))
		}
		return "${" + name + "}"
	})
}

// quoteScalar is s as a YAML double-quoted scalar (JSON string escapes are valid YAML).
func quoteScalar(s string) string {
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	return strings.TrimSuffix(b.String(), "\n")
}

var jobIDLine = regexp.MustCompile(`Job ID:\s*([0-9a-f]{32})`)

// submit renders the pipeline with its secrets into a private file, submits it and deletes the
// file.
func (m *JobManager) submit(ctx context.Context, restore string) (string, error) {
	m.update(func(s *Status) { s.Phase, s.Error, s.ErrorCode = PhaseSubmitting, "", "" })
	tmpl, err := os.ReadFile(filepath.Join(m.Layout.ConfigDir, "pipeline.yaml"))
	if err != nil {
		return "", err
	}
	rendered := RenderSecrets(string(tmpl), m.Getenv)
	path := filepath.Join(m.Layout.WorkDir, "pipeline.yaml")
	if err := os.WriteFile(path, []byte(rendered), 0o600); err != nil { //nolint:gosec // the helper's own work dir
		return "", err
	}
	defer func() { _ = os.Remove(path) }()
	args := []string{path, "-t", "remote", "--flink-home", m.Layout.FlinkHome}
	if restore != "" {
		args = append(args, "-s", "file://"+restore, "-cm", "no_claim")
		if m.Settings.AllowNonRestored {
			args = append(args, "-n")
		}
	}
	out, err := m.Launch.Submit(ctx, args)
	if err != nil {
		return "", fmt.Errorf("submitting the pipeline failed: %s", lastLines(out, 3))
	}
	match := jobIDLine.FindStringSubmatch(out)
	if match == nil {
		return "", fmt.Errorf("submitting the pipeline returned no job: %s", lastLines(out, 3))
	}
	m.update(func(s *Status) { s.JobID, s.Submits = match[1], s.Submits+1 })
	logf("submitted job %s (restore: %q)", match[1], restore)
	return match[1], nil
}

type outcome int

const (
	outcomeStopped outcome = iota
	outcomeRace
	outcomeFatal
	outcomeFailed
	outcomeFinished
)

// watch follows a job until it ends or ctx is cancelled.
func (m *JobManager) watch(ctx context.Context, jobID string) outcome {
	lastState := ""
	completedHere := int64(0)
	for {
		if !sleep(ctx, m.Poll) {
			return outcomeStopped
		}
		job, err := m.REST.Job(ctx, jobID)
		if err != nil {
			continue
		}
		cps, _ := m.REST.Checkpoints(ctx, jobID)
		excs, _ := m.REST.Exceptions(ctx, jobID)
		if cps != nil {
			completedHere = cps.Counts.Completed
			m.storeCheckpoint(ctx, cps)
		}
		m.observe(job, cps, excs, lastState)
		lastState = job.State
		m.progress(ctx, job)
		if code, text, ok := fatalOf(excs); ok {
			m.fail(PhaseFailed, code, text)
			return outcomeFatal
		}
		if job.State == "RESTARTING" || job.State == "FAILING" || job.State == "FAILED" {
			if raceOf(excs) && (completedHere == 0 || m.recordsWritten() == 0) {
				return outcomeRace
			}
		}
		switch job.State {
		case "FAILED":
			m.fail(PhaseFailed, "job_failed", "The Flink job failed: "+lastException(excs)+". BeziSync restarts it from the last saved position.")
			return outcomeFailed
		case "FINISHED":
			m.update(func(s *Status) { s.Phase = PhaseFinished })
			return outcomeFinished
		case "CANCELED":
			m.fail(PhaseFailed, "job_cancelled", "The Flink job was cancelled outside BeziSync. BeziSync starts it again from the last saved position.")
			return outcomeFailed
		}
	}
}

func (m *JobManager) recordsWritten() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.status.RecordsWritten
}

func (m *JobManager) observe(job *Job, cps *Checkpoints, excs []JobException, lastState string) {
	m.update(func(s *Status) {
		s.JobState = job.State
		if job.StartTime > 0 {
			t := time.UnixMilli(job.StartTime).UTC()
			s.JobStartedAt = &t
		}
		switch job.State {
		case "RUNNING":
			s.Phase, s.Error, s.ErrorCode = PhaseRunning, "", ""
		case "RESTARTING", "FAILING", "INITIALIZING", "CREATED":
			s.Phase = PhaseRestarting
			if job.State == "RESTARTING" && lastState != "RESTARTING" {
				s.Restarts++
			}
		}
		var read, written int64
		for i, v := range job.Vertices {
			if i == 0 {
				read += v.Metrics.WriteRecords
			}
			if i == len(job.Vertices)-1 || strings.Contains(v.Name, "Sink Writer") {
				written = max(written, v.Metrics.WriteRecords, v.Metrics.ReadRecords)
			}
		}
		s.RecordsRead, s.RecordsWritten = read, written
		if cps != nil {
			s.CheckpointsDone, s.CheckpointsFailed = cps.Counts.Completed, cps.Counts.Failed
			if c := cps.Latest.Completed; c != nil && c.LatestAck > 0 {
				t := time.UnixMilli(c.LatestAck).UTC()
				s.LastCheckpointAt = &t
			}
		}
		s.Exceptions = nil
		for _, e := range excs[max(0, len(excs)-3):] {
			s.Exceptions = append(s.Exceptions, firstLine(e.Trace))
		}
	})
}

// storeCheckpoint stores the newest completed checkpoint once.
func (m *JobManager) storeCheckpoint(ctx context.Context, cps *Checkpoints) {
	c := cps.Latest.Completed
	if c == nil || c.ExternalPath == "" {
		return
	}
	m.mu.Lock()
	seen := m.lastStored
	m.mu.Unlock()
	path := strings.TrimPrefix(c.ExternalPath, "file:")
	rel, err := filepath.Rel(m.Settings.StateDir, path)
	if err != nil || !validRel(rel) {
		return
	}
	key := c.ID
	if key == seen {
		return
	}
	saved, err := m.Store.Save(ctx, "checkpoint", "periodic", c.ID, m.Settings.StateDir, rel)
	m.mu.Lock()
	if err != nil {
		m.status.StoreError = err.Error()
	} else {
		m.lastStored = key
		m.status.LastStored, m.status.StoreError = saved, ""
	}
	m.mu.Unlock()
	if err == nil {
		_ = m.Store.Prune(ctx, m.Settings.KeepCheckpoints)
	} else {
		logf("storing checkpoint %d failed: %v", c.ID, err)
	}
}

var (
	splitMetric = regexp.MustCompile(`\.split\.(.+)_(\d+)\.watermark\.currentWatermark$`)
	// The split that reads changes: MySQL's binlog-split, PostgreSQL's and SQL Server's stream-split.
	streamSplit = regexp.MustCompile(`\.split\.(binlog|stream)-split\.watermark\.currentWatermark$`)
)

// progress reads copy progress (the chunk each table's readers reached) and source lag.
func (m *JobManager) progress(ctx context.Context, job *Job) {
	if len(job.Vertices) == 0 || job.State != "RUNNING" {
		return
	}
	src := job.Vertices[0].ID
	names, err := m.REST.MetricNames(ctx, job.ID, src)
	if err != nil {
		return
	}
	chunks := map[string]int64{}
	var lag []string
	streaming := false
	for _, n := range names {
		streaming = streaming || streamSplit.MatchString(n)
		if mm := splitMetric.FindStringSubmatch(n); mm != nil {
			if c, err := strconv.ParseInt(mm[2], 10, 64); err == nil && c+1 > chunks[mm[1]] {
				chunks[mm[1]] = c + 1
			}
		}
		if strings.HasSuffix(n, ".currentEmitEventTimeLag") {
			lag = append(lag, n)
		}
	}
	var lagMS *float64
	if len(lag) > 0 {
		if vals, err := m.REST.SubtaskMetrics(ctx, job.ID, src, lag, "max"); err == nil {
			for _, v := range vals {
				if v > 0 && (lagMS == nil || v > *lagMS) {
					x := v
					lagMS = &x
				}
			}
		}
	}
	m.update(func(s *Status) {
		if s.Chunks == nil {
			s.Chunks = map[string]int64{}
		}
		for t, c := range chunks {
			if c > s.Chunks[t] {
				s.Chunks[t] = c
			}
		}
		s.LagMS = lagMS
		s.Streaming = s.Streaming || streaming
	})
}

func fatalOf(excs []JobException) (string, string, bool) {
	for i := len(excs) - 1; i >= 0; i-- {
		for _, f := range fatal {
			if f.re.MatchString(excs[i].Trace) || f.re.MatchString(excs[i].Name) {
				return f.code, f.text, true
			}
		}
	}
	return "", "", false
}

func raceOf(excs []JobException) bool {
	for _, e := range excs {
		if schemaRace.MatchString(e.Trace) {
			return true
		}
	}
	return false
}

func lastException(excs []JobException) string {
	if len(excs) == 0 {
		return "no exception was reported"
	}
	return firstLine(excs[len(excs)-1].Trace)
}

func (m *JobManager) waitGone(ctx context.Context, jobID string) {
	for i := 0; i < 120; i++ {
		j, err := m.REST.Job(ctx, jobID)
		if err != nil || j.State == "CANCELED" || j.State == "FAILED" || j.State == "FINISHED" {
			return
		}
		if !sleep(ctx, time.Second) {
			return
		}
	}
}

// stop stops the job with a savepoint and stores it; when that fails the newest checkpoint is
// already stored and the next start uses it.
func (m *JobManager) stop() {
	st := m.Status()
	if st.JobID == "" || (st.JobState != "RUNNING") {
		logf("stopping: no running job to save")
		return
	}
	m.update(func(s *Status) { s.Phase = PhaseStopping })
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.Settings.StopTimeoutSeconds)*time.Second)
	defer cancel()
	location, err := m.REST.StopWithSavepoint(ctx, st.JobID, "file://"+filepath.Join(m.Settings.StateDir, "sp"))
	if err != nil {
		logf("stop with savepoint failed (the newest checkpoint is kept): %v", err)
		if cps, cerr := m.REST.Checkpoints(ctx, st.JobID); cerr == nil {
			m.storeCheckpoint(ctx, cps)
		}
		return
	}
	rel, err := filepath.Rel(m.Settings.StateDir, strings.TrimPrefix(location, "file:"))
	if err != nil || !validRel(rel) {
		logf("savepoint at an unexpected path %s", location)
		return
	}
	if _, err := m.Store.Save(ctx, "savepoint", "stop", 0, m.Settings.StateDir, rel); err != nil {
		logf("storing the savepoint failed (the newest checkpoint is kept): %v", err)
		return
	}
	logf("stopped with savepoint %s", rel)
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	var keep []string
	for _, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), "at ") {
			continue
		}
		keep = append(keep, strings.TrimSpace(l))
	}
	if len(keep) > n {
		keep = keep[len(keep)-n:]
	}
	return strings.Join(keep, " ")
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "bezisync-helper: "+format+"\n", args...)
}
