// Package domain holds the pure product rules: health derivation, pipeline specs,
// policies, maintenance windows, schema diffs and manifests.
package domain

import (
	"fmt"

	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
)

// Lifecycle and health values.
const (
	LifecycleDraft        = "draft"
	LifecycleDeploying    = "deploying"
	LifecycleSnapshotting = "snapshotting"
	LifecycleStreaming    = "streaming"
	LifecyclePaused       = "paused"
	LifecycleFailed       = "failed"

	HealthHealthy = "healthy"
	HealthLagging = "lagging"
	HealthAtRisk  = "at_risk"
	HealthDown    = "down"
	HealthUnknown = "unknown"
)

// DeployGrace is how long a fresh deploy may look incomplete before it counts as failed.
const DeployGrace = 5 * time.Minute

// Metrics are the Debezium JMX values BeziSync reads (nil = not reported).
type Metrics struct {
	Connected               *bool
	SnapshotRunning         *bool
	SnapshotCompleted       *bool
	SnapshotTablesTotal     *int
	SnapshotTablesRemaining *int
	SnapshotRowsScanned     *int
	MSBehindSource          *float64
	MSSinceLastEvent        *float64
	EventsSeenStreaming     *float64
	EventsSeenSnapshot      *float64
	QueueRemaining          *float64
	QueueTotal              *float64
}

// AsDict returns the reported values only (Python Metrics.as_dict), in field order.
func (m Metrics) AsDict() map[string]any {
	out := map[string]any{}
	putB := func(k string, v *bool) {
		if v != nil {
			out[k] = *v
		}
	}
	putI := func(k string, v *int) {
		if v != nil {
			out[k] = *v
		}
	}
	putF := func(k string, v *float64) {
		if v != nil {
			out[k] = pyjson.Float(*v) // Python floats: 250.0, not 250
		}
	}
	putB("connected", m.Connected)
	putB("snapshot_running", m.SnapshotRunning)
	putB("snapshot_completed", m.SnapshotCompleted)
	putI("snapshot_tables_total", m.SnapshotTablesTotal)
	putI("snapshot_tables_remaining", m.SnapshotTablesRemaining)
	putI("snapshot_rows_scanned", m.SnapshotRowsScanned)
	putF("ms_behind_source", m.MSBehindSource)
	putF("ms_since_last_event", m.MSSinceLastEvent)
	putF("events_seen_streaming", m.EventsSeenStreaming)
	putF("events_seen_snapshot", m.EventsSeenSnapshot)
	putF("queue_remaining", m.QueueRemaining)
	putF("queue_total", m.QueueTotal)
	return out
}

// Reason explains part of a verdict.
type Reason struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	Fix      string `json:"fix"`
	Severity string `json:"severity"` // info | warn | error
}

// NewReason builds a reason with Python's defaults (fix "", severity "warn").
func NewReason(code, message string) Reason {
	return Reason{Code: code, Message: message, Severity: "warn"}
}

// Verdict is the derived lifecycle and health.
type Verdict struct {
	Lifecycle string
	Health    string
	Reasons   []Reason
	Summary   string
}

var (
	metricRe  = regexp.MustCompile(`^(debezium_metrics_[A-Za-z]+)\{([^}]*)\}\s+([-+0-9.eE]+|NaN)$`)
	contextRe = regexp.MustCompile(`context="([^"]+)"`)
)

// ParseMetrics parses the Prometheus text of Debezium's JMX exporter.
func ParseMetrics(text string) Metrics {
	type key struct{ name, ctx string }
	values := map[key]float64{}
	for _, line := range SplitLines(text) {
		m := metricRe.FindStringSubmatch(PyStrip(line))
		if m == nil {
			continue
		}
		name, labels, raw := m[1], m[2], m[3]
		ctx := ""
		if c := contextRe.FindStringSubmatch(labels); c != nil {
			ctx = c[1]
		}
		v, ok := PyFloat(raw)
		if !ok {
			continue
		}
		values[key{strings.TrimPrefix(name, "debezium_metrics_"), ctx}] = v
	}
	get := func(name, ctx string) *float64 {
		if v, ok := values[key{name, ctx}]; ok {
			return &v
		}
		return nil
	}
	flag := func(name, ctx string) *bool {
		v := get(name, ctx)
		if v == nil {
			return nil
		}
		b := *v >= 1
		return &b
	}
	num := func(name, ctx string) *int {
		v := get(name, ctx)
		if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) {
			return nil
		}
		n := int(*v) // int() truncates toward zero
		return &n
	}
	nonNeg := func(v *float64) *float64 {
		if v != nil && *v >= 0 {
			return v
		}
		return nil
	}
	return Metrics{
		Connected:               flag("Connected", "streaming"),
		SnapshotRunning:         flag("SnapshotRunning", "snapshot"),
		SnapshotCompleted:       flag("SnapshotCompleted", "snapshot"),
		SnapshotTablesTotal:     num("TotalTableCount", "snapshot"),
		SnapshotTablesRemaining: num("RemainingTableCount", "snapshot"),
		SnapshotRowsScanned:     num("RowsScanned", "snapshot"),
		MSBehindSource:          nonNeg(get("MilliSecondsBehindSource", "streaming")),
		MSSinceLastEvent:        nonNeg(get("MilliSecondsSinceLastEvent", "streaming")),
		EventsSeenStreaming:     get("TotalNumberOfEventsSeen", "streaming"),
		EventsSeenSnapshot:      get("TotalNumberOfEventsSeen", "snapshot"),
		QueueRemaining:          get("QueueRemainingCapacity", "streaming"),
		QueueTotal:              get("QueueTotalCapacity", "streaming"),
	}
}

// OnlyChosenTables keeps the snapshot table counts to the tables the pipeline captures (chosen):
// Debezium also counts the signal table BeziSync adds to every source, and never copies it, so
// its counts would read one table short of done. A finished copy has nothing remaining.
func (m *Metrics) OnlyChosenTables(chosen int) {
	if m.SnapshotTablesTotal != nil && chosen > 0 && *m.SnapshotTablesTotal > chosen {
		extra := *m.SnapshotTablesTotal - chosen
		total := chosen
		m.SnapshotTablesTotal = &total
		if m.SnapshotTablesRemaining != nil {
			remaining := min(max(*m.SnapshotTablesRemaining-extra, 0), chosen)
			m.SnapshotTablesRemaining = &remaining
		}
	}
	running := m.SnapshotRunning != nil && *m.SnapshotRunning
	if m.SnapshotTablesRemaining != nil && m.SnapshotCompleted != nil && *m.SnapshotCompleted && !running {
		none := 0
		m.SnapshotTablesRemaining = &none
	}
}

// PyFloat parses like Python float() (accepts "NaN", "inf", "+1.5", "1e3").
func PyFloat(s string) (float64, bool) {
	t := strings.TrimSpace(s)
	switch strings.ToLower(strings.TrimLeft(t, "+-")) {
	case "nan":
		return math.NaN(), true
	case "inf", "infinity":
		if strings.HasPrefix(t, "-") {
			return math.Inf(-1), true
		}
		return math.Inf(1), true
	}
	if strings.ContainsAny(strings.ToLower(t), "xp_") { // Go accepts hex floats and underscores
		return 0, false
	}
	v, err := strconv.ParseFloat(t, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

type diagnosisRule struct {
	re           *regexp.Regexp
	code, msg, f string
}

// Known failure signatures in Debezium Server logs → plain cause + fix.
var diagnoses = []diagnosisRule{
	{regexp.MustCompile(`(?i)password authentication failed|Access denied for user|Login failed for user|Authentication failed`),
		"source_auth", "The source rejected the connector's credentials.",
		"The password was probably rotated. Update the connection's secret, then restart."},
	{regexp.MustCompile(`(?i)replication slot "[^"]+" is active`),
		"slot_in_use", "Another process is using this pipeline's replication slot.",
		"Make sure only one connector runs for this pipeline (check for an old deployment)."},
	{regexp.MustCompile(`(?i)logical decoding requires wal_level|wal_level.*logical`),
		"wal_level", "The database is no longer set up for logical replication.",
		"Set wal_level = logical (or rds.logical_replication = 1) and restart the database."},
	{regexp.MustCompile(`(?i)must be superuser or replication role|permission denied to start WAL sender|must have REPLICATION`),
		"replication_permission", "The connector user lost the replication permission.",
		"ALTER ROLE <user> WITH REPLICATION; (RDS: GRANT rds_replication TO <user>;)"},
	{regexp.MustCompile(`(?i)Topic \S+ not present in metadata|UNKNOWN_TOPIC_OR_PARTITION|TopicAuthorizationException`),
		"sink_topic", "The destination topic does not exist or the connector may not write to it.",
		"Create the topic or grant Write/Create on it, or enable 'Create topics automatically'."},
	{regexp.MustCompile(`(?i)Could not resolve host|UnknownHostException|Name or service not known`),
		"dns", "A hostname could not be resolved from the runtime.",
		"Check the host name and the runtime's DNS."},
	{regexp.MustCompile(`(?i)Connection refused|connect timed out|Connection to .* refused|SocketTimeoutException`),
		"network", "The connector could not reach the source, destination or state database.",
		"Check that the runtime can reach the hosts (firewall, security groups)."},
	{regexp.MustCompile(`(?i)OutOfMemoryError|Java heap space`),
		"oom", "The connector ran out of memory.",
		"Choose a larger resource preset or lower the batch size."},
	{regexp.MustCompile(`(?i)requested WAL segment .* has already been removed|cannot read from logical replication slot`),
		"wal_removed", "The database removed WAL the pipeline still needed; changes were lost.",
		"Re-snapshot the pipeline (reset offsets). Raise max_slot_wal_keep_size to avoid this."},
	{regexp.MustCompile(`(?i)The db history topic or its content is fully or partially missing|schema history.*missing`),
		"history_missing", "Schema history is missing.",
		"Reset offsets and re-snapshot, or restore the state database."},
}

var (
	errorLineRe = regexp.MustCompile(`(?i)\b(ERROR|FATAL)\b|Caused by:|Exception`)
	levelRe     = regexp.MustCompile(`(?i)^\S+ \S+ +(TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL)\b`)
)

// errorLines are lines that report an error. Stack-trace and exception lines belong to
// the log record above them, so a WARN with a stack trace is not mistaken for an error.
func errorLines(logs string) []string {
	level := "ERROR" // no record seen yet: an unattributed exception counts
	var out []string
	for _, line := range SplitLines(logs) {
		if m := levelRe.FindStringSubmatch(line); m != nil {
			level = strings.ToUpper(m[1])
		}
		if (level == "ERROR" || level == "FATAL") && errorLineRe.MatchString(line) {
			out = append(out, PyStrip(line))
		}
	}
	return out
}

// Diagnose maps the most recent error in the logs to a known cause, or summarises it.
func Diagnose(logs string) *Reason {
	for _, d := range diagnoses {
		// Python re.search on the whole text: "." does not match newlines.
		if d.re.MatchString(logs) {
			return &Reason{Code: d.code, Message: d.msg, Fix: d.f, Severity: "error"}
		}
	}
	errs := errorLines(logs)
	if len(errs) == 0 {
		return nil
	}
	var caused []string
	for _, l := range errs {
		if strings.HasPrefix(l, "Caused by") {
			caused = append(caused, l)
		}
	}
	pick := errs
	if len(caused) > 0 {
		pick = caused
	}
	return &Reason{Code: "connector_error", Message: Truncate(pick[len(pick)-1], 400), Severity: "error"}
}

// DeriveInput are the observations the verdict is derived from.
type DeriveInput struct {
	Now        time.Time
	Deployed   bool
	Desired    string
	DeployedAt *time.Time
	Phase      string // "" = None
	Restarts   int
	HealthUp   *bool
	// HealthError is why the health check gave no answer ("": not probed, or no reason known).
	HealthError     string
	Metrics         *Metrics
	Logs            string
	LatencyTargetMS float64
	RuntimeError    string
	SourceRisk      *Reason
	WorkloadReason  string
	LastLifecycle   string
	// ImagePull describes an image download the runtime is doing for this pipeline ("" = none).
	ImagePull string
	// EngineDown is why an engine that explains itself (Flink CDC) is down; it replaces reading
	// Debezium's logs. Code "health_down" while a young pipeline starts means it is deploying.
	EngineDown *Reason
	// StartedAt is when the workload last started: an engine that explains itself is starting,
	// not failed, for DeployGrace after a resume or restart too.
	StartedAt *time.Time
}

// Derive turns observations into one honest verdict: nothing is healthy without fresh,
// positive evidence.
func Derive(in DeriveInput) Verdict {
	v := derive(in)
	if in.ImagePull != "" && in.RuntimeError == "" && in.Desired != "paused" && !hasReason(v.Reasons, "image_pull") {
		// The current connector keeps running on its old image until the new one is downloaded.
		v.Reasons = append(v.Reasons, Reason{Code: "image_pull", Severity: "info",
			Message: in.ImagePull + " The connector is replaced with the new version once it finishes."})
	}
	return v
}

func hasReason(reasons []Reason, code string) bool {
	for _, r := range reasons {
		if r.Code == code {
			return true
		}
	}
	return false
}

func derive(in DeriveInput) Verdict {
	if in.LatencyTargetMS == 0 {
		in.LatencyTargetMS = 60_000
	}
	if !in.Deployed {
		return Verdict{Lifecycle: LifecycleDraft, Health: HealthUnknown, Summary: "Not deployed yet."}
	}
	if in.RuntimeError != "" {
		last := LifecycleDeploying
		if in.Desired != "running" {
			last = LifecyclePaused
		} else if in.LastLifecycle == LifecycleSnapshotting || in.LastLifecycle == LifecycleStreaming || in.LastLifecycle == LifecycleFailed {
			last = in.LastLifecycle
		}
		return Verdict{last, HealthUnknown, []Reason{{
			Code: "runtime_unreachable", Message: "BeziSync cannot reach the runtime: " + in.RuntimeError,
			Fix: "Check the runtime under Settings → Runtimes.", Severity: "error",
		}}, "Runtime unreachable — status unknown."}
	}
	if in.Desired == "paused" {
		reasons := []Reason{{Code: "paused", Message: "Paused. The source keeps retaining changes until the pipeline resumes.", Severity: "info"}}
		health := HealthUnknown
		if in.SourceRisk != nil {
			reasons = append(reasons, *in.SourceRisk)
			health = HealthAtRisk
		}
		return Verdict{LifecyclePaused, health, reasons, "Paused."}
	}
	young := in.DeployedAt != nil && in.Now.Sub(*in.DeployedAt) < DeployGrace
	switch in.Phase {
	case "", "missing", "pending", "stopped":
		if in.ImagePull != "" {
			// A download makes progress however long it takes; the runtime's image download
			// timeout ends it with an error (apply_failed), so this cannot hide a stuck deploy.
			return Verdict{LifecycleDeploying, HealthUnknown, []Reason{{Code: "image_pull", Message: in.ImagePull +
				" The connector starts once it finishes.", Severity: "info"}}, "Downloading the connector image…"}
		}
		if young {
			return Verdict{Lifecycle: LifecycleDeploying, Health: HealthUnknown, Summary: "Starting the connector…"}
		}
		return Verdict{LifecycleFailed, HealthDown, []Reason{{Code: "not_started", Message: "The connector has not started.", Severity: "error"}}, "The connector has not started."}
	}
	if in.Phase == "failed" && in.WorkloadReason != "" {
		r := Reason{Code: "workload_failed", Message: "The runtime cannot start the connector: " + in.WorkloadReason, Severity: "error"}
		return Verdict{LifecycleFailed, HealthDown, []Reason{r}, r.Message}
	}
	if in.Phase == "restarting" || in.Phase == "failed" {
		d := in.EngineDown
		if d == nil {
			d = Diagnose(in.Logs)
		}
		if d == nil {
			d = &Reason{Code: "crashing", Message: fmt.Sprintf("The connector keeps restarting (%d restarts).", in.Restarts), Severity: "error"}
		}
		return Verdict{LifecycleFailed, HealthDown, []Reason{*d}, d.Message}
	}
	if in.HealthUp != nil && !*in.HealthUp {
		d := in.EngineDown
		if d == nil {
			d = Diagnose(in.Logs)
		}
		if d == nil {
			d = &Reason{Code: "health_down", Message: "Debezium Server reports DOWN.", Severity: "error"}
		}
		starting := young || (in.EngineDown != nil && in.StartedAt != nil && in.Now.Sub(*in.StartedAt) < DeployGrace)
		if starting && d.Code == "health_down" {
			return Verdict{Lifecycle: LifecycleDeploying, Health: HealthUnknown, Summary: "Connector starting…"}
		}
		return Verdict{LifecycleFailed, HealthDown, []Reason{*d}, d.Message}
	}
	if in.Metrics == nil || in.HealthUp == nil {
		if young {
			return Verdict{Lifecycle: LifecycleDeploying, Health: HealthUnknown, Summary: "Connector starting…"}
		}
		noMetrics := Reason{Code: "no_metrics", Message: "Running, but no metrics were received.", Severity: "warn"}
		if in.HealthUp == nil && in.HealthError != "" {
			noHealth := Reason{Code: "no_health", Message: "Running, but its health check gave no answer: " + in.HealthError, Severity: "warn"}
			if in.Metrics == nil {
				return Verdict{LifecycleStreaming, HealthUnknown, []Reason{noHealth, noMetrics}, "Running — no health or metrics."}
			}
			return Verdict{LifecycleStreaming, HealthUnknown, []Reason{noHealth}, "Running — health unknown."}
		}
		return Verdict{LifecycleStreaming, HealthUnknown, []Reason{noMetrics}, "Running — no metrics."}
	}
	m := in.Metrics
	var reasons []Reason
	var lifecycle string
	switch {
	case m.SnapshotRunning != nil && *m.SnapshotRunning:
		lifecycle = LifecycleSnapshotting
	case m.Connected != nil && *m.Connected:
		lifecycle = LifecycleStreaming
	case m.Connected != nil && !*m.Connected:
		found := Diagnose(in.Logs)
		if young && found == nil {
			return Verdict{Lifecycle: LifecycleDeploying, Health: HealthUnknown, Summary: "Connecting to the source…"}
		}
		if found == nil {
			found = &Reason{Code: "not_connected", Message: "The connector is not connected to the source.", Severity: "error"}
		}
		return Verdict{LifecycleFailed, HealthDown, []Reason{*found}, found.Message}
	default:
		lifecycle = LifecycleStreaming
		if young {
			lifecycle = LifecycleDeploying
		}
	}
	health := HealthHealthy
	if m.MSBehindSource != nil && *m.MSBehindSource > in.LatencyTargetMS {
		health = HealthLagging
		reasons = append(reasons, Reason{
			Code: "lagging",
			Message: fmt.Sprintf("%s s behind the source (target %s s).",
				FormatFixed(*m.MSBehindSource/1000, 0), FormatFixed(in.LatencyTargetMS/1000, 0)),
			Severity: "warn",
		})
	}
	if m.QueueTotal != nil && *m.QueueTotal != 0 && m.QueueRemaining != nil && *m.QueueRemaining < *m.QueueTotal*0.1 {
		health = HealthLagging
		reasons = append(reasons, Reason{
			Code:     "queue_full",
			Message:  "The internal queue is almost full: the destination is slower than the source.",
			Fix:      "Check destination throughput, or choose a larger resource preset.",
			Severity: "warn",
		})
	}
	if in.Restarts > 0 {
		reasons = append(reasons, Reason{Code: "restarted", Message: fmt.Sprintf("Restarted %d time(s) since deploy.", in.Restarts), Severity: "info"})
	}
	if in.SourceRisk != nil {
		health = HealthAtRisk
		reasons = append(reasons, *in.SourceRisk)
	}
	summary := map[string]string{
		LifecycleSnapshotting: "Copying existing rows.",
		LifecycleStreaming:    "Streaming changes.",
		LifecycleDeploying:    "Starting…",
	}[lifecycle]
	return Verdict{lifecycle, health, reasons, summary}
}

// FormatFixed is Python's f"{v:.{digits}f}" (correctly rounded, like Go's 'f' format).
func FormatFixed(v float64, digits int) string {
	return strconv.FormatFloat(v, 'f', digits, 64) // both keep "-0" for small negatives
}

// HumanBytes renders a size like "512 B", "1.5 KB", "12 MB".
func HumanBytes(value *float64) string {
	if value == nil {
		return "unknown"
	}
	size := *value
	for _, unit := range []string{"B", "KB", "MB", "GB", "TB"} {
		if size < 1024 || unit == "TB" {
			if unit == "B" || size >= 10 {
				return FormatFixed(size, 0) + " " + unit
			}
			return FormatFixed(size, 1) + " " + unit
		}
		size /= 1024
	}
	return FormatFixed(size, 1) + " TB"
}

// Truncate cuts s to at most n code points (Python s[:n]).
func Truncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	i := 0
	for pos := range s {
		if i == n {
			return s[:pos]
		}
		i++
	}
	return s
}

// SplitLines is Python's str.splitlines().
func SplitLines(s string) []string {
	var out []string
	start := 0
	runes := []rune(s)
	for i := 0; i < len(runes); i++ {
		switch runes[i] {
		case '\n', '\v', '\f', '\x1c', '\x1d', '\x1e', '\u0085', ' ', ' ':
			out = append(out, string(runes[start:i]))
			start = i + 1
		case '\r':
			out = append(out, string(runes[start:i]))
			if i+1 < len(runes) && runes[i+1] == '\n' {
				i++
			}
			start = i + 1
		}
	}
	if start < len(runes) {
		out = append(out, string(runes[start:]))
	}
	return out
}

// PyStrip is Python's str.strip() (Unicode whitespace).
func PyStrip(s string) string {
	return strings.TrimFunc(s, isPySpace)
}

func isPySpace(r rune) bool {
	switch r {
	case ' ', '\t', '\n', '\v', '\f', '\r', '\x1c', '\x1d', '\x1e', '\x1f', '\u0085', ' ',
		' ', ' ', ' ', ' ', ' ', '　':
		return true
	}
	return r >= ' ' && r <= ' '
}
