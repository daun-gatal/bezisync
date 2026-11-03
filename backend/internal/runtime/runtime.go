// Package runtime has the drivers for where connector workloads run (Docker, Kubernetes)
// and the agent-backed driver.
//
// Drivers are idempotent and level-based: Apply makes the runtime match the workload spec
// (creating or replacing it when the config hash changes), Observe reports what is
// actually there. Both are safe to call on every controller tick.
package runtime

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// Labels on every workload BeziSync manages.
const (
	Label     = "app.bezisync.io/pipeline"
	ManagedBy = "app.bezisync.io/managed-by"
	// Owner is the runtime a workload belongs to, so agents sharing a host or namespace
	// never touch each other's connectors.
	Owner = "app.bezisync.io/runtime"
)

// Workload is the desired connector. JSON field names are the agent protocol (asdict).
type Workload struct {
	PipelineID       string            `json:"pipeline_id"`
	ShortID          string            `json:"short_id"`
	Name             string            `json:"name"`
	Image            string            `json:"image"`
	Properties       string            `json:"properties"`
	Env              map[string]string `json:"env"`
	ConfigHash       string            `json:"config_hash"`
	CPU              string            `json:"cpu"`
	Memory           string            `json:"memory"`
	Heap             string            `json:"heap"`
	Running          bool              `json:"running"`
	Files            map[string]string `json:"files"`
	Owner            string            `json:"owner"`
	ImagePullSecrets []string          `json:"image_pull_secrets"`
	ServiceAccount   string            `json:"service_account"`
	Tunnel           map[string]any    `json:"tunnel"`
	// HealthPort serves /q/health (0: 8080). Agents that predate it probe 8080 regardless.
	HealthPort int `json:"health_port,omitempty"`
}

// Health is the port serving /q/health.
func (w *Workload) Health() int {
	if w.HealthPort == 0 {
		return 8080
	}
	return w.HealthPort
}

// ResourceName is the container / Deployment name.
func (w *Workload) ResourceName() string { return "bz-" + w.ShortID }

// Normalize fills nil collections (dataclass defaults).
func (w *Workload) Normalize() {
	if w.Env == nil {
		w.Env = map[string]string{}
	}
	if w.Files == nil {
		w.Files = map[string]string{}
	}
	if w.ImagePullSecrets == nil {
		w.ImagePullSecrets = []string{}
	}
}

// Phases.
const (
	PhaseMissing    = "missing"
	PhasePending    = "pending"
	PhaseRunning    = "running"
	PhaseRestarting = "restarting"
	PhaseFailed     = "failed"
	PhaseStopped    = "stopped"
)

// Observation is what is actually running.
type Observation struct {
	Phase      string
	Ready      bool
	Restarts   int
	StartedAt  *time.Time
	ExitCode   *int
	Reason     string
	ConfigHash *string
	Details    map[string]any
	// ImagePull describes the image download the workload waits for ("" = none): the
	// connector is created, or replaced, once it finishes.
	ImagePull string
}

// TestResult is a runtime connectivity test.
type TestResult struct {
	OK      bool
	Message string
	Details map[string]any
}

// Error is a runtime failure whose message is safe to show users.
type Error struct{ Msg string }

func (e *Error) Error() string { return e.Msg }

// Errorf builds a runtime Error.
func Errorf(format string, a ...any) error { return &Error{fmt.Sprintf(format, a...)} }

// Driver is one runtime.
type Driver interface {
	Apply(ctx context.Context, w Workload) error
	Delete(ctx context.Context, shortID string) error
	Observe(ctx context.Context, shortID string) (Observation, error)
	Logs(ctx context.Context, shortID string, tail int, previous bool) (string, error)
	// HTTPGet GETs a URL served inside the workload (health, metrics).
	HTTPGet(ctx context.Context, shortID string, port int, path string, timeout time.Duration) (int, string, error)
	Test(ctx context.Context) TestResult
	// ListWorkloads returns the short ids this runtime manages, optionally of one owner.
	ListWorkloads(ctx context.Context, owner string) ([]string, error)
	Capacity(ctx context.Context) map[string]any
}

// JVMEnv is the workload environment plus JVM settings.
func JVMEnv(w Workload) map[string]string {
	out := make(map[string]string, len(w.Env)+3)
	for k, v := range w.Env {
		out[k] = v
	}
	out["JAVA_OPTS"] = fmt.Sprintf("-Xmx%s -XX:+ExitOnOutOfMemoryError", w.Heap)
	out["JMX_EXPORTER_PORT"] = "9404"
	// Debezium 3.7 ships its SMTs in lib_opt/, which run.sh only adds to the classpath when
	// scripting is enabled.
	out["ENABLE_DEBEZIUM_SCRIPTING"] = "true"
	return out
}

// Architectures normalizes CPU architectures to Go/OCI names (Docker reports x86_64 and
// aarch64), sorted and without duplicates, for matching images' platforms.
func Architectures(archs ...string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, a := range archs {
		switch a {
		case "x86_64":
			a = "amd64"
		case "aarch64":
			a = "arm64"
		case "":
			continue
		}
		if !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	sort.Strings(out)
	return out
}
