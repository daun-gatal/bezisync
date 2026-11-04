// Package flinkhelper is `bezisync engine flink-cdc jobmanager|taskmanager`: the process BeziSync
// runs in each container of a Flink CDC pipeline.
//
// On a TaskManager it puts the pipeline's connectors on Flink's own classpath and becomes the
// TaskManager. On the JobManager it does the same, starts the JobManager, waits for the
// TaskManagers, submits the pipeline (restoring the saved position it was told to), stores every
// completed checkpoint in BeziSync's state database, serves health and status to BeziSync, and on
// SIGTERM stops the job with a savepoint that it stores before exiting.
//
// Connectors are loaded from Flink's classpath (FLINK_LIB_DIR) with parent-first class loading:
// loading them per job leaked classes and killed TaskManagers after a few restarts.
package flinkhelper

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
)

// Paths in the container.
const (
	// ConfigDir holds the files BeziSync writes: settings.json, flink-conf.yaml, pipeline.yaml and
	// the TLS keystore.
	DefaultConfigDir = "/opt/bezisync/config"
	// WorkDir is writable scratch space (tmpfs or emptyDir): the per-pipeline lib and conf dirs.
	DefaultWorkDir = "/tmp/bezisync"
	// FlinkHome and FlinkCDCHome are where the image keeps Flink and Flink CDC.
	DefaultFlinkHome    = "/opt/flink"
	DefaultFlinkCDCHome = "/opt/flink-cdc"
)

// Settings is settings.json: what BeziSync tells the helper about this pipeline.
type Settings struct {
	PipelineID string `json:"pipeline_id"`
	ShortID    string `json:"short_id"`
	// Connectors are the JAR names under /opt/flink-cdc/connectors (without .jar) the pipeline
	// needs, e.g. ["mysql", "starrocks"]; extra JDBC drivers come from /opt/flink-cdc/drivers.
	Connectors []string `json:"connectors"`
	// Drivers are JDBC drivers the pipeline needs that images don't ship ("mysql").
	Drivers []string `json:"drivers"`
	// TaskManagers the JobManager waits for before it submits.
	TaskManagers int `json:"taskmanagers"`
	// Restore is the saved position to start from: "" (start fresh with the pipeline's startup
	// mode), "latest" (the newest stored checkpoint or savepoint) or a stored state id.
	Restore string `json:"restore"`
	// RestoreAfter: saved positions up to this id were set aside (Start over) and are never
	// restored by "latest".
	RestoreAfter int64 `json:"restore_after"`
	// AllowNonRestored drops saved state of operators the new pipeline no longer has (a removed
	// table).
	AllowNonRestored bool `json:"allow_non_restored"`
	// StateDir is where Flink writes checkpoints and savepoints (file:// URI root).
	StateDir string `json:"state_dir"`
	// KeepCheckpoints is how many stored checkpoints to keep.
	KeepCheckpoints int `json:"keep_checkpoints"`
	// HealthPort serves /q/health and /bz/status.
	HealthPort int `json:"health_port"`
	// StopTimeoutSeconds bounds stop-with-savepoint on SIGTERM.
	StopTimeoutSeconds int `json:"stop_timeout_seconds"`
}

var connectorName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// LoadSettings reads and checks settings.json.
func LoadSettings(path string) (Settings, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // BeziSync's config dir
	if err != nil {
		return Settings{}, err
	}
	var s Settings
	if err := json.Unmarshal(raw, &s); err != nil {
		return Settings{}, fmt.Errorf("%s: %w", filepath.Base(path), err)
	}
	return s, s.check()
}

func (s *Settings) check() error {
	if s.ShortID == "" || !regexp.MustCompile(`^[0-9a-f]{12}$`).MatchString(s.ShortID) {
		return fmt.Errorf("settings: invalid short_id %q", s.ShortID)
	}
	if len(s.Connectors) == 0 {
		return fmt.Errorf("settings: no connectors")
	}
	for _, c := range append(slices.Clone(s.Connectors), s.Drivers...) {
		if !connectorName.MatchString(c) {
			return fmt.Errorf("settings: invalid connector or driver name %q", c)
		}
	}
	if s.TaskManagers < 1 {
		s.TaskManagers = 1
	}
	if s.StateDir == "" {
		s.StateDir = "/state"
	}
	if !filepath.IsAbs(s.StateDir) {
		return fmt.Errorf("settings: state_dir must be absolute")
	}
	if s.KeepCheckpoints < 1 {
		s.KeepCheckpoints = 5
	}
	if s.HealthPort == 0 {
		s.HealthPort = 8080
	}
	if s.StopTimeoutSeconds <= 0 {
		s.StopTimeoutSeconds = 90
	}
	return nil
}
