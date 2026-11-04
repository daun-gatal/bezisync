package engine

import (
	"strings"
	"testing"
	"time"
)

func TestFlinkSettingsAreChecked(t *testing.T) {
	for _, c := range []struct {
		key, value string
		ok         bool
	}{
		{"execution.checkpointing.timeout", "10 min", true},
		{"execution.checkpointing.timeout", "30s", false}, // under the minute minimum
		{"execution.checkpointing.timeout", "2 h", false},
		{"execution.checkpointing.min-pause", "0 ms", true},
		{"execution.checkpointing.tolerable-failed-checkpoints", "0", true},
		{"execution.checkpointing.tolerable-failed-checkpoints", "-1", false},
		{"execution.checkpointing.tolerable-failed-checkpoints", "1.5", false},
		{"taskmanager.memory.managed.fraction", "0.25", true},
		{"taskmanager.memory.managed.fraction", "0.95", false},
		{"taskmanager.memory.managed.fraction", "1e-1", false},
		{"restart-strategy.type", "fixed-delay", true},
		{"restart-strategy.type", "none", false},
		{"jobmanager.rpc.address", "elsewhere", false}, // BeziSync's own: never chosen
		{"security.ssl.internal.enabled", "false", false},
	} {
		if msg := CheckFlinkSetting(c.key, c.value); (msg == "") != c.ok {
			t.Errorf("%s=%q: %q", c.key, c.value, msg)
		}
	}
	bad := CheckFlinkSettings(map[string]string{"heartbeat.timeout": "5 s", "pekko.ask.timeout": "20 s"})
	if len(bad) != 1 || !strings.Contains(bad["heartbeat.timeout"], "between 10s and 10m") {
		t.Fatal(bad)
	}
	for k, v := range FlinkDefaults {
		if FlinkSettingOf(k) == nil || FlinkSettingOf(k).Default != v || CheckFlinkSetting(k, v) != "" {
			t.Errorf("BeziSync's default %s=%s is not the catalogue's", k, v)
		}
	}
	// Every default is valid for its own key.
	for _, s := range FlinkSettings {
		if msg := CheckFlinkSetting(s.Key, s.Default); msg != "" {
			t.Errorf("%s default %q: %s", s.Key, s.Default, msg)
		}
	}
	if d, ok := ParseFlinkDuration("500 ms"); !ok || d != 500*time.Millisecond {
		t.Fatal(d)
	}
}

func TestFlinkSizes(t *testing.T) {
	for name, want := range map[string][2]int{"small": {2, 3072}, "medium": {4, 5120}, "large": {8, 9728}} {
		s := FlinkSizes[name]
		if s.Parallelism() != want[0] || s.TotalMemoryMiB() != want[1] {
			t.Errorf("%s: parallelism %d, memory %d MiB", name, s.Parallelism(), s.TotalMemoryMiB())
		}
	}
}
