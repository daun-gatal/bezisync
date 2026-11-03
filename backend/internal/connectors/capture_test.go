package connectors

import (
	"slices"
	"strings"
	"testing"
)

func TestTimeWindowImpact(t *testing.T) {
	cases := []struct {
		name             string
		behind, kept     *int64
		wantRisk         bool
		wantLost         bool
		wantSummaryParts []string
	}{
		{name: "nothing known", wantSummaryParts: []string{"Nothing measured"}},
		{name: "no position yet", kept: ptr(int64(86400)), wantSummaryParts: []string{"No stored position", "24 h"}},
		{name: "window unknown", behind: ptr(int64(90)), wantSummaryParts: []string{"1 min behind", "unknown"}},
		{name: "comfortably inside", behind: ptr(int64(600)), kept: ptr(int64(14400)), wantSummaryParts: []string{"10 min behind", "4 h"}},
		{name: "80% used is at risk", behind: ptr(int64(80)), kept: ptr(int64(100)), wantRisk: true},
		{name: "just under 80% is fine", behind: ptr(int64(79)), kept: ptr(int64(100))},
		{name: "past the window is lost", behind: ptr(int64(200)), kept: ptr(int64(100)), wantRisk: true, wantLost: true},
		{name: "exactly the window is lost", behind: ptr(int64(100)), kept: ptr(int64(100)), wantRisk: true, wantLost: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := TimeWindowImpact(c.behind, c.kept, "the change stream", "Raise the retention.")
			if (got.Risk != nil) != c.wantRisk || got.Lost != c.wantLost {
				t.Fatalf("risk=%v lost=%v: %+v", got.Risk, got.Lost, got)
			}
			if c.wantRisk && !strings.HasSuffix(got.RiskFix, "Raise the retention.") {
				t.Fatalf("fix: %q", got.RiskFix)
			}
			for _, part := range c.wantSummaryParts {
				if !strings.Contains(got.Summary, part) {
					t.Fatalf("summary %q lacks %q", got.Summary, part)
				}
			}
			if got.BehindSeconds != c.behind || got.RetentionSeconds != c.kept {
				t.Fatal("measurements not kept")
			}
		})
	}
}

func TestHumanSeconds(t *testing.T) {
	for s, want := range map[int64]string{-5: "0 s", 0: "0 s", 59: "59 s", 60: "1 min", 3599: "59 min", 3600: "1 h", 3660: "1 h 1 min",
		47 * 3600: "47 h", 48 * 3600: "2 d", 3*86400 + 4*3600: "3 d 4 h"} {
		if got := HumanSeconds(s); got != want {
			t.Errorf("HumanSeconds(%d) = %q, want %q", s, got, want)
		}
	}
}

// Every source states how it captures, and the canary flag is served next to it.
func TestEverySourceDescribesItsCapture(t *testing.T) {
	for _, d := range Descriptors() {
		if d.Kind != "source" {
			continue
		}
		if d.Capture.Maturity != "stable" && d.Capture.Maturity != "incubating" {
			t.Errorf("%s: maturity %q", d.Type, d.Capture.Maturity)
		}
		if d.Capture.SnapshotModes == nil {
			t.Errorf("%s: no snapshot_modes", d.Type)
		}
		if d.Capture.Incremental && len(d.Capture.SnapshotModes) == 0 {
			t.Errorf("%s: incremental snapshots without any snapshot", d.Type)
		}
		cv, _ := d.Descriptor.Get("capture")
		canary, ok := cv.(interface{ Get(string) (any, bool) }).Get("canary")
		if !ok || canary != d.SupportsCanary {
			t.Errorf("%s: served canary %v, entry says %v", d.Type, canary, d.SupportsCanary)
		}
		for mode := range d.Capture.ModeNames {
			if !slices.Contains(d.Capture.SnapshotModes, mode) {
				t.Errorf("%s: mode name for unoffered mode %s", d.Type, mode)
			}
		}
	}
}

func TestDebeziumMode(t *testing.T) {
	c := CaptureCaps{SnapshotModes: []string{"initial", "no_data"}, ModeNames: map[string]string{"no_data": "never"}}
	if c.DebeziumMode("initial") != "initial" || c.DebeziumMode("no_data") != "never" || c.DebeziumMode("always") != "" {
		t.Fatal(c)
	}
	if (CaptureCaps{}).DebeziumMode("initial") != "" {
		t.Fatal("a source without snapshots has no mode")
	}
}

func TestCanarySources(t *testing.T) {
	got := CanarySources()
	if !strings.Contains(got, "PostgreSQL") || !strings.Contains(got, " or ") {
		t.Fatal(got)
	}
}
