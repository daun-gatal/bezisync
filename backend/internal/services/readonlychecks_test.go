package services

import (
	"context"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
)

// ADR 0016 D7, E3-E5: an agent that predates read-only destination checks never runs one of the
// four destination families (it would create test tables); everything else is unchanged.
func TestReadonlyGate(t *testing.T) {
	current := []any{"health_port", runtime.FeatureFlinkCDC, runtime.FeatureReadonlyDestinationChecks}
	older := []any{"health_port", runtime.FeatureFlinkCDC}
	for _, c := range []struct {
		kind, typ string
		features  []any
		gated     bool
	}{
		{"sink", "iceberg", older, true},
		{"sink", "jdbc", older, true},
		{"sink", "starrocks", older, true},
		{"sink", "doris", older, true},
		{"sink", "oceanbase", older, true},
		{"sink", "elasticsearch", older, true},
		{"sink", "iceberg", nil, true}, // never polled, or an agent from before features
		{"sink", "kafka", older, false},
		{"sink", "paimon", older, false},
		{"source", "postgresql", older, false},
		{"source", "mysql", nil, false},
		{"sink", "iceberg", current, false},
		{"sink", "elasticsearch", current, false},
	} {
		if got := GatedForAgent(c.kind, c.typ, c.features); got != c.gated {
			t.Errorf("%s %s %v: gated=%v, want %v", c.kind, c.typ, c.features, got, c.gated)
		}
	}

	// checkWhere answers a gated check itself: one non-blocking Unknown, nothing sent anywhere.
	rt := &models.Runtime{Name: "k3s", Mode: "agent", AgentState: map[string]any{"version": "0.7.0", "features": older}}
	report, onAgent, err := checkWhere(context.Background(), rt, connectors.Resolved{Type: "iceberg"}, "sink", nil)
	if err != nil || onAgent {
		t.Fatal(onAgent, err)
	}
	if len(report.Checks) != 1 {
		t.Fatalf("%+v", report.Checks)
	}
	c := report.Checks[0]
	if c.Code != "agent_readonly_checks" || c.Status != checks.Unknown || c.Blocking ||
		c.Message != "The agent on k3s (0.7.0) predates read-only destination checks and would create test tables, "+
			"so this destination isn't checked: upgrade the agent to check it." {
		t.Fatalf("%+v", c)
	}
}
