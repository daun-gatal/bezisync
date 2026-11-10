package services

import (
	"fmt"
	"slices"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
)

// readonlyCheckSinks are the destinations whose checks created a test table or index before
// ADR 0016 and only read since.
var readonlyCheckSinks = map[string]bool{
	"iceberg": true, "jdbc": true, "starrocks": true, "doris": true, "oceanbase": true, "elasticsearch": true,
}

// GatedForAgent reports whether a connection check must not run on an agent with these features:
// an agent that predates read-only destination checks would create test tables (ADR 0016 D7).
// kind is "source" or "sink", sinkType the connection's type.
func GatedForAgent(kind, sinkType string, features []any) bool {
	return kind == "sink" && readonlyCheckSinks[sinkType] && !slices.Contains(features, any(runtime.FeatureReadonlyDestinationChecks))
}

// OlderAgentReport is what a gated check reports instead: nothing is checked, nothing blocks.
func OlderAgentReport(runtimeName string, version any) checks.Report {
	agent := "The agent"
	if runtimeName != "" {
		agent = "The agent on " + runtimeName
	}
	if v := fmt.Sprint(version); version != nil && v != "" {
		agent += " (" + v + ")"
	}
	c := checks.Check{Code: "agent_readonly_checks", Title: "Agent version", Status: checks.Unknown, Category: checks.CatDestination,
		Message: agent + " predates read-only destination checks and would create test tables, so this destination isn't checked: upgrade the agent to check it.",
		Fix:     &checks.Fix{Kind: "text", Who: "runtime admin", Code: "Upgrade the agent to this BeziSync version (Settings → Runtimes shows the install command)."}}
	return checks.Summarize([]checks.Check{c}, time.Now())
}
