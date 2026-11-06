package services

import (
	"fmt"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

// Footprint is what a pipeline asks for at its peak: a Debezium container, or a Flink CDC cluster
// during its first copy (a cluster that shrank still grows back to it), with its readers.
type Footprint struct {
	Need
	Readers int
}

// PeakSizeOf is a Flink CDC pipeline's copy shape: FlinkSizeOf, but never the streaming one.
func PeakSizeOf(p *models.Pipeline, spec domain.PipelineSpec, capacity map[string]any) engine.FlinkSize {
	if stored := storedShape(p, spec); stored != nil {
		if size, ok := sizeFromArtifact(stored["size"]); ok {
			return size
		}
	}
	return FlinkSizeOf(&models.Pipeline{}, spec, capacity).Size
}

// FootprintOf is p's peak footprint for spec.
func FootprintOf(p *models.Pipeline, spec domain.PipelineSpec, capacity map[string]any) Footprint {
	if EngineOf(p) == engine.FlinkCDC {
		size := PeakSizeOf(p, spec, capacity)
		return Footprint{Need: FlinkNeed(size), Readers: size.Parallelism()}
	}
	return Footprint{Need: DebeziumNeed(spec.Policies), Readers: 1}
}

// resourcePolicyChecks are the project's limits a pipeline's peak footprint must stay within:
// max_resources for a custom Flink CDC cluster (the totals of the Flink CDC preset it names; the
// presets and Debezium sizes are checked in domain.EvaluatePolicies) and resource_limits.
func resourcePolicyChecks(p *models.Pipeline, rules domain.ProjectPolicies, spec domain.PipelineSpec, f Footprint) []checks.Check {
	var out []checks.Check
	fail := func(code, title, msg, fix string) {
		out = append(out, checks.Check{Code: code, Title: title, Category: checks.CatPolicy, Status: checks.Fail, Blocking: true, Message: msg,
			Fix: &checks.Fix{Kind: "text", Who: "you", Code: fix}})
	}
	if EngineOf(p) == engine.FlinkCDC && spec.Policies.Resources == "custom" && rules.MaxResources != nil && *rules.MaxResources != "" {
		limit := FlinkNeed(engine.FlinkSizes[*rules.MaxResources])
		if f.CPU > limit.CPU+1e-9 || f.MiB > limit.MiB {
			fail("policy_resources", "Resources", fmt.Sprintf("This cluster asks for %s CPU and %d MiB at its peak; this project allows up to %s (%s CPU and %d MiB for Flink CDC).",
				cpuWords(f.CPU), f.MiB, *rules.MaxResources, cpuWords(limit.CPU), limit.MiB), fmt.Sprintf("Choose %s or smaller, or fewer or smaller readers.", *rules.MaxResources))
		}
	}
	cpu, mem, readers := rules.ResourceLimits.Amounts()
	var over []string
	if cpu > 0 && f.CPU*1000 > float64(cpu)+1e-6 {
		over = append(over, fmt.Sprintf("%s CPU (at most %s)", cpuWords(f.CPU), rules.ResourceLimits.MaxCPU))
	}
	if mem > 0 && f.MiB > mem {
		over = append(over, fmt.Sprintf("%d MiB (at most %s)", f.MiB, rules.ResourceLimits.MaxMemory))
	}
	if readers > 0 && f.Readers > readers {
		over = append(over, fmt.Sprintf("%d readers (at most %d)", f.Readers, readers))
	}
	if len(over) > 0 {
		fail("policy_resource_limits", "Resource limits", "At its peak this pipeline asks for "+strings.Join(over, ", ")+": over this project's resource limits.",
			"Choose a smaller size, fewer readers, or ask an Admin to raise the limits under Settings → Policies.")
	}
	return out
}

func cpuWords(c float64) string {
	return strings.TrimSuffix(strings.TrimRight(fmt.Sprintf("%.2f", c), "0"), ".")
}
