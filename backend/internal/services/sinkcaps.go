package services

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

// ValidateSinkSpec checks what the Destination step saves against the sink's capabilities:
// its options, naming and event format. Transform steps are checked when the workload is
// compiled (preflight), so changing the destination never needs a later step fixed first.
func ValidateSinkSpec(ctx context.Context, q db.Querier, projectID uuid.UUID, sinkID *uuid.UUID, spec domain.PipelineSpec, eng engine.ID) error {
	if sinkID == nil {
		return nil
	}
	conn, err := GetConnectionOfKind(ctx, q, projectID, *sinkID, "sink")
	if err != nil {
		return err
	}
	d, err := connectors.Describe("sink", conn.Type)
	if err != nil {
		return err
	}
	if _, err := d.Sink.Options(spec.Sink.Options); err != nil {
		return err
	}
	if eng == engine.Debezium {
		if _, err := d.Sink.EventFormat(d.Title, spec.Sink.Format.Type); err != nil {
			return err
		}
	} else if err := connectors.FlinkFormatProblem(conn.Type, spec.Sink.Format); err != nil {
		// Flink CDC: tables take no format; Kafka takes only its JSON.
		return err
	}
	return d.Sink.CheckNaming(d.Title, spec.Sink.Naming)
}

// discoverOnce reads the source's tables at most once per preflight.
type discoverOnce struct {
	src   connectors.Resolved
	typ   string
	done  bool
	found connectors.Discovery
	err   error
}

func (d *discoverOnce) get(ctx context.Context) (connectors.Discovery, error) {
	if !d.done {
		d.done = true
		var src connectors.Source
		if src, d.err = connectors.GetSource(d.typ); d.err == nil {
			d.found, d.err = src.Discover(ctx, d.src)
		}
	}
	return d.found, d.err
}

// personalDataChecks compares the pipeline's masking choices with the captured columns.
func personalDataChecks(ctx context.Context, desc *connectors.Descriptor, discover *discoverOnce, spec domain.PipelineSpec) []checks.Check {
	title, canMask := discover.typ, false
	var caps connectors.TransformCaps
	if desc != nil {
		title, canMask, caps = desc.Title, desc.SupportsMasking && len(desc.Transforms.MaskableTypes) > 0, desc.Transforms
	}
	found, err := discover.get(ctx)
	if err != nil {
		return domain.PersonalDataChecks(spec, nil, title, canMask, err)
	}
	tables := map[string][]domain.DataColumn{}
	for _, t := range found.Tables {
		cols := make([]domain.DataColumn, len(t.Columns))
		for i, c := range t.Columns {
			cols[i] = domain.DataColumn{Name: c.Name, Type: c.Type, PII: c.PII, Maskable: canMask && caps.CanMask(c.Type)}
		}
		tables[t.Identifier()] = cols
	}
	return domain.PersonalDataChecks(spec, tables, title, canMask, nil)
}

// sinkChecks are preflight's pipeline-level destination checks: the connector size the sink
// needs, and the sink's own checks on the captured tables (keys, existing destinations).
func sinkChecks(ctx context.Context, sink connectors.Resolved, desc *connectors.Descriptor, cctx connectors.CompileContext,
	destinations map[string]string, discover *discoverOnce, flink bool) []checks.Check {
	var out []checks.Check
	if min := desc.Sink.MinResources; min != "" && !flink && !domain.ResourcePresets[min].Within(cctx.Spec.Policies.Amount()) {
		out = append(out, checks.Check{Code: "sink_resources", Title: "Connector size", Category: checks.CatResilience, Status: checks.Fail,
			Blocking: true, Message: fmt.Sprintf("%s needs at least the %s connector size (%s).", desc.Title, min, domain.ResourcePresets[min]),
			Why: "Smaller connectors run out of memory while the destination buffers its writes.",
			Fix: &checks.Fix{Kind: "text", Who: "you", Code: fmt.Sprintf("Choose the %s size (or larger) in the Review step.", min)}})
	}
	checker, ok := connectors.GetSinkPipelineChecker(sink.Type)
	if !ok || len(destinations) == 0 {
		return out
	}
	options, err := desc.Sink.Options(cctx.Spec.Sink.Options)
	if err != nil {
		return out
	}
	facts := connectors.PipelineFacts{Destinations: destinations, Options: options, FlinkCDC: flink}
	if discover != nil {
		facts.SourceType = discover.typ
	}
	if found, err := discover.get(ctx); err == nil {
		for _, t := range found.Tables {
			if contains(cctx.Spec.Source.Tables.Include, t.Identifier()) {
				facts.Tables = append(facts.Tables, t)
			}
		}
	}
	return append(out, checker.CheckPipeline(ctx, sink, cctx, facts)...)
}

// PlatformCheck warns when a sink's own image is not published for the runtime's CPU
// architecture (nil when the runtime does not report it, e.g. an older agent).
func PlatformCheck(capacity map[string]any, desc *connectors.Descriptor) *checks.Check {
	if desc == nil || desc.Sink.Image == nil || len(desc.Sink.Image.Platforms) == 0 {
		return nil
	}
	raw, _ := capacity["architectures"].([]any)
	var archs, unsupported []string
	for _, a := range raw {
		if s, ok := a.(string); ok {
			archs = append(archs, s)
			if !contains(desc.Sink.Image.Platforms, s) {
				unsupported = append(unsupported, s)
			}
		}
	}
	if len(archs) == 0 || len(unsupported) == 0 {
		return nil
	}
	platforms := strings.Join(desc.Sink.Image.Platforms, ", ")
	c := &checks.Check{Code: "sink_image_platform", Title: "Image platform", Category: checks.CatCapacity, Status: checks.Warn,
		Message: fmt.Sprintf("The %s image is built for %s only; this runtime also has %s.", desc.Title, platforms, strings.Join(unsupported, ", ")),
		Why:     "A connector scheduled on another architecture cannot start (exec format error).",
		Fix: &checks.Fix{Kind: "text", Who: "runtime admin",
			Code: fmt.Sprintf("Run these connectors where nodes are %s (a runtime namespace pinned to those nodes, or a Docker host of that architecture).", platforms)}}
	if len(unsupported) == len(archs) {
		c.Status, c.Blocking = checks.Fail, true
		c.Message = fmt.Sprintf("The %s image is built for %s only; this runtime is %s.", desc.Title, platforms, strings.Join(archs, ", "))
	}
	return c
}

// AgentHealthPortCheck blocks a sink whose image serves health off 8080 on an agent that predates
// health_port: it would probe 8080 and restart the connector forever.
func AgentHealthPortCheck(rt *models.Runtime, desc *connectors.Descriptor) *checks.Check {
	if rt == nil || rt.Mode != "agent" || desc == nil || desc.Sink.Image == nil || desc.Sink.Image.HealthPort == 0 {
		return nil
	}
	if features, ok := rt.AgentState["features"].([]any); ok {
		for _, f := range features {
			if f == "health_port" {
				return nil
			}
		}
	}
	return &checks.Check{Code: "agent_upgrade_required", Title: "Agent version", Category: checks.CatCapacity, Status: checks.Fail, Blocking: true,
		Message: fmt.Sprintf("This agent is too old for %s connectors: their health is on another port.", desc.Title),
		Fix:     &checks.Fix{Kind: "text", Who: "runtime admin", Code: "Upgrade the agent to this BeziSync version (Settings → Runtimes shows the install command)."}}
}
