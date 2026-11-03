package compiler

import (
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

// What a sink's capabilities (connectors.SinkCaps) change in the compiled workload.

// withSinkUnwrap returns the transform steps with the row-after-change step the sink needs:
// its fields merged into the user's step (and its delete handling, when a sink option decides
// it), or the step appended when the spec has none (no field-level step can exist without it,
// so appending keeps every step in its stage).
func withSinkUnwrap(steps []domain.Transform, rule *connectors.UnwrapRule, options map[string]any) []domain.Transform {
	if rule == nil {
		return steps
	}
	out := make([]domain.Transform, len(steps))
	copy(out, steps)
	for i, t := range out {
		if t.Type != "unwrap" {
			continue
		}
		fields := append([]string{}, t.AddFields...)
		for _, f := range rule.AddFields {
			if !containsString(fields, f) {
				fields = append(fields, f)
			}
		}
		t.AddFields = fields
		if h, ok := rule.DeleteMap[connectors.Str(options, rule.DeleteOption)]; ok && rule.DeleteOption != "" {
			t.DeleteHandling = h
		}
		out[i] = t
		return out
	}
	if !rule.Required {
		return out
	}
	step := domain.Transform{Type: "unwrap", DeleteHandling: "rewrite", AddFields: append([]string{}, rule.AddFields...)}
	if len(rule.DeleteHandling) > 0 {
		step.DeleteHandling = rule.DeleteHandling[0]
	}
	return append(out, step)
}

// sinkSourceProps are the source settings the sink needs: no tombstones after deletes, and
// temporal values it can read (ISO strings where the source offers them, else Kafka Connect's
// types) unless the user chose a non-default mode.
func sinkSourceProps(caps connectors.SinkCaps, spec domain.PipelineSpec, srcDesc *connectors.Descriptor) map[string]string {
	props := map[string]string{}
	if caps.NoTombstones {
		props["debezium.source.tombstones.on.delete"] = "false"
	}
	if caps.TimePrecision && srcDesc != nil {
		allowed := srcDesc.Transforms.ValueFormats["timestamps"]
		mode := spec.Values.Timestamps
		if len(allowed) > 0 && (mode == "" || mode == allowed[0]) {
			if containsString(allowed, "isostring") {
				props["debezium.source.time.precision.mode"] = "isostring"
			} else {
				props["debezium.source.time.precision.mode"] = "connect"
			}
		}
	}
	return props
}

func containsString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
