package controller

import (
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

// ADR 0010: the drift alert checks a Flink CDC key against the schema just read; a column whose
// nullability wasn't recorded counts as one that can be NULL (the key check fails closed).
func TestTransformDriftFlinkKeyFailsClosed(t *testing.T) {
	spec := domain.PipelineSpec{
		Transforms: []domain.Transform{{Type: "computed", Table: "public.t", Name: "c", Function: "upper", Args: []domain.ComputedArg{{Column: "code"}}}},
		Keys:       map[string][]string{"public.t": {"id", "c"}},
	}
	spec.Source.Tables.Include = []string{"public.t"}
	drift := func(code map[string]any) string {
		out := transformDrift("flink-cdc", "postgresql", "p", spec, domain.Schema{"public.t": {"id": {"type": "bigint", "nullable": false}, "code": code}})
		var msgs []string
		for _, f := range out {
			msgs = append(msgs, f.(map[string]any)["code"].(string))
		}
		return strings.Join(msgs, ",")
	}
	if got := drift(map[string]any{"type": "text", "nullable": false}); got != "" {
		t.Fatalf("NOT NULL input: %q", got)
	}
	if got := drift(map[string]any{"type": "text"}); got != "transform_key_column_unfit" {
		t.Fatalf("unrecorded nullability: %q", got)
	}
}
