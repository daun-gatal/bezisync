package services

import (
	"context"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

// R4: a custom size below a destination's minimum preset fails like a smaller preset does.
func TestCustomSizeMeetsTheSinkMinimum(t *testing.T) {
	desc := &connectors.Descriptor{Title: "Big sink", Sink: connectors.SinkCaps{MinResources: "medium"}}
	check := func(p domain.Policies) string {
		spec := domain.DefaultSpec()
		spec.Policies = p
		out := sinkChecks(context.Background(), connectors.Resolved{Type: "kafka"}, desc, connectors.CompileContext{Spec: spec}, nil, nil, false)
		if len(out) == 0 {
			return ""
		}
		return out[0].Message
	}
	if got := check(domain.Policies{Resources: "custom", Custom: &domain.CustomResources{CPU: "1", Memory: "1Gi"}}); !strings.Contains(got, "needs at least the medium connector size (1 CPU · 1536 MiB)") {
		t.Fatal(got)
	}
	if got := check(domain.Policies{Resources: "custom", Custom: &domain.CustomResources{CPU: "1.25", Memory: "2Gi"}}); got != "" {
		t.Fatal(got)
	}
	if got := check(domain.Policies{Resources: "small"}); got == "" {
		t.Fatal("a smaller preset still fails")
	}
}
