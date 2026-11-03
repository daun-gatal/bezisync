package services

import (
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
)

func TestPlatformCheck(t *testing.T) {
	iceberg := &connectors.Descriptor{Title: "Apache Iceberg", Sink: connectors.SinkCaps{Image: &connectors.SinkImage{Platforms: []string{"amd64"}}}}
	kafka := &connectors.Descriptor{Title: "Kafka"}
	cases := []struct {
		name     string
		capacity map[string]any
		desc     *connectors.Descriptor
		want     string
	}{
		{"official image", map[string]any{"architectures": []any{"arm64"}}, kafka, ""},
		{"unknown architecture", map[string]any{}, iceberg, ""},
		{"supported", map[string]any{"architectures": []any{"amd64"}}, iceberg, ""},
		{"mixed nodes", map[string]any{"architectures": []any{"amd64", "arm64"}}, iceberg, checks.Warn},
		{"unsupported", map[string]any{"architectures": []any{"arm64"}}, iceberg, checks.Fail},
	}
	for _, c := range cases {
		got := PlatformCheck(c.capacity, c.desc)
		switch {
		case c.want == "" && got != nil:
			t.Errorf("%s: unexpected %+v", c.name, got)
		case c.want != "" && (got == nil || got.Status != c.want):
			t.Errorf("%s: got %+v, want %s", c.name, got, c.want)
		}
	}
}
