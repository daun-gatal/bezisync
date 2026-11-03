package compiler_test

import (
	"errors"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

func TestSnapshotProps(t *testing.T) {
	spec := domain.MustSpec(map[string]any{"snapshot": map[string]any{"mode": "no_data"}})
	full := &connectors.Descriptor{Title: "Full", Capture: connectors.CaptureCaps{SnapshotModes: []string{"initial", "no_data"}, Incremental: true}}
	props, err := compiler.SnapshotProps(full, spec)
	if err != nil || props["debezium.source.snapshot.mode"] != "no_data" || props["debezium.source.incremental.snapshot.chunk.size"] == "" {
		t.Fatalf("%v %v", props, err)
	}
	renamed := &connectors.Descriptor{Title: "Vitess", Capture: connectors.CaptureCaps{SnapshotModes: []string{"initial", "no_data"},
		ModeNames: map[string]string{"no_data": "never"}}}
	props, err = compiler.SnapshotProps(renamed, spec)
	if err != nil || props["debezium.source.snapshot.mode"] != "never" {
		t.Fatalf("%v %v", props, err)
	}
	if _, ok := props["debezium.source.incremental.snapshot.chunk.size"]; ok {
		t.Fatal("no incremental chunk size without incremental snapshots")
	}
	none := &connectors.Descriptor{Title: "Spanner", Capture: connectors.CaptureCaps{SnapshotModes: []string{}}}
	if props, err = compiler.SnapshotProps(none, spec); err != nil || len(props) != 0 {
		t.Fatalf("%v %v", props, err)
	}
	always := domain.MustSpec(map[string]any{"snapshot": map[string]any{"mode": "always"}})
	_, err = compiler.SnapshotProps(renamed, always)
	var p *problem.Problem
	if !errors.As(err, &p) || p.Code != "snapshot_mode_unsupported" {
		t.Fatalf("%v", err)
	}
}
