package compiler

import (
	"reflect"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

func TestWithSinkUnwrap(t *testing.T) {
	rule := &connectors.UnwrapRule{Required: true, AddFields: []string{"op", "source.ts_ms"}, DeleteHandling: []string{"rewrite"}}
	filter := domain.Transform{Type: "filter", ID: "f1"}
	cases := []struct {
		name  string
		steps []domain.Transform
		rule  *connectors.UnwrapRule
		want  []domain.Transform
	}{
		{"no rule keeps the steps", []domain.Transform{filter}, nil, []domain.Transform{filter}},
		{"required step is appended", []domain.Transform{filter}, rule,
			[]domain.Transform{filter, {Type: "unwrap", DeleteHandling: "rewrite", AddFields: []string{"op", "source.ts_ms"}}}},
		{"user step gets the missing fields", []domain.Transform{{Type: "unwrap", DeleteHandling: "rewrite", AddFields: []string{"table", "op"}}}, rule,
			[]domain.Transform{{Type: "unwrap", DeleteHandling: "rewrite", AddFields: []string{"table", "op", "source.ts_ms"}}}},
		{"optional rule adds nothing when absent", []domain.Transform{filter}, &connectors.UnwrapRule{DeleteHandling: []string{"drop"}},
			[]domain.Transform{filter}},
	}
	for _, c := range cases {
		original := append([]domain.Transform{}, c.steps...)
		got := withSinkUnwrap(c.steps, c.rule, nil)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
		if !reflect.DeepEqual(c.steps, original) {
			t.Errorf("%s: the spec's steps were modified", c.name)
		}
	}
}

func TestWithSinkUnwrapDeleteOption(t *testing.T) {
	rule := &connectors.UnwrapRule{DeleteOption: "deletes", DeleteMap: map[string]string{"delete": "tombstone", "ignore": "drop"}}
	steps := []domain.Transform{{Type: "unwrap", DeleteHandling: "rewrite", AddFields: []string{"op"}}}
	if got := withSinkUnwrap(steps, rule, map[string]any{"deletes": "delete"}); got[0].DeleteHandling != "tombstone" {
		t.Fatalf("delete: %+v", got)
	}
	if got := withSinkUnwrap(steps, rule, map[string]any{"deletes": "ignore"}); got[0].DeleteHandling != "drop" {
		t.Fatalf("ignore: %+v", got)
	}
	if got := withSinkUnwrap(nil, rule, map[string]any{"deletes": "delete"}); len(got) != 0 {
		t.Fatalf("no step is added when the sink reads the envelope: %+v", got)
	}
}

func TestSinkSourceProps(t *testing.T) {
	pg := &connectors.Descriptor{Transforms: connectors.TransformCaps{ValueFormats: map[string][]string{"timestamps": {"epoch", "isostring"}}}}
	mssql := &connectors.Descriptor{Transforms: connectors.TransformCaps{ValueFormats: map[string][]string{"timestamps": {"epoch"}}}}
	mongo := &connectors.Descriptor{}
	caps := connectors.SinkCaps{NoTombstones: true, TimePrecision: true}
	spec := domain.DefaultSpec()
	cases := []struct {
		name       string
		caps       connectors.SinkCaps
		desc       *connectors.Descriptor
		timestamps string
		want       map[string]string
	}{
		{"nothing needed", connectors.SinkCaps{}, pg, "", map[string]string{}},
		{"postgres default becomes iso strings", caps, pg, "",
			map[string]string{"debezium.source.tombstones.on.delete": "false", "debezium.source.time.precision.mode": "isostring"}},
		{"an explicit choice is kept", caps, pg, "isostring", map[string]string{"debezium.source.tombstones.on.delete": "false"}},
		{"sql server falls back to connect", caps, mssql, "",
			map[string]string{"debezium.source.tombstones.on.delete": "false", "debezium.source.time.precision.mode": "connect"}},
		{"mongodb has no temporal modes", caps, mongo, "", map[string]string{"debezium.source.tombstones.on.delete": "false"}},
	}
	for _, c := range cases {
		s := spec
		s.Values.Timestamps = c.timestamps
		if got := sinkSourceProps(c.caps, s, c.desc); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
