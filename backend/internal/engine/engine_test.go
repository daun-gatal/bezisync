package engine

import (
	"slices"
	"testing"
)

func TestSupports(t *testing.T) {
	cases := []struct {
		id           ID
		source, sink string
		want         bool
	}{
		{Debezium, "mysql", "kafka", true},
		{Debezium, "mongodb", "iceberg", true},
		{Debezium, "mysql", "starrocks", false},
		{Debezium, "postgresql", "doris", false},
		{Debezium, "postgresql", "fluss", false},
		{Debezium, "oracle", "", true},
		{FlinkCDC, "mysql", "starrocks", true},
		{FlinkCDC, "postgresql", "iceberg", true},
		{FlinkCDC, "sqlserver", "fluss", true},
		{FlinkCDC, "sqlserver", "doris", true},
		{FlinkCDC, "mysql", "kafka", false},
		{FlinkCDC, "mongodb", "iceberg", false},
		{FlinkCDC, "oracle", "starrocks", false},
		{FlinkCDC, "mysql", "paimon", false},
		{FlinkCDC, "postgresql", "", true},
		{FlinkCDC, "cockroachdb", "", false},
		{ID("spark"), "mysql", "kafka", false},
	}
	for _, c := range cases {
		if got := Supports(c.id, c.source, c.sink); got != c.want {
			t.Errorf("Supports(%s, %s, %s) = %v, want %v", c.id, c.source, c.sink, got, c.want)
		}
	}
}

func TestChoicesAndDefault(t *testing.T) {
	cases := []struct {
		source, sink string
		choices      []ID
		def          ID
	}{
		{"mysql", "kafka", []ID{Debezium}, Debezium},
		{"mysql", "iceberg", []ID{Debezium, FlinkCDC}, Debezium},
		{"mongodb", "iceberg", []ID{Debezium}, Debezium},
		{"postgresql", "starrocks", []ID{FlinkCDC}, FlinkCDC},
		{"mongodb", "starrocks", nil, ""},
	}
	for _, c := range cases {
		if got := Choices(c.source, c.sink); !slices.Equal(got, c.choices) {
			t.Errorf("Choices(%s, %s) = %v, want %v", c.source, c.sink, got, c.choices)
		}
		if got := Default(c.source, c.sink); got != c.def {
			t.Errorf("Default(%s, %s) = %q, want %q", c.source, c.sink, got, c.def)
		}
	}
}

func TestOfAndValid(t *testing.T) {
	if Of("") != Debezium || Of("flink-cdc") != FlinkCDC {
		t.Fatal("Of must map empty to Debezium and keep stored values")
	}
	if !Debezium.Valid() || !FlinkCDC.Valid() || ID("x").Valid() {
		t.Fatal("Valid")
	}
	if FlinkCDC.Title() != "Flink CDC" || Debezium.Title() != "Debezium" || ID("x").Title() != "x" {
		t.Fatal("Title")
	}
}

func TestCaps(t *testing.T) {
	d := CapsFor(Debezium, "postgresql")
	if !d.EditPosition || !d.RecopyTable || !d.EventFormats || !d.HashMask || d.SavedPositions || d.ParallelCopy || d.StartFromTime {
		t.Fatalf("debezium caps: %+v", d)
	}
	for _, action := range []string{"saved_positions", "schema_changes", "parallel_copy", "start_from_time"} {
		if d.Why[action] == "" {
			t.Errorf("debezium: no reason for unavailable %s", action)
		}
	}
	f := CapsFor(FlinkCDC, "postgresql")
	if f.EditPosition || f.RecopyTable || f.EventFormats || f.HashMask || f.ResizeParallelism || f.StartFromTime || !f.SavedPositions || !f.AddTables || !f.ParallelCopy {
		t.Fatalf("flink caps: %+v", f)
	}
	for _, action := range []string{"edit_position", "recopy_table", "event_formats", "hash_mask", "resize_parallelism", "start_from_time"} {
		if f.Why[action] == "" {
			t.Errorf("flink: no reason for unavailable %s", action)
		}
	}
	for _, src := range []string{"mysql", "sqlserver"} {
		if c := CapsFor(FlinkCDC, src); !c.StartFromTime || c.Why["start_from_time"] != "" {
			t.Errorf("flink %s should start from a time: %+v", src, c)
		}
	}
}
