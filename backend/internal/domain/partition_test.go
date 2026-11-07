package domain

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

type partitionVectors struct {
	Kinds []struct {
		Engine, Source, Type, Decimals, Binary, Kind string
	} `json:"kinds"`
	Fits map[string][]string `json:"fits"`
}

// The vectors the wizard's copy (PartitionBy.tsx) is tested against too.
func loadPartitionVectors(t *testing.T) partitionVectors {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "partition_kinds.json"))
	if err != nil {
		t.Fatal(err)
	}
	var v partitionVectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestPartitionKind(t *testing.T) {
	for _, c := range loadPartitionVectors(t).Kinds {
		got := PartitionKind(c.Engine, c.Source, c.Type, ValueFormats{Decimals: c.Decimals, Binary: c.Binary})
		if got != c.Kind {
			t.Errorf("%s %s %q (decimals %q, binary %q): %q, want %q", c.Engine, c.Source, c.Type, c.Decimals, c.Binary, got, c.Kind)
		}
	}
}

func TestPartitionFits(t *testing.T) {
	v := loadPartitionVectors(t)
	if len(v.Fits) != len(PartitionTransforms) {
		t.Fatalf("transforms: %d in the vectors, %d in PartitionTransforms", len(v.Fits), len(PartitionTransforms))
	}
	kinds := []string{"int", "decimal", "float", "bool", "text", "uuid", "date", "time", "timestamp", "timestamptz", "binary", ""}
	for tr, fits := range v.Fits {
		for _, k := range kinds {
			if got := PartitionFits(tr, k); got != slices.Contains(fits, k) {
				t.Errorf("%s on %q: %v", tr, k, got)
			}
		}
	}
	// Iceberg: no bucket of a boolean or a float, no truncate of a date, hour needs a time of day.
	for _, bad := range [][2]string{{"bucket", "bool"}, {"bucket", "float"}, {"truncate", "timestamp"}, {"hour", "date"}, {"day", "int"}, {"truncate", "uuid"}} {
		if PartitionFits(bad[0], bad[1]) {
			t.Errorf("%s takes %s", bad[0], bad[1])
		}
	}
}
