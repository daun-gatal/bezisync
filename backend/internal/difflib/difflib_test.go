package difflib_test

import (
	"reflect"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/contract"
	"github.com/daun-gatal/bezisync/backend/internal/difflib"
)

func TestUnifiedDiffMatchesPython(t *testing.T) {
	var fx struct {
		Cases []struct {
			A    []string `json:"a"`
			B    []string `json:"b"`
			Diff []string `json:"diff"`
		} `json:"cases"`
	}
	contract.Load(t, "difflib", &fx)
	for i, c := range fx.Cases {
		got := difflib.UnifiedDiff(c.A, c.B, "v1", "v2", 3)
		if len(got) == 0 && len(c.Diff) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, c.Diff) {
			t.Errorf("case %d (%d lines):\n got %q\nwant %q", i, len(c.A), got, c.Diff)
		}
	}
}
