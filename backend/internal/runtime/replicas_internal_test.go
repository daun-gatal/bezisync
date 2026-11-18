package runtime

import (
	"math"
	"testing"
)

func TestReplicaCountClamps(t *testing.T) {
	for in, want := range map[int]int32{0: 0, 3: 3, 16: 16, -1: 0, math.MaxInt32 + 1: math.MaxInt32} {
		if got := replicaCount(in); got != want {
			t.Errorf("replicaCount(%d) = %d, want %d", in, got, want)
		}
	}
}
