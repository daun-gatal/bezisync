package connectors_test

import (
	"fmt"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
)

func TestParsePGCapacity(t *testing.T) {
	msg := fmt.Sprintf("Replication slots %d of %d used · WAL senders %d of %d.", 3, 10, 2, 12)
	for _, c := range []checks.Check{{Message: msg}, {Message: "Replication slots are almost exhausted.", Technical: msg}} {
		got, ok := connectors.ParsePGCapacity(c)
		if !ok || got != (connectors.PGCapacity{UsedSlots: 3, MaxSlots: 10, UsedSenders: 2, MaxSenders: 12}) {
			t.Fatal(got, ok)
		}
	}
	if _, ok := connectors.ParsePGCapacity(checks.Check{Message: "Signed in."}); ok {
		t.Fatal("parsed nothing")
	}
}
