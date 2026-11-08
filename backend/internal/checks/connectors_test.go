package checks_test

import (
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
)

// Every connection type has its section in the reference, named as the type's title, so the
// engines line follows the connector registry.
func TestCatalogConnectorsMatchTheRegistry(t *testing.T) {
	want := map[string]checks.Connector{}
	for _, d := range connectors.Descriptors() {
		want[d.Title] = checks.Connector{Kind: d.Kind, Type: d.Type}
	}
	got := checks.LoadCatalog().Connectors
	for title, c := range want {
		if got[title] != c {
			t.Errorf("catalog connectors[%q] = %+v, want %+v", title, got[title], c)
		}
	}
	for title := range got {
		if _, ok := want[title]; !ok {
			t.Errorf("catalog connectors[%q] names no connection type", title)
		}
	}
}
