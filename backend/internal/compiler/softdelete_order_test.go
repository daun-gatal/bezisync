package compiler_test

import (
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

func lakeWith(t *testing.T, transforms []domain.Transform) (chain []string, hash string) {
	t.Helper()
	sink := icebergSink(map[string]any{"catalog_type": "rest", "uri": "https://catalog.example.com/api/catalog", "warehouse": "lake",
		"namespace": "cdc", "auth": "none", "storage": "vended"}, map[string]any{})
	got, err := compilePipeline(t, "pg", sink, func(s *domain.PipelineSpec) { s.Transforms = transforms })
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(got.Properties["debezium.transforms"], ","), got.ConfigHash()
}

// On a destination that needs the event shape (Iceberg), Keep deleted rows followed by a field
// step and no explicit unwrap compiles the unwrap first, so the field step reads rows (Known
// issue 18). Every other chain compiles byte-identically to before (the hashes are pinned).
func TestSoftDeleteBeforeFieldStepsOnIceberg(t *testing.T) {
	rename := domain.Transform{Type: "rename", ID: "r9c1", Renames: []domain.Rename{{From: "total", To: "amount"}}}
	route := domain.Transform{Type: "route", ID: "rt01", Routes: []domain.Route{{Match: "shop.orders", To: "orders_all"}}}
	filter := domain.Transform{Type: "filter", ID: "f001", Operations: []string{"insert", "update", "delete"}}
	sd := domain.Transform{Type: "soft_delete", ID: "sd01"}
	unwrap := domain.Transform{Type: "unwrap", ID: "u1a0", DeleteHandling: "rewrite"}

	chain, _ := lakeWith(t, []domain.Transform{sd, rename})
	if got := strings.Join(chain, ","); got != "bzmeta,t0unwrap,t_r9c1,bzroute" {
		t.Fatalf("[soft_delete, rename]: %s", got)
	}
	for name, c := range map[string]struct {
		steps []domain.Transform
		hash  string
	}{
		"none":                     {nil, "66416860634af46d"},
		"[soft_delete]":            {[]domain.Transform{sd}, "66416860634af46d"},
		"[filter, soft_delete]":    {[]domain.Transform{filter, sd}, "35ee88b475bc0710"},
		"[soft_delete, route]":     {[]domain.Transform{sd, route}, "8207d2335ca638fd"},
		"[unwrap, soft_delete, …]": {[]domain.Transform{unwrap, sd, rename}, "ae92c4a834b4323d"},
	} {
		if _, h := lakeWith(t, c.steps); h != c.hash {
			t.Errorf("%s: config_hash %s, want %s (a running pipeline would restart)", name, h, c.hash)
		}
	}
}
