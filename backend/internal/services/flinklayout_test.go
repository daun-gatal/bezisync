package services

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

// ADR 0012 L5/L6: what a version changes in the columns a pipeline writes, per table: added and
// gone names (a rollback is the same change the other way); order alone and added tables aren't.
func TestProducedChanges(t *testing.T) {
	was := map[string][]domain.FlinkColumn{"shop.orders": {{Name: "id"}, {Name: "a"}, {Name: "__op"}}, "shop.items": {{Name: "id"}}}
	now := map[string][]domain.FlinkColumn{"shop.orders": {{Name: "id"}, {Name: "test_field"}, {Name: "__op"}}, "shop.items": {{Name: "id"}}, "shop.new": {{Name: "x"}}}
	got := producedChanges(was, now)
	if !reflect.DeepEqual(got, []string{"shop.orders adds test_field and no longer writes a"}) {
		t.Fatal(got)
	}
	if got := producedChanges(now, was); !reflect.DeepEqual(got, []string{"shop.orders adds a and no longer writes test_field"}) {
		t.Fatal("rolling back:", got)
	}
	reordered := map[string][]domain.FlinkColumn{"shop.orders": {{Name: "__op"}, {Name: "a"}, {Name: "id"}}, "shop.items": {{Name: "id"}}}
	if got := producedChanges(was, reordered); len(got) != 0 {
		t.Fatal(got)
	}
}

// L16: a saved position at or below the layout floor was written by a job with other columns.
func TestRestoreBeforeLayout(t *testing.T) {
	p := &models.Pipeline{Artifacts: db.Map{ArtifactLayout: map[string]any{"floor": json.Number("12")}}}
	if err := restoreBeforeLayout(p, 12); err == nil || !strings.Contains(err.Error(), "#12") {
		t.Fatal(err)
	}
	if err := restoreBeforeLayout(p, 13); err != nil {
		t.Fatal(err)
	}
	if err := restoreBeforeLayout(&models.Pipeline{Artifacts: db.Map{}}, 1); err != nil {
		t.Fatal("no layout, nothing refused:", err)
	}
}

// The stored layout reads back as the compiler takes it, however the artifact was decoded.
func TestStoredLayout(t *testing.T) {
	var artifacts db.Map
	raw := `{"flink_layout":{"floor":3,"tables":{"lake.orders":[{"name":"id"},{"name":"gone","type":"VARCHAR","retired":true},{"name":"n","type":"BIGINT","cast":true}]}},"flink_fresh_start":true}`
	if err := json.Unmarshal([]byte(raw), &artifacts); err != nil {
		t.Fatal(err)
	}
	p := &models.Pipeline{Artifacts: artifacts}
	want := map[string][]domain.LayoutColumn{"lake.orders": {{Name: "id"}, {Name: "gone", Type: "VARCHAR", Retired: true}, {Name: "n", Type: "BIGINT", Cast: true}}}
	if got := storedLayout(p); !reflect.DeepEqual(got, want) || layoutFloor(p) != 3 || !freshStartPlanned(p) {
		t.Fatal(got, layoutFloor(p), freshStartPlanned(p))
	}
	if storedLayout(&models.Pipeline{Artifacts: db.Map{}}) != nil {
		t.Fatal("no layout is nil, so the compile is unchanged")
	}
}
