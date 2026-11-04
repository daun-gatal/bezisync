package services

import (
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

func TestNeedsFlinkColumns(t *testing.T) {
	spec := domain.DefaultSpec()
	if !NeedsFlinkColumns(spec) {
		t.Fatal("new columns are held back by default")
	}
	spec.FlinkCDC.NewColumns = "send"
	if NeedsFlinkColumns(spec) {
		t.Fatal("nothing is written column by column")
	}
	spec.Source.Columns = map[string]domain.ColumnRules{"s.t": {Exclude: []string{"card"}}}
	if !NeedsFlinkColumns(spec) {
		t.Fatal("a column left out")
	}
	spec.Source.Columns = nil
	spec.Transforms = []domain.Transform{{Type: "rename", Renames: []domain.Rename{{From: "a", To: "b"}}}}
	if !NeedsFlinkColumns(spec) {
		t.Fatal("a rename")
	}
}

func TestStoredColumnsReadTheArtifact(t *testing.T) {
	p := &models.Pipeline{Artifacts: db.Map{ArtifactColumns: map[string]any{"s.t": []any{"id", "name"}, "s.u": []any{}}}}
	got := storedColumns(p)
	if len(got) != 1 || len(got["s.t"]) != 2 || got["s.t"][1] != "name" {
		t.Fatal(got)
	}
	if len(storedColumns(&models.Pipeline{})) != 0 {
		t.Fatal("no artifact, no columns")
	}
	p.Artifacts[ArtifactDefaults] = map[string]any{"s.t": []any{"id"}}
	if d := storedColumns(p, ArtifactDefaults); len(d) != 1 || len(d["s.t"]) != 1 || d["s.t"][0] != "id" {
		t.Fatal(d)
	}
	if n := namesArtifact(map[string][]string{"s.t": {"id"}}); len(n["s.t"].([]any)) != 1 {
		t.Fatal(n)
	}
}
