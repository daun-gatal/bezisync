package mcpserver

import (
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
)

func TestGroupPutsEngineAndPositionToolsWhereTheyBelong(t *testing.T) {
	for name, want := range map[string]string{
		"list_engines":                       "Engine versions",
		"get_debezium_catalog":               "Engine versions",
		"list_saved_positions":               "Pipelines",
		"list_engine_settings":               "Pipelines",
		"restore_saved_position":             "Pipelines",
		"move_pipelines_to_debezium_version": "Pipelines",
	} {
		if got := (&Tool{Name: name}).Group(); got != want {
			t.Errorf("%s: group %q, want %q", name, got, want)
		}
	}
}

// A tool asks for the permission its REST route enforces, so it is never offered and then
// refused (Known issue 6), and a tool that changes state isn't a read.
func TestToolPermissionsMatchTheirRoutes(t *testing.T) {
	want := map[string]struct {
		perm auth.Perm
		kind Kind
	}{
		"delete_pipeline": {auth.PipelineDeploy, Destructive}, // DELETE /pipelines/{id}
		"deploy_pipeline": {auth.PipelineDeploy, Write},       // POST …/deploy
		"update_pipeline": {auth.PipelineEdit, Write},         // PATCH /pipelines/{id}
		"test_runtime":    {auth.RuntimeManage, Write},        // POST …/runtimes/{id}/test writes last_test
	}
	for _, tool := range Catalog() {
		if w, ok := want[tool.Name]; ok {
			if tool.Perm != w.perm || tool.Kind != w.kind {
				t.Errorf("%s: %s/%s, want %s/%s", tool.Name, tool.Perm, tool.Kind, w.perm, w.kind)
			}
			delete(want, tool.Name)
		}
	}
	for name := range want {
		t.Errorf("%s: not in the catalog", name)
	}
}
