package mcpserver

import "testing"

func TestGroupPutsEngineAndPositionToolsWhereTheyBelong(t *testing.T) {
	for name, want := range map[string]string{
		"list_engines":                       "Engine versions",
		"get_debezium_catalog":               "Engine versions",
		"list_saved_positions":               "Pipelines",
		"restore_saved_position":             "Pipelines",
		"move_pipelines_to_debezium_version": "Pipelines",
	} {
		if got := (&Tool{Name: name}).Group(); got != want {
			t.Errorf("%s: group %q, want %q", name, got, want)
		}
	}
}
