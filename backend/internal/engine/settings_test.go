package engine

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// C1: the Flink settings moved into the registry unchanged: same keys, titles, types, defaults,
// choices, bounds and reasons, in the same order (testdata/flink_settings.json, recorded before).
func TestFlinkSettingsUnchanged(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/flink_settings.json")
	if err != nil {
		t.Fatal(err)
	}
	var want struct {
		Settings []map[string]any  `json:"settings"`
		Defaults map[string]string `json:"defaults"`
	}
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(FlinkDefaults, want.Defaults) {
		t.Fatal(FlinkDefaults)
	}
	if len(FlinkSettings) != len(want.Settings) {
		t.Fatalf("%d settings, want %d", len(FlinkSettings), len(want.Settings))
	}
	for i, s := range FlinkSettings {
		b, _ := json.Marshal(s)
		var got map[string]any
		_ = json.Unmarshal(b, &got)
		for _, k := range []string{"engine", "scope", "stage", "impact"} {
			delete(got, k)
		}
		if !reflect.DeepEqual(got, want.Settings[i]) {
			t.Errorf("%d: %v, want %v", i, got, want.Settings[i])
		}
		if s.Engine != FlinkCDC || s.Scope != ScopeEngine || s.Impact != ImpactRestart {
			t.Errorf("%s: %s %s %s", s.Key, s.Engine, s.Scope, s.Impact)
		}
	}
}

func TestRegistry(t *testing.T) {
	count := func(id ID, src, snk string) int { return len(SettingsFor(id, src, snk)) }
	if count(FlinkCDC, "postgresql", "iceberg") != len(FlinkSettings)+len(FlinkPipelineSettings) {
		t.Fatal(count(FlinkCDC, "postgresql", "iceberg"))
	}
	// Connector-specific settings are offered only where they apply.
	if count(FlinkCDC, "sqlserver", "iceberg") != len(FlinkSettings) || count(Debezium, "vitess", "kafka") != len(DebeziumSettings)-1 {
		t.Fatal("connector filter")
	}
	if SettingOf(FlinkCDC, "heartbeat.timeout") == nil || SettingOf(Debezium, "heartbeat.timeout") != nil {
		t.Fatal("SettingOf is per engine")
	}
	keys := map[string]bool{}
	for _, s := range Settings() {
		id := string(s.Engine) + "/" + s.Key
		if keys[id] {
			t.Errorf("%s twice", id)
		}
		keys[id] = true
		if s.Impact.Rank() < 0 || s.Title == "" || s.Why == "" || CheckSetting(s, s.Default) != "" {
			t.Errorf("%s: incomplete or its default fails its own check", id)
		}
	}
	for i := 1; i < len(Impacts); i++ {
		if Impacts[i].Rank() <= Impacts[i-1].Rank() {
			t.Fatal("impacts are ordered")
		}
	}
	bool := Setting{Type: "bool"}
	if CheckSetting(bool, "true") != "" || CheckSetting(bool, "TRUE") == "" || CheckSetting(bool, "1") == "" || CheckSetting(bool, "yes") == "" {
		t.Fatal("bools are true or false")
	}
}
