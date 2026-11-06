package engine

import (
	"encoding/json"
	"os"
	"reflect"
	"slices"
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
	if count(FlinkCDC, "sqlserver", "iceberg") != len(FlinkSettings) {
		t.Fatal("connector filter")
	}
	// C19: a connector's own entries only for that connector, source and destination alike.
	for _, pair := range [][2]string{{"vitess", "kafka"}, {"postgresql", "http"}, {"mongodb", "pubsub"}, {"oracle", "redis"}} {
		for _, st := range SettingsFor(Debezium, pair[0], pair[1]) {
			want := map[string]string{ScopeSource: pair[0], ScopeSink: pair[1]}[st.Scope]
			if st.Connectors != nil && !slices.Contains(st.Connectors, want) {
				t.Errorf("%v offers %s", pair, st.Key)
			}
		}
	}
	has := func(src, snk, key string) bool {
		return slices.ContainsFunc(SettingsFor(Debezium, src, snk), func(st Setting) bool { return st.Key == key })
	}
	if !has("postgresql", "kafka", "debezium.source.slot.max.retries") || has("mysql", "kafka", "debezium.source.slot.max.retries") ||
		!has("mysql", "kafka", "debezium.sink.kafka.producer.linger.ms") || has("mysql", "http", "debezium.sink.kafka.producer.linger.ms") {
		t.Fatal("per-connector entries")
	}
	// C20: no destination entry for a key a connection field sets.
	for _, k := range []string{"debezium.sink.http.timeout.ms", "debezium.sink.http.retries", "debezium.sink.kafka.producer.bootstrap.servers", "debezium.sink.redis.address"} {
		if SettingOf(Debezium, k) != nil {
			t.Errorf("%s is set by the connection", k)
		}
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
