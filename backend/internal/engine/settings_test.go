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
	// F2, F3: a source's or destination's own options only there.
	for _, st := range SettingsFor(FlinkCDC, "postgresql", "iceberg") {
		if st.Connectors != nil && !slices.Contains(st.Connectors, map[string]string{ScopeSource: "postgresql", ScopeSink: "iceberg"}[st.Scope]) {
			t.Errorf("postgresql → iceberg offers %s", st.Key)
		}
	}
	has := func(id ID, src, snk, key string) bool {
		return slices.ContainsFunc(SettingsFor(id, src, snk), func(st Setting) bool { return st.Key == key })
	}
	if !has(FlinkCDC, "postgresql", "iceberg", "source.scan.lsn-commit.checkpoints-num-delay") || has(FlinkCDC, "mysql", "iceberg", "source.scan.lsn-commit.checkpoints-num-delay") ||
		!has(FlinkCDC, "mysql", "starrocks", "sink.sink.io.thread-count") || has(FlinkCDC, "mysql", "iceberg", "sink.sink.io.thread-count") {
		t.Fatal("Flink connector filter")
	}
	// SQL Server has no Flink CDC source options of its own: only the engine's and the rest.
	if has(FlinkCDC, "sqlserver", "iceberg", "source.connection.pool.size") || count(FlinkCDC, "sqlserver", "iceberg") < len(FlinkSettings) {
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
	if !has(Debezium, "postgresql", "kafka", "debezium.source.slot.max.retries") || has(Debezium, "mysql", "kafka", "debezium.source.slot.max.retries") ||
		!has(Debezium, "mysql", "kafka", "debezium.sink.kafka.producer.linger.ms") || has(Debezium, "mysql", "http", "debezium.sink.kafka.producer.linger.ms") {
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

// E9 (ADR 0006): Oracle on Flink CDC gets the connection options its reader has (pool, timeout,
// attempts), not PostgreSQL's WAL delay or the open-ended chunk option it doesn't accept.
func TestSettingsForFlinkOracle(t *testing.T) {
	has := func(key string) bool {
		return slices.ContainsFunc(SettingsFor(FlinkCDC, "oracle", "starrocks"), func(st Setting) bool { return st.Key == key })
	}
	for _, key := range []string{"source.connection.pool.size", "source.connect.timeout", "source.connect.max-retries"} {
		if !has(key) {
			t.Errorf("oracle lacks %s", key)
		}
	}
	for _, key := range []string{"source.scan.lsn-commit.checkpoints-num-delay", "source.scan.incremental.snapshot.unbounded-chunk-first.enabled"} {
		if has(key) {
			t.Errorf("oracle offers %s", key)
		}
	}
	if msg := CheckSetting(*SettingOf(FlinkCDC, "source.connect.timeout"), "45 s"); msg != "" {
		t.Errorf("connect timeout: %s", msg)
	}
}
