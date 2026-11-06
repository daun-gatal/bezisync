package services

import (
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
)

// A Flink CDC spec with a Debezium-only setting is refused when saved (the compiler would refuse it
// only when the configuration is built); each engine keeps its own settings.
func TestValidateEngineSpecRefusesTheOtherEnginesSettings(t *testing.T) {
	window, image := "22:00-06:00", "quay.io/debezium/server:3.7.0.Final"
	for name, c := range map[string]struct {
		edit func(*domain.PipelineSpec)
		want string
	}{
		"value formats": {func(s *domain.PipelineSpec) { s.Values.Decimals = "double" }, "Value formats"},
		// Raw properties are no longer refused wholesale: compiler.ClassifyFlinkAdvanced judges each key.
		"copy delay":     {func(s *domain.PipelineSpec) { s.Snapshot.DelaySeconds = 60 }, "copy delay or window"},
		"copy window":    {func(s *domain.PipelineSpec) { s.Snapshot.Window = &window }, "copy delay or window"},
		"debezium image": {func(s *domain.PipelineSpec) { s.DebeziumImage = &image }, "Debezium image"},
	} {
		spec := domain.DefaultSpec()
		c.edit(&spec)
		if err := ValidateEngineSpec(engine.FlinkCDC, spec); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s on Flink CDC: %v", name, err)
		}
		if err := ValidateEngineSpec(engine.Debezium, spec); err != nil {
			t.Errorf("%s on Debezium: %v", name, err)
		}
	}
	if err := ValidateEngineSpec(engine.FlinkCDC, domain.DefaultSpec()); err != nil {
		t.Fatal(err)
	}
	spec := domain.DefaultSpec()
	spec.FlinkCDC.NewColumns = "send"
	if err := ValidateEngineSpec(engine.Debezium, spec); err == nil {
		t.Fatal("Flink CDC settings accepted on Debezium")
	}
}
