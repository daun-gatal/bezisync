package services

import (
	"errors"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

func TestValidateTuning(t *testing.T) {
	spec := func(tuning map[string]string) domain.PipelineSpec {
		s := domain.DefaultSpec()
		s.Tuning = tuning
		return s
	}
	messages := func(err error) map[string]string {
		out := map[string]string{}
		var p *problem.Problem
		if errors.As(err, &p) {
			for _, e := range p.Errors {
				loc := e["loc"].([]string)
				out[loc[1]] = e["message"].(string)
			}
		}
		return out
	}
	if err := ValidateTuning(engine.Debezium, "postgresql", "kafka", spec(map[string]string{"debezium.source.snapshot.max.threads": "4"})); err != nil {
		t.Fatal(err)
	}
	if err := ValidateTuning(engine.Debezium, "postgresql", "kafka", spec(nil)); err != nil {
		t.Fatal(err)
	}
	got := messages(ValidateTuning(engine.Debezium, "vitess", "kafka", spec(map[string]string{
		"debezium.source.snapshot.max.threads": "4",
		"debezium.source.poll.interval.ms":     "0250",
		"source.connection.pool.size":          "5",
		"debezium.source.made.up.key":          "1",
	})))
	want := map[string]string{
		"debezium.source.snapshot.max.threads": "doesn't apply to this source or destination",
		"debezium.source.poll.interval.ms":     "write it as 250",
		"source.connection.pool.size":          "applies only to pipelines that run on Flink CDC",
		"debezium.source.made.up.key":          "is not a setting BeziSync offers",
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s: %q, want %q", k, got[k], w)
		}
	}
	// Flink settings live in flink_cdc.flink_conf, not in tuning.
	got = messages(ValidateTuning(engine.FlinkCDC, "postgresql", "iceberg", spec(map[string]string{"heartbeat.timeout": "50 s", "source.connection.pool.size": "101"})))
	if got["heartbeat.timeout"] != "is a Flink setting: it goes in flink_cdc.flink_conf" || got["source.connection.pool.size"] != "must be a whole number from 2 to 100" {
		t.Fatal(got)
	}
}
