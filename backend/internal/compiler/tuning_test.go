package compiler_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

func debeziumCompile(t *testing.T, src, snk string, tuning map[string]string, advanced map[string]string) (*compiler.Compiled, error) {
	t.Helper()
	spec, err := matrixSpec(t, debeziumMatrixSpecs["basic"])
	if err != nil {
		t.Fatal(err)
	}
	spec.Tuning, spec.Advanced = tuning, advanced
	sd, _ := connectors.Describe("source", src)
	kd, _ := connectors.Describe("sink", snk)
	ctx := connectors.CompileContext{PipelineID: "3f2c1d4e-5b6a-4789-8abc-0123456789ab", ShortID: "3f2c1d4e5b6a", Name: "orders-cdc", Spec: spec}
	return compiler.Compile(ctx, matrixConn(t, sd), matrixConn(t, kd), "")
}

func problemCode(err error) string {
	var p *problem.Problem
	if errors.As(err, &p) {
		return p.Code
	}
	return ""
}

func TestCompileAppliesTuning(t *testing.T) {
	flinkConfig(t)
	c, err := debeziumCompile(t, "postgresql", "kafka", map[string]string{"debezium.source.snapshot.max.threads": "4", "debezium.source.poll.interval.ms": "250"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Properties["debezium.source.snapshot.max.threads"] != "4" || c.Properties["debezium.source.poll.interval.ms"] != "250" {
		t.Fatal(c.Properties)
	}
	// A tuned key is BeziSync's now: an advanced property can't change it, but may set it when untuned.
	if v := compiler.ClassifyAdvanced("debezium.source.poll.interval.ms", "1", c.Managed); !v.Refused() {
		t.Fatal("advanced overrides tuning")
	}
	plain, _ := debeziumCompile(t, "postgresql", "kafka", nil, nil)
	if v := compiler.ClassifyAdvanced("debezium.source.poll.interval.ms", "1", plain.Managed); v.Refused() {
		t.Fatal(v)
	}
	// Refused: another source's setting, a value out of range, a key that isn't a setting.
	for _, tc := range []struct {
		src    string
		tuning map[string]string
	}{
		{"vitess", map[string]string{"debezium.source.snapshot.max.threads": "4"}},
		{"postgresql", map[string]string{"debezium.source.snapshot.max.threads": "0"}},
		{"postgresql", map[string]string{"debezium.source.snapshot.max.threads": "17"}},
		{"postgresql", map[string]string{"debezium.source.max.batch.size": "1"}},
		{"postgresql", map[string]string{"source.connection.pool.size": "5"}},
	} {
		if _, err := debeziumCompile(t, tc.src, "kafka", tc.tuning, nil); problemCode(err) != "tuning" {
			t.Errorf("%s %v: %v", tc.src, tc.tuning, err)
		}
	}
}

func TestCompileFlinkAppliesTuning(t *testing.T) {
	flinkConfig(t)
	spec, err := matrixSpec(t, flinkMatrixSpecs["basic"])
	if err != nil {
		t.Fatal(err)
	}
	ctx := connectors.CompileContext{PipelineID: "3f2c1d4e-5b6a-4789-8abc-0123456789ab", ShortID: "3f2c1d4e5b6a", Name: "orders-lake", Spec: spec}
	pg, _ := connectors.Describe("source", "postgresql")
	ms, _ := connectors.Describe("source", "sqlserver")
	ice, _ := connectors.Describe("sink", "iceberg")
	spec.Tuning = map[string]string{"source.connection.pool.size": "8"}
	ctx.Spec = spec
	f, err := compiler.CompileFlink(ctx, matrixConn(t, pg), matrixConn(t, ice), "flink-cdc:3.7.0-2.2.1", matrixFlinkInput)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.Pipeline, "connection.pool.size: \"8\"") {
		t.Fatal(f.Pipeline)
	}
	if _, err := compiler.CompileFlink(ctx, matrixConn(t, ms), matrixConn(t, ice), "flink-cdc:3.7.0-2.2.1", matrixFlinkInput); problemCode(err) != "tuning" {
		t.Fatal("SQL Server has no pool size setting:", err)
	}
	spec.Tuning = map[string]string{"debezium.source.poll.interval.ms": "250"}
	ctx.Spec = spec
	if _, err := compiler.CompileFlink(ctx, matrixConn(t, pg), matrixConn(t, ice), "flink-cdc:3.7.0-2.2.1", matrixFlinkInput); problemCode(err) != "tuning" {
		t.Fatal("a Debezium setting on Flink CDC:", err)
	}
}

// No registry setting is a key BeziSync compiles for a pipeline, unless it replaces a default
// BeziSync derives: otherwise tuning it would change what BeziSync owns.
func TestRegistryKeysAreNotCompiledByBeziSync(t *testing.T) {
	flinkConfig(t)
	for _, src := range connectors.Descriptors() {
		if src.Kind != "source" {
			continue
		}
		for _, snk := range connectors.Descriptors() {
			if snk.Kind != "sink" || flinkOnly[snk.Type] || snk.Type == "flinktest" {
				continue
			}
			for name, raw := range debeziumMatrixSpecs {
				spec, _ := matrixSpec(t, raw)
				ctx := connectors.CompileContext{PipelineID: "3f2c1d4e-5b6a-4789-8abc-0123456789ab", ShortID: "3f2c1d4e5b6a", Name: "orders-cdc", Spec: spec}
				c, err := compiler.Compile(ctx, matrixConn(t, src), matrixConn(t, snk), "")
				if err != nil {
					continue
				}
				for _, s := range engine.SettingsFor(engine.Debezium, src.Type, snk.Type) {
					if _, set := c.Properties[s.Key]; set && !s.Derived && spec.Advanced[s.Key] == "" {
						t.Errorf("%s → %s (%s): BeziSync compiles %s", src.Type, snk.Type, name, s.Key)
					}
				}
			}
		}
	}
}
