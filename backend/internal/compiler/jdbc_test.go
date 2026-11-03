package compiler_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

func jdbcSink(cfg map[string]any) connectors.Resolved {
	return connectors.Resolved{Type: "jdbc", Provider: Str(cfg, "database_type"), Config: cfg, Secrets: map[string]any{"password": "p@ss"}}
}

// Str reads a string from a test config.
func Str(m map[string]any, k string) string { s, _ := m[k].(string); return s }

func TestCompileJdbc(t *testing.T) {
	cases := []struct {
		name string
		cfg  map[string]any
		url  string
		coll string
		dest string
	}{
		{"postgresql", map[string]any{"database_type": "postgresql", "host": "wh", "port": 5432, "database": "analytics", "schema": "cdc", "user": "bz", "tls": "verify"},
			"jdbc:postgresql://wh:5432/analytics?sslmode=verify-full&currentSchema=cdc", "", "cdc.shop_orders"},
		{"mysql", map[string]any{"database_type": "mysql", "host": "wh", "port": 3306, "database": "analytics", "user": "bz", "tls": "require"},
			"jdbc:mysql://wh:3306/analytics?sslMode=REQUIRED", "", "shop_orders"},
		{"mariadb", map[string]any{"database_type": "mariadb", "host": "wh", "port": 3306, "database": "analytics", "user": "bz", "tls": "disable"},
			"jdbc:mariadb://wh:3306/analytics?sslMode=disable", "", "shop_orders"},
		{"sqlserver", map[string]any{"database_type": "sqlserver", "host": "wh", "port": 1433, "database": "Analytics", "schema": "dbo", "user": "bz", "tls": "require"},
			"jdbc:sqlserver://wh:1433;databaseName=Analytics;encrypt=true;trustServerCertificate=true", "", "shop_orders"},
	}
	for _, c := range cases {
		got, err := compilePipeline(t, "pg", jdbcSink(c.cfg), nil)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		p := got.Properties
		for k, v := range map[string]string{
			"debezium.sink.type":                        "jdbc",
			"debezium.sink.jdbc.connection.url":         c.url,
			"debezium.sink.jdbc.connection.password":    "${BZ_SINK_PASSWORD}",
			"debezium.sink.jdbc.insert.mode":            "upsert",
			"debezium.sink.jdbc.primary.key.mode":       "record_key",
			"debezium.sink.jdbc.delete.enabled":         "true",
			"debezium.sink.jdbc.schema.evolution":       "basic",
			"debezium.sink.jdbc.collection.name.format": c.coll,
			"debezium.format.value":                     "connect",
		} {
			if p[k] != v {
				t.Errorf("%s: %s = %q, want %q", c.name, k, p[k], v)
			}
		}
		if got.Env["BZ_SINK_PASSWORD"] != "p@ss" || got.Destinations["shop.orders"] != c.dest {
			t.Errorf("%s: env %v destinations %v", c.name, got.Env, got.Destinations)
		}
	}
}

func TestCompileJdbcDeletesFollowTheOption(t *testing.T) {
	pg := jdbcSink(map[string]any{"database_type": "postgresql", "host": "wh", "database": "analytics", "schema": "public", "user": "bz"})
	unwrap := func(handling string) func(*domain.PipelineSpec) {
		return func(s *domain.PipelineSpec) {
			s.Transforms = []domain.Transform{{Type: "unwrap", DeleteHandling: "rewrite", AddFields: []string{"op"}}}
			s.Sink.Options = map[string]any{"deletes": handling}
		}
	}
	for option, want := range map[string]string{"delete": "tombstone", "ignore": "drop"} {
		got, err := compilePipeline(t, "pg", pg, unwrap(option))
		if err != nil {
			t.Fatal(err)
		}
		chain := strings.Split(got.Properties["debezium.transforms"], ",")
		if h := got.Properties["debezium.transforms."+chain[1]+".delete.tombstone.handling.mode"]; h != want {
			t.Errorf("deletes=%s: row-after-change handles deletes as %q, want %q", option, h, want)
		}
	}
	// Insert-only tables have no key: no upsert key, no deletes.
	got, err := compilePipeline(t, "pg", pg, func(s *domain.PipelineSpec) { s.Sink.Options = map[string]any{"write_mode": "insert"} })
	if err != nil {
		t.Fatal(err)
	}
	if got.Properties["debezium.sink.jdbc.primary.key.mode"] != "none" || got.Properties["debezium.sink.jdbc.delete.enabled"] != "false" {
		t.Errorf("insert only: %v", got.Properties)
	}
}

func TestCompileJdbcRefusesUnsafeNames(t *testing.T) {
	_, err := compilePipeline(t, "pg", jdbcSink(map[string]any{"database_type": "postgresql", "host": "wh", "database": "a;b", "user": "bz"}), nil)
	var p *problem.Problem
	if !errors.As(err, &p) || p.Code != "jdbc_config" {
		t.Fatalf("got %v", err)
	}
}
