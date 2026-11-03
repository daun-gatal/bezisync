package compiler_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/contract"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

func icebergSink(cfg, secrets map[string]any) connectors.Resolved {
	return connectors.Resolved{Type: "iceberg", Provider: "rest", Config: cfg, Secrets: secrets}
}

func compilePipeline(t *testing.T, sourceName string, sink connectors.Resolved, mutate func(*domain.PipelineSpec)) (*compiler.Compiled, error) {
	t.Helper()
	var fx compileFixture
	contract.Load(t, "compile", &fx)
	s, err := config.Load(func(k string) (string, bool) {
		if k == "BEZISYNC_STATE_DATABASE_URL" {
			return "postgresql://state:pw@state-db:5432/bz", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	config.Set(s)
	t.Cleanup(config.Reset)
	spec := domain.DefaultSpec()
	spec.Source.Tables.Include = []string{"shop.orders", "shop.events"}
	spec.Sink.Naming = "{schema}_{table}"
	spec.Transforms = nil
	if mutate != nil {
		mutate(&spec)
	}
	ctx := connectors.CompileContext{PipelineID: fx.PipelineID, ShortID: "3f2c1d4e5b6a", Name: "orders-lake", Spec: spec}
	return compiler.Compile(ctx, resolved(t, fx.Sources[sourceName]), sink, "")
}

func TestCompileIcebergRest(t *testing.T) {
	sink := icebergSink(map[string]any{"catalog_type": "rest", "uri": "https://catalog.example.com/api/catalog", "warehouse": "lake",
		"namespace": "cdc", "auth": "oauth2", "client_id": "bezisync", "scope": "PRINCIPAL_ROLE:ALL", "storage": "vended"},
		map[string]any{"client_secret": "s3cret"})
	got, err := compilePipeline(t, "pg", sink, func(s *domain.PipelineSpec) {
		s.Sink.Options = map[string]any{"partitions": map[string]any{"shop.orders": "id", "shop.events": ""}}
	})
	if err != nil {
		t.Fatal(err)
	}
	p := got.Properties
	want := map[string]string{
		"debezium.sink.type":                                       "iceberg",
		"debezium.sink.iceberg.type":                               "rest",
		"debezium.sink.iceberg.uri":                                "https://catalog.example.com/api/catalog",
		"debezium.sink.iceberg.credential":                         "${BZ_ICEBERG_CREDENTIAL}",
		"debezium.sink.iceberg.scope":                              "PRINCIPAL_ROLE:ALL",
		"debezium.sink.iceberg.header.X-Iceberg-Access-Delegation": "vended-credentials",
		"debezium.sink.iceberg.upsert":                             "true",
		"debezium.sink.iceberg.upsert-keep-deletes":                "true",
		"debezium.sink.iceberg.format-version":                     "2",
		"debezium.sink.iceberg.partition-by.shop_orders":           `bucket(id\,16)`,
		"debezium.sink.iceberg.partition-by.shop_events":           "day(__source_ts_ms)",
		"debezium.sink.batch.batch-size-wait":                      "MaxBatchSizeWait",
		"debezium.sink.batch.batch-size-wait.max-wait-ms":          "60000",
		"debezium.format.value":                                    "connect",
		"debezium.format.key":                                      "connect",
		"debezium.source.time.precision.mode":                      "isostring",
		"debezium.sink.iceberg.destination-lowercase-table-names":  "true",
		"debezium.sink.iceberg.table-namespace":                    "cdc",
	}
	for k, v := range want {
		if p[k] != v {
			t.Errorf("%s = %q, want %q", k, p[k], v)
		}
	}
	if _, ok := p["debezium.format.value.schemas.enable"]; ok {
		t.Error("connect format has no JSON schemas setting")
	}
	if got.Env["BZ_ICEBERG_CREDENTIAL"] != "bezisync:s3cret" {
		t.Errorf("credential env: %q", got.Env["BZ_ICEBERG_CREDENTIAL"])
	}
	for _, v := range p {
		if strings.Contains(v, "s3cret") {
			t.Fatal("a secret reached application.properties")
		}
	}
	// The row-after-change step the build needs is added with __op and __source_ts_ms.
	chain := strings.Split(p["debezium.transforms"], ",")
	unwrap := chain[len(chain)-2]
	if p["debezium.transforms."+unwrap+".type"] != "io.debezium.transforms.ExtractNewRecordState" ||
		p["debezium.transforms."+unwrap+".add.fields"] != "op,source.ts_ms" ||
		p["debezium.transforms."+unwrap+".delete.tombstone.handling.mode"] != "rewrite" {
		t.Errorf("unwrap step: %v", chain)
	}
	// Compiled with an official image (as services.ResolveImage never does for Iceberg), health
	// stays on 8080; the Iceberg build serves it on its management port.
	if got.HealthPort != 8080 || connectors.HealthPort("iceberg", connectors.SinkImageFor("iceberg")) != 9000 {
		t.Errorf("health ports: %d %d", got.HealthPort, connectors.HealthPort("iceberg", connectors.SinkImageFor("iceberg")))
	}
	if got.Destinations["shop.orders"] != "cdc.shop_orders" {
		t.Errorf("destinations: %v", got.Destinations)
	}
}

func TestCompileIcebergRustFSAndGlue(t *testing.T) {
	rustfs := icebergSink(map[string]any{"catalog_type": "rest", "uri": "http://rustfs:9000/iceberg", "warehouse": "lake", "auth": "sigv4",
		"signing_name": "s3", "region": "us-east-1", "storage": "s3", "s3_endpoint": "http://rustfs:9000", "path_style": true, "access_key_id": "AKIA"},
		map[string]any{"secret_access_key": "SECRET"})
	got, err := compilePipeline(t, "mssql", rustfs, func(s *domain.PipelineSpec) {
		s.Sink.Options = map[string]any{"write_mode": "append", "commit_interval": "now"}
	})
	if err != nil {
		t.Fatal(err)
	}
	p := got.Properties
	for k, v := range map[string]string{
		"debezium.sink.iceberg.rest.sigv4-enabled":       "true",
		"debezium.sink.iceberg.rest.signing-name":        "s3",
		"debezium.sink.iceberg.rest.signing-region":      "us-east-1",
		"debezium.sink.iceberg.s3.endpoint":              "http://rustfs:9000",
		"debezium.sink.iceberg.s3.path-style-access":     "true",
		"debezium.sink.iceberg.upsert":                   "false",
		"debezium.sink.iceberg.partition-by.shop_orders": "day(__source_ts_ms)",
		"debezium.sink.batch.batch-size-wait":            "NoBatchSizeWait",
		"debezium.source.time.precision.mode":            "connect", // SQL Server has no ISO mode
	} {
		if p[k] != v {
			t.Errorf("%s = %q, want %q", k, p[k], v)
		}
	}
	if got.Env["AWS_ACCESS_KEY_ID"] != "AKIA" || got.Env["AWS_SECRET_ACCESS_KEY"] != "SECRET" || got.Env["AWS_REGION"] != "us-east-1" {
		t.Errorf("env: %v", got.Env)
	}

	glue := icebergSink(map[string]any{"catalog_type": "glue", "warehouse": "s3://lake/warehouse", "region": "eu-west-1"}, map[string]any{})
	got, err = compilePipeline(t, "pg", glue, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Properties["debezium.sink.iceberg.type"] != "glue" || got.Properties["debezium.sink.iceberg.client.region"] != "eu-west-1" {
		t.Errorf("glue: %v", got.Properties)
	}
	if _, ok := got.Properties["debezium.sink.iceberg.partition-by.shop_orders"]; ok {
		t.Error("a table without a recorded key stays unpartitioned")
	}
}

func TestCompileIcebergRefuses(t *testing.T) {
	ok := map[string]any{"catalog_type": "rest", "uri": "https://c.example.com", "warehouse": "lake"}
	cases := []struct {
		name   string
		cfg    map[string]any
		mutate func(*domain.PipelineSpec)
		code   string
	}{
		{"rest without uri", map[string]any{"catalog_type": "rest", "warehouse": "lake"}, nil, "iceberg_config"},
		{"expansion in a value", map[string]any{"catalog_type": "rest", "uri": "https://${HOST}", "warehouse": "lake"}, nil, "iceberg_config"},
		{"glue without s3 warehouse", map[string]any{"catalog_type": "glue", "warehouse": "lake", "region": "eu-west-1"}, nil, "iceberg_config"},
		{"dots-free naming rules", ok, func(s *domain.PipelineSpec) { s.Sink.Naming = "cdc-{schema}-{table}" }, "naming_invalid"},
		{"outbox events", ok, func(s *domain.PipelineSpec) { s.Transforms = []domain.Transform{{Type: "outbox"}} }, "transform_unsupported"},
		{"tombstone deletes", ok, func(s *domain.PipelineSpec) {
			s.Transforms = []domain.Transform{{Type: "unwrap", DeleteHandling: "tombstone", AddFields: []string{"op"}}}
		}, "transform_unsupported"},
		{"unknown option value", ok, func(s *domain.PipelineSpec) { s.Sink.Options = map[string]any{"write_mode": "merge"} }, "validation_failed"},
	}
	for _, c := range cases {
		_, err := compilePipeline(t, "pg", icebergSink(c.cfg, map[string]any{}), c.mutate)
		var p *problem.Problem
		if !errors.As(err, &p) || p.Code != c.code {
			b, _ := json.Marshal(err)
			t.Errorf("%s: got %v %s, want %s", c.name, err, b, c.code)
		}
	}
}
