package connectors

import (
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

func icebergFlink(t *testing.T, cfg map[string]any, sec map[string]any, options map[string]any) (*FlinkBlock, error) {
	t.Helper()
	spec := domain.DefaultSpec()
	spec.Sink.Options = options
	return (&icebergSink{}).FlinkSink(Resolved{Type: "iceberg", Config: cfg, Secrets: sec}, CompileContext{ShortID: "0123456789ab", Spec: spec})
}

// Storage and signing keys travel as catalog properties the helper fills in (the TaskManagers
// write the files and have no environment secrets); deleted rows stay, marked, by default.
func TestIcebergFlinkSink(t *testing.T) {
	rest := map[string]any{"catalog_type": "rest", "uri": "https://cat.example/api", "warehouse": "s3://lake/", "namespace": "lake", "auth": "sigv4",
		"signing_name": "glue", "region": "eu-west-1", "storage": "s3", "access_key_id": "AKIA1"}
	b, err := icebergFlink(t, rest, map[string]any{"secret_access_key": "s3cr3t"}, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]string{"catalog.properties.type": "rest", "catalog.properties.rest.sigv4-enabled": "true",
		"catalog.properties.rest.access-key-id": "${BZ_AWS_ACCESS_KEY_ID}", "catalog.properties.s3.secret-access-key": "${BZ_AWS_SECRET_ACCESS_KEY}",
		"catalog.properties.client.region": "eu-west-1", "catalog.properties.io-impl": "org.apache.iceberg.aws.s3.S3FileIO", "table.properties.format-version": "2"} {
		if got := b.Options.Get(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	if b.Env["BZ_AWS_SECRET_ACCESS_KEY"] != "s3cr3t" || b.Converter != "SOFT_DELETE" || b.Projection[0] != "__data_event_type__ AS `__op`" || b.Extra[0] != "iceberg-aws" {
		t.Fatalf("%+v", b)
	}
	for _, o := range b.Options {
		if strings.Contains(o[1], "s3cr3t") {
			t.Fatal("a secret in the pipeline definition")
		}
	}
	// Plain upserts: no marker column.
	if b, _ := icebergFlink(t, rest, map[string]any{"secret_access_key": "s3cr3t"}, map[string]any{"write_mode": "upsert"}); b.Converter != "" || len(b.Projection) != 0 {
		t.Fatalf("%+v", b)
	}
	// Vended credentials: no keys, the catalog hands them out.
	vended := map[string]any{"catalog_type": "rest", "uri": "https://cat.example/api", "warehouse": "lake", "namespace": "lake", "auth": "token", "storage": "vended"}
	if b, err := icebergFlink(t, vended, map[string]any{"token": "tok"}, map[string]any{}); err != nil ||
		b.Options.Get("catalog.properties.header.X-Iceberg-Access-Delegation") != "vended-credentials" || b.Options.Get("catalog.properties.token") != "${BZ_ICEBERG_TOKEN}" {
		t.Fatalf("%+v %v", b, err)
	}
	for name, c := range map[string]struct {
		cfg, opts map[string]any
	}{
		"append":     {rest, map[string]any{"write_mode": "append"}},
		"partitions": {rest, map[string]any{"partitions": map[string]any{"s.t": "id"}}},
		"glue keys":  {map[string]any{"catalog_type": "glue", "warehouse": "s3://lake/", "namespace": "lake", "region": "eu-west-1", "access_key_id": "AKIA1"}, map[string]any{}},
		"namespace":  {map[string]any{"catalog_type": "rest", "uri": "https://cat.example/api", "warehouse": "s3://lake/", "namespace": "a.b"}, map[string]any{}},
	} {
		if _, err := icebergFlink(t, c.cfg, map[string]any{"secret_access_key": "s3cr3t"}, c.opts); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// Debezium's row-identity and partitioning checks don't apply on Flink CDC.
func TestIcebergChecksOnFlinkCDC(t *testing.T) {
	conn := Resolved{Type: "iceberg", Config: map[string]any{"catalog_type": "rest", "uri": "http://127.0.0.1:1/", "warehouse": "w"}}
	facts := PipelineFacts{Tables: []TableInfo{{Schema: "s", Name: "nokey"}}, Options: map[string]any{"write_mode": "upsert", "partitioning": "auto"}}
	c := CompileContext{Spec: domain.DefaultSpec()}
	if got := (&icebergSink{}).CheckPipeline(t.Context(), conn, c, facts); len(got) != 2 {
		t.Fatalf("Debezium: %+v", got)
	}
	facts.FlinkCDC = true
	if got := (&icebergSink{}).CheckPipeline(t.Context(), conn, c, facts); len(got) != 0 {
		t.Fatalf("Flink CDC: %+v", got)
	}
}
