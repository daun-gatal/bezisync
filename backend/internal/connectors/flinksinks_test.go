package connectors

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/checks"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

// The destinations Flink CDC 3.7.0 writes that BeziSync added (ADR 0009). Their options were run
// on the real engine before they were written down here.

func TestKafkaFlinkSink(t *testing.T) {
	c := CompileContext{ShortID: "0123456789ab", Spec: domain.DefaultSpec()}
	c.Spec.Sink.Format.Type = "json"
	s := &kafkaSink{}
	plain := Resolved{Type: "kafka", Config: map[string]any{"bootstrap_servers": "k1:9092,k2:9092", "security_protocol": "PLAINTEXT"}, Secrets: map[string]any{}}
	b, err := s.FlinkSink(plain, c)
	if err != nil || b.Options.Get("properties.bootstrap.servers") != "k1:9092,k2:9092" || b.Options.Get("partition.strategy") != "hash-by-key" ||
		b.Options.Get("value.format") != "debezium-json" || b.Connector != "kafka" || len(b.Env) != 0 {
		t.Fatalf("%+v %v", b, err)
	}
	sasl := Resolved{Type: "kafka", Config: map[string]any{"bootstrap_servers": "k1:9093", "security_protocol": "SASL_SSL", "sasl_mechanism": "SCRAM-SHA-512", "username": `cdc"x\`},
		Secrets: map[string]any{"password": `p"w\d`, "ssl_ca_pem": "-----BEGIN CERTIFICATE-----"}}
	b, err = s.FlinkSink(sasl, c)
	if err != nil || b.Options.Get("properties.sasl.jaas.config") != "${BZ_SINK_JAAS}" || b.Options.Get("properties.ssl.truststore.certificates") != "${BZ_SINK_CA}" {
		t.Fatalf("%+v %v", b, err)
	}
	// JAAS reads \ and " as escapes: both are escaped, so the password can't end the value.
	if want := `username="cdc\"x\\" password="p\"w\\d";`; !strings.HasSuffix(b.Env["BZ_SINK_JAAS"], want) {
		t.Fatal(b.Env["BZ_SINK_JAAS"])
	}
	for _, o := range b.Options {
		if strings.Contains(o[1], "p\"w") {
			t.Fatal("a secret in an option")
		}
	}
	avro := c
	avro.Spec.Sink.Format.Type = "avro"
	if _, err := s.FlinkSink(plain, avro); err == nil || !strings.Contains(err.Error(), "JSON") {
		t.Fatal(err)
	}
	withSchema := c
	withSchema.Spec.Sink.Format.IncludeSchema = true
	if _, err := s.FlinkSink(plain, withSchema); err == nil || !strings.Contains(err.Error(), "JSON") {
		t.Fatal("S-2: JSON with the schema", err)
	}
	if d := s.FlinkDestination(plain, c, "cdc.shop.orders"); d != "cdc.shop.orders" {
		t.Fatal(d)
	}
}

func TestElasticsearchFlinkSink(t *testing.T) {
	c := CompileContext{Spec: domain.DefaultSpec()}
	s := &elasticsearchSink{}
	conn := Resolved{Type: "elasticsearch", Config: map[string]any{"hosts": "http://es-a:9200,https://es-b:9243", "version": "7", "username": "cdc"}, Secrets: map[string]any{"password": "es-pw"}}
	b, err := s.FlinkSink(conn, c)
	if err != nil || b.Options.Get("hosts") != "http://es-a:9200,https://es-b:9243" || b.Options.Get("version") != "7" || b.Options.Get("username") != "cdc" ||
		b.Options.Get("password") != "${BZ_SINK_PASSWORD}" || b.Env["BZ_SINK_PASSWORD"] != "es-pw" {
		t.Fatalf("%+v %v", b, err)
	}
	open := Resolved{Type: "elasticsearch", Config: map[string]any{"hosts": "http://es:9200"}, Secrets: map[string]any{}}
	if b, err := s.FlinkSink(open, c); err != nil || b.Options.Get("version") != "8" || b.Options.Get("username") != "" || len(b.Env) != 0 {
		t.Fatalf("%+v %v", b, err)
	}
	if _, err := s.FlinkSink(Resolved{Type: "elasticsearch", Config: map[string]any{"hosts": "es:9200"}}, c); err == nil {
		t.Fatal("a host without a scheme")
	}
	if _, err := s.FlinkSink(Resolved{Type: "elasticsearch", Config: map[string]any{"hosts": "http://es:9200", "username": "cdc"}, Secrets: map[string]any{}}, c); err == nil {
		t.Fatal("a user without a password")
	}
	if d := s.FlinkDestination(open, c, "Shop_Orders"); d != "shop_orders" {
		t.Fatal(d)
	}
	// The naming rule checks names with dots flattened: the index is named the same way.
	if d := s.FlinkDestination(open, c, "cdc.Shop.orders"); d != "cdc_shop_orders" {
		t.Fatal(d)
	}
	if _, _, err := s.CompileSink(open, c); err == nil {
		t.Fatal("Debezium can't write Elasticsearch")
	}
}

func TestPaimonFlinkSink(t *testing.T) {
	c := CompileContext{Spec: domain.DefaultSpec()}
	s := &paimonSink{}
	conn := Resolved{Type: "paimon", Config: map[string]any{"warehouse": "s3://lake/paimon/", "s3_endpoint": "http://s3:9000", "path_style": true, "access_key_id": "AK"},
		Secrets: map[string]any{"secret_access_key": "SK"}}
	b, err := s.FlinkSink(conn, c)
	if err != nil || b.Options.Get("catalog.properties.metastore") != "filesystem" || b.Options.Get("catalog.properties.warehouse") != "s3://lake/paimon" ||
		b.Options.Get("catalog.properties.s3.path.style.access") != "true" || b.Options.Get("catalog.properties.s3.secret-key") != "${BZ_S3_SECRET_KEY}" ||
		b.Env["BZ_S3_ACCESS_KEY"] != "AK" || b.Env["BZ_S3_SECRET_KEY"] != "SK" || !strings.Contains(strings.Join(b.Extra, ","), "paimon-s3") {
		t.Fatalf("%+v %v", b, err)
	}
	for _, bad := range []map[string]any{{"warehouse": "/data/paimon"}, {"warehouse": "s3://lake", "database": "a-b"}, {"warehouse": "s3://lake", "access_key_id": "AK"}} {
		if _, err := s.FlinkSink(Resolved{Type: "paimon", Config: bad, Secrets: map[string]any{}}, c); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
	// No key: the runtime's own credentials (no placeholders, nothing in the environment).
	if b, err := s.FlinkSink(Resolved{Type: "paimon", Config: map[string]any{"warehouse": "s3://lake"}, Secrets: map[string]any{}}, c); err != nil || b.Options.Get("catalog.properties.s3.access-key") != "" || len(b.Env) != 0 {
		t.Fatalf("%+v %v", b, err)
	}
	if _, _, err := s.CompileSink(conn, c); err == nil {
		t.Fatal("S-12: Debezium can't write Paimon")
	}
	if d := s.FlinkDestination(conn, c, "Shop_Orders"); d != "cdc.shop_orders" {
		t.Fatal(d)
	}
	res, _ := s.CleanupSink(t.Context(), conn, c, map[string]string{"shop.orders": "cdc.shop_orders"})
	if len(res) != 1 || res[0].Outcome != "retained" {
		t.Fatal(res)
	}
}

func TestOceanBaseFlinkSink(t *testing.T) {
	c := CompileContext{Spec: domain.DefaultSpec()}
	s := &oceanbaseSink{}
	conn := Resolved{Type: "oceanbase", Config: map[string]any{"host": "ob", "user": "cdc@test#prod", "database": "lake"}, Secrets: map[string]any{"password": "ob-pw"}}
	b, err := s.FlinkSink(conn, c)
	// The URL is built from checked parts: UTF-8 and a UTC session (the tenant default shifts timestamps).
	if err != nil || b.Options.Get("url") != "jdbc:oceanbase://ob:2881/lake?characterEncoding=UTF-8&serverTimezone=UTC" ||
		b.Options.Get("driver-class-name") != "com.oceanbase.jdbc.Driver" || b.Options.Get("password") != "${BZ_SINK_PASSWORD}" || b.Extra[0] != "oceanbase-client" {
		t.Fatalf("%+v %v", b, err)
	}
	if _, err := s.FlinkSink(Resolved{Type: "oceanbase", Config: map[string]any{"host": "ob", "user": "u", "database": "a;b"}, Secrets: map[string]any{}}, c); err == nil {
		t.Fatal("a database name that isn't one")
	}
	if g := oceanbase.grant(olapConfig{User: "cdc@test#prod", Database: "lake"}); !strings.Contains(g, "TO 'cdc'") {
		t.Fatal(g)
	}
	if g := oceanbase.grant(olapConfig{User: "o'brien", Database: "lake"}); !strings.Contains(g, "TO 'o''brien'") {
		t.Fatal(g)
	}
	// B1: the host goes into JDBC and load URLs, so it can't carry a path or driver options.
	for _, sink := range []olapSink{oceanbase, starrocks, doris} {
		for _, host := range []string{"h/x?allowLoadLocalInfile=true&autoDeserialize=true&a=", "h?x=1", "h#x", "h x", "http://h"} {
			if _, err := sink.cfg(Resolved{Type: sink.typ, Config: map[string]any{"host": host, "user": "u", "database": "lake"}, Secrets: map[string]any{}}); err == nil {
				t.Errorf("%s: host %q accepted", sink.typ, host)
			}
		}
		for _, host := range []string{"ob.internal", "10.0.0.5", "fd00::5"} {
			if _, err := sink.cfg(Resolved{Type: sink.typ, Config: map[string]any{"host": host, "user": "u", "database": "lake"}, Secrets: map[string]any{}}); err != nil {
				t.Errorf("%s: host %q refused: %v", sink.typ, host, err)
			}
		}
	}
	if b, err := s.FlinkSink(Resolved{Type: "oceanbase", Config: map[string]any{"host": "fd00::5", "user": "u", "database": "lake"}, Secrets: map[string]any{}}, c); err != nil ||
		!strings.HasPrefix(b.Options.Get("url"), "jdbc:oceanbase://[fd00::5]:2881/lake?") {
		t.Fatalf("%+v %v", b, err)
	}
	if _, _, err := s.CompileSink(conn, c); err == nil {
		t.Fatal("S-12: Debezium can't write OceanBase")
	}
	if oceanbase.drop("`a`.`b`") != "DROP TABLE `a`.`b`" || starrocks.drop("`a`.`b`") != "DROP TABLE `a`.`b` FORCE" {
		t.Fatal("drop syntax")
	}
}

type fakeObjects struct {
	objects map[string]string
	fail    bool
}

func (f fakeObjects) list(_ context.Context, prefix string) ([]string, error) {
	if f.fail {
		return nil, errors.New("AccessDenied")
	}
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (f fakeObjects) get(_ context.Context, key string) ([]byte, error) {
	return []byte(f.objects[key]), nil
}

// An existing Paimon table without a primary key blocks (it can't take updates and deletes); the
// current schema is the highest-numbered one; a table the pipeline creates passes.
func TestPaimonKeylessChecks(t *testing.T) {
	store := fakeObjects{objects: map[string]string{
		"lake/cdc.db/orders/schema/schema-0":  `{"primaryKeys":[]}`,
		"lake/cdc.db/orders/schema/schema-1":  `{"primaryKeys":["id"]}`,
		"lake/cdc.db/events/schema/schema-0":  `{"primaryKeys":["id"]}`,
		"lake/cdc.db/events/schema/schema-10": `{"primaryKeys":[]}`,
		"lake/cdc.db/eventsx/schema/schema-0": `{"primaryKeys":[]}`,
	}}
	dest := map[string]string{"shop.orders": "cdc.orders", "shop.events": "cdc.events", "shop.new": "cdc.new"}
	got := paimonKeylessChecks(t.Context(), store, "lake", dest)
	if len(got) != 1 || got[0].Status != checks.Fail || !got[0].Blocking || !strings.Contains(got[0].Message, "cdc.events") || strings.Contains(got[0].Message, "orders") {
		t.Fatalf("%+v", got)
	}
	got = paimonKeylessChecks(t.Context(), store, "lake", map[string]string{"shop.orders": "cdc.orders", "shop.new": "cdc.new"})
	if len(got) != 1 || got[0].Status != checks.Pass {
		t.Fatalf("%+v", got)
	}
	got = paimonKeylessChecks(t.Context(), fakeObjects{fail: true}, "", dest)
	if len(got) != 1 || got[0].Status != checks.Warn {
		t.Fatalf("%+v", got)
	}
}
