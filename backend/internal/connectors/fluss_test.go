package connectors

import (
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

func TestFlussFlinkSink(t *testing.T) {
	c := CompileContext{ShortID: "0123456789ab", Spec: domain.DefaultSpec()}
	open := Resolved{Type: "fluss", Config: map[string]any{"bootstrap_servers": "fluss-a:9123,fluss-b:9123", "database": "lake"}, Secrets: map[string]any{}}
	s := &flussSink{}
	b, err := s.FlinkSink(open, c)
	if err != nil || b.Options.Get("bootstrap.servers") != "fluss-a:9123,fluss-b:9123" || b.Options.Get("properties.client.security.protocol") != "" || len(b.Env) != 0 {
		t.Fatalf("%+v %v", b, err)
	}
	sasl := Resolved{Type: "fluss", Config: map[string]any{"bootstrap_servers": "fluss-a:9123", "auth": "sasl_plain", "username": "cdc"},
		Secrets: map[string]any{"password": "fl-s3cret"}}
	b, err = s.FlinkSink(sasl, c)
	if err != nil || b.Options.Get("properties.client.security.sasl.password") != "${BZ_SINK_PASSWORD}" || b.Env["BZ_SINK_PASSWORD"] != "fl-s3cret" ||
		b.Options.Get("properties.client.security.sasl.username") != "cdc" {
		t.Fatalf("%+v %v", b, err)
	}
	sasl.Secrets = map[string]any{}
	if _, err := s.FlinkSink(sasl, c); err == nil || !strings.Contains(err.Error(), "user name and password") {
		t.Fatal(err)
	}
	if d := s.FlinkDestination(open, c, "Shop.Orders"); d != "lake.shop_orders" {
		t.Fatal(d)
	}
	res, _ := s.CleanupSink(t.Context(), open, c, map[string]string{"shop.orders": "lake.shop_orders"})
	if len(res) != 1 || res[0].Outcome != "retained" {
		t.Fatal(res)
	}
	if _, _, err := s.CompileSink(open, c); err == nil {
		t.Fatal("Debezium can't write Fluss")
	}
}

func TestOLAPFlinkSinks(t *testing.T) {
	c := CompileContext{ShortID: "0123456789ab", Spec: domain.DefaultSpec()}
	conn := Resolved{Config: map[string]any{"host": "fe", "database": "lake", "user": "u", "replicas": 1}, Secrets: map[string]any{"password": "p"}}
	conn.Type = "starrocks"
	sr, err := (&starrocksSink{}).FlinkSink(conn, c)
	if err != nil || sr.Options.Get("load-url") != "fe:8030" || sr.Options.Get("jdbc-url") != "jdbc:mysql://fe:9030" || sr.Options.Get("sink.buffer-flush.interval-ms") != "5000" ||
		sr.Options.Get("sink.label-prefix") != "bz_0123456789ab" || len(sr.Drivers) != 0 {
		t.Fatalf("%+v %v", sr, err)
	}
	conn.Type = "doris"
	conn.Config["be_nodes"] = "be1:8040,be2:8040"
	do, err := (&dorisSink{}).FlinkSink(conn, c)
	if err != nil || do.Options.Get("fenodes") != "fe:8030" || do.Options.Get("benodes") != "be1:8040,be2:8040" || do.Options.Get("sink.buffer-flush.interval") != "5s" || len(do.Drivers) != 0 {
		t.Fatalf("%+v %v", do, err)
	}
	conn.Config["database"] = "bad-name"
	if _, err := (&dorisSink{}).FlinkSink(conn, c); err == nil {
		t.Fatal("a database name Flink CDC can't route to")
	}
}
