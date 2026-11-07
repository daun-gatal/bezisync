package compiler_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
)

// Oracle on Flink CDC (ADR 0006): the source block Flink CDC 3.7's Oracle reader accepts.

func oraConn(config map[string]any) connectors.Resolved {
	cfg := map[string]any{"host": "ora.internal", "port": 1521, "database": "ORCLCDB", "user": "C##CDC_READER"}
	for k, v := range config {
		cfg[k] = v
	}
	return connectors.Resolved{Type: "oracle", Config: cfg, Secrets: map[string]any{"password": "pw"}}
}

// oraIn is a deploy's input: the tables' columns as they are now.
var oraIn = func() compiler.FlinkInput {
	in := secrets
	in.Columns = map[string][]string{"SHOP.ORDERS": {"ID", "STATUS", "TOTAL"}, "SHOP.ORDER_ITEMS": {"ID", "ORDER_ID", "SKU"}}
	return in
}()

func oraSpec(t *testing.T, extra map[string]any) map[string]any {
	t.Helper()
	raw := map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"SHOP.ORDERS", "SHOP.ORDER_ITEMS"}}},
		"sink": map[string]any{"naming": "{schema}_{table}"}}
	for k, v := range extra {
		raw[k] = v
	}
	return raw
}

// E1, E6, E8: a multitenant database mines the CDB and lists and copies tables through the PDB's
// service; the heartbeat goes to Debezium; nothing the reader would refuse is set.
func TestFlinkOracleSource(t *testing.T) {
	flinkConfig(t)
	spec := flinkSpec(t, oraSpec(t, map[string]any{"policies": map[string]any{"heartbeat_seconds": 15}}))
	got, err := compiler.CompileFlink(flinkCtx(spec), oraConn(map[string]any{"pdb": "orclpdb1", "lob_enabled": true}), testSink, "acme/flink-cdc:3.7.0", oraIn)
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "oracle-cdb.yaml", got.Pipeline)
	for _, want := range []string{`type: "oracle"`, `database: "ORCLCDB"`, `jdbc.url: "jdbc:oracle:thin:@//ora.internal:1521/orclpdb1"`,
		`debezium.database.pdb.name: "ORCLPDB1"`, `debezium.lob.enabled: "true"`, `debezium.heartbeat.interval.ms: "15000"`,
		`tables: "SHOP.ORDERS,SHOP.ORDER_ITEMS"`, `source-table: "SHOP.ORDERS"`, `scan.startup.mode: "initial"`, `password: ${BZ_SRC_PASSWORD}`} {
		if !strings.Contains(got.Pipeline, want) {
			t.Errorf("lacks %s:\n%s", want, got.Pipeline)
		}
	}
	for _, refused := range []string{"heartbeat.interval:", "scan.newly-added-table.enabled", "schema-change.enabled", "jdbc.properties."} {
		if strings.Contains(got.Pipeline, refused) {
			t.Errorf("sets %s, which the Oracle reader refuses or ignores:\n%s", refused, got.Pipeline)
		}
	}
	if got.Env["BZ_SRC_PASSWORD"] != "pw" || strings.Contains(got.Pipeline, `"pw"`) {
		t.Fatal("the password belongs in the environment only")
	}
	// Its JDBC driver is inside the connector JAR: no driver to add.
	if !strings.Contains(got.Settings, `"oracle"`) || strings.Contains(got.Settings, `"drivers": [`) {
		t.Fatalf("connectors: %s", got.Settings)
	}

	// The PDB's own service name, when it differs from its name.
	got, err = compiler.CompileFlink(flinkCtx(spec), oraConn(map[string]any{"pdb": "ORCLPDB1", "service_name": "sales.example.com"}), testSink, "acme/flink-cdc:3.7.0", oraIn)
	if err != nil || !strings.Contains(got.Pipeline, `jdbc.url: "jdbc:oracle:thin:@//ora.internal:1521/sales.example.com"`) {
		t.Fatalf("%v\n%v", err, got)
	}

	// E2: a database without PDBs is read through its own service; no pdb.name, no heartbeat at 0.
	plain := flinkSpec(t, oraSpec(t, map[string]any{"policies": map[string]any{"heartbeat_seconds": 0}}))
	got, err = compiler.CompileFlink(flinkCtx(plain), oraConn(map[string]any{"database": "ORCL"}), testSink, "acme/flink-cdc:3.7.0", oraIn)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Pipeline, `jdbc.url: "jdbc:oracle:thin:@//ora.internal:1521/ORCL"`) || strings.Contains(got.Pipeline, "pdb.name") ||
		strings.Contains(got.Pipeline, "heartbeat") || !strings.Contains(got.Pipeline, `debezium.lob.enabled: "false"`) {
		t.Fatal(got.Pipeline)
	}

	// E6: the snapshot modes the reader has.
	for mode, want := range map[string]string{"when_needed": "initial", "no_data": "latest-offset", "initial_only": "snapshot"} {
		s := flinkSpec(t, oraSpec(t, map[string]any{"snapshot": map[string]any{"mode": mode}}))
		got, err := compiler.CompileFlink(flinkCtx(s), oraConn(nil), testSink, "acme/flink-cdc:3.7.0", oraIn)
		if err != nil || !strings.Contains(got.Pipeline, `scan.startup.mode: "`+want+`"`) {
			t.Errorf("%s: %v", mode, err)
		}
	}
	s := flinkSpec(t, oraSpec(t, map[string]any{"snapshot": map[string]any{"mode": "always"}}))
	if _, err := compiler.CompileFlink(flinkCtx(s), oraConn(nil), testSink, "acme/flink-cdc:3.7.0", oraIn); code(err) != "engine_settings" {
		t.Errorf("always: %v", err)
	}
	// E7: Start over from a time isn't something the Oracle reader can do.
	in := oraIn
	in.Startup = map[string]any{"mode": "timestamp", "timestamp_ms": int64(1_700_000_000_000)}
	if _, err := compiler.CompileFlink(flinkCtx(spec), oraConn(nil), testSink, "acme/flink-cdc:3.7.0", in); code(err) != "engine_unsupported" {
		t.Errorf("timestamp: %v", err)
	}
}

// E3: through an SSH tunnel both the host and the JDBC URL are the tunnel's.
func TestFlinkOracleTunnel(t *testing.T) {
	flinkConfig(t)
	spec := flinkSpec(t, oraSpec(t, nil))
	conn := oraConn(map[string]any{"pdb": "ORCLPDB1", "ssh_host": "bastion", "ssh_user": "jump"})
	conn.Secrets["ssh_private_key"] = "KEY"
	got, err := compiler.CompileFlink(flinkCtx(spec), conn, testSink, "acme/flink-cdc:3.7.0", oraIn)
	if err != nil {
		t.Fatal(err)
	}
	port := connectors.TunnelPort
	for _, want := range []string{`hostname: "bz-tunnel"`, `jdbc:oracle:thin:@//bz-tunnel:` + strconv.Itoa(port) + `/ORCLPDB1`} {
		if !strings.Contains(got.Pipeline, want) {
			t.Errorf("lacks %s:\n%s", want, got.Pipeline)
		}
	}
	if got.Tunnel["host"] != "ora.internal" || got.Env["BZ_SSH_KEY"] != "KEY" {
		t.Fatalf("tunnel: %v", got.Tunnel)
	}
}

// E4, E5: names the reader can't match, and services that can't stand in the URL, are refused.
func TestFlinkOracleNames(t *testing.T) {
	flinkConfig(t)
	for _, table := range []string{"shop.orders", "SHOP.Orders", "SHOP.ORDERS$"} {
		spec := flinkSpec(t, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{table}}}})
		_, err := compiler.CompileFlink(flinkCtx(spec), oraConn(nil), testSink, "acme/flink-cdc:3.7.0", oraIn)
		if code(err) != "flink_table_name" || !strings.Contains(err.Error(), "Debezium") {
			t.Errorf("%s: %v", table, err)
		}
	}
	spec := flinkSpec(t, oraSpec(t, nil))
	for _, cfg := range []map[string]any{
		{"pdb": "ORCLPDB1", "service_name": "x)(ADDRESS=(HOST=evil)"},
		{"database": "ORCL/../X"},
		{"pdb": "PDB 1"},
		{"host": "ora.internal/x?a=b"},
	} {
		if _, err := compiler.CompileFlink(flinkCtx(spec), oraConn(cfg), testSink, "acme/flink-cdc:3.7.0", oraIn); code(err) != "flink_oracle_service" {
			t.Errorf("%v: %v", cfg, err)
		}
	}
	// An IPv6 address is written in brackets.
	got, err := compiler.CompileFlink(flinkCtx(spec), oraConn(map[string]any{"host": "fd00::5"}), testSink, "acme/flink-cdc:3.7.0", oraIn)
	if err != nil || !strings.Contains(got.Pipeline, "@//[fd00::5]:1521/ORCLCDB") {
		t.Fatalf("%v\n%v", err, got)
	}
}

// E10: raw source options go through Debezium's rules; jdbc.properties.* do nothing on Oracle.
func TestFlinkOracleRawOptions(t *testing.T) {
	flinkConfig(t)
	spec := flinkSpec(t, oraSpec(t, map[string]any{"advanced": map[string]any{"source.debezium.log.mining.batch.size.default": "50000"}}))
	got, err := compiler.CompileFlink(flinkCtx(spec), oraConn(nil), testSink, "acme/flink-cdc:3.7.0", oraIn)
	if err != nil || !strings.Contains(got.Pipeline, `debezium.log.mining.batch.size.default: "50000"`) {
		t.Fatalf("%v\n%v", err, got)
	}
	spec = flinkSpec(t, oraSpec(t, map[string]any{"advanced": map[string]any{"source.jdbc.properties.oracle.net.CONNECT_TIMEOUT": "5000"}}))
	if _, err := compiler.CompileFlink(flinkCtx(spec), oraConn(nil), testSink, "acme/flink-cdc:3.7.0", oraIn); code(err) != "managed_property" {
		t.Fatalf("jdbc.properties: %v", err)
	}
	// MySQL still passes them on.
	my := flinkSpec(t, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}}},
		"advanced": map[string]any{"source.jdbc.properties.connectTimeout": "5000"}})
	myIn := secrets
	myIn.Columns = map[string][]string{"shop.orders": {"id"}}
	if _, err := compiler.CompileFlink(flinkCtx(my), mysqlConn, testSink, "acme/flink-cdc:3.7.0", myIn); err != nil {
		t.Fatal(err)
	}
}

// E10: Debezium 1.9 options that load classes, move the schema history or open sessions to other
// hosts are refused on every Flink CDC source; harmless ones still pass.
func TestClassifyFlinkAdvancedDebezium19(t *testing.T) {
	for _, key := range []string{"source.debezium.database.history", "source.debezium.database.history.kafka.bootstrap.servers",
		"source.debezium.rac.nodes", "source.debezium.database.connection.adapter"} {
		if v := compiler.ClassifyFlinkAdvanced(key, "x", nil); v.Class != compiler.AdvancedForbidden {
			t.Errorf("%s: %s %s", key, v.Class, v.Message)
		}
	}
	for _, key := range []string{"source.debezium.log.mining.batch.size.default", "source.debezium.log.mining.strategy"} {
		if v := compiler.ClassifyFlinkAdvanced(key, "1000", nil); v.Refused() {
			t.Errorf("%s: %s", key, v.Message)
		}
	}
	if connectors.FlinkSourceIgnores("oracle", "jdbc.properties.x") == "" || connectors.FlinkSourceIgnores("mysql", "jdbc.properties.x") != "" ||
		connectors.FlinkSourceIgnores("oracle", "debezium.x") != "" {
		t.Fatal("FlinkSourceIgnores")
	}
}
