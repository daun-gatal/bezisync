package compiler_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// flinkTestSink is a destination for compiler tests: Flink CDC's own "values" sink.
type flinkTestSink struct{ connectors.BaseSink }

func (*flinkTestSink) Kind() string { return "sink" }
func (*flinkTestSink) Type() string { return "flinktest" }
func (*flinkTestSink) Check(context.Context, connectors.Resolved, []string) (checks.Report, error) {
	return checks.Report{}, nil
}
func (*flinkTestSink) CompileSink(connectors.Resolved, connectors.CompileContext) (map[string]string, map[string]string, error) {
	return nil, nil, nil
}
func (*flinkTestSink) FlinkSink(conn connectors.Resolved, _ connectors.CompileContext) (*connectors.FlinkBlock, error) {
	b := &connectors.FlinkBlock{Options: connectors.FlinkOptions{{"type", "values"}, {"token", "${BZ_SINK_TOKEN}"}},
		Env: map[string]string{"BZ_SINK_TOKEN": conn.Secrets["token"].(string)}, Connector: "values"}
	if conn.Config["soft"] == true { // a destination that keeps deleted rows, marked
		b.Projection, b.Converter, b.Extra = []string{"__data_event_type__ AS `__op`"}, "SOFT_DELETE", []string{"values-extra"}
	}
	b.NoDefaults = conn.Config["no_defaults"] == true // like StarRocks and Doris
	return b, nil
}
func (*flinkTestSink) FlinkDestination(_ connectors.Resolved, _ connectors.CompileContext, routed string) string {
	return "lake." + routed
}

func init() { connectors.Register(&flinkTestSink{}) }

func flinkConfig(t *testing.T) {
	t.Helper()
	s, err := config.Load(func(k string) (string, bool) {
		switch k {
		case "BEZISYNC_STATE_DATABASE_URL":
			return "postgresql://state_user:p%40ss%3Aw@state-db.internal:5433/bzstate?sslmode=require", true
		case "BEZISYNC_AGENT_IMAGE":
			return "ghcr.io/example/bezisync:9.9.9", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	config.Set(s)
	t.Cleanup(config.Reset)
}

func flinkSpec(t *testing.T, raw map[string]any) domain.PipelineSpec {
	t.Helper()
	v := &val.V{}
	spec, ok := domain.ParseSpec(v, nil, raw)
	if !ok {
		t.Fatalf("%v", v.Errors)
	}
	return spec
}

var (
	mysqlConn = connectors.Resolved{Type: "mysql", Config: map[string]any{"host": "db.internal", "port": 3306, "user": "cdc", "ssl_mode": "required"},
		Secrets: map[string]any{"password": "pw"}}
	pgConn = connectors.Resolved{Type: "postgresql", Config: map[string]any{"host": "pg.internal", "port": 5432, "database": "shop", "user": "cdc",
		"ssl_mode": "disable", "ssh_host": "bastion", "ssh_user": "jump"}, Secrets: map[string]any{"password": "pw", "ssh_private_key": "KEY"}}
	testSink = connectors.Resolved{Type: "flinktest", Config: map[string]any{}, Secrets: map[string]any{"token": "t0k"}}
	secrets  = compiler.FlinkInput{RuntimeKind: "docker", StateKey: "c3RhdGU=", Keystore: "a2V5c3RvcmU=", KeystorePassword: "ks-pw"}
)

func flinkCtx(spec domain.PipelineSpec) connectors.CompileContext {
	return connectors.CompileContext{PipelineID: "6f1c2d3e-4b5a-4c7d-8e9f-0a1b2c3d4e5f", ShortID: "6f1c2d3e4b5a", Name: "orders lake", Spec: spec}
}

// golden compares got with testdata/flink/name (UPDATE_GOLDEN=1 rewrites it).
func golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", "flink", name)
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path) //nolint:gosec // test data
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != got {
		t.Errorf("%s differs (UPDATE_GOLDEN=1 to accept):\n%s", name, got)
	}
}

// MySQL with the defaults: new columns held back (written column by column), one reader per slot.
func TestCompileFlinkMySQL(t *testing.T) {
	flinkConfig(t)
	spec := flinkSpec(t, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders", "shop.customers"}}},
		"policies": map[string]any{"resources": "medium", "heartbeat_seconds": 10}, "sink": map[string]any{"naming": "{schema}_{table}"}})
	in := secrets
	in.Columns = map[string][]string{"shop.orders": {"id", "customer_id", "total"}, "shop.customers": {"id", "email"}}
	got, err := compiler.CompileFlink(flinkCtx(spec), mysqlConn, testSink, "acme/flink-cdc:3.7.0", in)
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "mysql.pipeline.yaml", got.Pipeline)
	golden(t, "mysql.flink-conf.yaml", got.FlinkConf)
	golden(t, "mysql.settings.json", got.Settings)
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(got.Pipeline), &doc); err != nil {
		t.Fatal(err)
	}
	if got.Size.TaskManagers != 2 || got.Size.Parallelism() != 4 || got.Destinations["shop.orders"] != "lake.shop_orders" {
		t.Fatalf("%+v %v", got.Size, got.Destinations)
	}
	for k, want := range map[string]string{"BZ_SRC_PASSWORD": "pw", "BZ_SINK_TOKEN": "t0k", "BZ_STATE_KEY": "c3RhdGU=",
		"BZ_STATE_DSN": "postgresql://state-db.internal:5433/bzstate?sslmode=require", "BZ_STATE_USER": "state_user", "BZ_STATE_PASSWORD": "p@ss:w"} {
		if got.Env[k] != want {
			t.Errorf("env %s = %q, want %q", k, got.Env[k], want)
		}
	}
	for _, text := range []string{got.Pipeline, got.FlinkConf, got.Settings} {
		for _, secret := range []string{"pw\"", "t0k", "ks-pw", "p@ss"} {
			if strings.Contains(text, secret) {
				t.Fatalf("a secret (%s) is in a config file", secret)
			}
		}
	}
	if got.Files["keystore.p12.b64"] != "a2V5c3RvcmU=" || !strings.Contains(got.Files["flink-secrets.conf"], "security.ssl.internal.keystore-password: ks-pw") {
		t.Fatal(got.Files)
	}
	// The hash follows what the cluster runs, and only that.
	again, _ := compiler.CompileFlink(flinkCtx(spec), mysqlConn, testSink, "acme/flink-cdc:3.7.0", in)
	if again.ConfigHash() != got.ConfigHash() {
		t.Fatal("the hash is not stable")
	}
	in.RestoreAfter = 7
	moved, _ := compiler.CompileFlink(flinkCtx(spec), mysqlConn, testSink, "acme/flink-cdc:3.7.0", in)
	if moved.ConfigHash() == got.ConfigHash() || !strings.Contains(moved.Settings, `"restore_after": 7`) {
		t.Fatal("Start over must change what runs")
	}
	// On Kubernetes the TaskManagers reach the JobManager through its Service.
	in.RuntimeKind = "kubernetes"
	k8s, _ := compiler.CompileFlink(flinkCtx(spec), mysqlConn, testSink, "acme/flink-cdc:3.7.0", in)
	if !strings.Contains(k8s.FlinkConf, "jobmanager.rpc.address: bz-6f1c2d3e4b5a-jm\n") {
		t.Fatal(k8s.FlinkConf)
	}
}

// PostgreSQL behind a bastion, with columns left out and renamed, a field added, rows filtered,
// a chosen key, schema changes stopping the pipeline and chosen Flink settings.
func TestCompileFlinkPostgresTransforms(t *testing.T) {
	flinkConfig(t)
	spec := flinkSpec(t, map[string]any{
		"source": map[string]any{"tables": map[string]any{"include": []any{"public.orders", "public.events"}},
			"columns": map[string]any{"public.orders": map[string]any{"mask": []any{}, "exclude": []any{"card"}}}},
		"transforms": []any{
			map[string]any{"type": "filter", "id": "keepship", "tables": []any{"public.orders"}, "operations": []any{"insert", "snapshot", "update"},
				"match": "any", "rules": []any{
					map[string]any{"field": "status", "op": "ne", "value": "it's done"},
					map[string]any{"field": "region", "op": "not_in", "values": []any{"eu", "us"}},
					map[string]any{"field": "total", "op": "gte", "value": 10.5},
					map[string]any{"field": "paid", "op": "is_true"}}},
			map[string]any{"type": "unwrap"}, // the event shape: nothing on Flink CDC, which always writes rows
			map[string]any{"type": "rename", "id": "renamer", "tables": []any{"public.orders"}, "renames": []any{map[string]any{"from": "total", "to": "amount"}}},
			map[string]any{"type": "add_field", "id": "addsrc", "name": "source_system", "value": map[string]any{"kind": "pipeline"}},
		},
		"keys":      map[string]any{"public.events": []any{"event_id"}},
		"sink":      map[string]any{"naming": "{schema}_{table}"},
		"flink_cdc": map[string]any{"schema_changes": "stop", "new_columns": "send", "commit_seconds": 30, "flink_conf": map[string]any{"restart-strategy.type": "fixed-delay"}},
	})
	in := secrets
	in.Columns = map[string][]string{"public.orders": {"id", "status", "region", "total", "paid", "card"}}
	in.Startup = map[string]any{"mode": "latest-offset"}
	got, err := compiler.CompileFlink(flinkCtx(spec), pgConn, testSink, "acme/flink-cdc:3.7.0", in)
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "postgres.pipeline.yaml", got.Pipeline)
	golden(t, "postgres.flink-conf.yaml", got.FlinkConf)
	if got.Env["BZ_SSH_KEY"] != "KEY" || got.Tunnel["host"] != "pg.internal" || got.Tunnel["listen_port"] != connectors.TunnelPort {
		t.Fatal(got.Tunnel)
	}
	var doc struct {
		Transform []map[string]string `yaml:"transform"`
	}
	if err := yaml.Unmarshal([]byte(got.Pipeline), &doc); err != nil || len(doc.Transform) != 2 {
		t.Fatal(err, doc)
	}
	rules := map[string]map[string]string{}
	for _, r := range doc.Transform {
		rules[r["source-table"]] = r
	}
	orders, events := rules["public.orders"], rules["public.events"]
	if orders["projection"] != "`id`, `status`, `region`, `total` AS `amount`, `paid`, 'orders lake' AS `source_system`" ||
		!strings.Contains(orders["filter"], "(`status` IS NULL OR `status` <> 'it''s done')") ||
		events["projection"] != "*, 'orders lake' AS `source_system`" || events["primary-keys"] != "event_id" || events["filter"] != "" {
		t.Fatalf("%+v", doc.Transform)
	}
	// A PostgreSQL slot can't go back in time.
	in.Startup = map[string]any{"mode": "timestamp", "timestamp_ms": float64(1_700_000_000_000)}
	if _, err := compiler.CompileFlink(flinkCtx(spec), pgConn, testSink, "acme/flink-cdc:3.7.0", in); code(err) != "engine_unsupported" {
		t.Fatal(err)
	}
}

func code(err error) string {
	var p *problem.Problem
	if errors.As(err, &p) {
		return p.Code
	}
	if err != nil {
		return err.Error()
	}
	return ""
}

// What Flink CDC can't do is refused with the reason, before anything deploys.
func TestCompileFlinkRefuses(t *testing.T) {
	flinkConfig(t)
	base := func() map[string]any {
		return map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}}},
			"flink_cdc": map[string]any{"new_columns": "send"}}
	}
	with := func(key string, v any) map[string]any {
		m := base()
		m[key] = v
		return m
	}
	for _, c := range []struct {
		name, code string
		spec       map[string]any
		image      string
		columns    bool
	}{
		{"no image", "engine_unavailable", base(), "-", false},
		{"masked column", "mask_unsupported", with("source", map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}},
			"columns": map[string]any{"shop.orders": map[string]any{"mask": []any{"email"}, "exclude": []any{}}}}), "", false},
		{"script filter", "transform_unsupported", with("transforms", []any{map[string]any{"type": "filter", "condition": "value.op == 'c'"}}), "", false},
		{"outbox", "transform_unsupported", with("transforms", []any{map[string]any{"type": "outbox"}}), "", false},
		{"inserts without the first copy", "transform_unsupported", with("transforms", []any{map[string]any{"type": "filter", "id": "onlyins",
			"operations": []any{"insert"}}}), "", false},
		{"held columns unknown", "columns_unknown", with("flink_cdc", map[string]any{"new_columns": "hold"}), "", false},
		{"unknown excluded column", "column_unknown", with("source", map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}},
			"columns": map[string]any{"shop.orders": map[string]any{"mask": []any{}, "exclude": []any{"nope"}}}}), "", true},
		{"table name", "flink_table_name", with("source", map[string]any{"tables": map[string]any{"include": []any{"shop.order-lines"}}}), "", false},
		{"flink setting", "flink_conf", with("flink_cdc", map[string]any{"flink_conf": map[string]any{"jobmanager.rpc.address": "evil"}}), "", false},
		{"copy on every start", "engine_settings", with("snapshot", map[string]any{"mode": "always"}), "", false},
		{"advanced", "managed_property", with("advanced", map[string]any{"debezium.source.x": "1"}), "", false},
		{"collision", "destination_collision", with("source", map[string]any{"tables": map[string]any{"include": []any{"shop.orders", "crm.orders"}}}), "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			spec := c.spec
			if c.name == "collision" {
				spec["sink"] = map[string]any{"naming": "{table}"}
			}
			in := secrets
			if c.columns {
				in.Columns = map[string][]string{"shop.orders": {"id"}}
			}
			image := "acme/flink-cdc:3.7.0"
			if c.image == "-" {
				image = ""
			}
			_, err := compiler.CompileFlink(flinkCtx(flinkSpec(t, spec)), mysqlConn, testSink, image, in)
			if code(err) != c.code {
				t.Fatalf("got %v, want %s", err, c.code)
			}
		})
	}
}

// A destination's own columns and converter reach every table's transform, even one written as
// it is; its extra JARs are linked.
func TestCompileFlinkSinkTransform(t *testing.T) {
	flinkConfig(t)
	spec := flinkSpec(t, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}}},
		"sink": map[string]any{"naming": "{schema}_{table}"}, "flink_cdc": map[string]any{"new_columns": "send"}})
	soft := testSink
	soft.Config = map[string]any{"soft": true}
	got, err := compiler.CompileFlink(flinkCtx(spec), mysqlConn, soft, "acme/flink-cdc:3.7.0", secrets)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Pipeline, "projection: \"*, __data_event_type__ AS `__op`\"") || !strings.Contains(got.Pipeline, `converter-after-transform: "SOFT_DELETE"`) ||
		!strings.Contains(got.Settings, `"values-extra"`) {
		t.Fatal(got.Pipeline, got.Settings)
	}
}

// A destination that refuses source defaults gets every column with one as an expression (which
// carries none), renamed or not; the other columns stay plain. New columns must be held back.
func TestCompileFlinkDropsDefaultsForDestinationsThatRefuseThem(t *testing.T) {
	flinkConfig(t)
	spec := flinkSpec(t, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}}},
		"sink":       map[string]any{"naming": "{schema}_{table}"},
		"transforms": []any{map[string]any{"type": "unwrap"}, map[string]any{"type": "rename", "renames": []any{map[string]any{"from": "created_at", "to": "placed_at"}}}}})
	strict := testSink
	strict.Config = map[string]any{"no_defaults": true}
	in := secrets
	in.Columns = map[string][]string{"shop.orders": {"id", "status", "created_at", "total"}}
	in.Defaults = map[string][]string{"shop.orders": {"id", "created_at"}}
	got, err := compiler.CompileFlink(flinkCtx(spec), mysqlConn, strict, "acme/flink-cdc:3.7.0", in)
	if err != nil {
		t.Fatal(err)
	}
	want := "projection: \"CASE WHEN TRUE THEN `id` END AS `id`, `status`, CASE WHEN TRUE THEN `created_at` END AS `placed_at`, `total`\""
	if !strings.Contains(got.Pipeline, want) {
		t.Fatal(got.Pipeline)
	}
	// Other destinations keep the columns as they are.
	got, err = compiler.CompileFlink(flinkCtx(spec), mysqlConn, testSink, "acme/flink-cdc:3.7.0", in)
	if err != nil || !strings.Contains(got.Pipeline, "projection: \"`id`, `status`, `created_at` AS `placed_at`, `total`\"") {
		t.Fatal(err, got.Pipeline)
	}
	spec.FlinkCDC.NewColumns = "send"
	if _, err := compiler.CompileFlink(flinkCtx(spec), mysqlConn, strict, "acme/flink-cdc:3.7.0", in); err == nil || !strings.Contains(err.Error(), "hold new columns back") {
		t.Fatalf("sent new columns to a destination that refuses defaults: %v", err)
	}
}

// Start over from a time on MySQL: the stored time reads back whatever number type it decoded to.
func TestCompileFlinkStartFromTime(t *testing.T) {
	flinkConfig(t)
	spec := flinkSpec(t, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}}},
		"sink": map[string]any{"naming": "{schema}_{table}"}, "flink_cdc": map[string]any{"new_columns": "send"}})
	for _, ms := range []any{json.Number("1700000000000"), float64(1_700_000_000_000), int64(1_700_000_000_000), "1700000000000"} {
		in := secrets
		in.Startup = map[string]any{"mode": "timestamp", "timestamp_ms": ms}
		got, err := compiler.CompileFlink(flinkCtx(spec), mysqlConn, testSink, "acme/flink-cdc:3.7.0", in)
		if err != nil || !strings.Contains(got.Pipeline, `scan.startup.timestamp-millis: "1700000000000"`) || !strings.Contains(got.Pipeline, `scan.startup.mode: "timestamp"`) {
			t.Fatalf("%T: %v", ms, err)
		}
	}
}

// SQL Server reads the connection's database; routes and transforms name tables with it, and no
// option it lacks (heartbeat.interval) is set.
func TestCompileFlinkSQLServer(t *testing.T) {
	flinkConfig(t)
	spec := flinkSpec(t, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"dbo.orders"}}},
		"sink": map[string]any{"naming": "{schema}_{table}"}, "keys": map[string]any{"dbo.orders": []any{"order_id"}},
		"policies": map[string]any{"heartbeat_seconds": 10}, "flink_cdc": map[string]any{"new_columns": "send"}})
	ms := connectors.Resolved{Type: "sqlserver", Config: map[string]any{"host": "mssql", "port": 1433, "database": "shop", "user": "cdc", "encrypt": true},
		Secrets: map[string]any{"password": "pw"}}
	got, err := compiler.CompileFlink(flinkCtx(spec), ms, testSink, "acme/flink-cdc:3.7.0", secrets)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`tables: "shop.dbo.orders"`, `source-table: "shop.dbo.orders"`, `jdbc.properties.encrypt: "true"`, `primary-keys: "order_id"`} {
		if !strings.Contains(got.Pipeline, want) {
			t.Errorf("lacks %s:\n%s", want, got.Pipeline)
		}
	}
	if strings.Contains(got.Pipeline, "heartbeat") {
		t.Fatal(got.Pipeline)
	}
}

// A custom cluster compiles to the shape the services resolved (packed, or stored at deploy), and
// never without one.
func TestCompileFlinkCustomCluster(t *testing.T) {
	flinkConfig(t)
	spec := flinkSpec(t, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}}},
		"policies": map[string]any{"resources": "custom"},
		"flink_cdc": map[string]any{"cluster": map[string]any{"parallelism": 6, "slot": map[string]any{"cpu": "1", "memory": "2Gi"},
			"job_manager": map[string]any{"cpu": "0.5", "memory": "1536Mi"}}}})
	in := secrets
	in.Columns = map[string][]string{"shop.orders": {"id", "customer_id", "total"}}
	if _, err := compiler.CompileFlink(flinkCtx(spec), mysqlConn, testSink, "acme/flink-cdc:3.7.0", in); code(err) != "flink_cluster" {
		t.Fatal(err)
	}
	size := engine.Pack(6, 1000, 2048, "0.5", "1536Mi", engine.Node{CPUMilli: 4000, MemoryMiB: 8192}).Size
	in.Cluster = &size
	got, err := compiler.CompileFlink(flinkCtx(spec), mysqlConn, testSink, "acme/flink-cdc:3.7.0", in)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"taskmanager.numberOfTaskSlots: 3", "parallelism.default: 6", "taskmanager.memory.process.size: 6656m", "jobmanager.memory.process.size: 1536m"} {
		if !strings.Contains(got.FlinkConf, line) {
			t.Errorf("%s not in\n%s", line, got.FlinkConf)
		}
	}
	if got.Size.TaskManagers != 2 || !strings.Contains(got.Pipeline, `parallelism: "6"`) {
		t.Fatalf("%+v\n%s", got.Size, got.Pipeline)
	}
	// Another shape is another config (the controller replaces the cluster only on a redeploy,
	// which packs again).
	other := engine.Pack(6, 1000, 2048, "0.5", "1536Mi", engine.Node{CPUMilli: 16000, MemoryMiB: 65536}).Size
	in.Cluster = &other
	again, _ := compiler.CompileFlink(flinkCtx(spec), mysqlConn, testSink, "acme/flink-cdc:3.7.0", in)
	if again.ConfigHash() == got.ConfigHash() || again.Size.TaskManagers != 1 {
		t.Fatal("the shape must be part of what runs")
	}
}

// F1, F4–F8: raw options of a Flink CDC pipeline pass only through their connector's prefixes,
// go into the YAML through its encoder, and never replace what BeziSync sets.
func TestCompileFlinkAdvanced(t *testing.T) {
	flinkConfig(t)
	tricky := ` a: b # c 'd' "e"`
	spec := flinkSpec(t, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}}},
		"advanced": map[string]any{"source.jdbc.properties.connectTimeout": "30000", "sink.table.properties.write.parquet.compression-codec": tricky}})
	in := secrets
	in.Columns = map[string][]string{"shop.orders": {"id", "total"}}
	got, err := compiler.CompileFlink(flinkCtx(spec), mysqlConn, testSink, "acme/flink-cdc:3.7.0", in)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(got.Pipeline), &doc); err != nil {
		t.Fatal(err)
	}
	src, snk := doc["source"].(map[string]any), doc["sink"].(map[string]any)
	if src["jdbc.properties.connectTimeout"] != "30000" || snk["table.properties.write.parquet.compression-codec"] != tricky {
		t.Fatalf("%v\n%v", src, snk)
	}
	for key, want := range map[string]string{
		"source.jdbc.properties.allowLoadLocalInfile": "load files or code", // F4
		"source.jdbc.properties.sslMode":              "BeziSync sets this", // set from the connection
		"sink.table.properties.format-version":        "move or rewrite",    // F5
		"sink.table.properties.location":              "move or rewrite",
		"sink.catalog.properties.uri":                 "Catalog and Hadoop",
		"sink.hadoop.conf.fs.s3a.secret.key":          "Catalog and Hadoop",
		"source.debezium.slot.drop.on.stop":           "WAL",             // F6: Debezium's own rules
		"taskmanager.memory.process.size":             "Engine settings", // F8
		"source.scan.startup.mode":                    "typed settings",
		"pipeline.parallelism":                        "Pipeline options are",
	} {
		v := compiler.ClassifyFlinkAdvanced(key, "x", map[string]bool{"source.jdbc.properties.sslMode": true})
		if !v.Refused() || !strings.Contains(v.Message, want) {
			t.Errorf("%s: %+v", key, v)
		}
	}
	// What BeziSync sets can't be replaced at compile either.
	owned := flinkSpec(t, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}}},
		"advanced": map[string]any{"source.jdbc.properties.sslMode": "DISABLED"}})
	if _, err := compiler.CompileFlink(flinkCtx(owned), mysqlConn, testSink, "acme/flink-cdc:3.7.0", in); code(err) != "managed_property" {
		t.Fatal(err)
	}
}

// T3, T4: on Flink CDC a soft delete keeps deleted rows, marked with their change type; a source
// column of that name is refused.
func TestSoftDeleteFlink(t *testing.T) {
	flinkConfig(t)
	spec := flinkSpec(t, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}}},
		"transforms": []any{map[string]any{"type": "soft_delete", "id": "sd01"}}})
	in := secrets
	in.Columns = map[string][]string{"shop.orders": {"id", "total"}}
	got, err := compiler.CompileFlink(flinkCtx(spec), mysqlConn, testSink, "acme/flink-cdc:3.7.0", in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Pipeline, `converter-after-transform: "SOFT_DELETE"`) || !strings.Contains(got.Pipeline, "__data_event_type__ AS `__op`") {
		t.Fatal(got.Pipeline)
	}
	in.Columns = map[string][]string{"shop.orders": {"id", "__op"}}
	if _, err := compiler.CompileFlink(flinkCtx(spec), mysqlConn, testSink, "acme/flink-cdc:3.7.0", in); code(err) != "transform_field_conflict" {
		t.Fatal(err)
	}
}

// On Flink CDC a Truncate step is SUBSTR in the projection (any source).
func TestTruncateFlink(t *testing.T) {
	flinkConfig(t)
	spec := flinkSpec(t, map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}}},
		"transforms": []any{map[string]any{"type": "truncate", "columns": []any{map[string]any{"table": "shop.orders", "column": "notes", "length": 40}}}}})
	in := secrets
	in.Columns = map[string][]string{"shop.orders": {"id", "notes"}}
	got, err := compiler.CompileFlink(flinkCtx(spec), mysqlConn, testSink, "acme/flink-cdc:3.7.0", in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Pipeline, "SUBSTR(`notes`, 1, 40) AS `notes`") {
		t.Fatal(got.Pipeline)
	}
}
