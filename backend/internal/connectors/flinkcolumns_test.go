package connectors

import (
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

// ADR 0012: a Paimon table's columns come from its newest schema file, in order, with the key,
// the partition keys and NOT NULL; a table without schema files doesn't exist yet.
func TestPaimonColumns(t *testing.T) {
	store := fakeObjects{objects: map[string]string{
		"lake/cdc.db/orders/schema/schema-0": `{"fields":[{"id":0,"name":"id","type":"INT NOT NULL"}],"primaryKeys":["id"]}`,
		"lake/cdc.db/orders/schema/schema-2": `{"fields":[{"id":0,"name":"id","type":"INT NOT NULL"},{"id":1,"name":"day","type":"DATE"},` +
			`{"id":2,"name":"total","type":"DECIMAL(10, 2)"},{"id":3,"name":"tags","type":{"type":"ARRAY","element":"STRING"}}],` +
			`"partitionKeys":["day"],"primaryKeys":["id","day"]}`,
	}}
	cols, exists, err := paimonColumns(t.Context(), store, "lake", "cdc.orders")
	if err != nil || !exists || len(cols) != 4 {
		t.Fatal(cols, exists, err)
	}
	if c := cols[0]; c.Name != "id" || c.Type != "INT" || c.Nullable || !c.Key {
		t.Fatalf("%+v", c)
	}
	if c := cols[1]; c.Type != "DATE" || !c.Partition || !c.Key || !c.Nullable {
		t.Fatalf("%+v", c)
	}
	if c := cols[2]; c.Type != "DECIMAL(10, 2)" {
		t.Fatalf("%+v", c)
	}
	if c := cols[3]; c.Type != "" || !strings.Contains(c.Raw, "array") {
		t.Fatalf("a nested type is never mapped: %+v", c)
	}
	if _, exists, err := paimonColumns(t.Context(), store, "lake", "cdc.missing"); exists || err != nil {
		t.Fatal(exists, err)
	}
	if _, _, err := paimonColumns(t.Context(), fakeObjects{fail: true}, "lake", "cdc.orders"); err == nil {
		t.Fatal("an unreadable warehouse is an error, never 'missing'")
	}
}

// Paimon tables can't be changed by BeziSync: the refusal carries the statements to run.
func TestPaimonAddColumnsSaysWhatToRun(t *testing.T) {
	_, err := (&paimonSink{}).AddFlinkColumns(t.Context(), Resolved{}, "cdc.orders", []domain.FlinkColumn{{Name: "test_field", Type: "VARCHAR"}})
	if !IsNotSupported(err) || !strings.Contains(err.Error(), "ALTER TABLE cdc.orders ADD `test_field` STRING;") {
		t.Fatal(err)
	}
}

// The types BeziSync adds: only those it names exactly.
func TestFlinkColumnTypes(t *testing.T) {
	for in, want := range map[string]string{"VARCHAR": "string", "BIGINT": "long", "DOUBLE": "double", "BOOLEAN": "boolean", "DATE": "date"} {
		typ, err := icebergTypeOf(in)
		if err != nil || typ.String() != want {
			t.Errorf("%s: %v %v", in, typ, err)
		}
	}
	for in, want := range map[string]string{"DECIMAL(10, 2)": "decimal(10, 2)", "TIMESTAMP(6)": "timestamp", "TIMESTAMP_LTZ(6)": "timestamptz"} {
		if typ, err := icebergTypeOf(in); err != nil || typ.String() != want {
			t.Errorf("%s: %v %v", in, typ, err)
		}
	}
	if _, err := icebergTypeOf("TIME"); err == nil {
		t.Fatal("a type BeziSync doesn't name is refused")
	}
	if typ, _ := doris.olapColumnType("TIMESTAMP_LTZ(6)"); typ != "DATETIME" {
		t.Fatal(typ)
	}
	if typ, _ := starrocks.olapColumnType("VARCHAR"); typ != "STRING" {
		t.Fatal(typ)
	}
	if typ, _ := oceanbase.olapColumnType("VARCHAR"); typ != "LONGTEXT" {
		t.Fatal(typ)
	}
	if _, err := doris.olapColumnType("BYTES"); err == nil {
		t.Fatal("refused")
	}
	// Every Flink CDC destination either plans its tables or has none to plan.
	for _, typ := range []string{"iceberg", "paimon", "starrocks", "doris", "oceanbase"} {
		snk, err := GetSink(typ)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := snk.(FlinkColumns); !ok {
			t.Errorf("%s: no FlinkColumns", typ)
		}
	}
	for _, typ := range []string{"kafka", "elasticsearch"} {
		snk, _ := GetSink(typ)
		if _, ok := snk.(NoFlinkTable); !ok {
			t.Errorf("%s: not marked as having no table", typ)
		}
	}
	if snk, _ := GetSink("iceberg"); !snk.(FlinkColumns).FlinkLayoutTarget().Positional {
		t.Fatal("Iceberg writes by position")
	}
	if snk, _ := GetSink("starrocks"); snk.(FlinkColumns).FlinkLayoutTarget().Positional {
		t.Fatal("StarRocks writes by name")
	}
}

// ADR 0012 E8/L17: StarRocks reserves __op (its own upsert/delete marker), so keeping deleted rows
// writes the change type into __bz_op there; every other destination keeps __op.
func TestStarRocksSoftDeleteColumn(t *testing.T) {
	conn := Resolved{Type: "starrocks", Config: map[string]any{"host": "sr.internal", "query_port": 9030, "http_port": 8030, "database": "cdc", "user": "bz", "replicas": 1},
		Secrets: map[string]any{"password": "pw"}}
	b, err := (&starrocksSink{}).FlinkSink(conn, CompileContext{ShortID: "0123456789ab"})
	if err != nil || b.SoftDeleteField != "__bz_op" {
		t.Fatal(b, err)
	}
	if domain.FlinkSoftDeleteField("iceberg") != "__op" || domain.FlinkSoftDeleteField("starrocks") != "__bz_op" {
		t.Fatal("soft-delete columns")
	}
}
