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
	if _, err := icebergTypeOf("DECIMAL(10, 2)"); err == nil {
		t.Fatal("a decimal BeziSync didn't name exactly is refused")
	}
	if typ, _ := starrocks.olapColumnType("VARCHAR"); typ != "STRING" {
		t.Fatal(typ)
	}
	if typ, _ := oceanbase.olapColumnType("VARCHAR"); typ != "LONGTEXT" {
		t.Fatal(typ)
	}
	if _, err := doris.olapColumnType("TIMESTAMP_LTZ(6)"); err == nil {
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
