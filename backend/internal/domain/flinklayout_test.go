package domain

import (
	"reflect"
	"strings"
	"testing"
)

func col(name string) DestColumn {
	return DestColumn{Name: name, Type: "VARCHAR", Raw: "string", Nullable: true}
}

func names(cols []LayoutColumn) []string {
	var out []string
	for _, c := range cols {
		n := c.Name
		if c.Retired {
			n += "~"
		}
		out = append(out, n)
	}
	return out
}

// ADR 0012 L1: a table the job creates needs nothing, keeps the compiled order.
func TestPlanFlinkLayoutTableMissing(t *testing.T) {
	plan, problems := PlanFlinkLayout([]FlinkColumn{{Name: "id"}, {Name: "__op", Type: "VARCHAR"}}, nil, false, LayoutTarget{Positional: true})
	if len(problems) > 0 || plan.Pinned || len(plan.Add) > 0 || !reflect.DeepEqual(names(plan.Columns), []string{"id", "__op"}) {
		t.Fatal(plan, problems)
	}
}

// L3 (E7): after live ADD b, RENAME b→bb, DROP a, a start writes the table's order: the dropped
// and renamed-away columns empty, and nothing appended that the table already has.
func TestPlanFlinkLayoutPositionalFollowsTheTable(t *testing.T) {
	produced := []FlinkColumn{{Name: "id", Key: true}, {Name: "bb"}, {Name: "n"}, {Name: "f1", Type: "VARCHAR"}, {Name: "__op", Type: "VARCHAR"}}
	dest := []DestColumn{{Name: "id", Type: "INT", Raw: "int", Key: true}, col("a"), col("f1"), col("__op"), col("b"),
		{Name: "n", Type: "BIGINT", Raw: "long", Nullable: true}, col("bb")}
	plan, problems := PlanFlinkLayout(produced, dest, true, LayoutTarget{Positional: true})
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	if got := names(plan.Columns); !reflect.DeepEqual(got, []string{"id", "a~", "f1", "__op", "b~", "n", "bb"}) {
		t.Fatal(got)
	}
	if !plan.Pinned || len(plan.Add) > 0 {
		t.Fatal(plan)
	}
	if plan.Columns[1].Type != "VARCHAR" {
		t.Fatal("a retired column takes the table's type:", plan.Columns[1])
	}
}

// L4/L21: the playground case. A step added test_field before __op; the table has no test_field.
// It is appended after __op and added to the table; __op keeps its place.
func TestPlanFlinkLayoutAppendsWhatTheTableLacks(t *testing.T) {
	produced := []FlinkColumn{{Name: "id"}, {Name: "order_id"}, {Name: "test_field", Type: "VARCHAR"}, {Name: "__op", Type: "VARCHAR"}}
	dest := []DestColumn{{Name: "id", Type: "INT", Raw: "int"}, {Name: "order_id", Type: "INT", Raw: "int", Nullable: true}, col("__op")}
	plan, problems := PlanFlinkLayout(produced, dest, true, LayoutTarget{Positional: true})
	if len(problems) > 0 || !plan.Pinned {
		t.Fatal(plan, problems)
	}
	if got := names(plan.Columns); !reflect.DeepEqual(got, []string{"id", "order_id", "__op", "test_field"}) {
		t.Fatal(got)
	}
	if len(plan.Add) != 1 || plan.Add[0].Name != "test_field" || plan.Add[0].Type != "VARCHAR" {
		t.Fatal(plan.Add)
	}
}

// The order already matches: nothing is pinned, so the projection keeps * and live changes flow.
func TestPlanFlinkLayoutSameOrderIsNotPinned(t *testing.T) {
	produced := []FlinkColumn{{Name: "id"}, {Name: "a"}, {Name: "__op", Type: "VARCHAR"}}
	plan, problems := PlanFlinkLayout(produced, []DestColumn{col("id"), col("a"), col("__op")}, true, LayoutTarget{Positional: true})
	if len(problems) > 0 || plan.Pinned || len(plan.Add) > 0 {
		t.Fatal(plan, problems)
	}
}

// L8: a column the pipeline no longer writes can't be emptied when it is a key, a partition
// source or NOT NULL in the table.
func TestPlanFlinkLayoutRefusesRetiringWhatCantBeEmpty(t *testing.T) {
	produced := []FlinkColumn{{Name: "id"}}
	for _, c := range []struct {
		d    DestColumn
		want string
	}{
		{DestColumn{Name: "k", Type: "INT", Raw: "int", Key: true, Nullable: true}, "key or partitioning"},
		{DestColumn{Name: "p", Type: "DATE", Raw: "date", Partition: true, Nullable: true}, "key or partitioning"},
		{DestColumn{Name: "nn", Type: "INT", Raw: "int"}, "can't be empty"},
	} {
		_, problems := PlanFlinkLayout(produced, []DestColumn{col("id"), c.d}, true, LayoutTarget{Positional: true})
		if len(problems) != 1 || !strings.Contains(problems[0], c.want) || !strings.Contains(problems[0], c.d.Name) {
			t.Fatal(c.d.Name, problems)
		}
	}
}

// L10: a table type BeziSync can't map can't be written empty.
func TestPlanFlinkLayoutRefusesAnUnmappedType(t *testing.T) {
	_, problems := PlanFlinkLayout([]FlinkColumn{{Name: "id"}}, []DestColumn{col("id"), {Name: "u", Raw: "uuid", Nullable: true}}, true, LayoutTarget{Positional: true})
	if len(problems) != 1 || !strings.Contains(problems[0], "uuid") {
		t.Fatal(problems)
	}
}

// A new column with no known type can't be added; a new key column can't be added at all; a type
// that doesn't fit the table's is refused.
func TestPlanFlinkLayoutRefusesWhatItCantAdd(t *testing.T) {
	_, problems := PlanFlinkLayout([]FlinkColumn{{Name: "id"}, {Name: "src_new"}, {Name: "k2", Type: "VARCHAR", Key: true}},
		[]DestColumn{col("id")}, true, LayoutTarget{Positional: true})
	if len(problems) != 2 || !strings.Contains(problems[0], "src_new") || !strings.Contains(problems[1], "k2") {
		t.Fatal(problems)
	}
	_, problems = PlanFlinkLayout([]FlinkColumn{{Name: "id"}, {Name: "x", Type: "BIGINT", Cast: true}},
		[]DestColumn{col("id"), {Name: "x", Type: "INT", Raw: "int", Nullable: true}}, true, LayoutTarget{Positional: true})
	if len(problems) != 1 || !strings.Contains(problems[0], "x is int") {
		t.Fatal(problems)
	}
}

// By name (StarRocks, Doris, OceanBase): the compiled order stays, what the table lacks is added,
// a column the pipeline doesn't write is left alone unless the table can't leave it empty. L22:
// names match regardless of case where the destination does.
func TestPlanFlinkLayoutByName(t *testing.T) {
	produced := []FlinkColumn{{Name: "id"}, {Name: "Status"}, {Name: "f2", Type: "VARCHAR"}}
	dest := []DestColumn{{Name: "id", Type: "INT", Nullable: false, Key: true}, col("status"), col("old")}
	plan, problems := PlanFlinkLayout(produced, dest, true, LayoutTarget{CaseInsensitive: true})
	if len(problems) > 0 || plan.Pinned || !reflect.DeepEqual(names(plan.Columns), []string{"id", "Status", "f2"}) {
		t.Fatal(plan, problems)
	}
	if len(plan.Add) != 1 || plan.Add[0].Name != "f2" {
		t.Fatal(plan.Add)
	}
	_, problems = PlanFlinkLayout(produced, append(dest, DestColumn{Name: "must", Type: "INT"}), true, LayoutTarget{CaseInsensitive: true})
	if len(problems) != 1 || !strings.Contains(problems[0], "must") {
		t.Fatal(problems)
	}
}

func TestFlinkTypeFits(t *testing.T) {
	for _, c := range []struct {
		have, want string
		ok         bool
	}{
		{"INT", "INT", true}, {"INT", "BIGINT", true}, {"BIGINT", "INT", false}, {"FLOAT", "DOUBLE", true},
		{"DECIMAL(10, 2)", "DECIMAL(12, 2)", true}, {"DECIMAL(10, 2)", "DECIMAL(12, 3)", false}, {"DECIMAL(12,2)", "DECIMAL(10, 2)", false},
		{"VARCHAR", "VARCHAR(10)", true}, {"VARCHAR", "BIGINT", false},
	} {
		if FlinkTypeFits(c.have, c.want) != c.ok {
			t.Error(c)
		}
	}
}

func TestDestinationTypeMaps(t *testing.T) {
	for in, want := range map[string]string{"long": "BIGINT", "decimal(10, 2)": "DECIMAL(10, 2)", "timestamptz": "TIMESTAMP_LTZ(6)",
		"string": "VARCHAR", "uuid": "", "list<int>": "", "time": ""} {
		if got := IcebergFlinkType(in); got != want {
			t.Errorf("iceberg %s: %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string]struct {
		t        string
		nullable bool
	}{
		"INT NOT NULL": {"INT", false}, "STRING": {"VARCHAR", true}, "DECIMAL(10, 2)": {"DECIMAL(10, 2)", true},
		"TIMESTAMP(6) WITH LOCAL TIME ZONE": {"TIMESTAMP_LTZ(6)", true}, "TIMESTAMP(3)": {"TIMESTAMP(3)", true},
		"TIMESTAMP_LTZ(3)": {"TIMESTAMP_LTZ(3)", true}, "ARRAY<INT>": {"", true}, "VARCHAR(20) NOT NULL": {"VARCHAR", false},
	} {
		if got, n := PaimonFlinkType(in); got != want.t || n != want.nullable {
			t.Errorf("paimon %s: %q %v, want %v", in, got, n, want)
		}
	}
	if typ, cast := FlinkTypeOfKind("text"); typ != "VARCHAR" || !cast {
		t.Fatal(typ, cast)
	}
	if typ, cast := FlinkTypeOfKind("date"); typ != "DATE" || cast {
		t.Fatal(typ, cast)
	}
	if typ, _ := FlinkTypeOfKind("decimal"); typ != "" {
		t.Fatal(typ)
	}
}
