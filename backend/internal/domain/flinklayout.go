package domain

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// ADR 0012: a Flink CDC job that starts without a saved position writes, into every destination
// table that already exists, exactly that table's columns (in the table's order on a positional
// destination, none the table lacks on a by-name one), or it doesn't start. This file is the pure
// part: the planner and the Flink type names.

// FlinkColumn is a column a Flink CDC job writes into a destination table.
type FlinkColumn struct {
	// Name is the column's name in the destination.
	Name string `json:"name"`
	// Type is its Flink type when BeziSync knows it exactly (an added field, the soft-delete column,
	// a computed column BeziSync casts); "" when Flink CDC types it from the source.
	Type string `json:"type,omitempty"`
	// Cast: BeziSync writes the column as CAST(… AS Type) (a computed column added to an existing table).
	Cast bool `json:"cast,omitempty"`
	// Key: part of the destination table's primary key.
	Key bool `json:"key,omitempty"`
}

// DestColumn is a column a destination table has, as BeziSync read it.
type DestColumn struct {
	Name string
	// Type is its Flink type ("" when BeziSync can't map the table's type: Raw).
	Type, Raw string
	Nullable  bool
	// Key and Partition: part of the table's primary key / a partition source.
	Key, Partition bool
}

// LayoutColumn is one column of a stored layout, in the order the job writes it.
type LayoutColumn struct {
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
	// Cast: written as CAST(expression AS Type).
	Cast bool `json:"cast,omitempty"`
	// Retired: the table has it, the pipeline no longer writes it: written as CAST(NULL AS Type).
	Retired bool `json:"retired,omitempty"`
}

// LayoutPlan is what a start without a saved position writes into one destination table.
type LayoutPlan struct {
	// Columns in the order the job writes them.
	Columns []LayoutColumn
	// Add are the columns BeziSync adds to the table (at the end) before the start.
	Add []FlinkColumn
	// Pinned: the order differs from what the spec compiles to, so the projection is written column
	// by column from Columns (new source columns then wait for the next deploy).
	Pinned bool
}

// LayoutTarget is how a destination maps a job's rows onto its table.
type LayoutTarget struct {
	// Positional: values go into the table's columns by position (Iceberg, Paimon).
	Positional bool
	// CaseInsensitive: column names match regardless of case (StarRocks, Doris, OceanBase).
	CaseInsensitive bool
}

// PlanFlinkLayout plans one destination table. produced is what the spec writes, in the order it
// compiles to; dest the table's columns in its order (exists false: the job creates the table).
// Problems are why the start can't be made safely; each names the column.
func PlanFlinkLayout(produced []FlinkColumn, dest []DestColumn, exists bool, target LayoutTarget) (LayoutPlan, []string) {
	var plan LayoutPlan
	var problems []string
	if !exists {
		for _, p := range produced {
			plan.Columns = append(plan.Columns, LayoutColumn{Name: p.Name, Type: p.Type, Cast: p.Cast})
		}
		return plan, nil
	}
	same := func(a, b string) bool {
		if target.CaseInsensitive {
			return strings.EqualFold(a, b)
		}
		return a == b
	}
	findProduced := func(name string) (FlinkColumn, bool) {
		i := slices.IndexFunc(produced, func(p FlinkColumn) bool { return same(p.Name, name) })
		if i < 0 {
			return FlinkColumn{}, false
		}
		return produced[i], true
	}
	inDest := func(name string) bool {
		return slices.ContainsFunc(dest, func(d DestColumn) bool { return same(d.Name, name) })
	}
	if target.Positional {
		for _, d := range dest {
			p, ok := findProduced(d.Name)
			if ok {
				if p.Type != "" && d.Type != "" && !FlinkTypeFits(p.Type, d.Type) {
					problems = append(problems, fmt.Sprintf("%s is %s in the table, but the pipeline writes %s.", d.Name, d.Raw, p.Type))
				}
				plan.Columns = append(plan.Columns, LayoutColumn{Name: d.Name, Type: p.Type, Cast: p.Cast})
				continue
			}
			switch {
			case d.Key || d.Partition:
				problems = append(problems, fmt.Sprintf("%s is in the table's key or partitioning, and the pipeline no longer writes it.", d.Name))
			case !d.Nullable:
				problems = append(problems, fmt.Sprintf("%s can't be empty in the table, and the pipeline no longer writes it.", d.Name))
			case d.Type == "":
				problems = append(problems, fmt.Sprintf("%s is %s in the table, a type BeziSync can't write empty values of.", d.Name, d.Raw))
			}
			plan.Columns = append(plan.Columns, LayoutColumn{Name: d.Name, Type: d.Type, Retired: true})
		}
	} else {
		for _, p := range produced {
			plan.Columns = append(plan.Columns, LayoutColumn{Name: p.Name, Type: p.Type, Cast: p.Cast})
		}
		for _, d := range dest {
			if _, ok := findProduced(d.Name); !ok && !d.Nullable && !d.Key {
				problems = append(problems, fmt.Sprintf("%s can't be empty in the table, and the pipeline doesn't write it.", d.Name))
			}
		}
	}
	for _, p := range produced {
		if inDest(p.Name) {
			continue
		}
		if p.Key {
			problems = append(problems, fmt.Sprintf("%s is in the key, and the table doesn't have it: a table's key can't change.", p.Name))
		}
		if p.Type == "" {
			problems = append(problems, fmt.Sprintf("the table has no column %s, and BeziSync doesn't know the type it would get.", p.Name))
		}
		plan.Add = append(plan.Add, p)
		if target.Positional {
			plan.Columns = append(plan.Columns, LayoutColumn{Name: p.Name, Type: p.Type, Cast: p.Cast})
		}
	}
	if target.Positional {
		plan.Pinned = len(plan.Columns) != len(produced)
		for i := 0; !plan.Pinned && i < len(produced); i++ {
			plan.Pinned = !same(plan.Columns[i].Name, produced[i].Name) || plan.Columns[i].Retired || plan.Columns[i].Cast != produced[i].Cast
		}
	}
	return plan, problems
}

// FlinkTypeFits reports a value of Flink type have written into a column of Flink type want: the
// same type, or a widening Iceberg and Paimon allow (INT into BIGINT, FLOAT into DOUBLE, a DECIMAL
// into one with more digits and the same scale, any VARCHAR into VARCHAR).
func FlinkTypeFits(have, want string) bool {
	have, want = strings.ToUpper(strings.TrimSpace(have)), strings.ToUpper(strings.TrimSpace(want))
	if have == want {
		return true
	}
	switch {
	case have == "INT" && want == "BIGINT", have == "SMALLINT" && (want == "INT" || want == "BIGINT"),
		have == "TINYINT" && (want == "SMALLINT" || want == "INT" || want == "BIGINT"), have == "FLOAT" && want == "DOUBLE":
		return true
	case strings.HasPrefix(have, "VARCHAR") && strings.HasPrefix(want, "VARCHAR"):
		return true
	}
	hp, hs, ok1 := decimalOf(have)
	wp, ws, ok2 := decimalOf(want)
	return ok1 && ok2 && hs == ws && wp >= hp
}

var decimalType = regexp.MustCompile(`^DECIMAL\((\d+),\s*(\d+)\)$`)

func decimalOf(t string) (int, int, bool) {
	m := decimalType.FindStringSubmatch(t)
	if m == nil {
		return 0, 0, false
	}
	p, _ := strconv.Atoi(m[1])
	s, _ := strconv.Atoi(m[2])
	return p, s, true
}

// FlinkTypeOfKind is the Flink type BeziSync casts a computed column of this kind to when it adds
// the column to an existing table, and whether it casts (a date is already DATE: CAST … AS DATE
// doesn't compile on 3.7.0, ADR 0008). "" when BeziSync can't name it exactly (a decimal's
// precision, a timestamp's) or the kind is unknown.
func FlinkTypeOfKind(kind string) (string, bool) {
	switch kind {
	case "text":
		return "VARCHAR", true
	case "int":
		return "BIGINT", true
	case "float":
		return "DOUBLE", true
	case "bool":
		return "BOOLEAN", true
	case "date":
		return "DATE", false
	}
	return "", false
}

var (
	icebergDecimal = regexp.MustCompile(`^decimal\((\d+),\s*(\d+)\)$`)
	paimonType     = regexp.MustCompile(`^([A-Z_ ]+?)(?:\((\d+)(?:,\s*(\d+))?\))?( WITH LOCAL TIME ZONE)?$`)
)

// IcebergFlinkType maps an Iceberg type (as iceberg-go prints it) to the Flink type Flink CDC
// writes into it; "" when BeziSync doesn't map it (nested, uuid, fixed, time).
func IcebergFlinkType(t string) string {
	t = strings.TrimSpace(t)
	if m := icebergDecimal.FindStringSubmatch(t); m != nil {
		return "DECIMAL(" + m[1] + ", " + m[2] + ")"
	}
	return map[string]string{"boolean": "BOOLEAN", "int": "INT", "long": "BIGINT", "float": "FLOAT", "double": "DOUBLE",
		"date": "DATE", "timestamp": "TIMESTAMP(6)", "timestamptz": "TIMESTAMP_LTZ(6)", "string": "VARCHAR", "binary": "BYTES"}[t]
}

// PaimonFlinkType maps a Paimon type (as its schema file writes it, "INT NOT NULL",
// "DECIMAL(10, 2)", "TIMESTAMP(6) WITH LOCAL TIME ZONE") to a Flink type, and whether it may be
// empty; "" when BeziSync doesn't map it.
func PaimonFlinkType(t string) (string, bool) {
	t = strings.ToUpper(strings.TrimSpace(t))
	nullable := !strings.HasSuffix(t, " NOT NULL")
	t = strings.TrimSuffix(t, " NOT NULL")
	m := paimonType.FindStringSubmatch(t)
	if m == nil {
		return "", nullable
	}
	base, p, s, ltz := strings.TrimSpace(m[1]), m[2], m[3], m[4] != ""
	switch {
	case base == "TIMESTAMP" && ltz:
		return "TIMESTAMP_LTZ(" + cmpOr(p, "6") + ")", nullable
	case base == "TIMESTAMP_LTZ" || base == "TIMESTAMP WITH LOCAL TIME ZONE":
		return "TIMESTAMP_LTZ(" + cmpOr(p, "6") + ")", nullable
	case base == "TIMESTAMP":
		return "TIMESTAMP(" + cmpOr(p, "6") + ")", nullable
	case base == "DECIMAL" && p != "":
		return "DECIMAL(" + p + ", " + cmpOr(s, "0") + ")", nullable
	case base == "STRING" || base == "VARCHAR" || base == "CHAR":
		return "VARCHAR", nullable
	case base == "BYTES" || base == "VARBINARY" || base == "BINARY":
		return "BYTES", nullable
	case slices.Contains([]string{"BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DATE"}, base):
		return base, nullable
	}
	return "", nullable
}

func cmpOr(v, d string) string {
	if v == "" {
		return d
	}
	return v
}
