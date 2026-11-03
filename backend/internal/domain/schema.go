package domain

import (
	"fmt"
	"sort"
)

// Schema is table → column → {"type": string, "nullable": bool}.
type Schema map[string]map[string]map[string]any

// Change is one schema drift between two observations.
type Change struct {
	Table    string
	Kind     string // column_added | column_dropped | type_changed | nullability_changed | table_missing
	Column   *string
	Before   *string
	After    *string
	Breaking bool
}

func pyStr(p *string) string {
	if p == nil {
		return "None"
	}
	return *p
}

// Describe renders the change in plain words.
func (c Change) Describe() string {
	what := c.Table
	if c.Column != nil && *c.Column != "" {
		what = c.Table + "." + *c.Column
	}
	switch c.Kind {
	case "column_added":
		return fmt.Sprintf("%s added (%s)", what, pyStr(c.After))
	case "column_dropped":
		return fmt.Sprintf("%s dropped (was %s)", what, pyStr(c.Before))
	case "type_changed":
		return fmt.Sprintf("%s changed type %s → %s", what, pyStr(c.Before), pyStr(c.After))
	case "nullability_changed":
		return fmt.Sprintf("%s is now %s", what, pyStr(c.After))
	case "table_missing":
		return what + " no longer exists"
	}
	panic("unknown schema change kind " + c.Kind)
}

func typeOf(col map[string]any) *string {
	switch v := col["type"].(type) {
	case string:
		return &v
	case nil:
		return nil
	default:
		s := fmt.Sprint(v)
		return &s
	}
}

func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case float64:
		return x != 0
	case int:
		return x != 0
	}
	return true
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func strp(s string) *string { return &s }

// DiffSchema compares two observations. Dropped columns, type changes and missing tables
// break readers; added columns and nullability changes are compatible for JSON consumers.
func DiffSchema(old, new Schema) []Change {
	var changes []Change
	for _, table := range sortedKeys(old) {
		after, ok := new[table]
		if !ok {
			changes = append(changes, Change{Table: table, Kind: "table_missing", Breaking: true})
			continue
		}
		before := old[table]
		for _, col := range sortedKeys(before) {
			if _, ok := after[col]; !ok {
				changes = append(changes, Change{table, "column_dropped", strp(col), typeOf(before[col]), nil, true})
			}
		}
		for _, col := range sortedKeys(after) {
			if _, ok := before[col]; !ok {
				changes = append(changes, Change{table, "column_added", strp(col), nil, typeOf(after[col]), false})
			}
		}
		for _, col := range sortedKeys(before) {
			a, ok := after[col]
			if !ok {
				continue
			}
			b := before[col]
			bt, at := typeOf(b), typeOf(a)
			if pyStr(bt) != pyStr(at) || (bt == nil) != (at == nil) {
				changes = append(changes, Change{table, "type_changed", strp(col), bt, at, true})
			} else if truthy(b["nullable"]) != truthy(a["nullable"]) {
				state := "NOT NULL"
				if truthy(a["nullable"]) {
					state = "nullable"
				}
				changes = append(changes, Change{table, "nullability_changed", strp(col), nil, strp(state), false})
			}
		}
	}
	return changes
}
