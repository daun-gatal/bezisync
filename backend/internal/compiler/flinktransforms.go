package compiler

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/expr"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Flink CDC transforms: one rule per table with a projection (the columns written), a filter
// (the rows kept) and the destination primary key. Every name is a back-quoted identifier and
// every value a SQL literal, so no input can become an expression.

// flinkEventTypes are Flink CDC's __data_event_type__ values for each filter operation. A row
// copied by the first copy is an insert too: Flink CDC doesn't tell them apart.
var flinkEventTypes = map[string][]string{"insert": {"+I"}, "snapshot": {"+I"}, "update": {"-U", "+U"}, "delete": {"-D"}}

func flinkTransforms(c connectors.CompileContext, src connectors.FlinkSource, source connectors.Resolved, columns, defaults map[string][]string,
	newColumns string, sink *connectors.FlinkBlock) ([]yamlMap, error) {
	spec := c.Spec
	if sink.NoDefaults && newColumns != "hold" {
		return nil, problem.New(422, "engine_settings", "This destination refuses the column defaults Flink CDC copies from the source, "+
			"so BeziSync writes columns without them, and new columns wait for the next deploy: choose to hold new columns back.")
	}
	if masked := maskedColumns(spec); len(masked) > 0 {
		return nil, problem.New(422, "mask_unsupported",
			fmt.Sprintf("Flink CDC has no hash function to mask %s. Remove the column or run this pipeline on Debezium.", strings.Join(masked, ", ")))
	}
	for i, t := range spec.Transforms {
		switch {
		case t.Type == "unwrap":
			// The event shape: Flink CDC always writes rows.
		case t.Type == "soft_delete":
			// A delete becomes a row marked with its change type (T4).
			field := domain.SoftDeleteField["flink-cdc"]
			for _, table := range spec.Source.Tables.Include {
				if slices.Contains(columns[table], field) {
					return nil, problem.New(422, "transform_field_conflict", fmt.Sprintf("Step %d: %s already has a column %s, which the soft delete would overwrite (field name duplication).", i+1, table, field))
				}
			}
			if sink.Converter == "" {
				sink.Converter = "SOFT_DELETE"
				sink.Projection = append(sink.Projection, "__data_event_type__ AS `"+field+"`")
			}
		case t.Type == "rename" || t.Type == "add_field" || t.Type == "truncate" || t.Type == "route" || t.Type == "computed" || (t.Type == "filter" && t.Typed()):
		case t.Type == "filter":
			return nil, problem.New(422, "transform_unsupported", fmt.Sprintf("Step %d: a filter written as a script runs only on Debezium; build it from rules.", i+1))
		default:
			return nil, problem.New(422, "transform_unsupported", fmt.Sprintf("Step %d: %s runs only on Debezium.", i+1, transformTitle(t.Type)))
		}
	}
	var out []yamlMap
	for _, table := range spec.Source.Tables.Include {
		rules := spec.Source.Columns[table]
		renames := map[string]string{}
		cut := map[string]int{}
		var added, addedNames []string
		var filters []string
		for _, t := range spec.Transforms {
			if t.Tables != nil && !slices.Contains(t.Tables, table) {
				continue
			}
			switch t.Type {
			case "rename":
				for _, r := range t.Renames {
					renames[r.From] = r.To
				}
			case "truncate":
				for _, c := range t.Truncate {
					if c.Table == table {
						cut[c.Column] = c.Length
					}
				}
			case "computed":
				if t.Table != table {
					continue
				}
				if slices.Contains(columns[table], t.Name) && !slices.Contains(rules.Exclude, t.Name) {
					return nil, problem.New(422, "transform_field_conflict", fmt.Sprintf("%s already has a column %s: name the computed column differently.", table, t.Name))
				}
				if err := janinoColumns(t, table); err != nil {
					return nil, err
				}
				sql, err := domain.ComputedSQL(t)
				if err != nil {
					return nil, problem.New(422, "transform_expression", fmt.Sprintf("%s, step %s: %v", table, t.Name, err))
				}
				added = append(added, sql+" AS "+sqlIdent(t.Name))
				addedNames = append(addedNames, t.Name)
			case "add_field":
				addedNames = append(addedNames, t.Name)
				value := t.Value.Text
				if t.Value.Kind == "pipeline" {
					value = c.Name
				}
				added = append(added, sqlString(value)+" AS "+sqlIdent(t.Name))
			case "filter":
				if t.Type == "filter" && t.Typed() {
					if err := janinoColumns(t, table); err != nil {
						return nil, err
					}
					f, err := flinkFilter(t)
					if err != nil {
						return nil, err
					}
					filters = append(filters, f)
				}
			}
		}
		for i, n := range addedNames {
			if slices.Contains(addedNames[:i], n) {
				return nil, problem.New(422, "transform_field_conflict", fmt.Sprintf("%s would get two columns called %s.", table, n))
			}
		}
		// Column by column when anything must be left out or renamed; "*" otherwise, so new
		// columns reach the destination.
		explicit := newColumns == "hold" || len(rules.Exclude) > 0 || len(renames) > 0 || len(cut) > 0
		var projection []string
		if explicit {
			cols, ok := columns[table]
			if !ok || len(cols) == 0 {
				return nil, problem.New(422, "columns_unknown", fmt.Sprintf(
					"BeziSync could not read the columns of %s, which it needs to leave columns out, rename them or hold new ones back.", table))
			}
			for _, name := range rules.Exclude {
				if !slices.Contains(cols, name) {
					return nil, problem.New(422, "column_unknown", fmt.Sprintf("%s has no column %s to leave out.", table, name))
				}
			}
			for _, col := range cols {
				if slices.Contains(rules.Exclude, col) {
					continue
				}
				name := col
				if to, ok := renames[col]; ok {
					name = to
				}
				switch {
				case cut[col] > 0:
					projection = append(projection, fmt.Sprintf("SUBSTR(%s, 1, %d) AS %s", sqlIdent(col), cut[col], sqlIdent(name)))
				case sink.NoDefaults && slices.Contains(defaults[table], col):
					// An expression carries no default (a plain or renamed column keeps the source's).
					projection = append(projection, "CASE WHEN TRUE THEN "+sqlIdent(col)+" END AS "+sqlIdent(name))
				case name != col:
					projection = append(projection, sqlIdent(col)+" AS "+sqlIdent(name))
				default:
					projection = append(projection, sqlIdent(col))
				}
			}
		} else {
			projection = []string{"*"}
		}
		projection = append(append(projection, added...), sink.Projection...)
		keys := spec.Keys[table]
		if !explicit && len(added) == 0 && len(filters) == 0 && len(keys) == 0 && len(sink.Projection) == 0 && sink.Converter == "" {
			continue // written as it is
		}
		rule := yamlMap{{"source-table", src.FlinkTableID(source, table)}, {"projection", strings.Join(projection, ", ")}}
		if len(filters) > 0 {
			rule = append(rule, yamlKV{"filter", strings.Join(filters, " AND ")})
		}
		if len(keys) > 0 {
			names := make([]string, len(keys))
			for i, k := range keys {
				if to, ok := renames[k]; ok {
					k = to
				}
				if slices.Contains(rules.Exclude, k) {
					return nil, problem.New(422, "column_unknown", fmt.Sprintf("%s: the key column %s is left out.", table, k))
				}
				names[i] = k
			}
			rule = append(rule, yamlKV{"primary-keys", strings.Join(names, ",")})
		}
		if sink.Converter != "" {
			rule = append(rule, yamlKV{"converter-after-transform", sink.Converter})
		}
		out = append(out, rule)
	}
	return out, nil
}

func maskedColumns(spec domain.PipelineSpec) []string {
	var out []string
	for _, t := range domain.ColumnKeys(spec.Source.Columns) {
		for _, c := range spec.Source.Columns[t].Mask {
			out = append(out, t+"."+c)
		}
	}
	return out
}

func transformTitle(typ string) string {
	return map[string]string{"outbox": "the outbox router", "flatten": "flattening nested fields", "custom": "a custom transform"}[typ]
}

// flinkFilter is a typed filter as a Flink CDC filter expression. NULL never equals anything, as
// in Debezium: "not equal" and "not in" keep a row whose value is null.
func flinkFilter(t domain.Transform) (string, error) {
	if t.Expression != "" {
		n, err := domain.ExpressionOf(t)
		if err == nil {
			var s string
			if s, err = expr.FlinkChecked(n); err == nil {
				return s, nil
			}
		}
		return "", problem.New(422, "transform_expression", "A filter expression can't be compiled: "+err.Error())
	}
	var parts []string
	if len(t.Operations) > 0 {
		types := map[string]bool{}
		for _, o := range t.Operations {
			for _, e := range flinkEventTypes[o] {
				types[e] = true
			}
		}
		if slices.Contains(t.Operations, "insert") != slices.Contains(t.Operations, "snapshot") {
			return "", problem.New(422, "transform_unsupported",
				"Flink CDC can't tell rows from the first copy from new inserts: keep both or neither.")
		}
		var list []string
		for _, e := range []string{"+I", "-U", "+U", "-D"} {
			if types[e] {
				list = append(list, sqlString(e))
			}
		}
		parts = append(parts, "__data_event_type__ IN ("+strings.Join(list, ", ")+")")
	}
	if len(t.Rules) > 0 {
		rules := make([]string, len(t.Rules))
		for i, r := range t.Rules {
			e, err := flinkRule(r)
			if err != nil {
				return "", err
			}
			rules[i] = e
		}
		join := " AND "
		if t.Match == "any" {
			join = " OR "
		}
		parts = append(parts, "("+strings.Join(rules, join)+")")
	}
	return "(" + strings.Join(parts, " AND ") + ")", nil
}

func flinkRule(r domain.FilterRule) (string, error) {
	col := sqlIdent(r.Field)
	switch r.Op {
	case "eq", "ne", "gt", "gte", "lt", "lte":
		lit, err := sqlLiteral(r.Value)
		if err != nil {
			return "", err
		}
		op := map[string]string{"eq": "=", "ne": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[r.Op]
		// Flink CDC's "=" is Java equals: 5 never equals a BIGINT 5, nor 2.5 a DECIMAL 2.50. Equality
		// with a number compares both sides as DOUBLE (ADR 0008); "<" and ">" compare numbers already.
		left := col
		if numericValue(r.Value) && (r.Op == "eq" || r.Op == "ne") {
			left, lit = "TRY_CAST("+col+" AS DOUBLE)", "TRY_CAST("+lit+" AS DOUBLE)"
		}
		if r.Op == "ne" {
			return "(" + col + " IS NULL OR " + left + " <> " + lit + ")", nil
		}
		if left != col {
			return "(" + left + " " + op + " " + lit + ")", nil
		}
		return "(" + col + " " + op + " " + lit + ")", nil
	case "in", "not_in":
		lits := make([]string, len(r.Values))
		for i, v := range r.Values {
			lit, err := sqlLiteral(v)
			if err != nil {
				return "", err
			}
			lits[i] = lit
		}
		// A list with a number compares each value, numbers as DOUBLE (see eq above).
		if slices.ContainsFunc(r.Values, numericValue) {
			eqs := make([]string, len(r.Values))
			for i, v := range r.Values {
				if numericValue(v) {
					eqs[i] = "TRY_CAST(" + col + " AS DOUBLE) = TRY_CAST(" + lits[i] + " AS DOUBLE)"
				} else {
					eqs[i] = col + " = " + lits[i]
				}
			}
			either := "(" + strings.Join(eqs, " OR ") + ")"
			if r.Op == "not_in" {
				return "(" + col + " IS NULL OR NOT " + either + ")", nil
			}
			return either, nil
		}
		if r.Op == "not_in" {
			return "(" + col + " IS NULL OR " + col + " NOT IN (" + strings.Join(lits, ", ") + "))", nil
		}
		return "(" + col + " IN (" + strings.Join(lits, ", ") + "))", nil
	case "is_null":
		return "(" + col + " IS NULL)", nil
	case "is_not_null":
		return "(" + col + " IS NOT NULL)", nil
	case "is_true":
		return "(" + col + " = TRUE)", nil
	case "is_false":
		return "(" + col + " = FALSE)", nil
	}
	return "", problem.New(422, "transform_unsupported", "Unknown filter operator "+r.Op+".")
}

// sqlLiteral is a rule value as a SQL literal: text quoted, numbers as written, booleans.
func sqlLiteral(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return sqlString(x), nil
	case bool:
		if x {
			return "TRUE", nil
		}
		return "FALSE", nil
	case json.Number:
		if _, err := x.Float64(); err != nil {
			return "", problem.New(422, "transform_unsupported", "Not a number: "+x.String())
		}
		return x.String(), nil
	case float64, int, int64:
		return fmt.Sprint(x), nil
	}
	return "", problem.New(422, "transform_unsupported", fmt.Sprintf("Unsupported filter value %v.", v))
}

func sqlString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func sqlIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

// A minimal YAML writer: keys are BeziSync's own, every value is a JSON string (a valid YAML
// double-quoted scalar), and only ${BZ_…} placeholders stay bare for the helper to fill.

type yamlKV struct {
	k string
	v any
}

type yamlMap []yamlKV

func optionsMap(o connectors.FlinkOptions) yamlMap {
	out := make(yamlMap, len(o))
	for i, kv := range o {
		out[i] = yamlKV{kv[0], kv[1]}
	}
	return out
}

func renderYAML(doc yamlMap) string {
	var b strings.Builder
	b.WriteString("# Generated by BeziSync. Do not edit: changes are overwritten on deploy.\n")
	writeMap(&b, doc, "")
	return b.String()
}

func writeMap(b *strings.Builder, m yamlMap, indent string) {
	for _, kv := range m {
		switch v := kv.v.(type) {
		case string:
			b.WriteString(indent + kv.k + ": " + yamlScalar(v) + "\n")
		case yamlMap:
			b.WriteString(indent + kv.k + ":\n")
			writeMap(b, v, indent+"  ")
		case []yamlMap:
			b.WriteString(indent + kv.k + ":\n")
			for _, item := range v {
				for i, ikv := range item {
					prefix := indent + "  "
					if i == 0 {
						prefix = indent + "- "
					}
					b.WriteString(prefix + ikv.k + ": " + yamlScalar(ikv.v.(string)) + "\n")
				}
			}
		}
	}
}

func yamlScalar(s string) string {
	if strings.HasPrefix(s, "${BZ_") && strings.HasSuffix(s, "}") && !strings.ContainsAny(s[2:len(s)-1], "${} ") {
		return s
	}
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	return strings.TrimSuffix(b.String(), "\n")
}

// numericValue reports a rule value that is a number.
func numericValue(v any) bool {
	switch v.(type) {
	case json.Number, float64, int, int64:
		return true
	}
	return false
}

// janinoColumns refuses a step that reads a column Flink CDC 3.7.0 would compile as one of its
// time functions inside an expression (ADR 0008): the job would fail to start.
func janinoColumns(t domain.Transform, table string) error {
	var cols []string
	switch {
	case t.Type == "computed" && t.Expression == "":
		cols = domain.ComputedInputs(t)
	case t.Type == "filter":
		for _, r := range t.Rules {
			cols = append(cols, r.Field)
		}
	}
	for _, c := range cols {
		if expr.JaninoName(c) {
			return problem.New(422, "transform_expression", fmt.Sprintf(
				"%s: Flink CDC reads a column named %s as its %s function inside an expression, so a step can't use it. Leave the step out, or run this pipeline on Debezium.",
				table, c, strings.ToUpper(c)))
		}
	}
	return nil
}
