package domain

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/expr"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Expressions (ADR 0008): a computed column or a Flink CDC filter written as an expression in the
// language of internal/expr. The spec stores its canonical text, so decoding parses and renders
// it; what the parser refuses is a 422 at the field, with the position in the message.

// decodeExpression parses the expression field and returns its canonical text ("" when refused).
// Without the table's columns only the shape is checked here (and determinism for filters);
// preflight checks the columns and their kinds (transform_expression).
func decodeExpression(v *val.V, f val.Field, use expr.Use) string {
	s, ok := v.Constrained(f.Loc, f.Raw, val.StrRules{Min: 1, Max: 4 * expr.MaxLength})
	if !ok {
		return ""
	}
	n, err := expr.Parse(s)
	if err == nil {
		_, err = expr.Check(n, expr.Env{}, use)
	}
	if err != nil {
		v.ValueError(f.Loc, expressionMessage(s, err))
		return ""
	}
	return expr.Canonical(n)
}

// expressionMessage is a refusal with where it is, e.g. `at "nope(b)" (character 12)`.
func expressionMessage(s string, err error) string {
	var e *expr.Error
	if !errors.As(err, &e) {
		return err.Error()
	}
	at := ""
	if e.Pos < len(s) {
		end := min(e.Pos+max(e.Len, 1), len(s))
		at = fmt.Sprintf(" (at %q, character %d)", s[e.Pos:end], len([]rune(s[:e.Pos]))+1)
	}
	return e.Msg + at
}

// ExpressionOf parses a stored (canonical) expression; stored text always parses.
func ExpressionOf(t Transform) (expr.Node, error) { return expr.Parse(t.Expression) }

// expressionHidden are the table's columns an expression can't read: masked and left out (T14).
func expressionHidden(spec PipelineSpec, table string) map[string]string {
	out := map[string]string{}
	rules := spec.Source.Columns[table]
	for _, c := range rules.Mask {
		out[c] = "is masked"
	}
	for _, c := range rules.Exclude {
		out[c] = "is left out of the pipeline"
	}
	return out
}

// validateFilterExpression: no table the filter applies to has a column it reads masked or left
// out (T14).
func validateFilterExpression(v *val.V, at val.Loc, spec PipelineSpec, t Transform) {
	n, err := ExpressionOf(t)
	if err != nil {
		return
	}
	for _, table := range FilterTables(spec, t) {
		if _, err := expr.Check(n, expr.Env{Hidden: expressionHidden(spec, table)}, expr.UseFilter); err != nil {
			v.ValueError(at.Add("expression"), table+": "+expressionMessage(t.Expression, err))
			return
		}
	}
}

// FilterTables are the captured tables a filter applies to.
func FilterTables(spec PipelineSpec, t Transform) []string {
	if t.Tables != nil {
		return t.Tables
	}
	return spec.Source.Tables.Include
}

// ExpressionEnv is what an expression can read of a discovered table on Flink CDC: each column
// with the kind Flink CDC gives it, and the masked or left-out ones hidden.
func ExpressionEnv(sourceType string, info TableColumns, spec PipelineSpec, table string) expr.Env {
	cols := map[string]expr.Kind{}
	for _, c := range info.Columns {
		k := expr.Kind(PartitionKind("flink-cdc", sourceType, c.Type, ValueFormats{}))
		switch k {
		case "", "binary":
			k = expr.Other
		case "uuid", "json":
			k = expr.Text
		}
		cols[c.Name] = k
	}
	return expr.Env{Columns: cols, Hidden: expressionHidden(spec, table)}
}

// expressionFindings: transform_expression (ADR 0008 E7) for each expression step that applies to
// the table: a column it doesn't have, or a kind a function doesn't take. Each would fail the job.
func expressionFindings(sourceType, table string, info TableColumns, spec PipelineSpec) []TransformFinding {
	var out []TransformFinding
	env := ExpressionEnv(sourceType, info, spec, table)
	for i, t := range spec.Transforms {
		if t.Expression == "" {
			continue
		}
		use := expr.UseColumn
		switch {
		case t.Type == "computed" && t.Table == table:
		case t.Type == "filter" && slices.Contains(FilterTables(spec, t), table):
			use = expr.UseFilter
		default:
			continue
		}
		n, err := ExpressionOf(t)
		if err == nil {
			_, err = expr.Check(n, env, use)
		}
		if err != nil {
			out = append(out, TransformFinding{Code: "transform_expression", Fail: true, Table: table, Step: i,
				Message: fmt.Sprintf("%s, %s: %s", table, StepLabel(i, t), expressionMessage(t.Expression, err))})
		}
	}
	return out
}

// ComputedTree is a typed computed step (function and arguments) as an expression tree, so every
// function compiles through internal/expr's Flink rendering (NULL-safe, ADR 0008 E4). A column of
// any kind joined into text is cast to text first.
func ComputedTree(t Transform) expr.Node {
	arg := func(i int) expr.Node {
		if i >= len(t.Args) {
			return &expr.Null{}
		}
		a := t.Args[i]
		switch {
		case a.Column != "":
			return &expr.Col{Name: a.Column}
		case a.Text != nil:
			return &expr.Str{V: *a.Text}
		case a.Number != nil:
			return &expr.Num{Text: strconv.Itoa(*a.Number)}
		case a.Integer != nil:
			return &expr.Num{Text: strconv.Itoa(*a.Integer)}
		case a.Decimal != nil:
			if d, neg := strings.CutPrefix(*a.Decimal, "-"); neg {
				return &expr.Unary{Op: "-", X: &expr.Num{Text: d}}
			}
			return &expr.Num{Text: *a.Decimal}
		}
		return &expr.Null{}
	}
	asText := func(i int) expr.Node {
		n := arg(i)
		if _, col := n.(*expr.Col); col {
			return &expr.Cast{X: n, To: expr.Type{Name: "STRING"}}
		}
		return n
	}
	call := func(fn string, args ...expr.Node) expr.Node { return &expr.Call{Fn: fn, Args: args} }
	all := func(f func(int) expr.Node, from int) []expr.Node {
		var out []expr.Node
		for i := from; i < len(t.Args); i++ {
			out = append(out, f(i))
		}
		return out
	}
	switch t.Function {
	case "upper", "lower", "trim", "replace", "left", "right", "abs", "ceil", "floor", "year", "month", "hour":
		return call(strings.ToUpper(t.Function), all(arg, 0)...)
	case "length":
		return call("CHAR_LENGTH", arg(0))
	case "concat":
		return call("CONCAT", all(asText, 0)...)
	case "join_with":
		return call("CONCAT_WS", append([]expr.Node{arg(0)}, all(asText, 1)...)...)
	case "substr":
		return call("SUBSTR", arg(0), arg(1), arg(2))
	case "pad_left":
		return call("LPAD", arg(0), arg(1), arg(2))
	case "pad_right":
		return call("RPAD", arg(0), arg(1), arg(2))
	case "first_set":
		return call("COALESCE", all(arg, 0)...)
	case "round":
		return call("ROUND", arg(0), arg(1))
	case "date_format":
		return call("DATE_FORMAT", arg(0), arg(1))
	case "day":
		return call("DAYOFMONTH", arg(0))
	case "parse_date":
		return call("TO_DATE", arg(0), arg(1))
	case "days_between":
		return &expr.Call{Fn: "TIMESTAMPDIFF", Unit: "DAY", Args: []expr.Node{arg(0), arg(1)}}
	case "add", "subtract", "multiply", "divide":
		op := map[string]string{"add": "+", "subtract": "-", "multiply": "*", "divide": "/"}[t.Function]
		return &expr.Binary{Op: op, L: arg(0), R: arg(1)}
	case "to_text":
		return &expr.Cast{X: arg(0), To: expr.Type{Name: "STRING"}, Try: true}
	case "to_integer":
		return &expr.Cast{X: arg(0), To: expr.Type{Name: "BIGINT"}, Try: true}
	case "to_float":
		return &expr.Cast{X: arg(0), To: expr.Type{Name: "DOUBLE"}, Try: true}
	case "to_decimal":
		scale := 0
		if len(t.Args) > 1 && t.Args[1].Integer != nil {
			scale = *t.Args[1].Integer
		}
		return &expr.Cast{X: arg(0), To: expr.Type{Name: "DECIMAL", P: 19, S: scale}, Try: true}
	}
	return &expr.Null{}
}

// ComputedSQL is what a computed step compiles to on Flink CDC: its expression, or its function's
// tree, in internal/expr's Flink rendering.
func ComputedSQL(t Transform) (string, error) {
	if t.Expression != "" {
		n, err := ExpressionOf(t)
		if err != nil {
			return "", err
		}
		return expr.FlinkChecked(n)
	}
	return expr.FlinkChecked(ComputedTree(t))
}

// legacyFunctions are the computed functions from before ADR 0008, checked slot by slot
// (computedFindings); the others are checked as their expression tree.
var legacyFunctions = []string{"upper", "lower", "trim", "length", "concat", "substr", "date_format", "to_text", "to_integer"}

// typedExpressionFindings: a newer typed function's tree against the table (kinds a function
// doesn't take, values of mixed kinds), and a computed column reading a column Flink CDC compiles
// as one of its time functions (the job wouldn't start). Typed filters run on both engines: the
// Flink CDC compile refuses those names for them.
func typedExpressionFindings(sourceType, table string, info TableColumns, spec PipelineSpec) []TransformFinding {
	var out []TransformFinding
	env := ExpressionEnv(sourceType, info, spec, table)
	for i, t := range spec.Transforms {
		var cols []string
		switch {
		case t.Type == "computed" && t.Table == table && t.Expression == "":
			cols = ComputedInputs(t)
			if !slices.Contains(legacyFunctions, t.Function) {
				if _, err := expr.Check(ComputedTree(t), env, expr.UseColumn); err != nil {
					out = append(out, TransformFinding{Code: "transform_expression", Fail: true, Table: table, Step: i,
						Message: fmt.Sprintf("%s, %s: %v", table, StepLabel(i, t), err)})
				}
			}
		}
		for _, c := range cols {
			if expr.JaninoName(c) {
				out = append(out, TransformFinding{Code: "transform_expression", Fail: true, Table: table, Step: i,
					Message: fmt.Sprintf("%s, %s reads %s, which Flink CDC compiles as its %s function inside an expression: the pipeline wouldn't start.", table, StepLabel(i, t), c, strings.ToUpper(c))})
			}
		}
	}
	return out
}
