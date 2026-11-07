package expr

import (
	"strconv"
	"strings"
)

// The Flink CDC rendering (ADR 0008 E4). Flink CDC 3.7.0 compiles a projection to Java with
// Janino, and the rules below come from running each form on the real engine (the corpus in the
// ADR):
//
//   - Most functions and every arithmetic operator throw on a NULL argument (UPPER(NULL),
//     NULL + 1), which crash-loops the job. A strict sub-tree is guarded once:
//     CASE WHEN <its inputs> IS NULL THEN NULL ELSE … END.
//   - Arithmetic between a DECIMAL and an INT doesn't compile, DECIMAL division drops the
//     fraction, and integer division by zero throws: + - * / are done in DOUBLE, and a division by
//     zero gives NULL.
//   - "=" is Java equals (an Integer never equals a Long, nor 2.50 equal 2.5): a comparison with a
//     number compares both sides as DOUBLE.
//   - Unary minus doesn't exist (a negative number literal does); CAST to STRING doesn't
//     compile (VARCHAR does); LIKE without ESCAPE throws on NULL.
//
// Every operator node is parenthesised, so the text means the same whatever Calcite's
// precedence, and "--" can't appear.

// MaxFlinkLength bounds the rendering (NULL guards repeat their inputs).
const MaxFlinkLength = 20000

// nonStrict are the functions that take NULL arguments safely (they exist for it).
var nonStrict = map[string]bool{"COALESCE": true, "IFNULL": true, "NULLIF": true, "IF": true, "CONCAT_WS": true}

// Flink renders a checked tree for a Flink CDC transform.
func Flink(n Node) string { return fl(n) }

// FlinkChecked is Flink with the length bound (an *Error past it).
func FlinkChecked(n Node) (string, error) {
	s := fl(n)
	if len(s) > MaxFlinkLength {
		return "", errAt(n.pos(), 1, "The expression is too complex to run safely: simplify it (fewer nested COALESCE, IF or CASE inside text or number functions).")
	}
	return s, nil
}

func negativeNumber(n Node) bool {
	u, ok := n.(*Unary)
	if !ok || u.Op != "-" {
		return false
	}
	_, num := u.X.(*Num)
	return num
}

func literal(n Node) bool {
	switch n.(type) {
	case *Str, *Num, *Bool:
		return true
	}
	return negativeNumber(n)
}

// strict reports a node that throws on a NULL input.
func strict(n Node) bool {
	switch x := n.(type) {
	case *Call:
		return len(x.Args) > 0 && !nonStrict[x.Fn]
	case *Binary:
		return isArith(x.Op) || x.Op == "||"
	case *Unary:
		return x.Op == "-" && !negativeNumber(x)
	}
	return false
}

func isArith(op string) bool { return op == "+" || op == "-" || op == "*" || op == "/" || op == "%" }

// children are a node's operands.
func children(n Node) []Node {
	switch x := n.(type) {
	case *Call:
		return x.Args
	case *Binary:
		return []Node{x.L, x.R}
	case *Unary:
		return []Node{x.X}
	}
	return nil
}

// fl renders a node: a strict one with its NULL guard.
func fl(n Node) string {
	if !strict(n) {
		return plain(n)
	}
	var conds []string
	seen := map[string]bool{}
	add := func(c string) {
		if !seen[c] {
			seen[c] = true
			conds = append(conds, c)
		}
	}
	var zeros []string
	var collect func(Node)
	collect = func(n Node) {
		for _, c := range children(n) {
			switch {
			case strict(c):
				collect(c)
			case literal(c):
			default:
				add(fl(c) + " IS NULL")
			}
		}
		if b, ok := n.(*Binary); ok && (b.Op == "/" || b.Op == "%") && !nonZeroNumber(b.R) {
			zeros = append(zeros, doubleOf(b.R)+" = TRY_CAST(0 AS DOUBLE)")
		}
	}
	collect(n)
	for _, z := range zeros {
		add(z) // after every NULL check: OR stops at the first true, so no zero check reads a NULL
	}
	if len(conds) == 0 {
		return body(n)
	}
	return "(CASE WHEN " + strings.Join(conds, " OR ") + " THEN NULL ELSE " + body(n) + " END)"
}

// body renders a strict node without its guard (its strict children too; others guarded).
func body(n Node) string {
	if !strict(n) {
		return fl(n)
	}
	switch x := n.(type) {
	case *Binary:
		switch x.Op {
		case "||":
			return "(" + body(x.L) + " || " + body(x.R) + ")"
		case "%":
			return "(" + asBigint(body(x.L)) + " % " + asBigint(body(x.R)) + ")"
		}
		return "(" + doubleOf(x.L) + " " + x.Op + " " + doubleOf(x.R) + ")"
	case *Unary:
		return "(TRY_CAST(0 AS DOUBLE) - " + doubleOf(x.X) + ")"
	case *Call:
		args := make([]string, 0, len(x.Args)+1)
		if x.Unit != "" {
			args = append(args, x.Unit)
		}
		for _, a := range x.Args {
			args = append(args, body(a))
		}
		return x.Fn + "(" + strings.Join(args, ", ") + ")"
	}
	return plain(n)
}

func asDouble(s string) string { return "TRY_CAST(" + s + " AS DOUBLE)" }

// doubleOf is a strict operand of arithmetic as DOUBLE: arithmetic already is.
func doubleOf(n Node) string {
	if b, ok := n.(*Binary); ok && isArith(b.Op) && b.Op != "%" {
		return body(n)
	}
	if u, ok := n.(*Unary); ok && u.Op == "-" && !negativeNumber(u) {
		return body(n)
	}
	return asDouble(body(n))
}
func asBigint(s string) string { return "TRY_CAST(" + s + " AS BIGINT)" }

// numberish reports a node whose value is a number in Flink CDC's rendering (so a comparison with
// it compares as DOUBLE). A bare column isn't known here: the checker asks for a CAST when two
// number columns are compared.
func numberish(n Node) bool {
	switch x := n.(type) {
	case *Num:
		return true
	case *Unary:
		return x.Op == "-"
	case *Binary:
		return isArith(x.Op)
	case *Cast:
		return x.To.Name == "INT" || x.To.Name == "BIGINT" || x.To.Name == "DOUBLE" || x.To.Name == "DECIMAL"
	case *Call:
		f := Functions[x.Fn]
		return f.Out == Int || f.Out == Decimal || f.Out == Float || x.Fn == "ABS" || x.Fn == "CEIL" || x.Fn == "FLOOR" || x.Fn == "ROUND"
	}
	return false
}

func anyNumberish(ns ...Node) bool {
	for _, n := range ns {
		if numberish(n) {
			return true
		}
	}
	return false
}

// operand renders a comparison operand, as DOUBLE when the comparison is numeric.
func operand(n Node, numeric bool) string {
	if numeric {
		return asDouble(fl(n))
	}
	return fl(n)
}

// plain renders a node that takes NULL safely.
func plain(n Node) string {
	switch x := n.(type) {
	case *Col:
		return Ident(x.Name)
	case *Str:
		return Quote(x.V)
	case *Num:
		return x.Text
	case *Bool:
		if x.V {
			return "TRUE"
		}
		return "FALSE"
	case *Null:
		return "NULL"
	case *Unary:
		if x.Op == "NOT" {
			return "(NOT " + fl(x.X) + ")"
		}
		return "-" + x.X.(*Num).Text // a negative number literal
	case *Binary:
		if x.Op == "AND" || x.Op == "OR" {
			return "(" + fl(x.L) + " " + x.Op + " " + fl(x.R) + ")"
		}
		num := anyNumberish(x.L, x.R)
		return "(" + operand(x.L, num) + " " + x.Op + " " + operand(x.R, num) + ")"
	case *Is:
		return "(" + fl(x.X) + " IS " + map[bool]string{true: "NOT ", false: ""}[x.Not] + x.What + ")"
	case *Distinct:
		num := anyNumberish(x.L, x.R)
		return "(" + operand(x.L, num) + " IS " + map[bool]string{true: "NOT ", false: ""}[x.Not] + "DISTINCT FROM " + operand(x.R, num) + ")"
	case *In:
		num := anyNumberish(append([]Node{x.X}, x.List...)...)
		items := make([]string, len(x.List))
		for i, v := range x.List {
			items[i] = operand(v, num)
		}
		return "(" + operand(x.X, num) + notWord(x.Not) + "IN (" + strings.Join(items, ", ") + "))"
	case *Between:
		num := anyNumberish(x.X, x.Lo, x.Hi)
		return "(" + operand(x.X, num) + notWord(x.Not) + "BETWEEN " + operand(x.Lo, num) + " AND " + operand(x.Hi, num) + ")"
	case *Like:
		return "(" + fl(x.X) + notWord(x.Not) + "LIKE " + Quote(strings.ReplaceAll(x.Pattern, likeEscape, likeEscape+likeEscape)) + " ESCAPE " + Quote(likeEscape) + ")"
	case *Case:
		var b strings.Builder
		b.WriteString("(CASE")
		if x.Subject != nil {
			b.WriteString(" " + fl(x.Subject))
		}
		for _, w := range x.Whens {
			b.WriteString(" WHEN " + fl(w.Cond) + " THEN " + fl(w.Then))
		}
		if x.Else != nil {
			b.WriteString(" ELSE " + fl(x.Else))
		}
		return b.String() + " END)"
	case *Cast:
		t := x.To.Name
		switch t {
		case "STRING":
			t = "VARCHAR"
		case "DECIMAL":
			t += "(" + strconv.Itoa(x.To.P) + ", " + strconv.Itoa(x.To.S) + ")"
		}
		return "TRY_CAST(" + fl(x.X) + " AS " + t + ")"
	case *Call:
		if bareFunctions[x.Fn] {
			return x.Fn
		}
		args := make([]string, 0, len(x.Args))
		for _, a := range x.Args {
			args = append(args, fl(a))
		}
		return x.Fn + "(" + strings.Join(args, ", ") + ")"
	}
	return "NULL"
}
