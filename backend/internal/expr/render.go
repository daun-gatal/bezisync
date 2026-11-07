package expr

import (
	"slices"
	"strconv"
	"strings"
)

// Canonical is the text BeziSync stores and shows for a tree: keywords and functions in capitals,
// every column back-quoted, one space around binary operators, the fewest parentheses that keep the
// tree. Parse(Canonical(t)) gives t back, and Canonical never changes for a tree it rendered.
func Canonical(n Node) string { return render(n, false, 0) }

// Ident is a column name back-quoted for Flink CDC (a back-quote doubled).
func Ident(s string) string {
	if slices.Contains(MetadataColumns, s) {
		return s // Flink CDC's metadata columns are read by their bare names
	}
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// Quote is a text literal for Flink CDC (a quote doubled).
func Quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// likeEscape is the escape character BeziSync writes for LIKE: a "!" in a pattern is doubled.
const likeEscape = "!"

// Precedence levels, lowest first.
const (
	pOr = iota + 1
	pAnd
	pNot
	pCompare
	pAdd
	pMul
	pUnary
	pPrimary
)

func prec(n Node) int {
	switch x := n.(type) {
	case *Binary:
		switch x.Op {
		case "OR":
			return pOr
		case "AND":
			return pAnd
		case "+", "-", "||":
			return pAdd
		case "*", "/", "%":
			return pMul
		}
		return pCompare
	case *Unary:
		if x.Op == "NOT" {
			return pNot
		}
		return pUnary
	case *Is, *Distinct, *In, *Between, *Like:
		return pCompare
	}
	return pPrimary
}

func render(n Node, flink bool, min int) string {
	s := renderNode(n, flink)
	if prec(n) < min {
		return "(" + s + ")"
	}
	return s
}

func renderNode(n Node, flink bool) string {
	r := func(c Node, min int) string { return render(c, flink, min) }
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
			return "NOT " + r(x.X, pNot)
		}
		s := r(x.X, pUnary)
		if strings.HasPrefix(s, "-") { // never "--", which starts a comment
			s = "(" + s + ")"
		}
		return "-" + s
	case *Binary:
		p := prec(x)
		left, right := p, p+1
		if p == pCompare {
			left = pAdd
		}
		// "||" binds differently in Calcite than "+": its operands, and an arithmetic operand that is
		// a "||", are always parenthesised, so the text means the same under either precedence.
		if x.Op == "||" {
			left, right = pUnary, pUnary
		}
		l, rr := r(x.L, left), r(x.R, right)
		if p == pAdd || p == pMul {
			if isConcat(x.L) {
				l = "(" + renderNode(x.L, flink) + ")"
			}
			if isConcat(x.R) {
				rr = "(" + renderNode(x.R, flink) + ")"
			}
		}
		return l + " " + x.Op + " " + rr
	case *Is:
		not := ""
		if x.Not {
			not = "NOT "
		}
		return r(x.X, pAdd) + " IS " + not + x.What
	case *Distinct:
		not := ""
		if x.Not {
			not = "NOT "
		}
		return r(x.L, pAdd) + " IS " + not + "DISTINCT FROM " + r(x.R, pAdd)
	case *In:
		items := make([]string, len(x.List))
		for i, v := range x.List {
			items[i] = r(v, pAdd)
		}
		return r(x.X, pAdd) + notWord(x.Not) + "IN (" + strings.Join(items, ", ") + ")"
	case *Between:
		return r(x.X, pAdd) + notWord(x.Not) + "BETWEEN " + r(x.Lo, pAdd) + " AND " + r(x.Hi, pAdd)
	case *Like:
		s := r(x.X, pAdd) + notWord(x.Not) + "LIKE "
		return s + Quote(x.Pattern)
	case *Case:
		var b strings.Builder
		b.WriteString("CASE")
		if x.Subject != nil {
			b.WriteString(" " + r(x.Subject, 0))
		}
		for _, w := range x.Whens {
			b.WriteString(" WHEN " + r(w.Cond, 0) + " THEN " + r(w.Then, 0))
		}
		if x.Else != nil {
			b.WriteString(" ELSE " + r(x.Else, 0))
		}
		return b.String() + " END"
	case *Cast:
		word := "CAST"
		if x.Try {
			word = "TRY_CAST"
		}
		t := x.To.Name
		if t == "DECIMAL" {
			t += "(" + strconv.Itoa(x.To.P) + ", " + strconv.Itoa(x.To.S) + ")"
		}
		return word + "(" + r(x.X, 0) + " AS " + t + ")"
	case *Call:
		if bareFunctions[x.Fn] {
			return x.Fn
		}
		args := make([]string, 0, len(x.Args)+1)
		if x.Unit != "" {
			args = append(args, x.Unit)
		}
		for _, a := range x.Args {
			args = append(args, r(a, 0))
		}
		return x.Fn + "(" + strings.Join(args, ", ") + ")"
	}
	return "NULL"
}

func notWord(not bool) string {
	if not {
		return " NOT "
	}
	return " "
}

func nonZeroNumber(n Node) bool {
	x, ok := n.(*Num)
	return ok && strings.Trim(x.Text, "0.") != ""
}

func isConcat(n Node) bool {
	b, ok := n.(*Binary)
	return ok && b.Op == "||"
}
