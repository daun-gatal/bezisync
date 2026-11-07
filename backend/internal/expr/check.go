package expr

import (
	"fmt"
	"slices"
	"strings"
)

// Use is where an expression goes.
type Use string

const (
	UseColumn Use = "column" // a computed column: any value
	UseFilter Use = "filter" // a filter: a condition, deterministic
)

// Env is what the checker knows about the table. Columns nil means the columns aren't known yet
// (no discovery): every column exists with an Unknown kind, and preflight checks again.
type Env struct {
	Columns map[string]Kind
	// Hidden are columns that exist but can't be read (masked or left out), with why.
	Hidden map[string]string
}

// Result is what a checked expression is.
type Result struct {
	Kind          Kind
	Deterministic bool
	// Columns are the source columns it reads, in order of first use (metadata columns left out).
	Columns []string
}

// Check type-checks a tree for a use. Errors are *Error with the position of the node at fault.
func Check(n Node, env Env, use Use) (Result, error) {
	c := &checker{env: env, det: true, eventType: -1}
	k, err := c.kind(n)
	if err != nil {
		return Result{}, err
	}
	res := Result{Kind: k, Deterministic: c.det, Columns: c.cols}
	if use == UseFilter {
		if c.eventType >= 0 {
			return res, errAt(c.eventType, len("__data_event_type__"), "A filter expression can't read __data_event_type__: Flink CDC gives an update's old row -U and its new row +U, so the filter could keep half an update. Choose the operations to keep instead (Keep rows → Operations).")
		}
		if !c.det {
			return res, errAt(c.nondet, 1, "A filter can't use %s: the same row would be kept or dropped depending on when it is read.", c.nondetFn)
		}
		if k != BoolK && k != Unknown && k != NullK {
			return res, errAt(n.pos(), 1, "A filter is a condition (true or false); this is %s.", words(k))
		}
	}
	if use == UseColumn && k == NullK {
		return res, errAt(n.pos(), 1, "A column needs a value with a type: NULL alone has none (CAST it).")
	}
	return res, nil
}

type checker struct {
	env       Env
	det       bool
	nondet    int
	nondetFn  string
	cols      []string
	eventType int // where __data_event_type__ is read (-1: it isn't)
}

func words(k Kind) string {
	switch k {
	case Text:
		return "text"
	case Int:
		return "a whole number"
	case Decimal, Float:
		return "a number"
	case BoolK:
		return "a condition"
	case Date:
		return "a date"
	case Time:
		return "a time of day"
	case Timestamp:
		return "a timestamp"
	case TimestampTZ:
		return "a timestamp with a time zone"
	case Other:
		return "binary, JSON or a nested document"
	}
	return "a value"
}

func (c *checker) kind(n Node) (Kind, error) {
	switch x := n.(type) {
	case *Col:
		if slices.Contains(MetadataColumns, x.Name) {
			if x.Name == "__data_event_type__" && c.eventType < 0 {
				c.eventType = x.Pos
			}
			return Text, nil
		}
		if JaninoName(x.Name) {
			return "", errAt(x.Pos, len(Ident(x.Name)), "Flink CDC reads a column named %s as its %s function inside an expression, so it can't be used here.", x.Name, strings.ToUpper(x.Name))
		}
		if why, hidden := c.env.Hidden[x.Name]; hidden {
			return "", errAt(x.Pos, len(Ident(x.Name)), "%s %s: an expression can't read it.", x.Name, why)
		}
		if !slices.Contains(c.cols, x.Name) {
			c.cols = append(c.cols, x.Name)
		}
		if c.env.Columns == nil {
			return Unknown, nil
		}
		if k, ok := c.env.Columns[x.Name]; ok {
			return k, nil
		}
		return "", errAt(x.Pos, len(x.Name), "The table has no column %s.%s", x.Name, c.hint(x.Name))
	case *Str:
		return Text, nil
	case *Num:
		if strings.Contains(x.Text, ".") {
			return Decimal, nil
		}
		return Int, nil
	case *Bool:
		return BoolK, nil
	case *Null:
		return NullK, nil
	case *Unary:
		k, err := c.kind(x.X)
		if err != nil {
			return "", err
		}
		if x.Op == "NOT" {
			return BoolK, c.need(x.X, SBool, k, "NOT")
		}
		if negativeNumber(x) {
			return k, nil
		}
		if k == Unknown {
			return Unknown, nil
		}
		return Float, c.need(x.X, SNum, k, "\"-\"") // computed in DOUBLE (flink.go)
	case *Binary:
		return c.binary(x)
	case *Is:
		k, err := c.kind(x.X)
		if err != nil {
			return "", err
		}
		if x.What != "NULL" {
			return BoolK, c.need(x.X, SBool, k, "IS "+x.What)
		}
		return BoolK, nil
	case *Distinct:
		return BoolK, c.comparable(x.L, x.R, "IS DISTINCT FROM")
	case *In:
		for _, v := range x.List {
			if err := c.comparable(x.X, v, "IN"); err != nil {
				return "", err
			}
		}
		return BoolK, nil
	case *Between:
		if err := c.comparable(x.X, x.Lo, "BETWEEN"); err != nil {
			return "", err
		}
		return BoolK, c.comparable(x.X, x.Hi, "BETWEEN")
	case *Like:
		k, err := c.kind(x.X)
		if err != nil {
			return "", err
		}
		return BoolK, c.need(x.X, SText, k, "LIKE")
	case *Case:
		return c.caseKind(x)
	case *Cast:
		k, err := c.kind(x.X)
		if err != nil {
			return "", err
		}
		if k == Other {
			return "", errAt(x.Pos, 4, "%s can't be converted.", strings.ToUpper(words(k)[:1])+words(k)[1:])
		}
		return castKind(x.To), nil
	case *Call:
		return c.call(x)
	}
	return "", errAt(n.pos(), 1, "Unknown expression.")
}

// hint suggests a column whose name differs only by case.
func (c *checker) hint(name string) string {
	for col := range c.env.Columns {
		if strings.EqualFold(col, name) {
			return " Did you mean " + col + "? Names are case-sensitive."
		}
	}
	return ""
}

func castKind(t Type) Kind {
	switch t.Name {
	case "STRING":
		return Text
	case "BOOLEAN":
		return BoolK
	case "INT", "BIGINT":
		return Int
	case "DOUBLE":
		return Float
	case "DECIMAL":
		return Decimal
	case "DATE":
		return Date
	}
	return Timestamp
}

func (c *checker) need(n Node, s Slot, k Kind, what string) error {
	if Fits(s, k) {
		return nil
	}
	return errAt(n.pos(), 1, "%s needs %s; this is %s.", what, SlotWords(s), words(k))
}

func (c *checker) binary(x *Binary) (Kind, error) {
	switch x.Op {
	case "AND", "OR":
		for _, s := range []Node{x.L, x.R} {
			k, err := c.kind(s)
			if err != nil {
				return "", err
			}
			if err := c.need(s, SBool, k, x.Op); err != nil {
				return "", err
			}
		}
		return BoolK, nil
	case "=", "<>", "<", "<=", ">", ">=":
		return BoolK, c.comparable(x.L, x.R, x.Op)
	case "||":
		for _, s := range []Node{x.L, x.R} {
			k, err := c.kind(s)
			if err != nil {
				return "", err
			}
			if err := c.need(s, SText, k, "\"||\""); err != nil {
				return "", err
			}
		}
		return Text, nil
	}
	// + - * / %
	l, err := c.kind(x.L)
	if err != nil {
		return "", err
	}
	r, err := c.kind(x.R)
	if err != nil {
		return "", err
	}
	if err := c.need(x.L, SNum, l, "\""+x.Op+"\""); err != nil {
		return "", err
	}
	if err := c.need(x.R, SNum, r, "\""+x.Op+"\""); err != nil {
		return "", err
	}
	if x.Op == "%" {
		// A remainder is of whole numbers.
		if err := c.need(x.L, SWhole, l, "\"%\""); err != nil {
			return "", err
		}
		return Int, c.need(x.R, SWhole, r, "\"%\"")
	}
	if l == Unknown || r == Unknown {
		return Unknown, nil
	}
	return Float, nil // + - * / are computed in DOUBLE (flink.go)
}

// comparable: both sides are numbers, or of one kind (NULL and Unknown compare with anything).
// Flink CDC throws on comparing other kinds, which would stop the job on every row.
func (c *checker) comparable(a, b Node, what string) error {
	ka, err := c.kind(a)
	if err != nil {
		return err
	}
	kb, err := c.kind(b)
	if err != nil {
		return err
	}
	switch {
	case ka == Unknown || kb == Unknown || ka == NullK || kb == NullK:
		return nil
	case ka == Other || kb == Other:
		return errAt(a.pos(), 1, "%s can't compare binary, JSON or nested values.", what)
	case numeric(ka) && numeric(kb):
		// Flink CDC's "=" is Java equals: an INT never equals a BIGINT. A comparison with a number
		// literal or a computed number is made in DOUBLE (flink.go); two plain number values need it
		// written out.
		if what != "<" && what != "<=" && what != ">" && what != ">=" && !anyNumberish(a, b) {
			return errAt(b.pos(), 1, "%s compares two numbers of the table: write CAST(… AS DOUBLE) on both sides (Flink CDC never finds an INT equal to a BIGINT).", what)
		}
		return nil
	case ka == kb:
		return nil
	case timeLike(ka) && timeLike(kb) && ka != TimestampTZ && kb != TimestampTZ:
		// Dates and timestamps compare; a timestamp with a time zone only with another (Flink CDC
		// throws comparing an Instant with a LocalDateTime).
		return nil
	}
	return errAt(b.pos(), 1, "%s compares %s with %s: CAST one of them first.", what, words(ka), words(kb))
}

func (c *checker) caseKind(x *Case) (Kind, error) {
	if x.Subject != nil {
		for _, w := range x.Whens {
			if err := c.comparable(x.Subject, w.Cond, "CASE … WHEN"); err != nil {
				return "", err
			}
		}
	} else {
		for _, w := range x.Whens {
			k, err := c.kind(w.Cond)
			if err != nil {
				return "", err
			}
			if err := c.need(w.Cond, SBool, k, "WHEN"); err != nil {
				return "", err
			}
		}
	}
	results := make([]Node, 0, len(x.Whens)+1)
	for _, w := range x.Whens {
		results = append(results, w.Then)
	}
	if x.Else != nil {
		results = append(results, x.Else)
	}
	return c.same(results, "The results of CASE")
}

// same: values of one kind (numbers count as one), the kind of the result.
func (c *checker) same(ns []Node, what string) (Kind, error) {
	out := NullK
	for _, n := range ns {
		k, err := c.kind(n)
		if err != nil {
			return "", err
		}
		switch {
		case k == Other:
			return "", errAt(n.pos(), 1, "%s can't be binary, JSON or nested values.", what)
		case k == NullK:
		case out == NullK:
			out = k
		case k == Unknown || out == Unknown:
			out = Unknown
		case numeric(k) && numeric(out) && k != out:
			return "", errAt(n.pos(), 1, "%s mix %s and %s: CAST them to one kind (Flink CDC can't mix them).", what, kindName(out), kindName(k))
		case k != out:
			return "", errAt(n.pos(), 1, "%s mix %s and %s: CAST them to one kind.", what, words(out), words(k))
		}
	}
	return out, nil
}

func (c *checker) call(x *Call) (Kind, error) {
	f := Functions[x.Fn]
	if !f.Det && c.det {
		c.det, c.nondet, c.nondetFn = false, x.Pos, x.Fn
	}
	switch x.Fn {
	case "COALESCE", "IFNULL", "NULLIF":
		return c.same(x.Args, x.Fn+"'s values")
	case "IF":
		k, err := c.kind(x.Args[0])
		if err != nil {
			return "", err
		}
		if err := c.need(x.Args[0], SBool, k, "IF"); err != nil {
			return "", err
		}
		return c.same(x.Args[1:], "IF's values")
	}
	first := NullK
	for i, a := range x.Args {
		s := f.slot(i)
		if s == SPattern {
			lit, ok := a.(*Str)
			if !ok || !slices.Contains(DatePatterns, lit.V) {
				return "", errAt(a.pos(), 1, "%s's pattern is one of %s.", x.Fn, strings.Join(DatePatterns, ", "))
			}
			continue
		}
		k, err := c.kind(a)
		if err != nil {
			return "", err
		}
		if err := c.need(a, s, k, fmt.Sprintf("%s (argument %d)", x.Fn, i+1)); err != nil {
			return "", err
		}
		if i == 0 {
			first = k
		}
	}
	if f.Out != "" {
		return f.Out, nil
	}
	return first, nil // ABS, CEIL, FLOOR, ROUND: the number's own kind
}

// JaninoNames are names Flink CDC 3.7.0 compiles as its own time functions wherever they appear
// as an identifier inside an expression, back-quoted or not (JaninoCompiler.translateSqlIdentifier):
// a column of one of these names can't be read by an expression.
var JaninoNames = []string{"CURRENT_TIMESTAMP", "NOW", "LOCALTIME", "LOCALTIMESTAMP", "CURRENT_TIME", "CURRENT_DATE", "UNIX_TIMESTAMP",
	"TO_DATE", "TO_TIMESTAMP_LTZ", "TO_TIMESTAMP", "FROM_UNIXTIME", "TIMESTAMPADD", "TIMESTAMPDIFF", "TIMESTAMP_DIFF", "DATE_FORMAT", "DATE_ADD"}

// JaninoName reports a column name Flink CDC would read as a function inside an expression.
func JaninoName(col string) bool { return slices.Contains(JaninoNames, strings.ToUpper(col)) }

func kindName(k Kind) string {
	switch k {
	case Int:
		return "whole numbers"
	case Decimal:
		return "decimals"
	case Float:
		return "floating-point numbers"
	}
	return words(k)
}
