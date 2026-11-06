package domain

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// A computed column (Flink CDC only, T16): a new column of one table, from one function of a
// fixed set over the table's columns and typed literals. Never free text: every column is a
// back-quoted identifier and every literal a SQL literal when compiled, the inputs are the source's
// columns (before renames), a masked or left-out column can't be an input (T14), and an input of
// the wrong type is refused by preflight (T15). Casting to a number is TRY_CAST, so a value that
// isn't one becomes NULL instead of failing the job.

// ComputedArg is one argument: a column, a text literal or a number (exactly one).
type ComputedArg struct {
	Column string  `json:"column,omitempty"`
	Text   *string `json:"text,omitempty"`
	Number *int    `json:"number,omitempty"`
}

// Argument slots of a function.
const (
	ArgText    = "text"    // a text column
	ArgAny     = "any"     // a column of any scalar kind
	ArgTime    = "time"    // a timestamp or date column
	ArgTextLit = "any|lit" // a column of any scalar kind, or a text literal
	ArgNumber  = "number"  // a whole number 1..65535
	ArgPattern = "pattern" // one of DatePatterns
)

// ComputedFunction is one function a computed column can use.
type ComputedFunction struct {
	Title string
	Args  []string // slots; a variadic function repeats its last slot
	Min   int      // fewest arguments (variadic only)
	Max   int      // most arguments (variadic only)
	Out   string   // the kind of the result
}

// ComputedFunctions are the functions offered, by key.
var ComputedFunctions = map[string]ComputedFunction{
	"upper":       {Title: "Upper case", Args: []string{ArgText}, Out: "text"},
	"lower":       {Title: "Lower case", Args: []string{ArgText}, Out: "text"},
	"trim":        {Title: "Trim spaces", Args: []string{ArgText}, Out: "text"},
	"length":      {Title: "Length", Args: []string{ArgText}, Out: "int"},
	"concat":      {Title: "Join", Args: []string{ArgTextLit}, Min: 2, Max: 5, Out: "text"},
	"substr":      {Title: "Part of text", Args: []string{ArgText, ArgNumber, ArgNumber}, Out: "text"},
	"date_format": {Title: "Date as text", Args: []string{ArgTime, ArgPattern}, Out: "text"},
	"to_text":     {Title: "As text", Args: []string{ArgAny}, Out: "text"},
	"to_integer":  {Title: "As a whole number", Args: []string{ArgText}, Out: "int"},
}

// DatePatterns are the formats date_format offers (Java SimpleDateFormat).
var DatePatterns = []string{"yyyy", "yyyy-MM", "yyyy-MM-dd", "yyyy-MM-dd HH", "yyyy-MM-dd HH:mm:ss"}

const maxComputedText = 200

// argSlot is the slot of argument i of f.
func argSlot(f ComputedFunction, i int) string {
	if i < len(f.Args) {
		return f.Args[i]
	}
	return f.Args[len(f.Args)-1]
}

// KindFits reports whether a column of kind fits slot.
func KindFits(slot, kind string) bool {
	switch slot {
	case ArgText:
		return kind == "text"
	case ArgTime:
		return kind == "timestamp" || kind == "date"
	case ArgAny, ArgTextLit:
		return !slices.Contains([]string{"binary", "document", "list", "json", "unknown"}, kind)
	}
	return false
}

func decodeComputed(v *val.V, o *val.Object, out *Transform) {
	if f := o.Get("table"); f.Required() {
		out.Table, _ = v.Constrained(f.Loc, f.Raw, val.StrRules{Pattern: tableRe.String()})
	}
	if f := o.Get("name"); f.Required() {
		out.Name, _ = v.Constrained(f.Loc, f.Raw, val.StrRules{Pattern: newNameRe})
	}
	keys := make([]string, 0, len(ComputedFunctions))
	for k := range ComputedFunctions {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	if f := o.Get("function"); f.Required() {
		out.Function, _ = v.Literal(f.Loc, f.Raw, keys...)
	}
	fn, known := ComputedFunctions[out.Function]
	f := o.Get("args")
	if !f.Required() {
		return
	}
	items, ok := v.List(f.Loc, f.Raw)
	if !ok || !known {
		return
	}
	lo, hi := len(fn.Args), len(fn.Args)
	if fn.Max > 0 {
		lo, hi = fn.Min, fn.Max
	}
	if !v.ListLen(f.Loc, len(items), lo, hi) {
		return
	}
	for i, item := range items {
		at := f.Loc.Add(i)
		ao := v.Model(at, item, "ComputedArg")
		if ao.Failed {
			continue
		}
		var a ComputedArg
		slot := argSlot(fn, i)
		col, text, num := ao.Get("column"), ao.Get("text"), ao.Get("number")
		switch slot {
		case ArgText, ArgAny, ArgTime:
			if col.Required() {
				a.Column, _ = v.Constrained(col.Loc, col.Raw, val.StrRules{Pattern: fieldRe})
			}
		case ArgTextLit:
			switch {
			case col.Present && !text.Present:
				a.Column, _ = v.Constrained(col.Loc, col.Raw, val.StrRules{Pattern: fieldRe})
			case text.Present && !col.Present:
				if s, ok := v.Constrained(text.Loc, text.Raw, val.StrRules{Max: maxComputedText}); ok && computedText(v, text.Loc, s) {
					a.Text = &s
				}
			default:
				v.ValueError(at, "a column or a text, one of them")
			}
		case ArgNumber:
			if num.Required() {
				if n, ok := v.IntIn(num.Loc, num.Raw, val.Num{GE: val.F(1), LE: val.F(65535)}); ok {
					a.Number = &n
				}
			}
		case ArgPattern:
			if text.Required() {
				if s, ok := v.Literal(text.Loc, text.Raw, DatePatterns...); ok {
					a.Text = &s
				}
			}
		}
		ao.Forbid()
		out.Args = append(out.Args, a)
	}
}

// computedText refuses what would read as a variable or break a line.
func computedText(v *val.V, loc val.Loc, s string) bool {
	if strings.Contains(s, "${") {
		v.ValueError(loc, "must not contain \"${\"")
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			v.ValueError(loc, "must not contain control characters (line breaks, tabs)")
			return false
		}
	}
	return true
}

// validateComputed: the table is captured, and no input is masked (T14) or left out.
func validateComputed(v *val.V, at val.Loc, spec PipelineSpec, t Transform) {
	if !contains(spec.Source.Tables.Include, t.Table) {
		v.ValueError(at.Add("table"), t.Table+" is not captured by this pipeline")
		return
	}
	rules := spec.Source.Columns[t.Table]
	for j, a := range t.Args {
		switch {
		case a.Column == "":
		case contains(rules.Mask, a.Column):
			v.ValueError(at.Add("args", j), t.Table+"."+a.Column+" is masked: a computed column can't read it")
		case contains(rules.Exclude, a.Column):
			v.ValueError(at.Add("args", j), t.Table+"."+a.Column+" is left out of the pipeline")
		}
	}
}

func dumpComputed(m *om.Map, t Transform) {
	args := make([]any, len(t.Args))
	for i, a := range t.Args {
		x := om.New()
		switch {
		case a.Column != "":
			x.Set("column", a.Column)
		case a.Text != nil:
			x.Set("text", *a.Text)
		case a.Number != nil:
			x.Set("number", *a.Number)
		}
		args[i] = x
	}
	m.Set("table", t.Table).Set("name", t.Name).Set("function", t.Function).Set("args", args)
}

func cloneArgs(l []ComputedArg) []ComputedArg {
	out := make([]ComputedArg, len(l))
	for i, a := range l {
		if a.Text != nil {
			s := *a.Text
			a.Text = &s
		}
		if a.Number != nil {
			n := *a.Number
			a.Number = &n
		}
		out[i] = a
	}
	return out
}

// ComputedInputs are the columns a computed step reads.
func ComputedInputs(t Transform) []string {
	var out []string
	for _, a := range t.Args {
		if a.Column != "" {
			out = append(out, a.Column)
		}
	}
	return out
}

// computedFindings: an input is gone, or of a kind its slot doesn't take (T15). Each would fail
// the Flink CDC job. A name that clashes with a column (T13, one added later too, through the
// drift alert) is found by the preview.
func computedFindings(sourceType, table string, info TableColumns, spec PipelineSpec) []TransformFinding {
	var out []TransformFinding
	for i, t := range spec.Transforms {
		if t.Type != "computed" || t.Table != table {
			continue
		}
		label := StepLabel(i, t)
		// A name clash is the preview's (transform_field_conflict), on the row as it is then.
		fn := ComputedFunctions[t.Function]
		for j, a := range t.Args {
			if a.Column == "" {
				continue
			}
			col, found := columnOf(info.Columns, a.Column)
			slot := argSlot(fn, j)
			switch {
			case !found:
				out = append(out, TransformFinding{Code: "transform_computed_input", Fail: true, Table: table, Step: i,
					Message: fmt.Sprintf("%s, %s reads %s, which the table no longer has.", table, label, a.Column)})
			case !KindFits(slot, ColumnKind(sourceType, col.Type)):
				out = append(out, TransformFinding{Code: "transform_computed_input", Fail: true, Table: table, Step: i,
					Message: fmt.Sprintf("%s, %s: %s is %s, and %s needs %s.", table, label, a.Column, col.Type, fn.Title, slotWords(slot))})
			}
		}
	}
	return out
}

func slotWords(slot string) string {
	switch slot {
	case ArgText:
		return "text"
	case ArgTime:
		return "a timestamp or a date"
	}
	return "a plain value (not binary, JSON or a nested document)"
}

// computeSample evaluates a computed step on the preview's sample row (row: field → value).
func computeSample(t Transform, row *om.Map) any {
	arg := func(a ComputedArg) any {
		switch {
		case a.Column != "":
			v, _ := row.Get(a.Column)
			return v
		case a.Text != nil:
			return *a.Text
		case a.Number != nil:
			return *a.Number
		}
		return nil
	}
	text := func(x any) (string, bool) {
		if x == nil {
			return "", false
		}
		return fmt.Sprint(x), true
	}
	if len(t.Args) == 0 {
		return nil
	}
	first, ok := text(arg(t.Args[0]))
	switch t.Function {
	case "upper":
		if ok {
			return strings.ToUpper(first)
		}
	case "lower":
		if ok {
			return strings.ToLower(first)
		}
	case "trim":
		if ok {
			return strings.TrimSpace(first)
		}
	case "length":
		if ok {
			return len([]rune(first))
		}
	case "to_text":
		if ok {
			return first
		}
	case "to_integer":
		if n, err := strconv.ParseInt(strings.TrimSpace(first), 10, 64); ok && err == nil {
			return n
		}
	case "concat":
		var b strings.Builder
		for _, a := range t.Args {
			s, ok := text(arg(a))
			if !ok {
				return nil // CONCAT with a NULL is NULL
			}
			b.WriteString(s)
		}
		return b.String()
	case "substr":
		if !ok || len(t.Args) < 3 || t.Args[1].Number == nil || t.Args[2].Number == nil {
			return nil
		}
		r := []rune(first)
		start := min(*t.Args[1].Number-1, len(r))
		return string(r[start:min(start+*t.Args[2].Number, len(r))])
	case "date_format":
		if !ok || len(t.Args) < 2 || t.Args[1].Text == nil {
			return nil
		}
		ts, _ := time.Parse(time.RFC3339, sampleISO)
		layout := strings.NewReplacer("yyyy", "2006", "MM", "01", "dd", "02", "HH", "15", "mm", "04", "ss", "05").Replace(*t.Args[1].Text)
		return ts.Format(layout)
	}
	return nil
}
